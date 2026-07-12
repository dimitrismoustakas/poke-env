import asyncio
from concurrent.futures import Future as ConcurrentFuture
from concurrent.futures import ThreadPoolExecutor
from threading import Event, Lock
from types import SimpleNamespace

import pytest

import poke_env.ps_client.local_client as local_client_module
from poke_env import AccountConfiguration, LocalBattleStreamConfiguration
from poke_env.concurrency import POKE_LOOP
from poke_env.exceptions import ShowdownException
from poke_env.ps_client.local_client import (
    _LOCAL_CLIENT_REFCOUNTS,
    _LOCAL_WORKER_POOLS,
    LocalBattleStreamClient,
    LocalBattleStreamSession,
    _AsyncPayloadMailbox,
    _CrossLoopLocalBattleController,
    _expand_worker_events,
    _get_or_create_worker_pool,
    _LocalRuntimeShard,
    _protocol_batch_payload,
    _SharedBattleState,
    _SharedLocalBattleStreamWorker,
    _SharedLocalBattleStreamWorkerPool,
    _split_update_for_players,
    _start_worker_group,
    _translate_showdown_command,
    _worker_group_start_concurrency,
    run_local_battles,
)


class DummySession:
    def __init__(self):
        self.messages: list[tuple[str, str]] = []

    async def send_player_message(self, player_slot: str, message: str) -> None:
        self.messages.append((player_slot, message))


class DummyWorker:
    def __init__(self):
        self.lines: list[str] = []
        self.line_batches: list[list[str]] = []

    async def send_battle_line(self, line: str) -> None:
        self.lines.append(line)

    async def send_battle_lines(self, lines: list[str]) -> None:
        self.line_batches.append(list(lines))

    async def describe(self) -> str:
        return "dummy_worker active=True"


class DummyClient:
    def __init__(self):
        self.messages: list[tuple[str, str | list[str] | list[list[str]]]] = []

    async def dispatch_room_message(
        self, room: str, payload: str | list[str] | list[list[str]]
    ) -> None:
        self.messages.append((room, payload))


def _protocol_read_session(worker) -> LocalBattleStreamSession:
    session = object.__new__(LocalBattleStreamSession)
    session._worker = worker
    session._dispatch_tails = {}
    session._dispatch_tasks = set()
    session._dispatch_failure = asyncio.get_running_loop().create_future()
    return session


def _legacy_payload_to_split_messages(payload):
    if not payload:
        return []
    if isinstance(payload, str):
        return [line.split("|") for line in payload.split("\n")]
    if payload and isinstance(payload[0], list):
        return payload
    return [str(line).split("|") for line in payload]


def _legacy_payload_has_actionable_request(payload):
    for split_message in _legacy_payload_to_split_messages(payload):
        if len(split_message) <= 2 or split_message[1] != "request":
            continue
        if not split_message[2]:
            continue
        try:
            request = local_client_module.orjson.loads(split_message[2])
        except local_client_module.orjson.JSONDecodeError:
            return False
        if not request.get("wait", False):
            return True
    return False


def _legacy_payload_requires_dispatch_barrier(payload):
    return any(
        len(split_message) > 1
        and split_message[1] in {"showteam", "error", "win", "tie", "deinit"}
        for split_message in _legacy_payload_to_split_messages(payload)
    )


def _legacy_payload_terminal_message(payload):
    for split_message in _legacy_payload_to_split_messages(payload):
        if len(split_message) > 1 and split_message[1] in {"win", "tie"}:
            return split_message
    return None


def _local_runner_players(
    monkeypatch,
    config: LocalBattleStreamConfiguration,
    player_1_limit: int,
    player_2_limit: int,
):
    class RunnerClient:
        def __init__(self):
            self.local_server_configuration = config
            self.loop = asyncio.get_running_loop()

    monkeypatch.setattr(local_client_module, "LocalBattleStreamClient", RunnerClient)
    return (
        SimpleNamespace(
            ps_client=RunnerClient(),
            format="gen9randombattle",
            max_concurrent_battles=player_1_limit,
        ),
        SimpleNamespace(
            ps_client=RunnerClient(),
            format="gen9randombattle",
            max_concurrent_battles=player_2_limit,
        ),
    )


def test_split_update_for_players():
    payload = "\n".join(
        [
            "|turn|1",
            "|split|p1",
            "|request|secret-p1",
            "|request|shared-p1",
            "|split|p2",
            "|request|secret-p2",
            "|request|shared-p2",
            "|win|Player 1",
        ]
    )

    p1_lines, p2_lines = _split_update_for_players(payload)

    assert p1_lines == [
        "|turn|1",
        "|request|secret-p1",
        "|request|shared-p2",
        "|win|Player 1",
    ]
    assert p2_lines == [
        "|turn|1",
        "|request|shared-p1",
        "|request|secret-p2",
        "|win|Player 1",
    ]


def test_expand_worker_events_handles_batched_worker_payloads():
    first = {"type": "split-chunk", "battleId": "battle-1"}
    second = {"type": "battle-ended", "battleId": "battle-1"}

    assert _expand_worker_events(
        {"type": "event-batch", "events": [first, second]}
    ) == [first, second]
    assert _expand_worker_events(first) == [first]


def test_protocol_batch_payload_flattens_nested_worker_batches():
    assert _protocol_batch_payload(
        [
            {
                "type": "protocol-batch",
                "messages": [{"type": "split-chunk", "p1_messages": []}],
            },
            {
                "type": "protocol-batch",
                "messages": [{"type": "side-chunk", "player": "p1", "messages": []}],
            },
        ]
    ) == {
        "type": "protocol-batch",
        "messages": [
            {"type": "split-chunk", "p1_messages": []},
            {"type": "side-chunk", "player": "p1", "messages": []},
        ],
    }


@pytest.mark.parametrize(
    "payload",
    [
        "|turn|2\n|win|Player 1",
        ["|turn|2", "|tie"],
        [["", "win", "Player 1"]],
        [["", "tie"]],
    ],
)
def test_classify_payload_finds_terminal_battle_message(payload):
    assert local_client_module._classify_payload(payload).terminal_message is not None


@pytest.mark.asyncio
async def test_shared_worker_global_error_event_is_terminal_without_unpack_error():
    worker = _SharedLocalBattleStreamWorker(
        LocalBattleStreamConfiguration("C:/showdown"), worker_index=0
    )
    state = _SharedBattleState(
        message_queue=_AsyncPayloadMailbox(), battle_done=asyncio.Event()
    )
    worker._battle_states["battle-1"] = state

    result = await worker._handle_stdout_events(
        [{"type": "error", "detail": "worker failed"}]
    )

    assert result is False
    assert worker._fatal_error is not None
    assert state.active is False
    assert state.battle_done.is_set()
    payload = await state.message_queue.get(timeout=0.0)
    assert isinstance(payload, ShowdownException)
    assert "worker failed" in str(payload)


@pytest.mark.asyncio
async def test_shared_worker_battle_started_event_acknowledges_start():
    worker = _SharedLocalBattleStreamWorker(
        LocalBattleStreamConfiguration("C:/showdown"), worker_index=0
    )
    start_ack = asyncio.get_running_loop().create_future()
    worker._battle_states["battle-1"] = _SharedBattleState(
        message_queue=_AsyncPayloadMailbox(),
        battle_done=asyncio.Event(),
        battle_started=start_ack,
    )

    result = await worker._handle_stdout_events(
        [{"type": "battle-started", "battleId": "battle-1"}]
    )

    assert result is True
    assert start_ack.done()
    assert start_ack.result() is None


@pytest.mark.asyncio
async def test_shared_worker_battle_end_before_start_ack_fails_start_future():
    worker = _SharedLocalBattleStreamWorker(
        LocalBattleStreamConfiguration("C:/showdown"), worker_index=0
    )
    start_ack = asyncio.get_running_loop().create_future()
    state = _SharedBattleState(
        message_queue=_AsyncPayloadMailbox(),
        battle_done=asyncio.Event(),
        battle_started=start_ack,
    )
    worker._battle_states["battle-1"] = state

    result = await worker._handle_stdout_events(
        [{"type": "battle-ended", "battleId": "battle-1"}]
    )

    assert result is True
    assert state.active is False
    assert state.battle_done.is_set()
    assert start_ack.done()
    with pytest.raises(ShowdownException, match="before start was acknowledged"):
        start_ack.result()


@pytest.mark.asyncio
async def test_shared_worker_start_limiter_does_not_hold_until_ack():
    commands = []

    async def start():
        return None

    async def send_command(command):
        commands.append(command)

    worker = object.__new__(_SharedLocalBattleStreamWorker)
    worker._config = LocalBattleStreamConfiguration("C:/showdown", startup_timeout=1.0)
    worker._start_limiter = asyncio.Semaphore(1)
    worker._fatal_error = None
    worker._battle_states = {
        "battle-1": _SharedBattleState(
            message_queue=_AsyncPayloadMailbox(),
            battle_done=asyncio.Event(),
            battle_started=asyncio.get_running_loop().create_future(),
        ),
        "battle-2": _SharedBattleState(
            message_queue=_AsyncPayloadMailbox(),
            battle_done=asyncio.Event(),
            battle_started=asyncio.get_running_loop().create_future(),
        ),
    }
    worker.start = start
    worker._send_command = send_command

    first = asyncio.create_task(worker.start_battle("battle-1", [">start one"]))
    second = asyncio.create_task(worker.start_battle("battle-2", [">start two"]))
    await asyncio.sleep(0)
    await asyncio.sleep(0)

    assert [command["battleId"] for command in commands] == ["battle-1", "battle-2"]
    worker._battle_states["battle-1"].battle_started.set_result(None)
    worker._battle_states["battle-2"].battle_started.set_result(None)
    await asyncio.gather(first, second)


@pytest.mark.parametrize(
    ("message", "expected"),
    [
        ("/choose move 1", ("choice", "move 1")),
        ("/choose default", ("choice", "default")),
        ("/team 1234", ("choice", "team 1234")),
        ("/forfeit", ("forfeit", None)),
        ("/timer on", ("ignore", None)),
        ("/leave battle-gen9randombattle-1", ("ignore", None)),
    ],
)
def test_translate_showdown_command(message, expected):
    assert _translate_showdown_command(message) == expected


def test_translate_showdown_command_rejects_unknown_messages():
    with pytest.raises(ShowdownException, match="Unsupported message"):
        _translate_showdown_command("/challenge someone, gen9randombattle")


@pytest.mark.parametrize("payload_form", ["split", "lines", "text"])
@pytest.mark.parametrize(
    "split_messages",
    [
        [],
        [["", "turn", "1"]],
        [["", "request", '{"wait":true}']],
        [["", "request", '{"active":[{}]}']],
        [["", "request", '{"wait":true}'], ["", "request", '{"active":[{}]}']],
        [["", "request", "{"], ["", "request", '{"active":[{}]}']],
        [["", "request", '{"active":[{}]}'], ["", "request", "{"]],
        [["", "request", ""], ["", "request", '{"active":[{}]}']],
        [["", "showteam", "p1", "team"], ["", "request", '{"wait":true}']],
        [["", "error", "bad choice"], ["", "request", '{"active":[{}]}']],
        [["", "request", "{"], ["", "win", "Player 1"]],
        [["", "request", '{"active":[{}]}'], ["", "tie"]],
        [["", "deinit"]],
        [["", "request", "[]"]],
        [["", "request", "null"]],
        [["", "request", '{"wait":1}']],
        [["", "request", '{"wait":0}']],
    ],
)
def test_payload_metadata_matches_legacy_classification(payload_form, split_messages):
    if payload_form == "split":
        payload = [list(message) for message in split_messages]
    else:
        lines = ["|".join(message) for message in split_messages]
        payload = lines if payload_form == "lines" else "\n".join(lines)

    metadata = local_client_module._classify_payload(payload)
    expected_terminal = _legacy_payload_terminal_message(payload)
    expected_barrier = _legacy_payload_requires_dispatch_barrier(payload)

    assert metadata.actionable_request is None
    assert metadata.terminal_message == expected_terminal
    assert metadata.requires_dispatch_barrier is expected_barrier

    try:
        expected_actionable = _legacy_payload_has_actionable_request(payload)
    except AttributeError:
        with pytest.raises(AttributeError):
            local_client_module._resolve_actionable_request(metadata)
    else:
        resolved = local_client_module._resolve_actionable_request(metadata)
        assert resolved.actionable_request is expected_actionable


def test_get_or_create_worker_pool_is_thread_safe():
    class DummyPool:
        pass

    config = LocalBattleStreamConfiguration(
        "C:/showdown", worker_count=4, max_battles_per_worker=4, runtime_loop_count=2
    )
    created = []

    def create_pool():
        pool = DummyPool()
        created.append(pool)
        return pool

    _LOCAL_WORKER_POOLS.clear()
    try:
        with ThreadPoolExecutor(max_workers=16) as executor:
            pools = list(
                executor.map(
                    lambda _: _get_or_create_worker_pool(config, create_pool), range(64)
                )
            )

        assert len({id(pool) for pool in pools}) == 1
        assert created == [pools[0]]
        assert _LOCAL_WORKER_POOLS[config] is pools[0]
    finally:
        _LOCAL_WORKER_POOLS.clear()


def test_worker_group_start_concurrency_matches_parent_workers():
    assert (
        _worker_group_start_concurrency(worker_count=24, max_battles_per_worker=3) == 24
    )
    assert (
        _worker_group_start_concurrency(worker_count=4, max_battles_per_worker=1) == 4
    )
    assert (
        _worker_group_start_concurrency(worker_count=1, max_battles_per_worker=3) == 1
    )


@pytest.mark.asyncio
async def test_start_worker_group_limits_concurrent_starts():
    active_starts = 0
    max_active_starts = 0

    class DummyLifecycle:
        async def start(self):
            nonlocal active_starts, max_active_starts
            active_starts += 1
            max_active_starts = max(max_active_starts, active_starts)
            await asyncio.sleep(0.01)
            active_starts -= 1

        async def shutdown(self):
            return None

    workers = [DummyLifecycle() for _ in range(6)]

    await _start_worker_group(workers, max_concurrent=2)

    assert max_active_starts == 2


@pytest.mark.asyncio
async def test_shared_worker_pool_close_shuts_workers_down_in_parallel():
    release_shutdown = asyncio.Event()
    all_started = asyncio.Event()
    started = []

    class BlockingWorker:
        def __init__(self, worker_id):
            self.worker_id = worker_id

        async def shutdown(self):
            started.append(self.worker_id)
            if len(started) == 3:
                all_started.set()
            await release_shutdown.wait()

    pool = _SharedLocalBattleStreamWorkerPool(
        LocalBattleStreamConfiguration("C:/showdown")
    )
    pool._workers = [BlockingWorker(worker_id) for worker_id in range(3)]
    pool._started = True

    close_task = asyncio.create_task(pool.close())
    try:
        await asyncio.wait_for(all_started.wait(), timeout=1.0)
    finally:
        release_shutdown.set()
    await close_task

    assert sorted(started) == [0, 1, 2]
    assert pool._workers == []
    assert pool._started is False


@pytest.mark.asyncio
async def test_shared_worker_pool_close_finishes_cleanup_before_propagating_cancel():
    shutdown_started = asyncio.Event()
    release_shutdown = asyncio.Event()
    shutdown_finished = asyncio.Event()

    class BlockingWorker:
        async def shutdown(self):
            shutdown_started.set()
            await release_shutdown.wait()
            shutdown_finished.set()

    pool = _SharedLocalBattleStreamWorkerPool(
        LocalBattleStreamConfiguration("C:/showdown")
    )
    pool._workers = [BlockingWorker()]
    pool._started = True

    close_task = asyncio.create_task(pool.close())
    await asyncio.wait_for(shutdown_started.wait(), timeout=1.0)
    close_task.cancel()
    await asyncio.sleep(0)
    assert not close_task.done()

    release_shutdown.set()
    with pytest.raises(asyncio.CancelledError):
        await close_task

    assert shutdown_finished.is_set()
    assert pool._workers == []
    assert pool._started is False


@pytest.mark.asyncio
async def test_shared_worker_pool_rejects_missing_showdown_entrypoint(tmp_path):
    pool = _SharedLocalBattleStreamWorkerPool(LocalBattleStreamConfiguration(tmp_path))

    with pytest.raises(ShowdownException, match="pokemon-showdown entrypoint"):
        await pool.start()


@pytest.mark.asyncio
async def test_shared_worker_pool_limits_concurrent_start_command_submissions(
    monkeypatch: pytest.MonkeyPatch, tmp_path
):
    active_starts = 0
    max_active_starts = 0

    async def fake_start(self):
        return None

    async def fake_send_start_command_unlimited(self, battle_id, lines):
        nonlocal active_starts, max_active_starts
        active_starts += 1
        max_active_starts = max(max_active_starts, active_starts)
        await asyncio.sleep(0.01)
        active_starts -= 1
        return self._battle_states[battle_id]

    async def fake_wait_for_start_ack(self, battle_id, state):
        return None

    monkeypatch.setattr(_SharedLocalBattleStreamWorker, "start", fake_start)
    monkeypatch.setattr(
        _SharedLocalBattleStreamWorker,
        "_send_start_command_unlimited",
        fake_send_start_command_unlimited,
    )
    monkeypatch.setattr(
        _SharedLocalBattleStreamWorker, "_wait_for_start_ack", fake_wait_for_start_ack
    )

    (tmp_path / "pokemon-showdown").touch()
    pool = _SharedLocalBattleStreamWorkerPool(
        LocalBattleStreamConfiguration(
            tmp_path, worker_count=2, max_battles_per_worker=4
        )
    )
    await pool.start()
    handles = [await pool.acquire() for _ in range(6)]

    await asyncio.gather(*(handle.start_battle([">start {}"]) for handle in handles))

    assert max_active_starts == 2


@pytest.mark.asyncio
async def test_shared_worker_passes_battle_capacity_to_parent_process(
    monkeypatch: pytest.MonkeyPatch,
):
    calls = []

    class FakeStream:
        def __init__(self, lines):
            self._lines = list(lines)

        async def readline(self):
            return self._lines.pop(0) if self._lines else b""

    class FakeStdin:
        def write(self, data):
            pass

        async def drain(self):
            pass

        def is_closing(self):
            return False

        def close(self):
            pass

        async def wait_closed(self):
            pass

    class FakeProcess:
        def __init__(self):
            self.stdin = FakeStdin()
            self.stdout = FakeStream([b'{"type":"ready"}\n'])
            self.stderr = FakeStream([])
            self.returncode = 0
            self.pid = 1234

        async def wait(self):
            return self.returncode

        def terminate(self):
            self.returncode = 1

    async def fake_create_subprocess_exec(*args, **kwargs):
        calls.append((args, kwargs))
        return FakeProcess()

    monkeypatch.setattr(asyncio, "create_subprocess_exec", fake_create_subprocess_exec)

    worker = _SharedLocalBattleStreamWorker(
        LocalBattleStreamConfiguration(
            "C:/showdown", node_command=("node-test",), max_battles_per_worker=5
        ),
        worker_index=0,
    )

    await worker.start()

    assert calls
    args, kwargs = calls[0]
    assert args[0] == "node-test"
    assert str(args[2]).replace("\\", "/") == "C:/showdown"
    assert args[3] == "5"
    assert str(kwargs["cwd"]).replace("\\", "/") == "C:/showdown"


@pytest.mark.asyncio
async def test_local_client_routes_messages_to_battle_session():
    client = LocalBattleStreamClient(
        account_configuration=AccountConfiguration("local-user", None),
        server_configuration=LocalBattleStreamConfiguration("C:/showdown"),
        loop=POKE_LOOP,
    )
    session = DummySession()
    client.attach_battle("battle-gen9randombattle-1", session, "p1")

    await asyncio.wait_for(client.logged_in.wait(), timeout=1.0)
    await client.send_message("/choose move 1", "battle-gen9randombattle-1")
    await client.send_message("/timer on", "battle-gen9randombattle-1")
    await client.send_message("/leave battle-gen9randombattle-1")

    assert session.messages == [("p1", "/choose move 1")]
    assert client.local_server_configuration == LocalBattleStreamConfiguration(
        "C:/showdown"
    )
    await client.stop_listening()


@pytest.mark.asyncio
async def test_local_client_dispatches_payload_strings_and_cleans_deinit_lock():
    received = []

    async def on_battle_message(split_messages):
        received.append(split_messages)

    client = LocalBattleStreamClient(
        account_configuration=AccountConfiguration("local-user", None),
        on_battle_message=on_battle_message,
        server_configuration=LocalBattleStreamConfiguration("C:/showdown"),
        loop=POKE_LOOP,
    )

    await client.dispatch_room_message("battle-gen9randombattle-1", "|turn|1\n|deinit")

    assert received == [
        [[">battle-gen9randombattle-1"], ["", "turn", "1"], ["", "deinit"]]
    ]
    assert "battle-gen9randombattle-1" not in client._battle_locks
    await client.stop_listening()


@pytest.mark.asyncio
async def test_local_client_dispatch_propagates_cancelled_battle_message():
    async def on_battle_message(split_messages):
        del split_messages
        raise asyncio.CancelledError()

    client = LocalBattleStreamClient(
        account_configuration=AccountConfiguration("local-user", None),
        on_battle_message=on_battle_message,
        server_configuration=LocalBattleStreamConfiguration("C:/showdown"),
        loop=POKE_LOOP,
    )

    try:
        with pytest.raises(asyncio.CancelledError):
            await client.dispatch_room_message("battle-gen9randombattle-1", "|turn|1")
    finally:
        await client.stop_listening()


@pytest.mark.asyncio
async def test_local_client_dispatches_pre_split_messages_without_string_splitting():
    received = []

    async def on_battle_message(split_messages):
        received.append(split_messages)

    client = LocalBattleStreamClient(
        account_configuration=AccountConfiguration("local-user", None),
        on_battle_message=on_battle_message,
        server_configuration=LocalBattleStreamConfiguration("C:/showdown"),
        loop=POKE_LOOP,
    )

    await client.dispatch_room_message(
        "battle-gen9randombattle-1", [["", "turn", "1"], ["", "deinit"]]
    )

    assert received == [
        [[">battle-gen9randombattle-1"], ["", "turn", "1"], ["", "deinit"]]
    ]
    assert "battle-gen9randombattle-1" not in client._battle_locks
    await client.stop_listening()


@pytest.mark.asyncio
async def test_local_client_stop_listening_closes_pool_after_last_client():
    class DummyPool:
        def __init__(self):
            self.close_calls = 0

        async def close(self) -> None:
            self.close_calls += 1

    config = LocalBattleStreamConfiguration("C:/showdown")
    pool = DummyPool()
    _LOCAL_WORKER_POOLS.clear()
    _LOCAL_CLIENT_REFCOUNTS.clear()
    _LOCAL_WORKER_POOLS[config] = pool

    client_1 = LocalBattleStreamClient(
        account_configuration=AccountConfiguration("local-user-1", None),
        server_configuration=config,
        loop=POKE_LOOP,
    )
    client_2 = LocalBattleStreamClient(
        account_configuration=AccountConfiguration("local-user-2", None),
        server_configuration=config,
        loop=POKE_LOOP,
    )

    await client_1.stop_listening()

    assert pool.close_calls == 0
    assert _LOCAL_CLIENT_REFCOUNTS[config] == 1
    assert _LOCAL_WORKER_POOLS[config] is pool

    await client_2.stop_listening()

    assert pool.close_calls == 1
    assert config not in _LOCAL_CLIENT_REFCOUNTS
    assert config not in _LOCAL_WORKER_POOLS


@pytest.mark.asyncio
async def test_local_client_cancelled_stop_finishes_unregistered_pool_close():
    close_started = Event()
    release_close = Event()
    close_finished = Event()

    class SlowPool:
        async def close(self) -> None:
            close_started.set()
            await asyncio.to_thread(release_close.wait)
            close_finished.set()

    config = LocalBattleStreamConfiguration("C:/showdown")
    _LOCAL_WORKER_POOLS.clear()
    _LOCAL_CLIENT_REFCOUNTS.clear()
    _LOCAL_WORKER_POOLS[config] = SlowPool()
    client = LocalBattleStreamClient(
        account_configuration=AccountConfiguration("local-user", None),
        server_configuration=config,
        loop=POKE_LOOP,
    )

    stop_task = asyncio.create_task(client.stop_listening())
    assert await asyncio.to_thread(close_started.wait, 1.0)
    stop_task.cancel()
    try:
        await asyncio.sleep(0.05)
        assert not close_finished.is_set()
    finally:
        release_close.set()

    with pytest.raises(asyncio.CancelledError):
        await stop_task

    assert close_finished.wait(timeout=1.0)
    assert config not in _LOCAL_CLIENT_REFCOUNTS
    assert config not in _LOCAL_WORKER_POOLS


@pytest.mark.asyncio
async def test_local_client_retries_failed_last_client_pool_close():
    class FailOncePool:
        def __init__(self):
            self.close_calls = 0

        async def close(self) -> None:
            self.close_calls += 1
            if self.close_calls == 1:
                raise RuntimeError("first close failed")

    config = LocalBattleStreamConfiguration("C:/showdown")
    pool = FailOncePool()
    _LOCAL_WORKER_POOLS.clear()
    _LOCAL_CLIENT_REFCOUNTS.clear()
    _LOCAL_WORKER_POOLS[config] = pool
    client = LocalBattleStreamClient(
        account_configuration=AccountConfiguration("local-user", None),
        server_configuration=config,
        loop=POKE_LOOP,
    )

    with pytest.raises(RuntimeError, match="first close failed"):
        await client.stop_listening()

    assert client._stopped is True
    assert client._pending_pool_close is pool
    assert _LOCAL_WORKER_POOLS[config] is pool
    assert config not in _LOCAL_CLIENT_REFCOUNTS

    await client.stop_listening()

    assert pool.close_calls == 2
    assert client._pending_pool_close is None
    assert config not in _LOCAL_WORKER_POOLS


@pytest.mark.asyncio
async def test_failed_pool_close_does_not_overwrite_newer_registered_pool():
    config = LocalBattleStreamConfiguration("C:/showdown")
    replacement_pool = object()

    class ReplacedFailOncePool:
        def __init__(self):
            self.close_calls = 0

        async def close(self) -> None:
            self.close_calls += 1
            if self.close_calls == 1:
                _LOCAL_WORKER_POOLS[config] = replacement_pool
                raise RuntimeError("old pool close failed")

    old_pool = ReplacedFailOncePool()
    _LOCAL_WORKER_POOLS.clear()
    _LOCAL_CLIENT_REFCOUNTS.clear()
    _LOCAL_WORKER_POOLS[config] = old_pool
    client = LocalBattleStreamClient(
        account_configuration=AccountConfiguration("local-user", None),
        server_configuration=config,
        loop=POKE_LOOP,
    )

    with pytest.raises(RuntimeError, match="old pool close failed"):
        await client.stop_listening()

    assert _LOCAL_WORKER_POOLS[config] is replacement_pool
    assert client._pending_pool_close is old_pool

    await client.stop_listening()

    assert old_pool.close_calls == 2
    assert client._pending_pool_close is None
    assert _LOCAL_WORKER_POOLS[config] is replacement_pool
    _LOCAL_WORKER_POOLS.clear()


@pytest.mark.asyncio
async def test_local_client_stop_listening_is_idempotent():
    class DummyPool:
        def __init__(self):
            self.close_calls = 0

        async def close(self) -> None:
            self.close_calls += 1

    config = LocalBattleStreamConfiguration("C:/showdown")
    pool = DummyPool()
    _LOCAL_WORKER_POOLS.clear()
    _LOCAL_CLIENT_REFCOUNTS.clear()
    _LOCAL_WORKER_POOLS[config] = pool

    client = LocalBattleStreamClient(
        account_configuration=AccountConfiguration("local-user", None),
        server_configuration=config,
        loop=POKE_LOOP,
    )

    await client.stop_listening()
    await client.stop_listening()

    assert pool.close_calls == 1
    assert config not in _LOCAL_CLIENT_REFCOUNTS
    assert config not in _LOCAL_WORKER_POOLS


@pytest.mark.asyncio
async def test_local_client_concurrent_stop_closes_pool_once():
    class SlowPool:
        def __init__(self):
            self.close_calls = 0

        async def close(self) -> None:
            self.close_calls += 1
            await asyncio.sleep(0.05)

    config = LocalBattleStreamConfiguration("C:/showdown")
    pool = SlowPool()
    _LOCAL_WORKER_POOLS.clear()
    _LOCAL_CLIENT_REFCOUNTS.clear()
    _LOCAL_WORKER_POOLS[config] = pool
    client = LocalBattleStreamClient(
        account_configuration=AccountConfiguration("local-user", None),
        server_configuration=config,
        loop=POKE_LOOP,
    )

    await asyncio.gather(client.stop_listening(), client.stop_listening())

    assert pool.close_calls == 1
    assert client._pending_pool_close is None
    assert config not in _LOCAL_CLIENT_REFCOUNTS
    assert config not in _LOCAL_WORKER_POOLS


@pytest.mark.asyncio
async def test_local_session_ignores_late_player_messages_after_battle_end():
    session = object.__new__(LocalBattleStreamSession)
    session._accepting_player_messages = False
    session._worker = None

    await session.send_player_message("p1", "/choose move 1")


@pytest.mark.asyncio
async def test_local_session_stops_accepting_messages_after_forfeit():
    session = object.__new__(LocalBattleStreamSession)
    session._accepting_player_messages = True
    session._worker = DummyWorker()

    await session.send_player_message("p1", "/forfeit")

    assert session._accepting_player_messages is False
    assert session._worker.lines == [">forcelose p1"]


@pytest.mark.asyncio
async def test_local_session_sends_each_choice_immediately():
    session = object.__new__(LocalBattleStreamSession)
    session._accepting_player_messages = True
    session._worker = DummyWorker()

    await session.send_player_message("p1", "/choose move 1")

    assert session._worker.lines == [">p1 move 1"]

    await session.send_player_message("p2", "/choose move 2")

    assert session._worker.lines == [">p1 move 1", ">p2 move 2"]
    assert session._worker.line_batches == []


@pytest.mark.asyncio
async def test_local_session_keeps_active_battles_idle_without_timeout():
    class RecordingWorker:
        def __init__(self):
            self.timeouts: list[float | None] = []
            self.messages = [
                {
                    "type": "split-chunk",
                    "p1_payload": "|turn|1",
                    "p2_payload": "|turn|1",
                },
                {"type": "end"},
            ]

        async def read_protocol_message(self, timeout: float | None):
            self.timeouts.append(timeout)
            return self.messages.pop(0)

    session = object.__new__(LocalBattleStreamSession)
    session._config = LocalBattleStreamConfiguration("C:/showdown", startup_timeout=1.5)
    session._battle_started = False
    session._worker = RecordingWorker()
    start_calls = 0
    dispatched = []

    async def start_battle_room():
        nonlocal start_calls
        start_calls += 1

    async def dispatch_protocol_message(message):
        dispatched.append(message)
        return len(dispatched) == 2

    session._start_battle_room = start_battle_room
    session._dispatch_protocol_message = dispatch_protocol_message

    await session._consume_protocol_messages()

    assert start_calls == 1
    assert session._worker.timeouts == [1.5, None]
    assert dispatched == [
        {"type": "split-chunk", "p1_payload": "|turn|1", "p2_payload": "|turn|1"},
        {"type": "end"},
    ]


@pytest.mark.asyncio
async def test_local_session_startup_timeout_reports_worker_diagnostics():
    class TimeoutWorker:
        async def read_protocol_message(self, timeout: float | None):
            raise asyncio.TimeoutError

        async def describe(self) -> str:
            return "worker_index=7; battle_id=worker-7-battle-11"

    session = object.__new__(LocalBattleStreamSession)
    session._config = LocalBattleStreamConfiguration("C:/showdown", startup_timeout=1.5)
    session._room = "battle-test"
    session._battle_started = False
    session._worker = TimeoutWorker()

    with pytest.raises(ShowdownException) as exc_info:
        await session._consume_protocol_messages()

    message = str(exc_info.value)
    assert "Timed out waiting for initial local BattleStream output" in message
    assert "battle-test" in message
    assert "worker_index=7" in message
    assert "battle_id=worker-7-battle-11" in message


@pytest.mark.asyncio
async def test_local_session_protocol_read_fast_path_after_completed_dispatch(
    monkeypatch,
):
    class RecordingWorker:
        def __init__(self):
            self.timeouts = []

        async def read_protocol_message(self, timeout):
            self.timeouts.append(timeout)
            return "payload"

    async def completed_dispatch():
        return None

    worker = RecordingWorker()
    session = _protocol_read_session(worker)
    dispatch_task = asyncio.create_task(completed_dispatch())
    await dispatch_task
    session._dispatch_tasks.add(dispatch_task)

    async def unexpected_wait(*args, **kwargs):
        raise AssertionError("Completed dispatches should use the direct read path")

    monkeypatch.setattr(asyncio, "wait", unexpected_wait)

    assert await session._read_protocol_message(1.25) == "payload"
    assert worker.timeouts == [1.25]


@pytest.mark.asyncio
async def test_local_session_protocol_read_fast_path_propagates_completed_failure():
    class UnexpectedWorker:
        def __init__(self):
            self.calls = 0

        async def read_protocol_message(self, timeout):
            self.calls += 1
            raise AssertionError("A failed dispatch must prevent the worker read")

    error = RuntimeError("dispatch failed")

    async def failed_dispatch():
        raise error

    worker = UnexpectedWorker()
    session = _protocol_read_session(worker)
    dispatch_task = asyncio.create_task(failed_dispatch())
    await asyncio.gather(dispatch_task, return_exceptions=True)
    session._dispatch_tasks.add(dispatch_task)

    with pytest.raises(RuntimeError, match="dispatch failed") as exc_info:
        await session._read_protocol_message(None)

    assert exc_info.value is error
    assert worker.calls == 0
    assert session._dispatch_failure.exception() is error


@pytest.mark.asyncio
async def test_local_session_protocol_read_races_pending_dispatch_failure():
    dispatch_release = asyncio.Event()
    read_started = asyncio.Event()
    read_cancelled = asyncio.Event()
    never = asyncio.Event()
    error = RuntimeError("pending dispatch failed")

    class BlockingWorker:
        async def read_protocol_message(self, timeout):
            read_started.set()
            try:
                await never.wait()
            finally:
                read_cancelled.set()

    async def failed_dispatch():
        await dispatch_release.wait()
        raise error

    session = _protocol_read_session(BlockingWorker())
    dispatch_task = asyncio.create_task(failed_dispatch())
    session._track_dispatch_task(dispatch_task)
    protocol_read = asyncio.create_task(session._read_protocol_message(None))
    await asyncio.wait_for(read_started.wait(), timeout=0.2)

    dispatch_release.set()
    with pytest.raises(RuntimeError, match="pending dispatch failed") as exc_info:
        await asyncio.wait_for(protocol_read, timeout=0.2)

    assert exc_info.value is error
    assert read_cancelled.is_set()
    assert session._dispatch_failure.exception() is error


@pytest.mark.asyncio
async def test_local_session_protocol_read_does_not_wait_for_pending_dispatch():
    dispatch_release = asyncio.Event()
    read_started = asyncio.Event()
    payload_ready = asyncio.Event()

    class BlockingWorker:
        async def read_protocol_message(self, timeout):
            read_started.set()
            await payload_ready.wait()
            return "payload"

    session = _protocol_read_session(BlockingWorker())
    dispatch_task = asyncio.create_task(dispatch_release.wait())
    session._track_dispatch_task(dispatch_task)
    protocol_read = asyncio.create_task(session._read_protocol_message(None))
    await asyncio.wait_for(read_started.wait(), timeout=0.2)

    payload_ready.set()
    assert await asyncio.wait_for(protocol_read, timeout=0.2) == "payload"
    assert not dispatch_task.done()

    dispatch_release.set()
    await dispatch_task
    await asyncio.sleep(0)


@pytest.mark.asyncio
async def test_local_session_protocol_read_prefers_failure_when_read_also_completes():
    read_started = asyncio.Event()
    payload = asyncio.get_running_loop().create_future()
    pending_dispatch = asyncio.create_task(asyncio.Event().wait())
    error = RuntimeError("dispatch won race")

    class RacingWorker:
        async def read_protocol_message(self, timeout):
            read_started.set()
            return await payload

    session = _protocol_read_session(RacingWorker())
    session._dispatch_tasks.add(pending_dispatch)
    protocol_read = asyncio.create_task(session._read_protocol_message(None))
    await asyncio.wait_for(read_started.wait(), timeout=0.2)

    session._dispatch_failure.set_exception(error)
    payload.set_result("payload")
    with pytest.raises(RuntimeError, match="dispatch won race") as exc_info:
        await asyncio.wait_for(protocol_read, timeout=0.2)

    assert exc_info.value is error
    pending_dispatch.cancel()
    await asyncio.gather(pending_dispatch, return_exceptions=True)


@pytest.mark.asyncio
async def test_local_session_protocol_read_fast_path_propagates_timeout():
    class TimeoutWorker:
        def __init__(self):
            self.timeouts = []

        async def read_protocol_message(self, timeout):
            self.timeouts.append(timeout)
            raise asyncio.TimeoutError

    worker = TimeoutWorker()
    session = _protocol_read_session(worker)

    with pytest.raises(asyncio.TimeoutError):
        await session._read_protocol_message(0.125)

    assert worker.timeouts == [0.125]


@pytest.mark.asyncio
async def test_local_session_protocol_read_fast_path_propagates_cancellation():
    read_started = asyncio.Event()
    read_cancelled = asyncio.Event()
    never = asyncio.Event()

    class BlockingWorker:
        async def read_protocol_message(self, timeout):
            read_started.set()
            try:
                await never.wait()
            finally:
                read_cancelled.set()

    session = _protocol_read_session(BlockingWorker())
    protocol_read = asyncio.create_task(session._read_protocol_message(None))
    await asyncio.wait_for(read_started.wait(), timeout=0.2)

    protocol_read.cancel()
    with pytest.raises(asyncio.CancelledError):
        await protocol_read

    assert read_cancelled.is_set()


@pytest.mark.asyncio
async def test_split_update_for_players_handles_empty_public_split_line():
    payload = "\n".join(
        ["|turn|1", "|split|p2", "|request|secret-p2", "", "|request|shared-followup"]
    )

    p1_lines, p2_lines = _split_update_for_players(payload)

    assert p1_lines == ["|turn|1", "|request|shared-followup"]
    assert p2_lines == ["|turn|1", "|request|secret-p2", "|request|shared-followup"]


@pytest.mark.asyncio
async def test_local_session_dispatches_pre_split_worker_payloads():
    session = object.__new__(LocalBattleStreamSession)
    session._room = "battle-test"
    session._client_1 = DummyClient()
    session._client_2 = DummyClient()
    session._accepting_player_messages = True

    finished = await session._dispatch_protocol_message(
        {
            "type": "split-chunk",
            "p1_payload": "|turn|1\n|request|p1",
            "p2_payload": "|turn|1\n|request|p2",
        }
    )

    assert finished is False
    await session._wait_for_dispatches()
    assert session._client_1.messages == [("battle-test", "|turn|1\n|request|p1")]
    assert session._client_2.messages == [("battle-test", "|turn|1\n|request|p2")]


@pytest.mark.asyncio
async def test_local_session_dispatches_player_updates_concurrently():
    p1_started = asyncio.Event()
    p2_started = asyncio.Event()

    class BlockingClient:
        def __init__(self, player: str):
            self.player = player

        async def dispatch_room_message(self, room, payload) -> None:
            assert room == "battle-test"
            assert payload == ["|turn|1"]
            if self.player == "p1":
                p1_started.set()
                await asyncio.wait_for(p2_started.wait(), timeout=0.2)
            else:
                await asyncio.wait_for(p1_started.wait(), timeout=0.2)
                p2_started.set()

    session = object.__new__(LocalBattleStreamSession)
    session._room = "battle-test"
    session._client_1 = BlockingClient("p1")
    session._client_2 = BlockingClient("p2")
    session._accepting_player_messages = True

    finished = await session._dispatch_protocol_message("update\n|turn|1")

    assert finished is False
    await session._wait_for_dispatches()


@pytest.mark.asyncio
async def test_local_session_does_not_wait_for_parse_only_update_dispatch():
    release = asyncio.Event()
    p1_started = asyncio.Event()
    p2_started = asyncio.Event()

    class BlockingClient:
        def __init__(self, player: str):
            self.player = player

        async def dispatch_room_message(self, room, payload) -> None:
            assert room == "battle-test"
            assert payload == ["|turn|1"]
            if self.player == "p1":
                p1_started.set()
            else:
                p2_started.set()
            await release.wait()

    session = object.__new__(LocalBattleStreamSession)
    session._room = "battle-test"
    session._client_1 = BlockingClient("p1")
    session._client_2 = BlockingClient("p2")
    session._accepting_player_messages = True

    finished = await session._dispatch_protocol_message("update\n|turn|1")

    assert finished is False
    await asyncio.wait_for(p1_started.wait(), timeout=0.2)
    await asyncio.wait_for(p2_started.wait(), timeout=0.2)
    release.set()
    await session._wait_for_dispatches()


@pytest.mark.asyncio
async def test_local_session_actionable_split_request_waits_for_dispatch_before_reader_advances():
    release = asyncio.Event()
    p1_started = asyncio.Event()
    p2_started = asyncio.Event()

    class BlockingClient:
        def __init__(self, player: str):
            self.player = player

        async def dispatch_room_message(self, room, payload) -> None:
            assert room == "battle-test"
            if self.player == "p1":
                assert payload == '|turn|1\n|request|{"active":[{}]}'
                p1_started.set()
            else:
                assert payload == '|turn|1\n|request|{"active":[{}]}'
                p2_started.set()
            await release.wait()
            await session.send_player_message(
                self.player, f"/choose move {1 if self.player == 'p1' else 2}"
            )

    session = object.__new__(LocalBattleStreamSession)
    session._room = "battle-test"
    session._client_1 = BlockingClient("p1")
    session._client_2 = BlockingClient("p2")
    session._accepting_player_messages = True
    session._worker = DummyWorker()

    dispatch_task = asyncio.create_task(
        session._dispatch_protocol_message(
            {
                "type": "split-chunk",
                "p1_payload": '|turn|1\n|request|{"active":[{}]}',
                "p2_payload": '|turn|1\n|request|{"active":[{}]}',
            }
        )
    )

    await asyncio.wait_for(p1_started.wait(), timeout=0.2)
    await asyncio.wait_for(p2_started.wait(), timeout=0.2)
    await asyncio.sleep(0)
    assert not dispatch_task.done()

    release.set()
    assert await asyncio.wait_for(dispatch_task, timeout=0.2) is False
    assert session._worker.line_batches == [[">p1 move 1", ">p2 move 2"]]
    await session._wait_for_dispatches()


@pytest.mark.asyncio
async def test_local_session_wait_request_dispatch_runs_without_reader_barrier():
    release = asyncio.Event()
    p1_started = asyncio.Event()
    p2_started = asyncio.Event()

    class BlockingClient:
        def __init__(self, player: str):
            self.player = player

        async def dispatch_room_message(self, room, payload) -> None:
            assert room == "battle-test"
            if self.player == "p1":
                assert payload == '|turn|1\n|request|{"wait":true}'
                p1_started.set()
            else:
                assert payload == '|turn|1\n|request|{"wait":true}'
                p2_started.set()
            await release.wait()

    session = object.__new__(LocalBattleStreamSession)
    session._room = "battle-test"
    session._client_1 = BlockingClient("p1")
    session._client_2 = BlockingClient("p2")
    session._accepting_player_messages = True

    dispatch_task = asyncio.create_task(
        session._dispatch_protocol_message(
            {
                "type": "split-chunk",
                "p1_payload": '|turn|1\n|request|{"wait":true}',
                "p2_payload": '|turn|1\n|request|{"wait":true}',
            }
        )
    )

    await asyncio.wait_for(p1_started.wait(), timeout=0.2)
    await asyncio.wait_for(p2_started.wait(), timeout=0.2)
    await asyncio.sleep(0)
    assert dispatch_task.done()
    assert dispatch_task.result() is False

    release.set()
    await session._wait_for_dispatches()


@pytest.mark.asyncio
async def test_local_session_classifies_each_actionable_side_once(monkeypatch):
    class ChoosingClient:
        def __init__(self, player, choice):
            self.player = player
            self.choice = choice

        async def dispatch_room_message(self, room, payload) -> None:
            assert room == "battle-test"
            await session.send_player_message(self.player, self.choice)

    p1_request = '{"active":[{}],"side":{"id":"p1"}}'
    p2_request = '{"active":[{}],"side":{"id":"p2"}}'
    p1_payload = [["", "turn", "1"], ["", "request", p1_request]]
    p2_payload = [["", "turn", "1"], ["", "request", p2_request]]
    session = object.__new__(LocalBattleStreamSession)
    session._room = "battle-test"
    session._worker = DummyWorker()
    session._accepting_player_messages = True
    session._client_1 = ChoosingClient("p1", "/choose move 1")
    session._client_2 = ChoosingClient("p2", "/choose move 2")

    classified_payloads = []
    parsed_requests = []
    classify_payload = local_client_module._classify_payload
    orjson_loads = local_client_module.orjson.loads

    def counting_classify(payload):
        classified_payloads.append(payload)
        return classify_payload(payload)

    def counting_loads(payload):
        parsed_requests.append(payload)
        return orjson_loads(payload)

    monkeypatch.setattr(local_client_module, "_classify_payload", counting_classify)
    monkeypatch.setattr(local_client_module.orjson, "loads", counting_loads)

    finished = await session._dispatch_protocol_message(
        {"type": "split-chunk", "p1_messages": p1_payload, "p2_messages": p2_payload}
    )

    assert finished is False
    assert classified_payloads == [p1_payload, p2_payload]
    assert parsed_requests == [p1_request, p2_request]
    assert session._worker.line_batches == [[">p1 move 1", ">p2 move 2"]]


@pytest.mark.asyncio
async def test_local_session_classifies_each_protocol_batch_fragment_once(monkeypatch):
    class ChoosingClient:
        def __init__(self, player, choice):
            self.player = player
            self.choice = choice

        async def dispatch_room_message(self, room, payload) -> None:
            await session.send_player_message(self.player, self.choice)

    p1_request = '{"active":[{}],"side":{"id":"p1"}}'
    p2_request = '{"active":[{}],"side":{"id":"p2"}}'
    session = object.__new__(LocalBattleStreamSession)
    session._room = "battle-test"
    session._worker = DummyWorker()
    session._accepting_player_messages = True
    session._client_1 = ChoosingClient("p1", "/choose move 1")
    session._client_2 = ChoosingClient("p2", "/choose move 2")

    classified_payloads = []
    parsed_requests = []
    classify_payload = local_client_module._classify_payload
    orjson_loads = local_client_module.orjson.loads

    def counting_classify(payload):
        classified_payloads.append(payload)
        return classify_payload(payload)

    def counting_loads(payload):
        parsed_requests.append(payload)
        return orjson_loads(payload)

    monkeypatch.setattr(local_client_module, "_classify_payload", counting_classify)
    monkeypatch.setattr(local_client_module.orjson, "loads", counting_loads)

    finished = await session._dispatch_protocol_batch(
        [
            {
                "type": "split-chunk",
                "p1_messages": [["", "turn", "1"]],
                "p2_messages": [["", "turn", "1"]],
            },
            {
                "type": "split-chunk",
                "p1_messages": [["", "request", p1_request]],
                "p2_messages": [["", "request", p2_request]],
            },
        ]
    )

    assert finished is False
    assert classified_payloads == [
        [["", "turn", "1"]],
        [["", "turn", "1"]],
        [["", "request", p1_request]],
        [["", "request", p2_request]],
    ]
    assert parsed_requests == [p1_request, p2_request]
    assert session._worker.line_batches == [[">p1 move 1", ">p2 move 2"]]


@pytest.mark.asyncio
async def test_local_session_terminal_requests_are_dropped_without_json_parsing(
    monkeypatch,
):
    p1_payload = [["", "request", "[]"], ["", "win", "Player 1"]]
    p2_payload = [["", "request", "null"], ["", "win", "Player 1"]]
    session = object.__new__(LocalBattleStreamSession)
    session._room = "battle-test"
    session._worker = DummyWorker()
    session._accepting_player_messages = True
    session._client_1 = DummyClient()
    session._client_2 = DummyClient()

    def unexpected_loads(payload):
        raise AssertionError(f"Terminal request was parsed: {payload}")

    monkeypatch.setattr(local_client_module.orjson, "loads", unexpected_loads)

    finished = await session._dispatch_protocol_message(
        {"type": "split-chunk", "p1_messages": p1_payload, "p2_messages": p2_payload}
    )

    assert finished is True
    assert session._client_1.messages == [("battle-test", [["", "win", "Player 1"]])]
    assert session._client_2.messages == [("battle-test", [["", "win", "Player 1"]])]
    assert session._worker.lines == []
    assert session._worker.line_batches == []


@pytest.mark.asyncio
async def test_local_session_one_sided_terminal_preserves_other_action_request(
    monkeypatch,
):
    class ChoosingClient:
        async def dispatch_room_message(self, room, payload) -> None:
            await session.send_player_message("p2", "/choose move 2")

    p1_payload = [["", "request", "[]"], ["", "win", "Player 1"]]
    p2_request = '{"active":[{}],"side":{"id":"p2"}}'
    p2_payload = [["", "request", p2_request]]
    session = object.__new__(LocalBattleStreamSession)
    session._room = "battle-test"
    session._worker = DummyWorker()
    session._accepting_player_messages = True
    session._client_1 = DummyClient()
    session._client_2 = ChoosingClient()

    parsed_requests = []
    orjson_loads = local_client_module.orjson.loads

    def counting_loads(payload):
        parsed_requests.append(payload)
        return orjson_loads(payload)

    monkeypatch.setattr(local_client_module.orjson, "loads", counting_loads)

    finished = await session._dispatch_protocol_message(
        {"type": "split-chunk", "p1_messages": p1_payload, "p2_messages": p2_payload}
    )

    assert finished is True
    assert parsed_requests == [p2_request]
    assert session._client_1.messages == [("battle-test", [["", "win", "Player 1"]])]
    assert session._worker.line_batches == [[">p2 move 2"]]


@pytest.mark.asyncio
async def test_local_session_dispatches_concurrent_choices_exactly_once():
    class ChoosingClient:
        def __init__(self, session, player: str, choice: str):
            self.session = session
            self.player = player
            self.choice = choice

        async def dispatch_room_message(self, room, payload) -> None:
            assert room == "battle-test"
            await self.session.send_player_message(self.player, self.choice)

    session = object.__new__(LocalBattleStreamSession)
    session._room = "battle-test"
    session._worker = DummyWorker()
    session._accepting_player_messages = True
    session._client_1 = ChoosingClient(session, "p1", "/choose move 1")
    session._client_2 = ChoosingClient(session, "p2", "/choose move 2")

    finished = await session._dispatch_player_payloads(
        [["", "request", '{"active":[{}]}']],
        [["", "request", '{"active":[{}]}']],
        wait_for_dispatch=True,
    )

    assert finished is False
    assert session._worker.lines == []
    assert session._worker.line_batches == [[">p1 move 1", ">p2 move 2"]]


@pytest.mark.asyncio
async def test_local_session_first_choice_returns_while_peer_is_blocked():
    peer_release = asyncio.Event()
    first_choice_sent = asyncio.Event()

    class ChoosingClient:
        def __init__(self, session, player: str, choice: str):
            self.session = session
            self.player = player
            self.choice = choice

        async def dispatch_room_message(self, room, payload) -> None:
            assert room == "battle-test"
            if self.player == "p2":
                await peer_release.wait()
            await self.session.send_player_message(self.player, self.choice)
            if self.player == "p1":
                first_choice_sent.set()

    session = object.__new__(LocalBattleStreamSession)
    session._room = "battle-test"
    session._worker = DummyWorker()
    session._accepting_player_messages = True
    session._client_1 = ChoosingClient(session, "p1", "/choose move 1")
    session._client_2 = ChoosingClient(session, "p2", "/choose move 2")

    dispatch_task = asyncio.create_task(
        session._dispatch_player_payloads(
            [["", "request", '{"active":[{}]}']],
            [["", "request", '{"active":[{}]}']],
            wait_for_dispatch=True,
        )
    )

    await asyncio.wait_for(first_choice_sent.wait(), timeout=0.2)
    assert session._worker.lines == []
    assert session._worker.line_batches == []
    assert not dispatch_task.done()

    peer_release.set()
    assert await asyncio.wait_for(dispatch_task, timeout=0.2) is False
    assert session._worker.lines == []
    assert session._worker.line_batches == [[">p1 move 1", ">p2 move 2"]]


@pytest.mark.asyncio
async def test_local_session_flushes_single_actionable_choice():
    class ChoosingClient:
        async def dispatch_room_message(self, room, payload) -> None:
            assert room == "battle-test"
            await session.send_player_message("p1", "/choose move 1")

    session = object.__new__(LocalBattleStreamSession)
    session._room = "battle-test"
    session._worker = DummyWorker()
    session._accepting_player_messages = True
    session._client_1 = ChoosingClient()
    session._client_2 = DummyClient()

    finished = await session._dispatch_player_payloads(
        [["", "request", '{"active":[{}]}']],
        [["", "request", '{"wait":true}']],
        wait_for_dispatch=True,
    )

    assert finished is False
    assert session._worker.lines == []
    assert session._worker.line_batches == [[">p1 move 1"]]


@pytest.mark.asyncio
async def test_local_session_missing_actionable_choice_fails_after_dispatch():
    session = object.__new__(LocalBattleStreamSession)
    session._room = "battle-test"
    session._worker = DummyWorker()
    session._accepting_player_messages = True
    session._client_1 = DummyClient()
    session._client_2 = DummyClient()

    with pytest.raises(
        ShowdownException, match="without submitting choices for p1, p2"
    ):
        await session._dispatch_player_payloads(
            [["", "request", '{"active":[{}]}']],
            [["", "request", '{"active":[{}]}']],
            wait_for_dispatch=True,
        )

    assert session._pending_choice_batch is None
    assert session._worker.lines == []
    assert session._worker.line_batches == []


@pytest.mark.asyncio
async def test_local_session_callback_failure_discards_collected_choices():
    class ChoosingClient:
        async def dispatch_room_message(self, room, payload) -> None:
            await session.send_player_message("p1", "/choose move 1")

    class FailingClient:
        async def dispatch_room_message(self, room, payload) -> None:
            raise RuntimeError("dispatch failed")

    session = object.__new__(LocalBattleStreamSession)
    session._room = "battle-test"
    session._worker = DummyWorker()
    session._accepting_player_messages = True
    session._client_1 = ChoosingClient()
    session._client_2 = FailingClient()

    with pytest.raises(RuntimeError, match="dispatch failed"):
        await session._dispatch_player_payloads(
            [["", "request", '{"active":[{}]}']],
            [["", "request", '{"active":[{}]}']],
            wait_for_dispatch=True,
        )

    assert session._pending_choice_batch is None
    assert session._worker.lines == []
    assert session._worker.line_batches == []
    assert isinstance(session._dispatch_failure.exception(), RuntimeError)


@pytest.mark.asyncio
async def test_local_session_forfeit_bypasses_pending_choice_batch():
    class ForfeitingClient:
        async def dispatch_room_message(self, room, payload) -> None:
            await session.send_player_message("p1", "/forfeit")

    class ChoosingClient:
        async def dispatch_room_message(self, room, payload) -> None:
            await session.send_player_message("p2", "/choose move 2")

    session = object.__new__(LocalBattleStreamSession)
    session._room = "battle-test"
    session._worker = DummyWorker()
    session._accepting_player_messages = True
    session._client_1 = ForfeitingClient()
    session._client_2 = ChoosingClient()

    assert (
        await session._dispatch_player_payloads(
            [["", "request", '{"active":[{}]}']],
            [["", "request", '{"active":[{}]}']],
            wait_for_dispatch=True,
        )
        is False
    )

    assert session._accepting_player_messages is False
    assert session._pending_choice_batch is None
    assert session._worker.lines == [">forcelose p1"]
    assert session._worker.line_batches == []


@pytest.mark.asyncio
async def test_local_session_dispatches_async_choices_exactly_once():
    release = asyncio.Event()

    class ChoosingClient:
        def __init__(self, session, player: str, choice: str):
            self.session = session
            self.player = player
            self.choice = choice

        async def dispatch_room_message(self, room, payload) -> None:
            assert room == "battle-test"
            await release.wait()
            await self.session.send_player_message(self.player, self.choice)

    session = object.__new__(LocalBattleStreamSession)
    session._room = "battle-test"
    session._worker = DummyWorker()
    session._accepting_player_messages = True
    session._client_1 = ChoosingClient(session, "p1", "/choose move 1")
    session._client_2 = ChoosingClient(session, "p2", "/choose move 2")

    finished = await session._dispatch_player_payloads(
        [["", "request", '{"active":[{}]}']],
        [["", "request", '{"active":[{}]}']],
        wait_for_dispatch=False,
    )

    assert finished is False
    assert session._worker.line_batches == []
    release.set()
    await session._wait_for_dispatches()
    assert sorted(session._worker.lines) == [">p1 move 1", ">p2 move 2"]
    assert session._worker.line_batches == []


@pytest.mark.asyncio
async def test_local_session_single_side_request_does_not_block_other_side():
    p2_release = asyncio.Event()
    p1_received = asyncio.Event()

    class BlockingClient:
        def __init__(self, player: str):
            self.player = player
            self.messages = []

        async def dispatch_room_message(self, room, payload) -> None:
            self.messages.append((room, payload))
            if self.player == "p2":
                await p2_release.wait()
            else:
                p1_received.set()

    session = object.__new__(LocalBattleStreamSession)
    session._room = "battle-test"
    session._client_1 = BlockingClient("p1")
    session._client_2 = BlockingClient("p2")
    session._accepting_player_messages = True

    assert (
        await session._dispatch_protocol_message(
            {"type": "side-chunk", "player": "p2", "messages": [["", "request", "p2"]]}
        )
        is False
    )
    assert (
        await session._dispatch_protocol_message(
            {"type": "side-chunk", "player": "p1", "messages": [["", "request", "p1"]]}
        )
        is False
    )

    await asyncio.wait_for(p1_received.wait(), timeout=0.2)
    p2_release.set()
    await session._wait_for_dispatches()

    assert session._client_1.messages == [("battle-test", [["", "request", "p1"]])]
    assert session._client_2.messages == [("battle-test", [["", "request", "p2"]])]


@pytest.mark.asyncio
async def test_local_session_describe_includes_live_structure():
    session = object.__new__(LocalBattleStreamSession)
    session._room = "battle-test"
    session._battle_started = True
    session._accepting_player_messages = True
    session._worker = DummyWorker()
    session._dispatch_tasks = set()
    session._dispatch_failure = asyncio.get_running_loop().create_future()

    await session.send_player_message("p1", "/choose move 1")

    description = await session.describe()

    assert session._worker.lines == [">p1 move 1"]
    assert "battle_started=True" in description
    assert "accepting_player_messages=True" in description
    assert "dispatch_pending=0" in description
    assert "dummy_worker active=True" in description
    assert "move 1" not in description


@pytest.mark.asyncio
async def test_shared_worker_write_completes_after_command_delivery():
    commands = []

    async def send_command(command):
        commands.append(command)

    state = _SharedBattleState(
        message_queue=_AsyncPayloadMailbox(), battle_done=asyncio.Event()
    )
    worker = object.__new__(_SharedLocalBattleStreamWorker)
    worker._fatal_error = None
    worker._battle_states = {"battle-1": state}
    worker._send_command = send_command

    await worker.send_battle_lines("battle-1", [">p2 move 2"])

    assert commands == [
        {"type": "write", "battleId": "battle-1", "lines": [">p2 move 2"]}
    ]


@pytest.mark.asyncio
async def test_shared_worker_battle_ended_wakes_protocol_reader():
    state = _SharedBattleState(
        message_queue=_AsyncPayloadMailbox(), battle_done=asyncio.Event()
    )
    capacity_notifications = 0

    def notify_capacity():
        nonlocal capacity_notifications
        capacity_notifications += 1

    worker = object.__new__(_SharedLocalBattleStreamWorker)
    worker._battle_states = {"battle-1": state}
    worker._config = SimpleNamespace(max_battles_per_worker=1)
    worker._capacity_available_callback = notify_capacity

    result = await worker._handle_stdout_event(
        {"type": "battle-ended", "battleId": "battle-1"}
    )

    assert result is None
    assert state.active is False
    assert state.battle_done.is_set()
    assert worker.load == 0
    assert worker.has_capacity
    assert capacity_notifications == 1
    assert await state.message_queue.get(timeout=0) is None


@pytest.mark.asyncio
async def test_shared_worker_pool_acquire_wakes_on_capacity_notification():
    class DummySharedWorker:
        def __init__(self):
            self.ready = False
            self.allocated = False

        @property
        def has_capacity(self):
            return self.ready

        @property
        def load(self):
            return 0 if self.ready else 1

        async def allocate_battle(self):
            self.allocated = True
            return "battle-handle"

    worker = DummySharedWorker()
    pool = object.__new__(_SharedLocalBattleStreamWorkerPool)
    pool._workers = [worker]
    pool._started = True
    pool._start_lock = asyncio.Lock()
    pool._available = asyncio.Condition()
    pool._next_worker_index = 0

    acquire_task = asyncio.create_task(pool.acquire())
    await asyncio.sleep(0)
    assert not acquire_task.done()

    worker.ready = True
    pool._notify_capacity_available()

    result = await asyncio.wait_for(acquire_task, timeout=1.0)
    assert result == "battle-handle"
    assert worker.allocated


@pytest.mark.asyncio
async def test_shared_worker_flushes_protocol_before_battle_ended():
    state = _SharedBattleState(
        message_queue=_AsyncPayloadMailbox(), battle_done=asyncio.Event()
    )
    worker = object.__new__(_SharedLocalBattleStreamWorker)
    worker._battle_states = {"battle-1": state}

    assert await worker._handle_stdout_events(
        [
            {
                "type": "protocol-batch",
                "battleId": "battle-1",
                "messages": [
                    {
                        "type": "side-chunk",
                        "player": "p1",
                        "messages": [["", "win", "p1"]],
                    }
                ],
            },
            {"type": "battle-ended", "battleId": "battle-1"},
        ]
    )

    assert await state.message_queue.get(timeout=0) == {
        "type": "side-chunk",
        "player": "p1",
        "messages": [["", "win", "p1"]],
    }
    assert await state.message_queue.get(timeout=0) is None


@pytest.mark.asyncio
async def test_local_session_dispatches_worker_split_message_arrays():
    session = object.__new__(LocalBattleStreamSession)
    session._room = "battle-test"
    session._client_1 = DummyClient()
    session._client_2 = DummyClient()
    session._accepting_player_messages = True

    finished = await session._dispatch_protocol_message(
        {
            "type": "split-chunk",
            "p1_messages": [["", "turn", "1"], ["", "request", "p1"]],
            "p2_messages": [["", "turn", "1"], ["", "request", "p2"]],
        }
    )

    assert finished is False
    await session._wait_for_dispatches()
    assert session._client_1.messages == [
        ("battle-test", [["", "turn", "1"], ["", "request", "p1"]])
    ]
    assert session._client_2.messages == [
        ("battle-test", [["", "turn", "1"], ["", "request", "p2"]])
    ]


@pytest.mark.asyncio
async def test_local_session_dispatches_protocol_batches_until_terminal():
    session = object.__new__(LocalBattleStreamSession)
    session._room = "battle-test"
    session._client_1 = DummyClient()
    session._client_2 = DummyClient()
    session._accepting_player_messages = True

    finished = await session._dispatch_protocol_payload(
        {
            "type": "protocol-batch",
            "messages": [
                {
                    "type": "split-chunk",
                    "p1_messages": [["", "turn", "1"]],
                    "p2_messages": [["", "turn", "1"]],
                },
                {
                    "type": "side-chunk",
                    "player": "p1",
                    "messages": [["", "request", "p1"]],
                },
                {"type": "end"},
                {
                    "type": "side-chunk",
                    "player": "p2",
                    "messages": [["", "request", "unused"]],
                },
            ],
        }
    )

    assert finished is True
    assert session._accepting_player_messages is False
    await session._wait_for_dispatches()
    assert session._client_1.messages == [
        ("battle-test", [["", "turn", "1"], ["", "request", "p1"]])
    ]
    assert session._client_2.messages == [("battle-test", [["", "turn", "1"]])]


@pytest.mark.asyncio
async def test_local_session_treats_win_message_as_terminal_without_stream_end():
    session = object.__new__(LocalBattleStreamSession)
    session._room = "battle-test"
    session._client_1 = DummyClient()
    session._client_2 = DummyClient()
    session._accepting_player_messages = True

    finished = await session._dispatch_protocol_payload(
        {
            "type": "protocol-batch",
            "messages": [
                {
                    "type": "split-chunk",
                    "p1_messages": [["", "turn", "2"], ["", "win", "Player 1"]],
                    "p2_messages": [["", "turn", "2"], ["", "win", "Player 1"]],
                }
            ],
        }
    )

    assert finished is True
    assert session._accepting_player_messages is False
    await session._wait_for_dispatches()
    assert session._client_1.messages == [
        ("battle-test", [["", "turn", "2"], ["", "win", "Player 1"]])
    ]
    assert session._client_2.messages == [
        ("battle-test", [["", "turn", "2"], ["", "win", "Player 1"]])
    ]


@pytest.mark.asyncio
async def test_local_session_drains_terminal_protocol_batch_for_both_players():
    session = object.__new__(LocalBattleStreamSession)
    session._room = "battle-test"
    session._client_1 = DummyClient()
    session._client_2 = DummyClient()
    session._accepting_player_messages = True

    finished = await session._dispatch_protocol_payload(
        {
            "type": "protocol-batch",
            "messages": [
                {
                    "type": "side-chunk",
                    "player": "p1",
                    "messages": [["", "request", "p1-choice"]],
                },
                {
                    "type": "side-chunk",
                    "player": "p2",
                    "messages": [["", "request", "p2-choice"]],
                },
                {
                    "type": "side-chunk",
                    "player": "p1",
                    "messages": [["", "win", "Player 1"]],
                },
                {
                    "type": "side-chunk",
                    "player": "p2",
                    "messages": [["", "win", "Player 1"]],
                },
                {"type": "end"},
                {
                    "type": "side-chunk",
                    "player": "p1",
                    "messages": [["", "request", "unused"]],
                },
            ],
        }
    )

    assert finished is True
    assert session._accepting_player_messages is False
    await session._wait_for_dispatches()
    assert session._client_1.messages == [("battle-test", [["", "win", "Player 1"]])]
    assert session._client_2.messages == [("battle-test", [["", "win", "Player 1"]])]


@pytest.mark.asyncio
async def test_local_session_replays_global_terminal_result_to_missing_side():
    session = object.__new__(LocalBattleStreamSession)
    session._room = "battle-test"
    session._client_1 = DummyClient()
    session._client_2 = DummyClient()
    session._accepting_player_messages = True

    finished = await session._dispatch_protocol_payload(
        {
            "type": "protocol-batch",
            "messages": [
                {
                    "type": "side-chunk",
                    "player": "p1",
                    "messages": [["", "request", "p1-choice"]],
                },
                {
                    "type": "side-chunk",
                    "player": "p2",
                    "messages": [["", "request", "p2-choice"]],
                },
                {
                    "type": "side-chunk",
                    "player": "p1",
                    "messages": [["", "win", "Player 1"]],
                },
                {"type": "end"},
            ],
        }
    )

    assert finished is True
    assert session._accepting_player_messages is False
    await session._wait_for_dispatches()
    assert session._client_1.messages == [("battle-test", [["", "win", "Player 1"]])]
    assert session._client_2.messages == [("battle-test", [["", "win", "Player 1"]])]


@pytest.mark.asyncio
async def test_local_session_replays_end_event_winner_to_buffered_messages():
    session = object.__new__(LocalBattleStreamSession)
    session._room = "battle-test"
    session._client_1 = DummyClient()
    session._client_2 = DummyClient()
    session._accepting_player_messages = True

    finished = await session._dispatch_protocol_payload(
        {
            "type": "protocol-batch",
            "messages": [
                {
                    "type": "split-chunk",
                    "p1_messages": [["", "turn", "9"]],
                    "p2_messages": [["", "turn", "9"]],
                },
                {
                    "type": "side-chunk",
                    "player": "p1",
                    "messages": [["", "request", "p1-choice"]],
                },
                {
                    "type": "side-chunk",
                    "player": "p2",
                    "messages": [["", "request", "p2-choice"]],
                },
                {"type": "end", "payload": '{"winner":"Player 1"}'},
            ],
        }
    )

    assert finished is True
    assert session._accepting_player_messages is False
    await session._wait_for_dispatches()
    assert session._client_1.messages == [
        ("battle-test", [["", "turn", "9"], ["", "win", "Player 1"]])
    ]
    assert session._client_2.messages == [
        ("battle-test", [["", "turn", "9"], ["", "win", "Player 1"]])
    ]


@pytest.mark.asyncio
async def test_local_session_dispatches_standalone_end_event_winner():
    session = object.__new__(LocalBattleStreamSession)
    session._room = "battle-test"
    session._client_1 = DummyClient()
    session._client_2 = DummyClient()
    session._accepting_player_messages = True

    finished = await session._dispatch_protocol_message(
        {"type": "end", "payload": '{"winner":"Player 2"}'}
    )

    assert finished is True
    assert session._accepting_player_messages is False
    await session._wait_for_dispatches()
    assert session._client_1.messages == [("battle-test", [["", "win", "Player 2"]])]
    assert session._client_2.messages == [("battle-test", [["", "win", "Player 2"]])]


def test_cross_loop_local_controller_detects_terminal_protocol_batch():
    assert _CrossLoopLocalBattleController._is_terminal_payload(
        {
            "type": "protocol-batch",
            "messages": [
                {"type": "split-chunk", "p1_messages": [], "p2_messages": []},
                {"type": "end"},
            ],
        }
    )


@pytest.mark.asyncio
async def test_cross_loop_local_controller_batches_session_loop_posts():
    callbacks = []

    class DummyLoop:
        def call_soon_threadsafe(self, callback):
            callbacks.append(callback)

    controller = _CrossLoopLocalBattleController(
        runtime_controller=object(), session_loop=DummyLoop(), runtime_timeout=1.0
    )

    controller._post_to_session_loop("payload-1")
    controller._post_to_session_loop("payload-2")

    assert len(callbacks) == 1

    callbacks.pop()()

    assert await controller.read_protocol_message(timeout=0.1) == "payload-1"
    assert await controller.read_protocol_message(timeout=0.1) == "payload-2"


@pytest.mark.asyncio
async def test_cross_loop_local_controller_batches_battle_line_writes():
    class DummyRuntimeController:
        def __init__(self):
            self.calls: list[list[str]] = []

        async def send_battle_lines(self, lines: list[str]) -> None:
            self.calls.append(lines)

    runtime_controller = DummyRuntimeController()
    controller = _CrossLoopLocalBattleController(
        runtime_controller=runtime_controller,
        session_loop=asyncio.get_running_loop(),
        runtime_timeout=1.0,
    )

    await asyncio.gather(
        controller.send_battle_line(">p1 move 1"),
        controller.send_battle_line(">p2 move 2"),
    )

    assert runtime_controller.calls == [[">p1 move 1", ">p2 move 2"]]


@pytest.mark.asyncio
async def test_cross_loop_local_controller_submits_idle_multiline_batch_directly(
    monkeypatch,
):
    class DummyRuntimeController:
        def __init__(self):
            self.calls: list[list[str]] = []

        async def send_battle_lines(self, lines: list[str]) -> None:
            self.calls.append(list(lines))

    runtime_controller = DummyRuntimeController()
    controller = _CrossLoopLocalBattleController(
        runtime_controller=runtime_controller,
        session_loop=asyncio.get_running_loop(),
        runtime_loop=asyncio.get_running_loop(),
        runtime_timeout=1.0,
    )

    def unexpected_queue_flush() -> None:
        raise AssertionError("Idle multiline batch used the queued flush path")

    monkeypatch.setattr(
        controller, "_schedule_battle_line_flush", unexpected_queue_flush
    )

    await controller.send_battle_lines([">p1 move 1", ">p2 move 2"])

    assert runtime_controller.calls == [[">p1 move 1", ">p2 move 2"]]


@pytest.mark.asyncio
async def test_cross_loop_local_controller_keeps_scheduled_multiline_batch_queued():
    class DummyRuntimeController:
        def __init__(self):
            self.calls: list[list[str]] = []

        async def send_battle_lines(self, lines: list[str]) -> None:
            self.calls.append(list(lines))

    runtime_controller = DummyRuntimeController()
    loop = asyncio.get_running_loop()
    controller = _CrossLoopLocalBattleController(
        runtime_controller=runtime_controller,
        session_loop=loop,
        runtime_loop=loop,
        runtime_timeout=1.0,
    )

    await asyncio.gather(
        controller.send_battle_line(">p1 team 1234"),
        controller.send_battle_lines([">p2 team 5678", ">p2 move 2"]),
    )

    assert runtime_controller.calls == [
        [">p1 team 1234", ">p2 team 5678", ">p2 move 2"]
    ]


@pytest.mark.asyncio
async def test_cross_loop_local_controller_serializes_direct_and_queued_batches():
    first_started = asyncio.Event()
    release_first = asyncio.Event()

    class DummyRuntimeController:
        def __init__(self):
            self.calls: list[list[str]] = []

        async def send_battle_lines(self, lines: list[str]) -> None:
            self.calls.append(list(lines))
            if len(self.calls) == 1:
                first_started.set()
                await release_first.wait()

    runtime_controller = DummyRuntimeController()
    loop = asyncio.get_running_loop()
    controller = _CrossLoopLocalBattleController(
        runtime_controller=runtime_controller,
        session_loop=loop,
        runtime_loop=loop,
        runtime_timeout=1.0,
    )

    first_write = asyncio.create_task(
        controller.send_battle_lines([">p1 move 1", ">p2 move 2"])
    )
    await asyncio.wait_for(first_started.wait(), timeout=0.2)

    second_write = asyncio.create_task(
        controller.send_battle_lines([">p1 move 3", ">p2 move 4"])
    )
    third_write = asyncio.create_task(controller.send_battle_line(">p1 move 5"))
    await asyncio.sleep(0)

    assert runtime_controller.calls == [[">p1 move 1", ">p2 move 2"]]
    assert controller._pending_battle_lines == [
        ">p1 move 3",
        ">p2 move 4",
        ">p1 move 5",
    ]

    release_first.set()
    await asyncio.gather(first_write, second_write, third_write)

    assert runtime_controller.calls == [
        [">p1 move 1", ">p2 move 2"],
        [">p1 move 3", ">p2 move 4", ">p1 move 5"],
    ]


@pytest.mark.asyncio
async def test_cross_loop_local_controller_direct_failure_fails_queued_batches():
    first_started = asyncio.Event()
    release_first = asyncio.Event()

    class DummyRuntimeController:
        def __init__(self):
            self.calls: list[list[str]] = []

        async def send_battle_lines(self, lines: list[str]) -> None:
            self.calls.append(list(lines))
            first_started.set()
            await release_first.wait()
            raise ShowdownException("direct runtime write failed")

    runtime_controller = DummyRuntimeController()
    loop = asyncio.get_running_loop()
    controller = _CrossLoopLocalBattleController(
        runtime_controller=runtime_controller,
        session_loop=loop,
        runtime_loop=loop,
        runtime_timeout=1.0,
    )

    first_write = asyncio.create_task(
        controller.send_battle_lines([">p1 move 1", ">p2 move 2"])
    )
    await asyncio.wait_for(first_started.wait(), timeout=0.2)
    queued_write = asyncio.create_task(
        controller.send_battle_lines([">p1 move 3", ">p2 move 4"])
    )
    await asyncio.sleep(0)
    release_first.set()

    results = await asyncio.gather(first_write, queued_write, return_exceptions=True)

    assert runtime_controller.calls == [[">p1 move 1", ">p2 move 2"]]
    assert len(results) == 2
    for result in results:
        assert isinstance(result, ShowdownException)
        assert "direct runtime write failed" in str(result)
    assert controller._battle_line_write_future is None
    assert controller._pending_battle_lines == []


@pytest.mark.asyncio
async def test_cross_loop_local_controller_close_cancels_direct_write():
    write_started = asyncio.Event()
    write_cancelled = asyncio.Event()

    class DummyRuntimeController:
        def __init__(self):
            self.close_calls = 0

        async def send_battle_lines(self, lines: list[str]) -> None:
            del lines
            write_started.set()
            try:
                await asyncio.Event().wait()
            finally:
                write_cancelled.set()

        async def close_battle(self) -> None:
            self.close_calls += 1

    runtime_controller = DummyRuntimeController()
    loop = asyncio.get_running_loop()
    controller = _CrossLoopLocalBattleController(
        runtime_controller=runtime_controller,
        session_loop=loop,
        runtime_loop=loop,
        runtime_timeout=1.0,
    )

    write = asyncio.create_task(
        controller.send_battle_lines([">p1 move 1", ">p2 move 2"])
    )
    await asyncio.wait_for(write_started.wait(), timeout=0.2)
    await controller.close_battle()

    with pytest.raises(
        ShowdownException, match="closed before pending writes completed"
    ):
        await write
    await asyncio.wait_for(write_cancelled.wait(), timeout=0.2)

    assert runtime_controller.close_calls == 1
    assert controller._battle_line_write_future is None
    assert controller._battle_line_in_flight_lines == ()


@pytest.mark.asyncio
async def test_cross_loop_local_controller_close_wins_posted_completion_race(
    monkeypatch,
):
    class DummyRuntimeController:
        async def close_battle(self) -> None:
            return None

    loop = asyncio.get_running_loop()
    controller = _CrossLoopLocalBattleController(
        runtime_controller=DummyRuntimeController(),
        session_loop=loop,
        runtime_loop=loop,
        runtime_timeout=1.0,
    )
    completed_future: ConcurrentFuture[None] = ConcurrentFuture()
    completed_future.set_result(None)
    monkeypatch.setattr(
        controller, "_submit_battle_line_write", lambda lines: completed_future
    )

    write = asyncio.create_task(
        controller.send_battle_lines([">p1 move 1", ">p2 move 2"])
    )
    close = asyncio.create_task(controller.close_battle())
    write_result, close_result = await asyncio.gather(
        write, close, return_exceptions=True
    )
    await asyncio.sleep(0)

    assert isinstance(write_result, ShowdownException)
    assert "closed before pending writes completed" in str(write_result)
    assert close_result is None
    assert controller._battle_line_write_future is None
    assert controller._battle_line_in_flight_lines == ()
    assert controller._battle_line_in_flight_waiters == []


@pytest.mark.asyncio
async def test_cross_loop_local_controller_flushes_battle_lines_on_runtime_loop():
    class DummyRuntimeController:
        def __init__(self):
            self.calls: list[list[str]] = []
            self.loop_ids: list[int] = []

        async def send_battle_lines(self, lines: list[str]) -> None:
            self.calls.append(list(lines))
            self.loop_ids.append(id(asyncio.get_running_loop()))

    runtime_controller = DummyRuntimeController()
    controller = _CrossLoopLocalBattleController(
        runtime_controller=runtime_controller,
        session_loop=asyncio.get_running_loop(),
        runtime_loop=POKE_LOOP,
        runtime_timeout=1.0,
    )

    await asyncio.gather(
        controller.send_battle_line(">p1 team 1234"),
        controller.send_battle_line(">p2 team 5678"),
    )

    assert runtime_controller.calls == [[">p1 team 1234", ">p2 team 5678"]]
    assert runtime_controller.loop_ids == [id(POKE_LOOP)]


@pytest.mark.asyncio
async def test_cross_loop_local_controller_wakes_session_loop_for_threaded_flush():
    class DummyRuntimeController:
        def __init__(self):
            self.calls: list[list[str]] = []

        async def send_battle_lines(self, lines: list[str]) -> None:
            self.calls.append(list(lines))

    runtime_controller = DummyRuntimeController()
    session_loop = asyncio.get_running_loop()
    controller = _CrossLoopLocalBattleController(
        runtime_controller=runtime_controller,
        session_loop=session_loop,
        runtime_loop=POKE_LOOP,
        runtime_timeout=1.0,
    )

    waiter = session_loop.create_future()
    controller._pending_battle_lines.append(">p1 team 1234")
    controller._pending_battle_line_waiters.append(waiter)

    with ThreadPoolExecutor(max_workers=1) as executor:
        await session_loop.run_in_executor(
            executor, controller._schedule_battle_line_flush
        )

    await asyncio.wait_for(waiter, timeout=1.0)

    assert runtime_controller.calls == [[">p1 team 1234"]]


@pytest.mark.asyncio
async def test_cross_loop_local_controller_serializes_in_flight_and_pending_writes():
    first_started = ConcurrentFuture()
    release_first = ConcurrentFuture()

    class DummyRuntimeController:
        def __init__(self):
            self.calls: list[list[str]] = []

        async def send_battle_lines(self, lines: list[str]) -> None:
            self.calls.append(list(lines))
            if len(self.calls) == 1:
                first_started.set_result(None)
                await asyncio.wrap_future(release_first)

        async def describe(self) -> str:
            return f"runtime_call_count={len(self.calls)}"

    runtime_controller = DummyRuntimeController()
    controller = _CrossLoopLocalBattleController(
        runtime_controller=runtime_controller,
        session_loop=asyncio.get_running_loop(),
        runtime_loop=POKE_LOOP,
        runtime_timeout=1.0,
    )

    first_write = asyncio.create_task(controller.send_battle_line(">p1 move 1"))
    await asyncio.wait_for(asyncio.wrap_future(first_started), timeout=0.2)

    second_write = asyncio.create_task(controller.send_battle_line(">p2 move 2"))
    await asyncio.sleep(0)

    description = await controller.describe()
    assert "pending_writes=1" in description
    assert "write_in_flight=True" in description
    assert "in_flight_write_count=1" in description
    assert ">p1 move 1" not in description
    assert not first_write.done()
    assert not second_write.done()

    release_first.set_result(None)
    await asyncio.gather(first_write, second_write)

    assert runtime_controller.calls == [[">p1 move 1"], [">p2 move 2"]]


@pytest.mark.asyncio
async def test_cross_loop_local_controller_propagates_runtime_write_failure():
    class DummyRuntimeController:
        async def send_battle_lines(self, lines: list[str]) -> None:
            del lines
            raise ShowdownException("runtime write failed")

    controller = _CrossLoopLocalBattleController(
        runtime_controller=DummyRuntimeController(),
        session_loop=asyncio.get_running_loop(),
        runtime_loop=POKE_LOOP,
        runtime_timeout=1.0,
    )

    with pytest.raises(ShowdownException, match="runtime write failed"):
        await controller.send_battle_line(">p1 move 1")


@pytest.mark.asyncio
async def test_cross_loop_local_controller_fails_pending_writes_after_runtime_failure():
    first_started = ConcurrentFuture()
    release_first = ConcurrentFuture()

    class DummyRuntimeController:
        async def send_battle_lines(self, lines: list[str]) -> None:
            del lines
            if not first_started.done():
                first_started.set_result(None)
            await asyncio.wrap_future(release_first)
            raise ShowdownException("runtime write failed")

    controller = _CrossLoopLocalBattleController(
        runtime_controller=DummyRuntimeController(),
        session_loop=asyncio.get_running_loop(),
        runtime_loop=POKE_LOOP,
        runtime_timeout=1.0,
    )

    first_write = asyncio.create_task(controller.send_battle_line(">p1 move 1"))
    await asyncio.wait_for(asyncio.wrap_future(first_started), timeout=0.2)

    second_write = asyncio.create_task(controller.send_battle_line(">p2 move 2"))
    await asyncio.sleep(0)
    release_first.set_result(None)

    results = await asyncio.gather(first_write, second_write, return_exceptions=True)
    assert len(results) == 2
    for result in results:
        assert isinstance(result, ShowdownException)
        assert "runtime write failed" in str(result)


@pytest.mark.asyncio
async def test_runtime_shard_cancelled_acquire_releases_reservation():
    shard = object.__new__(_LocalRuntimeShard)
    shard._load = 0
    shard._load_lock = Lock()
    acquire_started = asyncio.Event()
    wait_forever = asyncio.Event()

    async def acquire():
        acquire_started.set()
        await wait_forever.wait()

    async def run(coroutine):
        return await coroutine

    shard._acquire = acquire
    shard._run = run

    acquire_task = asyncio.create_task(shard.acquire())
    await acquire_started.wait()
    assert shard.load == 1

    acquire_task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await acquire_task

    assert shard.load == 0


@pytest.mark.asyncio
async def test_local_session_cancelled_close_still_releases_worker_and_detaches_clients():
    events = []

    class CancellingWorker:
        async def close_battle(self):
            events.append("close")
            raise asyncio.CancelledError

    class RecordingPool:
        async def release(self, worker):
            events.append(("release", worker))

    session = object.__new__(LocalBattleStreamSession)
    session._battle_started = False
    session._accepting_player_messages = True
    session._worker = CancellingWorker()
    session._pool = RecordingPool()
    session._detach_clients = lambda: events.append("detach")
    worker = session._worker

    with pytest.raises(asyncio.CancelledError):
        await session._finalize_battle()

    assert events == ["close", ("release", worker), "detach"]
    assert session._worker is None


@pytest.mark.asyncio
@pytest.mark.parametrize(
    (
        "worker_count",
        "max_battles_per_worker",
        "player_1_limit",
        "player_2_limit",
        "n_battles",
        "expected_concurrency",
    ),
    [(2, 3, 4, 5, 10, 4), (2, 3, 0, 0, 10, 6), (3, 2, 0, 2, 10, 2), (4, 2, 0, 0, 3, 3)],
)
async def test_run_local_battles_respects_player_and_pool_limits(
    monkeypatch,
    tmp_path,
    worker_count,
    max_battles_per_worker,
    player_1_limit,
    player_2_limit,
    n_battles,
    expected_concurrency,
):
    config = LocalBattleStreamConfiguration(
        tmp_path,
        worker_count=worker_count,
        max_battles_per_worker=max_battles_per_worker,
    )
    player_1, player_2 = _local_runner_players(
        monkeypatch, config, player_1_limit, player_2_limit
    )
    battle_ids = []
    active = 0
    max_active = 0

    class RecordingSession:
        def __init__(self, player_1, player_2, battle_index):
            del player_1, player_2
            self.battle_index = battle_index
            battle_ids.append(battle_index)

        async def run(self):
            nonlocal active, max_active
            active += 1
            max_active = max(max_active, active)
            try:
                await asyncio.sleep(0)
            finally:
                active -= 1

    monkeypatch.setattr(
        local_client_module, "LocalBattleStreamSession", RecordingSession
    )

    await run_local_battles(player_1, player_2, n_battles)

    assert max_active == expected_concurrency
    assert active == 0
    assert len(battle_ids) == n_battles
    assert len(set(battle_ids)) == n_battles


@pytest.mark.asyncio
@pytest.mark.parametrize("n_battles", [0, -2])
async def test_run_local_battles_non_positive_count_is_a_no_op(
    monkeypatch, tmp_path, n_battles
):
    config = LocalBattleStreamConfiguration(tmp_path)
    player_1, player_2 = _local_runner_players(monkeypatch, config, 0, 0)

    class UnexpectedSession:
        def __init__(self, *args, **kwargs):
            raise AssertionError(
                "A non-positive battle count must not create a session"
            )

    monkeypatch.setattr(
        local_client_module, "LocalBattleStreamSession", UnexpectedSession
    )

    await run_local_battles(player_1, player_2, n_battles)


@pytest.mark.asyncio
async def test_run_local_battles_cancels_and_drains_siblings_after_failure(
    monkeypatch, tmp_path
):
    config = LocalBattleStreamConfiguration(tmp_path, worker_count=3)
    player_1, player_2 = _local_runner_players(monkeypatch, config, 0, 0)
    all_started = asyncio.Event()
    never = asyncio.Event()
    started = set()
    cancelled = set()
    cleaned = set()
    active = 0

    class FailingSession:
        def __init__(self, player_1, player_2, battle_index):
            del player_1, player_2, battle_index
            self.ordinal = len(started)

        async def run(self):
            nonlocal active
            active += 1
            started.add(self.ordinal)
            if len(started) == 3:
                all_started.set()
            try:
                await all_started.wait()
                if self.ordinal == 0:
                    raise RuntimeError("session failed")
                await never.wait()
            except asyncio.CancelledError:
                cancelled.add(self.ordinal)
                raise
            finally:
                active -= 1
                cleaned.add(self.ordinal)

    monkeypatch.setattr(local_client_module, "LocalBattleStreamSession", FailingSession)

    with pytest.raises(RuntimeError, match="session failed"):
        await run_local_battles(player_1, player_2, 10)

    assert started == {0, 1, 2}
    assert cancelled == {1, 2}
    assert cleaned == started
    assert active == 0


@pytest.mark.asyncio
async def test_run_local_battles_caller_cancellation_drains_sessions(
    monkeypatch, tmp_path
):
    config = LocalBattleStreamConfiguration(tmp_path, worker_count=3)
    player_1, player_2 = _local_runner_players(monkeypatch, config, 0, 0)
    all_started = asyncio.Event()
    never = asyncio.Event()
    started = set()
    cancelled = set()
    cleaned = set()
    active = 0

    class BlockingSession:
        def __init__(self, player_1, player_2, battle_index):
            del player_1, player_2, battle_index
            self.ordinal = len(started)

        async def run(self):
            nonlocal active
            active += 1
            started.add(self.ordinal)
            if len(started) == 3:
                all_started.set()
            try:
                await never.wait()
            except asyncio.CancelledError:
                cancelled.add(self.ordinal)
                raise
            finally:
                active -= 1
                cleaned.add(self.ordinal)

    monkeypatch.setattr(
        local_client_module, "LocalBattleStreamSession", BlockingSession
    )
    run_task = asyncio.create_task(run_local_battles(player_1, player_2, 10))
    await asyncio.wait_for(all_started.wait(), timeout=1.0)

    run_task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await run_task

    assert started == {0, 1, 2}
    assert cancelled == started
    assert cleaned == started
    assert active == 0
