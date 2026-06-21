import asyncio
from concurrent.futures import ThreadPoolExecutor

import pytest

from poke_env import AccountConfiguration, LocalBattleStreamConfiguration
from poke_env.concurrency import POKE_LOOP
from poke_env.exceptions import ShowdownException
from poke_env.ps_client.local_client import (
    _AsyncPayloadMailbox,
    LocalBattleStreamClient,
    LocalBattleStreamSession,
    _CrossLoopLocalBattleController,
    _LOCAL_CLIENT_REFCOUNTS,
    _LOCAL_WORKER_POOLS,
    _SharedBattleState,
    _SharedLocalBattleStreamWorker,
    _expand_worker_events,
    _get_or_create_worker_pool,
    _protocol_batch_payload,
    _split_update_for_players,
    _translate_showdown_command,
)


class DummySession:
    def __init__(self):
        self.messages: list[tuple[str, str]] = []

    async def send_player_message(self, player_slot: str, message: str) -> None:
        self.messages.append((player_slot, message))


class DummyWorker:
    def __init__(self):
        self.lines: list[str] = []

    async def send_battle_line(self, line: str) -> None:
        self.lines.append(line)


class DummyClient:
    def __init__(self):
        self.messages: list[tuple[str, str | list[str] | list[list[str]]]] = []

    async def dispatch_room_message(
        self, room: str, payload: str | list[str] | list[list[str]]
    ) -> None:
        self.messages.append((room, payload))


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


def test_get_or_create_worker_pool_is_thread_safe():
    class DummyPool:
        pass

    config = LocalBattleStreamConfiguration(
        "C:/showdown",
        worker_count=4,
        max_battles_per_worker=4,
        pool_mode="shared",
        runtime_loop_count=2,
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
async def test_local_session_keeps_active_battles_idle_without_event_timeout():
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
    session._config = LocalBattleStreamConfiguration(
        "C:/showdown", startup_timeout=1.5, event_timeout=0.01
    )
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
    assert session._client_1.messages == [
        ("battle-test", [["", "turn", "1"], ["", "request", "p1"]])
    ]
    assert session._client_2.messages == [("battle-test", [["", "turn", "1"]])]


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
