from __future__ import annotations

import atexit
import asyncio
import json
from collections import deque
from itertools import count
from pathlib import Path
from threading import Thread
from typing import TYPE_CHECKING, Deque, NoReturn

from poke_env.exceptions import ShowdownException
from poke_env.ps_client.account_configuration import AccountConfiguration
from poke_env.ps_client.ps_client import (
    BattleMessageCallback,
    ChallengeCallback,
    PSClient,
)
from poke_env.ps_client.server_configuration import (
    LocalBattleStreamConfiguration,
    LocalhostServerConfiguration,
)

if TYPE_CHECKING:
    from poke_env.player.player import Player


_LOCAL_BATTLE_COUNTER = count(1)
_LOCAL_WORKER_POOLS: dict[
    LocalBattleStreamConfiguration, _LocalBattleStreamWorkerPool
] = {}


def _run_loop(loop: asyncio.AbstractEventLoop) -> None:
    asyncio.set_event_loop(loop)
    loop.run_forever()


_LOCAL_WORKER_LOOP = asyncio.new_event_loop()
_LOCAL_WORKER_THREAD = Thread(
    target=_run_loop, args=(_LOCAL_WORKER_LOOP,), daemon=True
)
_LOCAL_WORKER_THREAD.start()


async def _run_on_local_worker_loop(coro):
    future = asyncio.run_coroutine_threadsafe(coro, _LOCAL_WORKER_LOOP)
    return await asyncio.wrap_future(future)


async def _shutdown_local_worker_pools() -> None:
    for pool in list(_LOCAL_WORKER_POOLS.values()):
        await pool.close()
    _LOCAL_WORKER_POOLS.clear()


def _stop_local_worker_loop() -> None:
    try:
        asyncio.run_coroutine_threadsafe(
            _shutdown_local_worker_pools(), _LOCAL_WORKER_LOOP
        ).result(timeout=5.0)
    except Exception:
        pass

    _LOCAL_WORKER_LOOP.call_soon_threadsafe(_LOCAL_WORKER_LOOP.stop)
    _LOCAL_WORKER_THREAD.join(timeout=5.0)
    _LOCAL_WORKER_LOOP.close()


atexit.register(_stop_local_worker_loop)


class _LocalWorkerRuntimeError(ShowdownException):
    pass


def _worker_script_path() -> Path:
    return Path(__file__).with_name("battle_stream_worker.js")


class _LocalBattleStreamWorker:
    def __init__(self, config: LocalBattleStreamConfiguration, worker_index: int):
        self._config = config
        self._worker_index = worker_index
        self._stderr_tail: Deque[str] = deque(maxlen=config.stderr_tail_lines)
        self._process: asyncio.subprocess.Process | None = None
        self._stdout_task: asyncio.Task[None] | None = None
        self._stderr_task: asyncio.Task[None] | None = None
        self._write_lock = asyncio.Lock()
        self._ready = asyncio.Event()
        self._battle_done = asyncio.Event()
        self._active = False
        self._message_queue: asyncio.Queue[str | Exception] | None = None
        self._fatal_error: ShowdownException | None = None

    async def start(self) -> None:
        if self._process is not None and self._process.returncode is None:
            return

        self._ready = asyncio.Event()
        self._battle_done = asyncio.Event()
        self._message_queue = None
        self._active = False
        self._fatal_error = None
        self._stderr_tail.clear()

        node_command = list(self._config.node_command or ["node"])
        if not node_command:
            self._raise_failure("node_command must not be empty")

        worker_script = _worker_script_path()
        if not worker_script.exists():
            self._raise_failure(f"Could not find worker script at {worker_script}")

        self._process = await asyncio.create_subprocess_exec(
            *node_command,
            str(worker_script),
            str(self._config.showdown_dir),
            cwd=str(self._config.showdown_dir),
            stdin=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        self._stdout_task = asyncio.create_task(self._consume_stdout())
        self._stderr_task = asyncio.create_task(self._consume_stderr())

        try:
            await asyncio.wait_for(self._ready.wait(), timeout=self._config.startup_timeout)
        except asyncio.TimeoutError as exc:
            await self.shutdown()
            raise ShowdownException(
                "Local BattleStream worker did not become ready in time"
            ) from exc

    async def start_battle(self, lines: list[str]) -> None:
        await self.start()
        if self._fatal_error is not None:
            raise self._fatal_error
        if self._active:
            self._raise_failure("Worker received a start command while a battle is active")

        self._message_queue = asyncio.Queue()
        self._battle_done = asyncio.Event()
        self._active = True
        await self._send_command({"type": "start", "lines": lines})

    async def send_battle_line(self, line: str) -> None:
        if self._fatal_error is not None:
            raise self._fatal_error
        if not self._active:
            self._raise_failure("Worker received a write command without an active battle")
        await self._send_command({"type": "write", "lines": [line]})

    async def read_protocol_message(self, timeout: float) -> str | None:
        if self._fatal_error is not None:
            raise self._fatal_error
        if self._message_queue is None:
            self._raise_failure("Worker has no active message queue")

        payload = await asyncio.wait_for(self._message_queue.get(), timeout=timeout)
        if isinstance(payload, Exception):
            raise payload
        return payload

    async def close_battle(self) -> None:
        if self._process is None or self._process.returncode is not None:
            self._active = False
            return
        if not self._active:
            return

        await self._send_command({"type": "close-battle"})
        try:
            await asyncio.wait_for(self._battle_done.wait(), timeout=1.0)
        except asyncio.TimeoutError:
            await self.shutdown()

    async def shutdown(self) -> None:
        process = self._process
        if process is None:
            return

        stdin = process.stdin

        try:
            if process.returncode is None:
                await self._send_command({"type": "shutdown"})
                if stdin is not None and not stdin.is_closing():
                    stdin.close()
                    await stdin.wait_closed()
                await asyncio.wait_for(process.wait(), timeout=1.0)
        except Exception:
            if process.returncode is None:
                process.terminate()
                await process.wait()
        finally:
            if stdin is not None and not stdin.is_closing():
                stdin.close()
                try:
                    await stdin.wait_closed()
                except Exception:
                    pass

        if self._stdout_task is not None:
            await self._stdout_task
        if self._stderr_task is not None:
            await self._stderr_task

        transport = getattr(process, "_transport", None)
        if transport is not None:
            try:
                transport.close()
            except Exception:
                pass

        self._process = None
        self._stdout_task = None
        self._stderr_task = None
        self._active = False

    async def _consume_stdout(self) -> None:
        assert self._process is not None
        assert self._process.stdout is not None

        try:
            while True:
                line = await self._process.stdout.readline()
                if not line:
                    break

                event = json.loads(line.decode("utf-8"))
                event_type = event.get("type")
                if event_type == "ready":
                    self._ready.set()
                    continue
                if event_type == "chunk":
                    if self._message_queue is not None:
                        await self._message_queue.put(event["payload"])
                    continue
                if event_type == "battle-ended":
                    self._active = False
                    self._battle_done.set()
                    continue
                if event_type == "error":
                    error = _LocalWorkerRuntimeError(
                        self._format_error(event.get("detail", "Unknown worker error"))
                    )
                    self._fatal_error = error
                    if self._message_queue is not None:
                        await self._message_queue.put(error)
                    self._battle_done.set()
                    self._ready.set()
                    continue

                error = _LocalWorkerRuntimeError(
                    self._format_error(f"Unexpected worker event: {event}")
                )
                self._fatal_error = error
                if self._message_queue is not None:
                    await self._message_queue.put(error)
                self._battle_done.set()
                self._ready.set()
                return
        finally:
            if self._fatal_error is None and self._process is not None:
                if self._process.returncode not in (None, 0):
                    self._fatal_error = _LocalWorkerRuntimeError(
                        self._format_error(
                            f"Local BattleStream worker exited with code {self._process.returncode}"
                        )
                    )
                elif self._active:
                    self._fatal_error = _LocalWorkerRuntimeError(
                        self._format_error(
                            "Local BattleStream worker exited while a battle was active"
                        )
                    )

            if self._fatal_error is not None and self._message_queue is not None:
                await self._message_queue.put(self._fatal_error)

            self._active = False
            self._battle_done.set()
            self._ready.set()

    async def _consume_stderr(self) -> None:
        assert self._process is not None
        assert self._process.stderr is not None

        while True:
            line = await self._process.stderr.readline()
            if not line:
                return
            self._stderr_tail.append(line.decode("utf-8", errors="replace").rstrip())

    async def _send_command(self, command: dict[str, object]) -> None:
        assert self._process is not None
        assert self._process.stdin is not None

        async with self._write_lock:
            self._process.stdin.write((json.dumps(command) + "\n").encode("utf-8"))
            await self._process.stdin.drain()

    def _format_error(self, detail: str) -> str:
        stderr = "\n".join(self._stderr_tail)
        if stderr:
            return f"{detail}\nLast stderr lines:\n{stderr}"
        return detail

    def _raise_failure(self, detail: str) -> NoReturn:
        raise ShowdownException(self._format_error(detail))


class _LocalBattleStreamWorkerPool:
    def __init__(self, config: LocalBattleStreamConfiguration):
        self._config = config
        self._workers: list[_LocalBattleStreamWorker] = []
        self._available: asyncio.Queue[_LocalBattleStreamWorker] = asyncio.Queue()
        self._started = False

    async def start(self) -> None:
        if self._started:
            return

        worker_count = max(1, self._config.worker_count)
        self._workers = [
            _LocalBattleStreamWorker(self._config, worker_index=index)
            for index in range(worker_count)
        ]
        for worker in self._workers:
            await worker.start()
            await self._available.put(worker)
        self._started = True

    async def acquire(self) -> _LocalBattleStreamWorker:
        await self.start()
        worker = await self._available.get()
        await worker.start()
        return worker

    async def release(self, worker: _LocalBattleStreamWorker) -> None:
        await self._available.put(worker)

    async def close(self) -> None:
        for worker in self._workers:
            await worker.shutdown()
        self._workers = []
        self._available = asyncio.Queue()
        self._started = False


async def _get_local_worker_pool(
    config: LocalBattleStreamConfiguration,
) -> _LocalBattleStreamWorkerPool:
    if config not in _LOCAL_WORKER_POOLS:
        _LOCAL_WORKER_POOLS[config] = _LocalBattleStreamWorkerPool(config)

    pool = _LOCAL_WORKER_POOLS[config]
    await pool.start()
    return pool


def _split_protocol_messages(payload: str) -> list[str]:
    return [message for message in payload.split("\n\n") if message]


def _split_update_for_players(payload: str) -> tuple[list[str], list[str]]:
    p1_lines: list[str] = []
    p2_lines: list[str] = []
    lines = payload.split("\n") if payload else []
    index = 0

    while index < len(lines):
        line = lines[index]
        if line == "|split|p1":
            secret = lines[index + 1] if index + 1 < len(lines) else ""
            shared = lines[index + 2] if index + 2 < len(lines) else ""
            if secret:
                p1_lines.append(secret)
            if shared:
                p2_lines.append(shared)
            index += 3
            continue
        if line == "|split|p2":
            secret = lines[index + 1] if index + 1 < len(lines) else ""
            shared = lines[index + 2] if index + 2 < len(lines) else ""
            if shared:
                p1_lines.append(shared)
            if secret:
                p2_lines.append(secret)
            index += 3
            continue

        p1_lines.append(line)
        p2_lines.append(line)
        index += 1

    return p1_lines, p2_lines


def _translate_showdown_command(message: str) -> tuple[str, str | None]:
    if message.startswith("/choose "):
        return "choice", message[len("/choose ") :]
    if message.startswith("/team "):
        return "choice", message[1:]
    if message == "/forfeit":
        return "forfeit", None
    if message.startswith("/timer "):
        return "ignore", None
    if message.startswith("/leave"):
        return "ignore", None
    if message in {"/acceptopenteamsheets", "/rejectopenteamsheets"}:
        return "ignore", None
    if message.startswith("/utm "):
        return "ignore", None

    raise ShowdownException(
        f"Unsupported message for LocalBattleStreamConfiguration: {message}"
    )


class LocalBattleStreamClient(PSClient):
    def __init__(
        self,
        account_configuration: AccountConfiguration,
        *,
        avatar: str | None = None,
        log_level: int | None = None,
        on_battle_message: BattleMessageCallback | None = None,
        on_update_challenges: ChallengeCallback | None = None,
        on_challenge_request: ChallengeCallback | None = None,
        server_configuration: LocalBattleStreamConfiguration,
        loop: asyncio.AbstractEventLoop,
    ):
        super().__init__(
            account_configuration=account_configuration,
            avatar=avatar,
            log_level=log_level,
            on_battle_message=on_battle_message,
            on_update_challenges=on_update_challenges,
            on_challenge_request=on_challenge_request,
            server_configuration=LocalhostServerConfiguration,
            start_listening=False,
            loop=loop,
        )
        self._local_server_configuration: LocalBattleStreamConfiguration = (
            server_configuration
        )
        self._room_sessions: dict[str, tuple[LocalBattleStreamSession, str]] = {}
        self.loop.call_soon_threadsafe(self.logged_in.set)

    def attach_battle(
        self, room: str, session: LocalBattleStreamSession, player_slot: str
    ) -> None:
        self._room_sessions[room] = (session, player_slot)

    def detach_battle(self, room: str) -> None:
        self._room_sessions.pop(room, None)

    async def dispatch_room_message(self, room: str, lines: list[str]) -> None:
        if not lines:
            return

        split_messages = [[f">{room}"]]
        split_messages.extend(line.split("|") for line in lines)

        try:
            if room not in self._battle_locks:
                self._battle_locks[room] = asyncio.Lock()
            async with self._battle_locks[room]:
                await self._handle_battle_message(split_messages)
            if "|deinit" in lines:
                self._battle_locks.pop(room, None)
        except asyncio.CancelledError as exception:
            self.logger.critical("CancelledError intercepted: %s", exception)
        except Exception:
            self.logger.exception(
                "Unhandled exception raised while handling local room message:\n>%s\n%s",
                room,
                "\n".join(lines),
            )
            raise

    async def accept_challenge(self, username: str, packed_team: str | None):
        raise ShowdownException(
            "Local BattleStream clients do not support server challenge workflows"
        )

    async def challenge(self, username: str, format_: str, packed_team: str | None):
        raise ShowdownException(
            "Local BattleStream clients do not support server challenge workflows"
        )

    async def listen(self):
        self.logged_in.set()

    async def search_ladder_game(self, format_: str, packed_team: str | None):
        raise ShowdownException(
            "Local BattleStream clients do not support ladder searches"
        )

    async def send_message(
        self, message: str, room: str = "", message_2: str | None = None
    ):
        if message_2 is not None:
            raise ShowdownException(
                "Local BattleStream clients do not support compound room messages"
            )

        action, _ = _translate_showdown_command(message)
        if action == "ignore":
            return

        if not room:
            raise ShowdownException(
                f"Local BattleStream message requires a battle room: {message}"
            )

        if room not in self._room_sessions:
            raise ShowdownException(
                f"No active local battle session found for room {room}"
            )

        session, player_slot = self._room_sessions[room]
        await session.send_player_message(player_slot, message)

    async def stop_listening(self):
        return None

    @property
    def local_server_configuration(self) -> LocalBattleStreamConfiguration:
        return self._local_server_configuration


class LocalBattleStreamSession:
    def __init__(self, player_1: Player, player_2: Player, battle_index: int):
        assert isinstance(player_1.ps_client, LocalBattleStreamClient)
        assert isinstance(player_2.ps_client, LocalBattleStreamClient)

        config = player_1.ps_client.local_server_configuration

        self._config: LocalBattleStreamConfiguration = config
        self._player_1: Player = player_1
        self._player_2: Player = player_2
        self._client_1: LocalBattleStreamClient = player_1.ps_client
        self._client_2: LocalBattleStreamClient = player_2.ps_client
        self._room: str = f"battle-{player_1.format}-{battle_index}"
        self._battle_started: bool = False
        self._pool: _LocalBattleStreamWorkerPool | None = None
        self._worker: _LocalBattleStreamWorker | None = None

    async def run(self) -> None:
        self._validate_configuration()
        self._attach_clients()
        try:
            packed_team_1 = self._player_1.get_next_team()
            packed_team_2 = self._player_2.get_next_team()

            self._pool = await _run_on_local_worker_loop(
                _get_local_worker_pool(self._config)
            )
            self._worker = await _run_on_local_worker_loop(self._pool.acquire())

            await _run_on_local_worker_loop(
                self._worker.start_battle(
                    [
                        ">start "
                        + json.dumps({"formatid": self._player_1.format}),
                        ">player p1 "
                        + json.dumps(
                            self._player_options(self._player_1, packed_team_1)
                        ),
                        ">player p2 "
                        + json.dumps(
                            self._player_options(self._player_2, packed_team_2)
                        ),
                    ]
                )
            )
            await self._consume_protocol_messages()
        finally:
            await self._finalize_battle()

    async def send_player_message(self, player_slot: str, message: str) -> None:
        action, translated = _translate_showdown_command(message)
        if action == "ignore":
            return
        if action == "forfeit":
            assert self._worker is not None
            await _run_on_local_worker_loop(
                self._worker.send_battle_line(f">forcelose {player_slot}")
            )
            return
        assert translated is not None
        assert self._worker is not None
        await _run_on_local_worker_loop(
            self._worker.send_battle_line(f">{player_slot} {translated}")
        )

    async def _consume_protocol_messages(self) -> None:
        assert self._worker is not None
        timeout = self._config.startup_timeout

        while True:
            message = await _run_on_local_worker_loop(
                self._worker.read_protocol_message(timeout)
            )
            if message is None:
                if self._battle_started:
                    return
                self._raise_failure(
                    "Local BattleStream process exited before producing battle output"
                )

            for protocol_message in _split_protocol_messages(message):
                if not self._battle_started:
                    await self._start_battle_room()
                    self._battle_started = True
                    timeout = self._config.event_timeout

                finished = await self._dispatch_protocol_message(protocol_message)
                if finished:
                    return

    async def _dispatch_protocol_message(self, message: str) -> bool:
        kind, _, payload = message.partition("\n")
        if kind == "update":
            p1_lines, p2_lines = _split_update_for_players(payload)
            await self._client_1.dispatch_room_message(self._room, p1_lines)
            await self._client_2.dispatch_room_message(self._room, p2_lines)
            return False
        if kind == "sideupdate":
            player_slot, _, body = payload.partition("\n")
            target_lines = body.split("\n") if body else []
            if player_slot == "p1":
                await self._client_1.dispatch_room_message(self._room, target_lines)
                return False
            if player_slot == "p2":
                await self._client_2.dispatch_room_message(self._room, target_lines)
                return False
            self._raise_failure(f"Unexpected sideupdate target: {player_slot}")
        if kind == "end":
            return True
        if kind == "requesteddata":
            return False

        self._raise_failure(f"Unexpected simulate-battle payload type: {kind}")

    async def _start_battle_room(self) -> None:
        init_lines = [
            "|init|battle",
            f"|title|{self._player_1.username} vs. {self._player_2.username}",
        ]
        await self._client_1.dispatch_room_message(self._room, init_lines)
        await self._client_2.dispatch_room_message(self._room, init_lines)

    async def _finalize_battle(self) -> None:
        if self._battle_started:
            deinit_lines = ["|deinit"]
            await self._client_1.dispatch_room_message(self._room, deinit_lines)
            await self._client_2.dispatch_room_message(self._room, deinit_lines)

        if self._worker is not None:
            await _run_on_local_worker_loop(self._worker.close_battle())
            if self._pool is not None:
                await _run_on_local_worker_loop(self._pool.release(self._worker))
            self._worker = None

        self._detach_clients()

    def _attach_clients(self) -> None:
        self._client_1.attach_battle(self._room, self, "p1")
        self._client_2.attach_battle(self._room, self, "p2")

    def _detach_clients(self) -> None:
        self._client_1.detach_battle(self._room)
        self._client_2.detach_battle(self._room)

    def _player_options(
        self, player: Player, packed_team: str | None
    ) -> dict[str, str | None]:
        payload: dict[str, str | None] = {"name": player.username}
        if packed_team is not None:
            payload["team"] = packed_team
        return payload

    def _validate_configuration(self) -> None:
        showdown_entrypoint = Path(self._config.showdown_dir) / "pokemon-showdown"
        if not showdown_entrypoint.exists():
            self._raise_failure(
                f"Could not find pokemon-showdown entrypoint at {showdown_entrypoint}"
            )
        if self._config.worker_count < 1:
            self._raise_failure(
                "worker_count must be at least 1 for LocalBattleStreamConfiguration"
            )
        if self._config.pool_mode != "single":
            self._raise_failure(
                "pool_mode must be 'single' for the initial local BattleStream implementation"
            )
        if self._config.protocol != "jsonl":
            self._raise_failure(
                "protocol must be 'jsonl' for the initial local BattleStream implementation"
            )

    def _raise_failure(self, detail: str) -> NoReturn:
        raise ShowdownException(detail)


async def run_local_battles(player_1: Player, player_2: Player, n_battles: int) -> None:
    if not isinstance(player_1.ps_client, LocalBattleStreamClient) or not isinstance(
        player_2.ps_client, LocalBattleStreamClient
    ):
        raise ShowdownException(
            "run_local_battles requires both players to use LocalBattleStreamConfiguration"
        )

    config_1 = player_1.ps_client.local_server_configuration
    config_2 = player_2.ps_client.local_server_configuration

    if player_1.ps_client.loop is not player_2.ps_client.loop:
        raise ShowdownException(
            "Local BattleStream battles require both players to share the same event loop"
        )
    if player_1.format != player_2.format:
        raise ShowdownException(
            "Local BattleStream battles require both players to use the same battle format"
        )
    if config_1 != config_2:
        raise ShowdownException(
            "Local BattleStream battles require both players to share the same local configuration"
        )

    for _ in range(n_battles):
        session = LocalBattleStreamSession(
            player_1, player_2, next(_LOCAL_BATTLE_COUNTER)
        )
        await session.run()
