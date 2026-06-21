from __future__ import annotations

import atexit
import asyncio
import json
from collections import deque
from concurrent.futures import Future
from dataclasses import dataclass, replace
from itertools import count
from pathlib import Path
from threading import Lock, Thread, current_thread
from typing import TYPE_CHECKING, Deque, NoReturn, Protocol

from poke_env.concurrency import POKE_LOOP
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
    LocalBattleStreamConfiguration, _LocalBattleStreamWorkerPoolProtocol
] = {}
_LOCAL_WORKER_POOLS_LOCK = Lock()
_LOCAL_CLIENT_REFCOUNTS: dict[LocalBattleStreamConfiguration, int] = {}
_LOCAL_CLIENT_REFCOUNTS_LOCK = Lock()


def _json_dumps(payload: object) -> str:
    return json.dumps(payload, separators=(",", ":"))


def _expand_worker_events(event: dict[str, object]) -> list[dict[str, object]]:
    if event.get("type") != "event-batch":
        return [event]

    events = event.get("events")
    if not isinstance(events, list):
        return [{"type": "error", "detail": f"Malformed worker event batch: {event}"}]
    return [child_event for child_event in events if isinstance(child_event, dict)]


def _flatten_protocol_messages(messages: list[object]) -> list[object]:
    flattened: list[object] = []
    for message in messages:
        if isinstance(message, dict) and message.get("type") == "protocol-batch":
            nested_messages = message.get("messages")
            if isinstance(nested_messages, list):
                flattened.extend(nested_messages)
                continue
        flattened.append(message)
    return flattened


class _LocalBattleControllerProtocol(Protocol):
    async def start_battle(self, lines: list[str]) -> None: ...  # noqa: E704

    async def send_battle_line(self, line: str) -> None: ...  # noqa: E704

    async def send_battle_lines(self, lines: list[str]) -> None: ...  # noqa: E704

    async def read_protocol_message(  # noqa: E704
        self, timeout: float | None
    ) -> object | None: ...  # noqa: E704

    async def close_battle(self) -> None: ...  # noqa: E704

    async def describe(self) -> str: ...  # noqa: E704


class _LocalBattleStreamWorkerPoolProtocol(Protocol):
    async def start(self) -> None: ...  # noqa: E704

    async def acquire(self) -> _LocalBattleControllerProtocol: ...  # noqa: E704

    async def release(  # noqa: E704
        self, controller: _LocalBattleControllerProtocol
    ) -> None: ...  # noqa: E704

    async def close(self) -> None: ...  # noqa: E704


class _LocalWorkerLifecycleProtocol(Protocol):
    async def start(self) -> None: ...  # noqa: E704

    async def shutdown(self) -> None: ...  # noqa: E704


class _AsyncPayloadMailbox:
    def __init__(self) -> None:
        self._ready_payloads: Deque[object | Exception] = deque()
        self._waiter: asyncio.Future[object | Exception] | None = None

    async def get(self, timeout: float | None) -> object | Exception:
        if self._ready_payloads:
            return self._ready_payloads.popleft()

        if self._waiter is not None and not self._waiter.done():
            raise ShowdownException("Local BattleStream mailbox already has a reader")

        waiter = asyncio.get_running_loop().create_future()
        self._waiter = waiter
        try:
            if timeout is None:
                return await waiter
            return await asyncio.wait_for(waiter, timeout=timeout)
        finally:
            if self._waiter is waiter:
                self._waiter = None

    def put(self, payload: object | Exception) -> None:
        waiter = self._waiter
        if waiter is not None and not waiter.done():
            self._waiter = None
            waiter.set_result(payload)
            return
        self._ready_payloads.append(payload)


@dataclass
class _SharedBattleState:
    message_queue: _AsyncPayloadMailbox
    battle_done: asyncio.Event
    active: bool = True


def _protocol_batch_payload(messages: list[object]) -> object | None:
    messages = _flatten_protocol_messages(messages)
    if not messages:
        return None
    if len(messages) == 1:
        return messages[0]
    return {"type": "protocol-batch", "messages": messages}


def _run_loop(loop: asyncio.AbstractEventLoop) -> None:
    asyncio.set_event_loop(loop)
    loop.run_forever()


async def _run_on_loop(coro, loop: asyncio.AbstractEventLoop):
    try:
        current_loop = asyncio.get_running_loop()
    except RuntimeError:
        current_loop = None

    if current_loop is loop:
        return await coro

    future = asyncio.run_coroutine_threadsafe(coro, loop)
    return await asyncio.wrap_future(future)


async def _run_on_local_worker_loop(coro):
    return await _run_on_loop(coro, POKE_LOOP)


def _retain_local_client_reference(config: LocalBattleStreamConfiguration) -> None:
    with _LOCAL_CLIENT_REFCOUNTS_LOCK:
        _LOCAL_CLIENT_REFCOUNTS[config] = _LOCAL_CLIENT_REFCOUNTS.get(config, 0) + 1


def _release_local_client_reference(config: LocalBattleStreamConfiguration) -> bool:
    with _LOCAL_CLIENT_REFCOUNTS_LOCK:
        remaining = _LOCAL_CLIENT_REFCOUNTS.get(config, 0) - 1
        if remaining > 0:
            _LOCAL_CLIENT_REFCOUNTS[config] = remaining
            return False
        _LOCAL_CLIENT_REFCOUNTS.pop(config, None)
        return True


async def _close_runtime_worker_pool(config: LocalBattleStreamConfiguration) -> None:
    with _LOCAL_WORKER_POOLS_LOCK:
        pool = _LOCAL_WORKER_POOLS.pop(config, None)
    if pool is not None:
        await pool.close()


async def _shutdown_local_worker_pools(
    loop: asyncio.AbstractEventLoop | None = None,
) -> None:
    del loop
    with _LOCAL_WORKER_POOLS_LOCK:
        pools = list(_LOCAL_WORKER_POOLS.values())
        _LOCAL_WORKER_POOLS.clear()

    for pool in pools:
        await pool.close()


async def _start_worker_group(workers: list[_LocalWorkerLifecycleProtocol]) -> None:
    try:
        await asyncio.gather(*(worker.start() for worker in workers))
    except Exception:
        await asyncio.gather(
            *(worker.shutdown() for worker in workers), return_exceptions=True
        )
        raise


def _stop_local_worker_pools() -> None:
    try:
        asyncio.run_coroutine_threadsafe(
            _shutdown_local_worker_pools(), POKE_LOOP
        ).result(timeout=5.0)
    except Exception:
        pass


atexit.register(_stop_local_worker_pools)


def _get_or_create_worker_pool(
    config: LocalBattleStreamConfiguration, create_pool
) -> _LocalBattleStreamWorkerPoolProtocol:
    with _LOCAL_WORKER_POOLS_LOCK:
        pool = _LOCAL_WORKER_POOLS.get(config)
        if pool is None:
            pool = create_pool()
            _LOCAL_WORKER_POOLS[config] = pool
        return pool


class _LocalWorkerRuntimeError(ShowdownException):
    pass


def _worker_script_path() -> Path:
    return Path(__file__).with_name("battle_stream_worker.js")


class _LocalBattleStreamWorker:
    def __init__(self, config: LocalBattleStreamConfiguration, worker_index: int):
        self._config = config
        self._worker_index = worker_index
        self._battle_id = f"worker-{worker_index}-battle"
        self._stderr_tail: Deque[str] = deque(maxlen=config.stderr_tail_lines)
        self._process: asyncio.subprocess.Process | None = None
        self._stdout_task: asyncio.Task[None] | None = None
        self._stderr_task: asyncio.Task[None] | None = None
        self._write_lock = asyncio.Lock()
        self._ready = asyncio.Event()
        self._battle_done = asyncio.Event()
        self._active = False
        self._message_queue: _AsyncPayloadMailbox | None = None
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
            await asyncio.wait_for(
                self._ready.wait(), timeout=self._config.startup_timeout
            )
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
            self._raise_failure(
                "Worker received a start command while a battle is active"
            )

        self._message_queue = _AsyncPayloadMailbox()
        self._battle_done = asyncio.Event()
        self._active = True
        await self._send_command(
            {"type": "start", "battleId": self._battle_id, "lines": lines}
        )

    async def send_battle_line(self, line: str) -> None:
        await self.send_battle_lines([line])

    async def send_battle_lines(self, lines: list[str]) -> None:
        if self._fatal_error is not None:
            raise self._fatal_error
        if not self._active:
            self._raise_failure(
                "Worker received a write command without an active battle"
            )
        if not lines:
            return
        await self._send_command(
            {"type": "write", "battleId": self._battle_id, "lines": lines}
        )

    async def read_protocol_message(self, timeout: float | None) -> object | None:
        if self._fatal_error is not None:
            raise self._fatal_error
        if self._message_queue is None:
            self._raise_failure("Worker has no active message queue")

        payload = await self._message_queue.get(timeout)
        if isinstance(payload, Exception):
            raise payload
        return payload

    async def close_battle(self) -> None:
        if self._process is None or self._process.returncode is not None:
            self._active = False
            return
        if not self._active:
            return

        await self._send_command({"type": "close-battle", "battleId": self._battle_id})
        try:
            await asyncio.wait_for(self._battle_done.wait(), timeout=1.0)
        except asyncio.TimeoutError:
            await self.shutdown()

    async def describe(self) -> str:
        process = self._process
        details = [
            f"worker_index={self._worker_index}",
            f"battle_id={self._battle_id}",
            f"pid={getattr(process, 'pid', None)}",
            f"returncode={getattr(process, 'returncode', None)}",
            f"active={self._active}",
            f"ready={self._ready.is_set()}",
            f"battle_done={self._battle_done.is_set()}",
            (
                f"fatal_error={self._fatal_error!s}"
                if self._fatal_error
                else "fatal_error=None"
            ),
        ]
        stderr = "\n".join(self._stderr_tail)
        if stderr:
            details.append(f"stderr_tail=\n{stderr}")
        return "; ".join(details)

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

                events = _expand_worker_events(json.loads(line))
                if not await self._handle_stdout_events(events):
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
                self._message_queue.put(self._fatal_error)

            self._active = False
            self._battle_done.set()
            self._ready.set()

    async def _handle_stdout_events(self, events: list[dict[str, object]]) -> bool:
        messages: list[object] = []
        for event in events:
            result = await self._handle_stdout_event(event)
            if result is False:
                return False
            if result is not None:
                messages.append(result)

        payload = _protocol_batch_payload(messages)
        if payload is not None and self._message_queue is not None:
            self._message_queue.put(payload)
        return True

    async def _handle_stdout_event(
        self, event: dict[str, object]
    ) -> object | bool | None:
        event_type = event.get("type")
        if event_type == "ready":
            self._ready.set()
            return None
        if event_type == "chunk":
            return event["payload"]
        if event_type == "protocol-batch":
            messages = event.get("messages")
            if isinstance(messages, list):
                return {"type": "protocol-batch", "messages": messages}
            error = _LocalWorkerRuntimeError(
                self._format_error(f"Malformed worker protocol batch: {event}")
            )
            self._fatal_error = error
            if self._message_queue is not None:
                self._message_queue.put(error)
            self._battle_done.set()
            self._ready.set()
            return False
        if event_type == "split-chunk":
            return {
                "type": "split-chunk",
                "p1_messages": event.get("p1_messages", event.get("p1Messages")),
                "p2_messages": event.get("p2_messages", event.get("p2Messages")),
                "p1_payload": event.get("p1Payload", ""),
                "p2_payload": event.get("p2Payload", ""),
            }
        if event_type == "side-chunk":
            return {
                "type": "side-chunk",
                "player": event.get("player"),
                "messages": event.get("messages"),
                "payload": event.get("payload", ""),
            }
        if event_type == "end":
            return {"type": "end", "payload": event.get("payload", "")}
        if event_type == "battle-ended":
            self._active = False
            self._battle_done.set()
            return None
        if event_type == "error":
            error = _LocalWorkerRuntimeError(
                self._format_error(event.get("detail", "Unknown worker error"))
            )
            self._fatal_error = error
            if self._message_queue is not None:
                self._message_queue.put(error)
            self._battle_done.set()
            self._ready.set()
            return None

        error = _LocalWorkerRuntimeError(
            self._format_error(f"Unexpected worker event: {event}")
        )
        self._fatal_error = error
        if self._message_queue is not None:
            self._message_queue.put(error)
        self._battle_done.set()
        self._ready.set()
        return False

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
            self._process.stdin.write((_json_dumps(command) + "\n").encode("utf-8"))
            await self._process.stdin.drain()

    def _format_error(self, detail: str) -> str:
        stderr = "\n".join(self._stderr_tail)
        if stderr:
            return f"{detail}\nLast stderr lines:\n{stderr}"
        return detail

    def _raise_failure(self, detail: str) -> NoReturn:
        raise ShowdownException(self._format_error(detail))


class _SharedLocalBattleStreamBattleHandle:
    def __init__(self, worker: _SharedLocalBattleStreamWorker, battle_id: str):
        self._worker = worker
        self._battle_id = battle_id

    async def start_battle(self, lines: list[str]) -> None:
        await self._worker.start_battle(self._battle_id, lines)

    async def send_battle_line(self, line: str) -> None:
        await self._worker.send_battle_line(self._battle_id, line)

    async def send_battle_lines(self, lines: list[str]) -> None:
        await self._worker.send_battle_lines(self._battle_id, lines)

    async def read_protocol_message(self, timeout: float | None) -> object | None:
        return await self._worker.read_protocol_message(self._battle_id, timeout)

    async def close_battle(self) -> None:
        await self._worker.close_battle(self._battle_id)

    async def describe(self) -> str:
        return await self._worker.describe_battle(self._battle_id)

    async def release(self) -> None:
        await self._worker.release_battle(self._battle_id)


class _SharedLocalBattleStreamWorker:
    def __init__(self, config: LocalBattleStreamConfiguration, worker_index: int):
        self._config = config
        self._worker_index = worker_index
        self._stderr_tail: Deque[str] = deque(maxlen=config.stderr_tail_lines)
        self._process: asyncio.subprocess.Process | None = None
        self._stdout_task: asyncio.Task[None] | None = None
        self._stderr_task: asyncio.Task[None] | None = None
        self._write_lock = asyncio.Lock()
        self._ready = asyncio.Event()
        self._battle_counter = count(1)
        self._battle_states: dict[str, _SharedBattleState] = {}
        self._fatal_error: ShowdownException | None = None

    @property
    def load(self) -> int:
        return len(self._battle_states)

    @property
    def has_capacity(self) -> bool:
        return self.load < self._config.max_battles_per_worker

    async def start(self) -> None:
        if self._process is not None and self._process.returncode is None:
            return

        self._ready = asyncio.Event()
        self._battle_states = {}
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
            await asyncio.wait_for(
                self._ready.wait(), timeout=self._config.startup_timeout
            )
        except asyncio.TimeoutError as exc:
            await self.shutdown()
            raise ShowdownException(
                "Local BattleStream worker did not become ready in time"
            ) from exc

    async def allocate_battle(self) -> _SharedLocalBattleStreamBattleHandle:
        await self.start()
        if self._fatal_error is not None:
            raise self._fatal_error
        if not self.has_capacity:
            self._raise_failure("Worker has no remaining battle capacity")

        battle_id = f"worker-{self._worker_index}-battle-{next(self._battle_counter)}"
        self._battle_states[battle_id] = _SharedBattleState(
            message_queue=_AsyncPayloadMailbox(), battle_done=asyncio.Event()
        )
        return _SharedLocalBattleStreamBattleHandle(self, battle_id)

    async def start_battle(self, battle_id: str, lines: list[str]) -> None:
        await self.start()
        if self._fatal_error is not None:
            raise self._fatal_error
        state = self._battle_state(battle_id)
        if not state.active:
            self._raise_failure(f"Battle {battle_id} is not active")
        await self._send_command(
            {"type": "start", "battleId": battle_id, "lines": lines}
        )

    async def send_battle_line(self, battle_id: str, line: str) -> None:
        await self.send_battle_lines(battle_id, [line])

    async def send_battle_lines(self, battle_id: str, lines: list[str]) -> None:
        if self._fatal_error is not None:
            raise self._fatal_error
        state = self._battle_state(battle_id)
        if not state.active:
            self._raise_failure(
                f"Worker received a write command without an active battle: {battle_id}"
            )
        if not lines:
            return
        await self._send_command(
            {"type": "write", "battleId": battle_id, "lines": lines}
        )

    async def read_protocol_message(
        self, battle_id: str, timeout: float | None
    ) -> object | None:
        if self._fatal_error is not None:
            raise self._fatal_error
        state = self._battle_state(battle_id)
        payload = await state.message_queue.get(timeout)
        if isinstance(payload, Exception):
            raise payload
        return payload

    async def close_battle(self, battle_id: str) -> None:
        if self._process is None or self._process.returncode is not None:
            return
        state = self._battle_states.get(battle_id)
        if state is None or not state.active:
            return

        await self._send_command({"type": "close-battle", "battleId": battle_id})
        try:
            await asyncio.wait_for(state.battle_done.wait(), timeout=1.0)
        except asyncio.TimeoutError:
            await self.shutdown()

    async def describe_battle(self, battle_id: str) -> str:
        process = self._process
        state = self._battle_states.get(battle_id)
        active_count = sum(
            1 for battle_state in self._battle_states.values() if battle_state.active
        )
        details = [
            f"worker_index={self._worker_index}",
            f"battle_id={battle_id}",
            f"pid={getattr(process, 'pid', None)}",
            f"returncode={getattr(process, 'returncode', None)}",
            f"battle_active={state.active if state is not None else None}",
            f"battle_done={state.battle_done.is_set() if state is not None else None}",
            f"active_battles={active_count}",
            f"tracked_battles={len(self._battle_states)}",
            f"capacity={self._config.max_battles_per_worker}",
            f"ready={self._ready.is_set()}",
            (
                f"fatal_error={self._fatal_error!s}"
                if self._fatal_error
                else "fatal_error=None"
            ),
        ]
        stderr = "\n".join(self._stderr_tail)
        if stderr:
            details.append(f"stderr_tail=\n{stderr}")
        return "; ".join(details)

    async def release_battle(self, battle_id: str) -> None:
        self._battle_states.pop(battle_id, None)

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
        self._battle_states = {}

    async def _consume_stdout(self) -> None:
        assert self._process is not None
        assert self._process.stdout is not None

        try:
            while True:
                line = await self._process.stdout.readline()
                if not line:
                    break

                events = _expand_worker_events(json.loads(line))
                if not await self._handle_stdout_events(events):
                    return
        finally:
            if self._fatal_error is None and self._process is not None:
                if self._process.returncode not in (None, 0):
                    self._fatal_error = _LocalWorkerRuntimeError(
                        self._format_error(
                            f"Local BattleStream worker exited with code {self._process.returncode}"
                        )
                    )
                elif self._battle_states:
                    self._fatal_error = _LocalWorkerRuntimeError(
                        self._format_error(
                            "Local BattleStream worker exited while battles were active"
                        )
                    )

            if self._fatal_error is not None:
                await self._broadcast_worker_error(self._fatal_error)

            self._ready.set()

    async def _handle_stdout_events(self, events: list[dict[str, object]]) -> bool:
        batched_messages: dict[str, list[object]] = {}
        for event in events:
            result = await self._handle_stdout_event(event)
            if result is False:
                return False
            if result is None:
                continue
            battle_id, message = result
            batched_messages.setdefault(battle_id, []).append(message)

        for battle_id, messages in batched_messages.items():
            state = self._battle_states.get(battle_id)
            payload = _protocol_batch_payload(messages)
            if state is not None and payload is not None:
                state.message_queue.put(payload)
        return True

    async def _handle_stdout_event(
        self, event: dict[str, object]
    ) -> tuple[str, object] | bool | None:
        event_type = event.get("type")
        battle_id = event.get("battleId")

        if event_type == "ready":
            self._ready.set()
            return None
        if event_type == "chunk":
            if battle_id is not None:
                return str(battle_id), event["payload"]
            return None
        if event_type == "protocol-batch":
            messages = event.get("messages")
            if battle_id is not None and isinstance(messages, list):
                return str(battle_id), {"type": "protocol-batch", "messages": messages}
            error = _LocalWorkerRuntimeError(
                self._format_error(f"Malformed worker protocol batch: {event}")
            )
            self._fatal_error = error
            await self._broadcast_worker_error(error)
            self._ready.set()
            return False
        if event_type == "split-chunk":
            if battle_id is not None:
                return str(battle_id), {
                    "type": "split-chunk",
                    "p1_messages": event.get("p1_messages", event.get("p1Messages")),
                    "p2_messages": event.get("p2_messages", event.get("p2Messages")),
                    "p1_payload": event.get("p1Payload", ""),
                    "p2_payload": event.get("p2Payload", ""),
                }
            return None
        if event_type == "side-chunk":
            if battle_id is not None:
                return str(battle_id), {
                    "type": "side-chunk",
                    "player": event.get("player"),
                    "messages": event.get("messages"),
                    "payload": event.get("payload", ""),
                }
            return None
        if event_type == "end":
            if battle_id is not None:
                return str(battle_id), {
                    "type": "end",
                    "payload": event.get("payload", ""),
                }
            return None
        if event_type == "battle-ended":
            state = self._battle_states.get(str(battle_id))
            if state is not None:
                state.active = False
                state.battle_done.set()
            return None
        if event_type == "error":
            error = _LocalWorkerRuntimeError(
                self._format_error(event.get("detail", "Unknown worker error"))
            )
            if battle_id is None:
                self._fatal_error = error
                await self._broadcast_worker_error(error)
                self._ready.set()
                return False

            state = self._battle_states.get(str(battle_id))
            if state is not None:
                state.active = False
                state.message_queue.put(error)
                state.battle_done.set()
            return None

        error = _LocalWorkerRuntimeError(
            self._format_error(f"Unexpected worker event: {event}")
        )
        self._fatal_error = error
        await self._broadcast_worker_error(error)
        self._ready.set()
        return False

    async def _broadcast_worker_error(self, error: ShowdownException) -> None:
        for state in self._battle_states.values():
            if state.active:
                state.active = False
            state.message_queue.put(error)
            state.battle_done.set()

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
            self._process.stdin.write((_json_dumps(command) + "\n").encode("utf-8"))
            await self._process.stdin.drain()

    def _battle_state(self, battle_id: str) -> _SharedBattleState:
        state = self._battle_states.get(battle_id)
        if state is None:
            self._raise_failure(f"Unknown battle id for worker: {battle_id}")
        return state

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
        self._start_lock = asyncio.Lock()

    async def start(self) -> None:
        async with self._start_lock:
            if self._started:
                return

            worker_count = max(1, self._config.worker_count)
            workers = [
                _LocalBattleStreamWorker(self._config, worker_index=index)
                for index in range(worker_count)
            ]
            available: asyncio.Queue[_LocalBattleStreamWorker] = asyncio.Queue()
            await _start_worker_group(workers)

            for worker in workers:
                await available.put(worker)

            self._workers = workers
            self._available = available
            self._started = True

    async def acquire(self) -> _LocalBattleStreamWorker:
        await self.start()
        worker = await self._available.get()
        await worker.start()
        return worker

    async def release(self, controller: _LocalBattleControllerProtocol) -> None:
        assert isinstance(controller, _LocalBattleStreamWorker)
        await self._available.put(controller)

    async def close(self) -> None:
        async with self._start_lock:
            for worker in self._workers:
                await worker.shutdown()
            self._workers = []
            self._available = asyncio.Queue()
            self._started = False


class _SharedLocalBattleStreamWorkerPool:
    def __init__(self, config: LocalBattleStreamConfiguration):
        self._config = config
        self._workers: list[_SharedLocalBattleStreamWorker] = []
        self._started = False
        self._start_lock = asyncio.Lock()
        self._available = asyncio.Condition()
        self._next_worker_index = 0

    async def start(self) -> None:
        async with self._start_lock:
            if self._started:
                return

            worker_count = max(1, self._config.worker_count)
            workers = [
                _SharedLocalBattleStreamWorker(self._config, worker_index=index)
                for index in range(worker_count)
            ]
            await _start_worker_group(workers)

            self._workers = workers
            self._started = True

    async def acquire(self) -> _SharedLocalBattleStreamBattleHandle:
        await self.start()
        async with self._available:
            while True:
                worker = self._select_worker()
                if worker is not None:
                    return await worker.allocate_battle()
                await self._available.wait()

    async def release(self, controller: _LocalBattleControllerProtocol) -> None:
        assert isinstance(controller, _SharedLocalBattleStreamBattleHandle)
        await controller.release()
        async with self._available:
            self._available.notify()

    async def close(self) -> None:
        async with self._start_lock:
            for worker in self._workers:
                await worker.shutdown()
            self._workers = []
            self._started = False
            self._next_worker_index = 0

    def _select_worker(self) -> _SharedLocalBattleStreamWorker | None:
        if not self._workers:
            return None

        selected_index: int | None = None
        selected_load: int | None = None
        selected_worker: _SharedLocalBattleStreamWorker | None = None
        for offset in range(len(self._workers)):
            index = (self._next_worker_index + offset) % len(self._workers)
            worker = self._workers[index]
            if not worker.has_capacity:
                continue
            if selected_load is None or worker.load < selected_load:
                selected_index = index
                selected_load = worker.load
                selected_worker = worker

        if selected_index is None:
            return None
        self._next_worker_index = (selected_index + 1) % len(self._workers)
        return selected_worker


class _CrossLoopLocalBattleController:
    def __init__(
        self,
        runtime_controller: _LocalBattleControllerProtocol,
        session_loop: asyncio.AbstractEventLoop,
        *,
        runtime_loop: asyncio.AbstractEventLoop = POKE_LOOP,
        runtime_timeout: float | None,
    ):
        self._runtime_controller = runtime_controller
        self._session_loop = session_loop
        self._runtime_loop = runtime_loop
        self._runtime_timeout = runtime_timeout
        self._ready_payloads: Deque[object | Exception] = deque()
        self._message_waiter: asyncio.Future[object | Exception] | None = None
        self._pump_future = None
        self._pending_payloads: Deque[object | Exception] = deque()
        self._pending_lock = Lock()
        self._flush_scheduled = False
        self._pending_battle_lines: list[str] = []
        self._pending_battle_line_waiters: list[asyncio.Future[None]] = []
        self._battle_line_flush_task: asyncio.Task[None] | None = None
        self._closed = False

    @property
    def runtime_controller(self) -> _LocalBattleControllerProtocol:
        return self._runtime_controller

    @property
    def runtime_loop(self) -> asyncio.AbstractEventLoop:
        return self._runtime_loop

    async def start_battle(self, lines: list[str]) -> None:
        await _run_on_loop(
            self._runtime_controller.start_battle(lines), self._runtime_loop
        )
        if self._pump_future is None or self._pump_future.done():
            self._pump_future = asyncio.run_coroutine_threadsafe(
                self._pump_protocol_messages(), self._runtime_loop
            )

    async def send_battle_line(self, line: str) -> None:
        await self.send_battle_lines([line])

    async def send_battle_lines(self, lines: list[str]) -> None:
        if not lines:
            return
        if self._closed:
            raise ShowdownException("Cannot send to a closed local battle")

        waiter = self._session_loop.create_future()
        self._pending_battle_lines.extend(lines)
        self._pending_battle_line_waiters.append(waiter)

        if self._battle_line_flush_task is None or self._battle_line_flush_task.done():
            self._battle_line_flush_task = asyncio.create_task(
                self._flush_battle_lines_soon()
            )

        await waiter

    async def read_protocol_message(self, timeout: float | None) -> object | None:
        if self._ready_payloads:
            payload = self._ready_payloads.popleft()
        else:
            waiter = self._session_loop.create_future()
            self._message_waiter = waiter
            try:
                payload = await asyncio.wait_for(waiter, timeout=timeout)
            finally:
                if self._message_waiter is waiter:
                    self._message_waiter = None
        if isinstance(payload, Exception):
            raise payload
        return payload

    async def close_battle(self) -> None:
        self._closed = True
        self._cancel_pending_messages()
        self._cancel_pending_battle_lines()
        await _run_on_loop(self._runtime_controller.close_battle(), self._runtime_loop)

    async def describe(self) -> str:
        try:
            return await _run_on_loop(
                self._runtime_controller.describe(), self._runtime_loop
            )
        except Exception as error:
            return f"failed_to_describe_runtime_controller={error!r}"

    def detach(self) -> None:
        self._closed = True
        self._cancel_pending_messages()
        self._cancel_pending_battle_lines()
        if self._pump_future is not None and not self._pump_future.done():
            self._pump_future.cancel()

    async def _flush_battle_lines_soon(self) -> None:
        waiters: list[asyncio.Future[None]] = []
        try:
            while self._pending_battle_lines:
                lines = self._pending_battle_lines
                waiters = self._pending_battle_line_waiters
                self._pending_battle_lines = []
                self._pending_battle_line_waiters = []

                try:
                    await _run_on_loop(
                        self._runtime_controller.send_battle_lines(lines),
                        self._runtime_loop,
                    )
                except Exception as error:
                    for waiter in waiters:
                        if not waiter.done():
                            waiter.set_exception(error)
                else:
                    for waiter in waiters:
                        if not waiter.done():
                            waiter.set_result(None)
                waiters = []
        except asyncio.CancelledError:
            error = ShowdownException(
                "Local battle closed before pending writes completed"
            )
            for waiter in waiters:
                if not waiter.done():
                    waiter.set_exception(error)
            for waiter in self._pending_battle_line_waiters:
                if not waiter.done():
                    waiter.set_exception(error)
            self._pending_battle_lines = []
            self._pending_battle_line_waiters = []
            raise

    def _cancel_pending_battle_lines(self) -> None:
        if (
            self._battle_line_flush_task is not None
            and not self._battle_line_flush_task.done()
        ):
            self._battle_line_flush_task.cancel()

        error = ShowdownException("Local battle closed before pending writes completed")
        for waiter in self._pending_battle_line_waiters:
            if not waiter.done():
                waiter.set_exception(error)
        self._pending_battle_lines = []
        self._pending_battle_line_waiters = []

    def _cancel_pending_messages(self) -> None:
        self._ready_payloads.clear()
        waiter = self._message_waiter
        self._message_waiter = None
        if waiter is not None and not waiter.done():
            waiter.set_exception(ShowdownException("Local battle closed"))

    async def _pump_protocol_messages(self) -> None:
        try:
            while True:
                try:
                    payload = await self._runtime_controller.read_protocol_message(
                        self._runtime_timeout
                    )
                except Exception as error:
                    self._post_to_session_loop(error)
                    return

                self._post_to_session_loop(payload)
                if self._is_terminal_payload(payload):
                    return
        except asyncio.CancelledError:
            return

    def _post_to_session_loop(self, payload: object | Exception) -> None:
        if self._closed:
            return

        try:
            should_schedule = False
            with self._pending_lock:
                if self._closed:
                    return
                self._pending_payloads.append(payload)
                if not self._flush_scheduled:
                    self._flush_scheduled = True
                    should_schedule = True
            if should_schedule:
                self._session_loop.call_soon_threadsafe(
                    self._flush_pending_to_session_loop
                )
        except RuntimeError:
            self._closed = True
            with self._pending_lock:
                self._pending_payloads.clear()
                self._flush_scheduled = False

    def _flush_pending_to_session_loop(self) -> None:
        while True:
            with self._pending_lock:
                if not self._pending_payloads:
                    self._flush_scheduled = False
                    return
                pending_payloads = self._pending_payloads
                self._pending_payloads = deque()

            if self._closed:
                with self._pending_lock:
                    self._pending_payloads.clear()
                    self._flush_scheduled = False
                return

            waiter = self._message_waiter
            if waiter is not None and not waiter.done() and pending_payloads:
                self._message_waiter = None
                waiter.set_result(pending_payloads.popleft())
            self._ready_payloads.extend(pending_payloads)

    @staticmethod
    def _is_terminal_payload(payload: object | Exception) -> bool:
        if payload is None or isinstance(payload, Exception):
            return True
        if isinstance(payload, dict):
            if payload.get("type") == "protocol-batch":
                messages = payload.get("messages")
                return isinstance(messages, list) and any(
                    _CrossLoopLocalBattleController._is_terminal_payload(message)
                    for message in messages
                )
            return payload.get("type") == "end"
        if isinstance(payload, str):
            return payload.partition("\n")[0] == "end"
        return False


def _runtime_worker_capacity(config: LocalBattleStreamConfiguration) -> int:
    worker_count = max(1, config.worker_count)
    if config.pool_mode == "shared":
        return worker_count * max(1, config.max_battles_per_worker)
    return worker_count


def _distribute_worker_counts(worker_count: int, shard_count: int) -> list[int]:
    worker_count = max(1, worker_count)
    shard_count = max(1, min(shard_count, worker_count))
    base_count, remainder = divmod(worker_count, shard_count)
    return [
        base_count + (1 if shard_index < remainder else 0)
        for shard_index in range(shard_count)
    ]


def _create_runtime_worker_pool(
    config: LocalBattleStreamConfiguration,
) -> _LocalBattleStreamWorkerPoolProtocol:
    if config.pool_mode == "shared":
        return _SharedLocalBattleStreamWorkerPool(config)
    return _LocalBattleStreamWorkerPool(config)


class _LocalRuntimeShard:
    def __init__(self, config: LocalBattleStreamConfiguration, shard_index: int):
        self._config = config
        self._shard_index = shard_index
        self._loop = asyncio.new_event_loop()
        self._thread = Thread(
            target=_run_loop,
            args=(self._loop,),
            name=f"poke-env-local-battlestream-{shard_index}",
            daemon=True,
        )
        self._thread.start()
        self._pool: _LocalBattleStreamWorkerPoolProtocol | None = None
        self._load = 0
        self._load_lock = Lock()

    @property
    def loop(self) -> asyncio.AbstractEventLoop:
        return self._loop

    @property
    def capacity(self) -> int:
        return _runtime_worker_capacity(self._config)

    @property
    def load(self) -> int:
        with self._load_lock:
            return self._load

    async def start(self) -> None:
        await self._run(self._start())

    async def acquire(self) -> _LocalBattleControllerProtocol:
        self._reserve()
        try:
            return await self._run(self._acquire())
        except Exception:
            self._release_reservation()
            raise

    async def release(self, controller: _LocalBattleControllerProtocol) -> None:
        try:
            await self._run(self._release(controller))
        finally:
            self._release_reservation()

    async def close(self) -> None:
        try:
            if self._loop.is_running():
                await self._run(self._close())
                await self._run(self._cancel_pending())
        finally:
            if self._loop.is_running():
                self._loop.call_soon_threadsafe(self._loop.stop)
            if self._thread is not current_thread():
                self._thread.join(timeout=5.0)
            if not self._loop.is_running():
                self._loop.close()

    def _reserve(self) -> None:
        with self._load_lock:
            self._load += 1

    def _release_reservation(self) -> None:
        with self._load_lock:
            self._load = max(0, self._load - 1)

    async def _run(self, coro):
        return await _run_on_loop(coro, self._loop)

    async def _start(self) -> None:
        if self._pool is None:
            self._pool = _create_runtime_worker_pool(self._config)
        await self._pool.start()

    async def _acquire(self) -> _LocalBattleControllerProtocol:
        if self._pool is None:
            await self._start()
        assert self._pool is not None
        return await self._pool.acquire()

    async def _release(self, controller: _LocalBattleControllerProtocol) -> None:
        if self._pool is None:
            return
        await self._pool.release(controller)

    async def _close(self) -> None:
        if self._pool is not None:
            await self._pool.close()
            self._pool = None

    async def _cancel_pending(self) -> None:
        current_task = asyncio.current_task()
        pending_tasks = [
            task for task in asyncio.all_tasks() if task is not current_task
        ]
        for task in pending_tasks:
            task.cancel()
        if pending_tasks:
            await asyncio.gather(*pending_tasks, return_exceptions=True)
        await asyncio.get_running_loop().shutdown_asyncgens()


class _ShardedLocalBattleStreamWorkerPool:
    def __init__(self, config: LocalBattleStreamConfiguration):
        worker_counts = _distribute_worker_counts(
            config.worker_count, config.runtime_loop_count
        )
        self._config = config
        self._shards = [
            _LocalRuntimeShard(
                replace(config, worker_count=worker_count, runtime_loop_count=0),
                shard_index=shard_index,
            )
            for shard_index, worker_count in enumerate(worker_counts)
        ]
        self._start_lock = Lock()
        self._start_future: Future[None] | None = None
        self._started = False
        self._next_shard_index = 0

    async def start(self) -> None:
        should_start = False
        with self._start_lock:
            if self._started:
                return
            if self._start_future is None:
                self._start_future = Future()
                should_start = True
            start_future = self._start_future

        if not should_start:
            await asyncio.wrap_future(start_future)
            return

        try:
            await asyncio.gather(*(shard.start() for shard in self._shards))
        except Exception as error:
            with self._start_lock:
                if not start_future.done():
                    start_future.set_exception(error)
                self._start_future = None
            await self.close()
            raise

        with self._start_lock:
            self._started = True
            if not start_future.done():
                start_future.set_result(None)

    async def acquire(self) -> _LocalBattleControllerProtocol:
        await self.start()
        session_loop = asyncio.get_running_loop()
        shard = self._select_shard()
        runtime_controller = await shard.acquire()
        return _CrossLoopLocalBattleController(
            runtime_controller,
            session_loop,
            runtime_loop=shard.loop,
            runtime_timeout=None,
        )

    async def release(self, controller: _LocalBattleControllerProtocol) -> None:
        runtime_controller = controller
        runtime_loop: asyncio.AbstractEventLoop | None = None
        if isinstance(controller, _CrossLoopLocalBattleController):
            controller.detach()
            runtime_controller = controller.runtime_controller
            runtime_loop = controller.runtime_loop

        shard = self._shard_for_loop(runtime_loop)
        await shard.release(runtime_controller)

    async def close(self) -> None:
        await asyncio.gather(
            *(shard.close() for shard in self._shards), return_exceptions=True
        )
        with self._start_lock:
            self._started = False
            self._start_future = None
            self._next_shard_index = 0

    def _select_shard(self) -> _LocalRuntimeShard:
        selected_index: int | None = None
        selected_load: int | None = None
        selected_shard: _LocalRuntimeShard | None = None
        selected_has_capacity = False
        for offset in range(len(self._shards)):
            index = (self._next_shard_index + offset) % len(self._shards)
            shard = self._shards[index]
            load = shard.load
            has_capacity = load < shard.capacity
            if (
                selected_load is None
                or (has_capacity and not selected_has_capacity)
                or (has_capacity == selected_has_capacity and load < selected_load)
            ):
                selected_index = index
                selected_load = load
                selected_shard = shard
                selected_has_capacity = has_capacity

        assert selected_index is not None
        assert selected_shard is not None
        self._next_shard_index = (selected_index + 1) % len(self._shards)
        return selected_shard

    def _shard_for_loop(
        self, runtime_loop: asyncio.AbstractEventLoop | None
    ) -> _LocalRuntimeShard:
        if runtime_loop is not None:
            for shard in self._shards:
                if shard.loop is runtime_loop:
                    return shard
        return self._select_shard()


class _CrossLoopLocalBattleStreamWorkerPool:
    def __init__(
        self,
        config: LocalBattleStreamConfiguration,
        session_loop: asyncio.AbstractEventLoop,
    ):
        self._config = config
        self._session_loop = session_loop
        self._runtime_pool: _LocalBattleStreamWorkerPoolProtocol | None = None

    async def start(self) -> None:
        self._runtime_pool = await _run_on_local_worker_loop(
            _get_runtime_worker_pool(self._config)
        )

    async def acquire(self) -> _LocalBattleControllerProtocol:
        if self._runtime_pool is None:
            await self.start()
        assert self._runtime_pool is not None
        runtime_controller = await _run_on_local_worker_loop(
            self._runtime_pool.acquire()
        )
        if self._session_loop is POKE_LOOP:
            return runtime_controller
        return _CrossLoopLocalBattleController(
            runtime_controller, self._session_loop, runtime_timeout=None
        )

    async def release(self, controller: _LocalBattleControllerProtocol) -> None:
        if self._runtime_pool is None:
            await self.start()
        assert self._runtime_pool is not None
        runtime_controller = controller
        if isinstance(controller, _CrossLoopLocalBattleController):
            controller.detach()
            runtime_controller = controller.runtime_controller
        await _run_on_local_worker_loop(self._runtime_pool.release(runtime_controller))

    async def close(self) -> None:
        return None


async def _get_runtime_worker_pool(
    config: LocalBattleStreamConfiguration,
) -> _LocalBattleStreamWorkerPoolProtocol:
    pool = _get_or_create_worker_pool(
        config, lambda: _create_runtime_worker_pool(config)
    )
    await pool.start()
    return pool


async def _get_local_worker_pool(
    config: LocalBattleStreamConfiguration,
) -> _LocalBattleStreamWorkerPoolProtocol:
    if config.runtime_loop_count > 0:
        pool = _get_or_create_worker_pool(
            config, lambda: _ShardedLocalBattleStreamWorkerPool(config)
        )
        await pool.start()
        return pool

    session_loop = asyncio.get_running_loop()
    if session_loop is POKE_LOOP:
        return await _get_runtime_worker_pool(config)

    pool = _CrossLoopLocalBattleStreamWorkerPool(config, session_loop)
    await pool.start()
    return pool


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


def _build_split_battle_messages(
    room: str, payload: str | list[str] | list[list[str]]
) -> tuple[list[list[str]], bool]:
    if isinstance(payload, str):
        if not payload:
            return [], False
        lines = payload.split("\n")
        has_deinit = "|deinit" in payload
    elif payload and isinstance(payload[0], list):
        split_messages = [[f">{room}"]]
        split_messages.extend(payload)
        has_deinit = any(
            len(split_message) > 1 and split_message[1] == "deinit"
            for split_message in payload
        )
        return split_messages, has_deinit
    else:
        if not payload:
            return [], False
        lines = payload
        has_deinit = "|deinit" in payload

    split_messages = [[f">{room}"]]
    split_messages.extend(line.split("|") for line in lines)
    return split_messages, has_deinit


def _format_payload_for_logging(payload: str | list[str] | list[list[str]]) -> str:
    if isinstance(payload, str):
        return payload
    if payload and isinstance(payload[0], list):
        return "\n".join("|".join(split_message) for split_message in payload)
    return "\n".join(payload)


def _payload_to_split_messages(
    payload: str | list[str] | list[list[str]] | object,
) -> list[list[str]]:
    if not payload:
        return []
    if isinstance(payload, str):
        return [line.split("|") for line in payload.split("\n")]
    if isinstance(payload, list) and payload and isinstance(payload[0], list):
        return payload
    if isinstance(payload, list):
        return [str(line).split("|") for line in payload]
    return [line.split("|") for line in str(payload).split("\n")]


def _payload_has_terminal_battle_message(
    payload: str | list[str] | list[list[str]] | object,
) -> bool:
    return _payload_terminal_battle_message(payload) is not None


def _payload_terminal_battle_message(
    payload: str | list[str] | list[list[str]] | object,
) -> list[str] | None:
    for split_message in _payload_to_split_messages(payload):
        if len(split_message) > 1 and split_message[1] in {"win", "tie"}:
            return split_message
    return None


def _ensure_terminal_battle_message(
    payload: list[list[str]],
    terminal_message: list[str] | None,
) -> None:
    if terminal_message is None or _payload_has_terminal_battle_message(payload):
        return
    payload.append(list(terminal_message))


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
        self._room_headers: dict[str, list[str]] = {}
        self._stopped = False
        _retain_local_client_reference(server_configuration)
        self.loop.call_soon_threadsafe(self.logged_in.set)

    def attach_battle(
        self, room: str, session: LocalBattleStreamSession, player_slot: str
    ) -> None:
        self._room_sessions[room] = (session, player_slot)
        self._room_headers[room] = [f">{room}"]

    def detach_battle(self, room: str) -> None:
        self._room_sessions.pop(room, None)
        self._room_headers.pop(room, None)

    async def dispatch_room_message(
        self, room: str, payload: str | list[str] | list[list[str]]
    ) -> None:
        split_messages, has_deinit = _build_split_battle_messages(room, payload)
        await self._dispatch_prepared_room_messages(
            room, split_messages, has_deinit, payload
        )

    async def _dispatch_split_room_messages(
        self, room: str, payload: list[list[str]]
    ) -> None:
        if not payload:
            return

        split_messages = [self._room_headers.get(room) or [f">{room}"]]
        split_messages.extend(payload)
        await self._dispatch_prepared_room_messages(
            room, split_messages, has_deinit=False, payload=payload
        )

    async def _dispatch_prepared_room_messages(
        self,
        room: str,
        split_messages: list[list[str]],
        has_deinit: bool,
        payload: str | list[str] | list[list[str]],
    ) -> None:
        if not split_messages:
            return

        try:
            lock = self._battle_locks.get(room)
            if lock is None:
                lock = asyncio.Lock()
                self._battle_locks[room] = lock
            async with lock:
                await self._handle_battle_message(split_messages)
            if has_deinit:
                self._battle_locks.pop(room, None)
        except asyncio.CancelledError as exception:
            self.logger.critical("CancelledError intercepted: %s", exception)
        except Exception:
            self.logger.exception(
                "Unhandled exception raised while handling local room message:\n>%s\n%s",
                room,
                _format_payload_for_logging(payload),
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
        if self._stopped:
            return None

        self._stopped = True
        self._room_sessions.clear()
        self._room_headers.clear()
        if _release_local_client_reference(self._local_server_configuration):
            await _run_on_local_worker_loop(
                _close_runtime_worker_pool(self._local_server_configuration)
            )
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
        self._accepting_player_messages: bool = True
        self._pool: _LocalBattleStreamWorkerPoolProtocol | None = None
        self._worker: _LocalBattleControllerProtocol | None = None

    async def run(self) -> None:
        self._validate_configuration()
        self._attach_clients()
        try:
            packed_team_1 = self._player_1.get_next_team()
            packed_team_2 = self._player_2.get_next_team()

            self._pool = await _get_local_worker_pool(self._config)
            self._worker = await self._pool.acquire()

            await self._worker.start_battle(
                [
                    ">start " + _json_dumps({"formatid": self._player_1.format}),
                    ">player p1 "
                    + _json_dumps(self._player_options(self._player_1, packed_team_1)),
                    ">player p2 "
                    + _json_dumps(self._player_options(self._player_2, packed_team_2)),
                ]
            )
            await self._consume_protocol_messages()
        finally:
            await self._finalize_battle()

    async def send_player_message(self, player_slot: str, message: str) -> None:
        if not self._accepting_player_messages:
            return
        action, translated = _translate_showdown_command(message)
        if action == "ignore":
            return
        if action == "forfeit":
            self._accepting_player_messages = False
            assert self._worker is not None
            await self._worker.send_battle_line(f">forcelose {player_slot}")
            return
        assert translated is not None
        assert self._worker is not None
        await self._worker.send_battle_line(f">{player_slot} {translated}")

    async def _consume_protocol_messages(self) -> None:
        assert self._worker is not None
        timeout: float | None = self._config.startup_timeout

        while True:
            try:
                message = await self._worker.read_protocol_message(timeout)
            except asyncio.TimeoutError as exc:
                if self._battle_started:
                    raise
                diagnostics = await self._describe_worker()
                raise ShowdownException(
                    "Timed out waiting for initial local BattleStream output "
                    f"for room {self._room} after {timeout:.1f}s. {diagnostics}"
                ) from exc
            if message is None:
                if self._battle_started:
                    return
                self._raise_failure(
                    "Local BattleStream process exited before producing battle output"
                )

            if not self._battle_started:
                await self._start_battle_room()
                self._battle_started = True
                timeout = None

            finished = await self._dispatch_protocol_payload(message)
            if finished:
                return

    async def _dispatch_protocol_payload(
        self, message: str | dict[str, object]
    ) -> bool:
        if isinstance(message, dict) and message.get("type") == "protocol-batch":
            messages = message.get("messages")
            if not isinstance(messages, list):
                self._raise_failure(f"Malformed local protocol batch: {message}")
            return await self._dispatch_protocol_batch(messages)
        return await self._dispatch_protocol_message(message)

    async def _dispatch_protocol_batch(self, messages: list[object]) -> bool:
        p1_messages: list[list[str]] = []
        p2_messages: list[list[str]] = []
        pending_terminal = False

        async def flush_player_messages() -> bool:
            nonlocal p1_messages, p2_messages, pending_terminal
            if p1_messages or p2_messages:
                if pending_terminal:
                    terminal_message = _payload_terminal_battle_message(
                        p1_messages
                    ) or _payload_terminal_battle_message(p2_messages)
                    _ensure_terminal_battle_message(p1_messages, terminal_message)
                    _ensure_terminal_battle_message(p2_messages, terminal_message)
                terminal = await self._dispatch_player_payloads(
                    p1_messages, p2_messages, terminal=pending_terminal
                )
                p1_messages = []
                p2_messages = []
                pending_terminal = False
                return terminal
            pending_terminal = False
            return False

        for child_message in messages:
            if not isinstance(child_message, (str, dict)):
                self._raise_failure(
                    f"Malformed local protocol batch message: {child_message}"
                )

            if isinstance(child_message, str):
                if await flush_player_messages():
                    return True
                if await self._dispatch_protocol_message(child_message):
                    return True
                continue

            event_type = child_message.get("type")
            if event_type == "split-chunk":
                p1_payload = child_message.get("p1_messages") or str(
                    child_message.get("p1_payload", "")
                )
                p2_payload = child_message.get("p2_messages") or str(
                    child_message.get("p2_payload", "")
                )
                p1_messages.extend(_payload_to_split_messages(p1_payload))
                p2_messages.extend(_payload_to_split_messages(p2_payload))
                pending_terminal = pending_terminal or (
                    _payload_has_terminal_battle_message(p1_payload)
                    or _payload_has_terminal_battle_message(p2_payload)
                )
                continue
            if event_type == "side-chunk":
                player_slot = str(child_message.get("player", ""))
                payload = child_message.get("messages") or str(
                    child_message.get("payload", "")
                )
                if player_slot == "p1":
                    p1_messages.extend(_payload_to_split_messages(payload))
                    pending_terminal = pending_terminal or (
                        _payload_has_terminal_battle_message(payload)
                    )
                    continue
                if player_slot == "p2":
                    p2_messages.extend(_payload_to_split_messages(payload))
                    pending_terminal = pending_terminal or (
                        _payload_has_terminal_battle_message(payload)
                    )
                    continue
                self._raise_failure(f"Unexpected side-chunk target: {player_slot}")
            if event_type == "end":
                if await flush_player_messages():
                    return True
                self._accepting_player_messages = False
                return True
            if event_type == "requesteddata":
                continue

            if await flush_player_messages():
                return True
            if await self._dispatch_protocol_message(child_message):
                return True

        return await flush_player_messages()

    async def _dispatch_protocol_message(
        self, message: str | dict[str, object]
    ) -> bool:
        if isinstance(message, dict):
            event_type = message.get("type")
            if event_type == "split-chunk":
                p1_payload = message.get("p1_messages") or str(
                    message.get("p1_payload", "")
                )
                p2_payload = message.get("p2_messages") or str(
                    message.get("p2_payload", "")
                )
                return await self._dispatch_player_payloads(p1_payload, p2_payload)
            if event_type == "side-chunk":
                player_slot = str(message.get("player", ""))
                payload = message.get("messages") or str(message.get("payload", ""))
                if player_slot == "p1":
                    await self._client_1.dispatch_room_message(self._room, payload)
                    return self._stop_after_terminal_payload(payload)
                if player_slot == "p2":
                    await self._client_2.dispatch_room_message(self._room, payload)
                    return self._stop_after_terminal_payload(payload)
                self._raise_failure(f"Unexpected side-chunk target: {player_slot}")
            if event_type == "end":
                self._accepting_player_messages = False
                return True
            self._raise_failure(f"Unexpected local worker payload type: {event_type}")

        kind, _, payload = message.partition("\n")
        if kind == "update":
            p1_lines, p2_lines = _split_update_for_players(payload)
            return await self._dispatch_player_payloads(p1_lines, p2_lines)
        if kind == "sideupdate":
            player_slot, _, body = payload.partition("\n")
            if player_slot == "p1":
                await self._client_1.dispatch_room_message(self._room, body)
                return self._stop_after_terminal_payload(body)
            if player_slot == "p2":
                await self._client_2.dispatch_room_message(self._room, body)
                return self._stop_after_terminal_payload(body)
            self._raise_failure(f"Unexpected sideupdate target: {player_slot}")
        if kind == "end":
            self._accepting_player_messages = False
            return True
        if kind == "requesteddata":
            return False

        self._raise_failure(f"Unexpected simulate-battle payload type: {kind}")

    async def _start_battle_room(self) -> None:
        init_lines = [
            "|init|battle",
            f"|title|{self._player_1.username} vs. {self._player_2.username}",
        ]
        await self._dispatch_player_payloads(init_lines, init_lines)

    async def _describe_worker(self) -> str:
        if self._worker is None:
            return "worker=None"
        try:
            return await self._worker.describe()
        except Exception as error:
            return f"failed_to_describe_worker={error!r}"

    async def _finalize_battle(self) -> None:
        self._accepting_player_messages = False
        if self._battle_started:
            deinit_lines = ["|deinit"]
            await self._dispatch_player_payloads(deinit_lines, deinit_lines)

        if self._worker is not None:
            await self._worker.close_battle()
            if self._pool is not None:
                await self._pool.release(self._worker)
            self._worker = None

        self._detach_clients()

    def _attach_clients(self) -> None:
        self._client_1.attach_battle(self._room, self, "p1")
        self._client_2.attach_battle(self._room, self, "p2")

    def _detach_clients(self) -> None:
        self._client_1.detach_battle(self._room)
        self._client_2.detach_battle(self._room)

    async def _dispatch_player_payloads(
        self,
        p1_payload: str | list[str] | list[list[str]],
        p2_payload: str | list[str] | list[list[str]],
        *,
        terminal: bool | None = None,
    ) -> bool:
        dispatches = []
        if p1_payload:
            dispatches.append(self._dispatch_player_payload(self._client_1, p1_payload))
        if p2_payload:
            dispatches.append(self._dispatch_player_payload(self._client_2, p2_payload))
        if dispatches:
            await asyncio.gather(*dispatches)
        if terminal is not None:
            if terminal:
                self._accepting_player_messages = False
            return terminal
        return self._stop_after_terminal_payloads(p1_payload, p2_payload)

    def _stop_after_terminal_payloads(
        self,
        *payloads: str | list[str] | list[list[str]] | object,
    ) -> bool:
        if any(_payload_has_terminal_battle_message(payload) for payload in payloads):
            self._accepting_player_messages = False
            return True
        return False

    def _stop_after_terminal_payload(
        self,
        payload: str | list[str] | list[list[str]] | object,
    ) -> bool:
        return self._stop_after_terminal_payloads(payload)

    def _dispatch_player_payload(
        self,
        client: LocalBattleStreamClient,
        payload: str | list[str] | list[list[str]],
    ):
        if (
            isinstance(client, LocalBattleStreamClient)
            and isinstance(payload, list)
            and payload
            and isinstance(payload[0], list)
        ):
            return client._dispatch_split_room_messages(self._room, payload)
        return client.dispatch_room_message(self._room, payload)

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
        if self._config.max_battles_per_worker < 1:
            self._raise_failure(
                "max_battles_per_worker must be at least 1 for LocalBattleStreamConfiguration"
            )
        if self._config.runtime_loop_count < 0:
            self._raise_failure(
                "runtime_loop_count must be non-negative for LocalBattleStreamConfiguration"
            )
        if self._config.pool_mode not in {"single", "shared"}:
            self._raise_failure(
                "pool_mode must be either 'single' or 'shared' for LocalBattleStreamConfiguration"
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
