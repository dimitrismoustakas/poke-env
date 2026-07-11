"""This module contains objects related to server configuration."""

from dataclasses import dataclass
from math import isfinite
from pathlib import Path
from typing import NamedTuple, Sequence


class ServerConfiguration(NamedTuple):
    """Server configuration object. Represented with a tuple with two entries: server url
    and authentication endpoint url."""

    websocket_url: str
    authentication_url: str


@dataclass(frozen=True)
class LocalBattleStreamConfiguration:
    """Configuration for direct local BattleStream execution.

    ``showdown_dir`` must point to a Pokemon Showdown checkout where
    ``npm install`` has completed. Each worker is a Node process that can host
    up to ``max_battles_per_worker`` simultaneous BattleStreams.

    ``runtime_loop_count`` shards workers across dedicated runtime loops when
    positive. The default of zero runs them on poke-env's shared loop.
    ``node_command`` can override the default ``node`` executable, for example
    when Node is not on ``PATH``.
    """

    showdown_dir: str | Path
    worker_count: int = 1
    max_battles_per_worker: int = 1
    startup_timeout: float = 10.0
    node_command: Sequence[str] | None = None
    runtime_loop_count: int = 0

    def __post_init__(self) -> None:
        if (
            isinstance(self.worker_count, bool)
            or not isinstance(self.worker_count, int)
            or self.worker_count < 1
        ):
            raise ValueError("worker_count must be a positive integer")
        if (
            isinstance(self.max_battles_per_worker, bool)
            or not isinstance(self.max_battles_per_worker, int)
            or self.max_battles_per_worker < 1
        ):
            raise ValueError("max_battles_per_worker must be a positive integer")
        if (
            isinstance(self.runtime_loop_count, bool)
            or not isinstance(self.runtime_loop_count, int)
            or self.runtime_loop_count < 0
        ):
            raise ValueError("runtime_loop_count must be a non-negative integer")
        if (
            isinstance(self.startup_timeout, bool)
            or not isinstance(self.startup_timeout, (int, float))
            or not isfinite(self.startup_timeout)
            or self.startup_timeout <= 0
        ):
            raise ValueError("startup_timeout must be positive")

        showdown_dir = Path(self.showdown_dir).expanduser().resolve()
        object.__setattr__(self, "showdown_dir", showdown_dir)
        if self.node_command is not None:
            if isinstance(self.node_command, (str, bytes)):
                raise ValueError("node_command must be a sequence of non-empty strings")
            node_command = tuple(self.node_command)
            if not node_command or any(
                not isinstance(part, str) or not part for part in node_command
            ):
                raise ValueError(
                    "node_command must contain at least one non-empty string"
                )
            object.__setattr__(self, "node_command", node_command)


LocalhostServerConfiguration = ServerConfiguration(
    "ws://localhost:8000/showdown/websocket",
    "https://play.pokemonshowdown.com/action.php?",
)
"""Server configuration with localhost and smogon's authentication endpoint."""

ShowdownServerConfiguration = ServerConfiguration(
    "wss://sim3.psim.us/showdown/websocket",
    "https://play.pokemonshowdown.com/action.php?",
)
"""Server configuration with smogon's server and authentication endpoint."""
