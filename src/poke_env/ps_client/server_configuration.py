"""This module contains objects related to server configuration."""

from dataclasses import dataclass
from pathlib import Path
from typing import NamedTuple, Sequence


class ServerConfiguration(NamedTuple):
    """Server configuration object. Represented with a tuple with two entries: server url
    and authentication endpoint url."""

    websocket_url: str
    authentication_url: str


@dataclass(frozen=True)
class LocalBattleStreamConfiguration:
    """Configuration for direct local BattleStream execution."""

    showdown_dir: str | Path
    worker_count: int = 1
    startup_timeout: float = 10.0
    event_timeout: float = 30.0
    protocol: str = "jsonl"
    stderr_tail_lines: int = 100
    node_command: Sequence[str] | None = None
    pool_mode: str = "single"

    def __post_init__(self):
        object.__setattr__(self, "showdown_dir", Path(self.showdown_dir))


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
