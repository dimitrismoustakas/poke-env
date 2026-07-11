from pathlib import Path
from typing import Any

import pytest

from poke_env import LocalBattleStreamConfiguration


def test_local_battle_stream_configuration_normalizes_values(tmp_path: Path):
    config = LocalBattleStreamConfiguration(
        tmp_path / "nested" / "..",
        worker_count=2,
        max_battles_per_worker=3,
        startup_timeout=1.5,
        node_command=["node", "--no-warnings"],
        runtime_loop_count=1,
    )

    assert config.showdown_dir == tmp_path.resolve()
    assert config.node_command == ("node", "--no-warnings")


@pytest.mark.parametrize(
    ("kwargs", "message"),
    [
        ({"worker_count": 0}, "worker_count"),
        ({"worker_count": True}, "worker_count"),
        ({"worker_count": 1.5}, "worker_count"),
        ({"max_battles_per_worker": 0}, "max_battles_per_worker"),
        ({"max_battles_per_worker": True}, "max_battles_per_worker"),
        ({"runtime_loop_count": -1}, "runtime_loop_count"),
        ({"runtime_loop_count": True}, "runtime_loop_count"),
        ({"startup_timeout": 0}, "startup_timeout"),
        ({"startup_timeout": float("inf")}, "startup_timeout"),
        ({"startup_timeout": float("nan")}, "startup_timeout"),
        ({"startup_timeout": True}, "startup_timeout"),
        ({"node_command": []}, "node_command"),
        ({"node_command": "node"}, "node_command"),
        ({"node_command": ["node", ""]}, "node_command"),
    ],
)
def test_local_battle_stream_configuration_rejects_invalid_values(
    tmp_path: Path, kwargs: dict[str, Any], message: str
):
    with pytest.raises(ValueError, match=message):
        LocalBattleStreamConfiguration(tmp_path, **kwargs)
