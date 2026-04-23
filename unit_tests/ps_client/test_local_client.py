import asyncio

import pytest

from poke_env import AccountConfiguration, LocalBattleStreamConfiguration
from poke_env.concurrency import POKE_LOOP
from poke_env.exceptions import ShowdownException
from poke_env.ps_client.local_client import (
    LocalBattleStreamClient,
    _split_protocol_messages,
    _split_update_for_players,
    _translate_showdown_command,
)


class DummySession:
    def __init__(self):
        self.messages: list[tuple[str, str]] = []

    async def send_player_message(self, player_slot: str, message: str) -> None:
        self.messages.append((player_slot, message))


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


def test_split_protocol_messages():
    payload = "\n\n".join(
        [
            "update\n|turn|1",
            "sideupdate\np1\n|request|{}",
            "sideupdate\np2\n|request|{}",
            "end\n|win|Player 1",
        ]
    )

    assert _split_protocol_messages(payload) == [
        "update\n|turn|1",
        "sideupdate\np1\n|request|{}",
        "sideupdate\np2\n|request|{}",
        "end\n|win|Player 1",
    ]


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
