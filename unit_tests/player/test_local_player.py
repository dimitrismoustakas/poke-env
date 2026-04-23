from unittest.mock import AsyncMock, patch

import pytest

from poke_env import LocalBattleStreamConfiguration
from poke_env.battle import AbstractBattle
from poke_env.exceptions import ShowdownException
from poke_env.player import BattleOrder, Player


class LocalTestPlayer(Player):
    def choose_move(self, battle: AbstractBattle) -> BattleOrder:
        return self.choose_default_move()


@patch("poke_env.player.player.run_local_battles", new_callable=AsyncMock)
@pytest.mark.asyncio
async def test_battle_against_uses_local_runner_for_two_local_players(
    run_local_battles,
):
    config = LocalBattleStreamConfiguration("C:/showdown")
    player_1 = LocalTestPlayer(server_configuration=config)
    player_2 = LocalTestPlayer(server_configuration=config)

    await player_1._battle_against(player_2, n_battles=3)

    run_local_battles.assert_awaited_once_with(player_1, player_2, 3)


@pytest.mark.asyncio
async def test_battle_against_rejects_mixed_local_and_websocket_players():
    config = LocalBattleStreamConfiguration("C:/showdown")
    player_1 = LocalTestPlayer(server_configuration=config)
    player_2 = LocalTestPlayer(start_listening=False)

    with pytest.raises(
        ShowdownException, match="Cannot battle local and websocket-backed players"
    ):
        await player_1._battle_against(player_2, n_battles=1)
