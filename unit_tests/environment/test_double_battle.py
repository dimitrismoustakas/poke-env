import copy
import pickle
from unittest.mock import MagicMock

import pytest

from poke_env.battle import DoubleBattle, Effect, Field, Move, Pokemon, PokemonType
from poke_env.player.battle_order import SkippedBattleOrder


def test_battle_request_parsing(example_doubles_request):
    logger = MagicMock()
    battle = DoubleBattle("tag", "username", logger, gen=8)

    battle.parse_request(example_doubles_request)
    assert len(battle.team) == 6

    pokemon_names = set(map(lambda pokemon: pokemon.species, battle.team.values()))
    assert "thundurus" in pokemon_names
    assert "raichualola" in pokemon_names
    assert "maractus" in pokemon_names
    assert "zamazentacrowned" in pokemon_names

    zamazenta = battle.get_pokemon("p1: Zamazenta")
    zamazenta_moves = zamazenta.moves
    assert (
        len(zamazenta_moves) == 4
        and "closecombat" in zamazenta_moves
        and "crunch" in zamazenta_moves
        and "psychicfangs" in zamazenta_moves
        and "behemothbash" in zamazenta_moves
    )


def test_battle_request_parsing_and_interactions(example_doubles_request):
    logger = MagicMock()
    battle = DoubleBattle("tag", "username", logger, gen=8)

    battle.parse_request(example_doubles_request)
    mr_rime, klinklang = battle.active_pokemon
    my_first_active, my_second_active, their_first_active, their_second_active = (
        battle.all_active_pokemons
    )
    assert my_first_active == mr_rime and my_second_active == klinklang
    assert their_first_active is None and their_second_active is None
    assert isinstance(mr_rime, Pokemon)
    assert isinstance(klinklang, Pokemon)
    assert battle.get_pokemon("p1: Nickname") == mr_rime
    assert battle.get_pokemon("p1: Klinklang") == klinklang

    assert set(battle.available_moves[0]) == set(
        battle.active_pokemon[0].moves.values()
    )
    assert set(battle.available_moves[1]) == set(
        battle.active_pokemon[1].moves.values()
    )

    assert len(battle.available_switches) == 2
    assert all(battle.can_dynamax)
    assert not any(battle.can_z_move)
    assert not any(battle.can_mega_evolve)
    assert not any(battle.trapped)
    assert not any(battle.force_switch)
    assert not any(battle.maybe_trapped)

    mr_rime.boosts = {
        "accuracy": -2,
        "atk": 1,
        "def": -6,
        "evasion": 4,
        "spa": -4,
        "spd": 2,
        "spe": 3,
    }
    klinklang.boosts = {
        "accuracy": -6,
        "atk": 6,
        "def": -1,
        "evasion": 1,
        "spa": 4,
        "spd": -3,
        "spe": 2,
    }

    battle.clear_all_boosts()

    cleared_boosts = {
        "accuracy": 0,
        "atk": 0,
        "def": 0,
        "evasion": 0,
        "spa": 0,
        "spd": 0,
        "spe": 0,
    }

    assert mr_rime.boosts == cleared_boosts
    assert klinklang.boosts == cleared_boosts

    assert battle.active_pokemon == [mr_rime, klinklang]
    battle.parse_message(["", "swap", "p1b: Klinklang", ""])
    assert battle.active_pokemon == [klinklang, mr_rime]

    battle.switch("p2a: Milotic", "Milotic, L50, F", "48/48")
    battle.switch("p2b: Tyranitar", "Tyranitar, L50, M", "48/48")

    milotic, tyranitar = battle.opponent_active_pokemon
    assert milotic.species == "milotic"
    assert tyranitar.species == "tyranitar"

    assert not battle.opponent_used_dynamax

    battle.logger = None
    pickle.loads(pickle.dumps(battle))


def test_check_heal_message_for_ability():
    logger = MagicMock()
    battle = DoubleBattle("tag", "username", logger, gen=8)
    battle.player_role = "p1"

    # Add two active opponent pokemon
    battle.parse_message(["", "switch", "p2a: Furret", "Furret, L50, F", "100/100"])
    battle.parse_message(["", "switch", "p2b: Sentret", "Sentret, L50, F", "100/100"])

    battle.parse_message(
        [
            "",
            "-heal",
            "p2a: Furret",
            "100/100",
            "[from] ability: Water Absorb",
            "[of] p2b: Sentret",
        ]
    )
    assert battle.opponent_team["p2: Furret"].ability == "waterabsorb"

    battle.parse_message(
        [
            "",
            "-heal",
            "p2b: Furret",
            "100/100",
            "[from] ability: Hospitality",
            "[of] p2a: Sentret",
        ]
    )
    assert battle.opponent_team["p2: Sentret"].ability == "hospitality"


def test_get_possible_showdown_targets(example_doubles_request):
    logger = MagicMock()
    battle = DoubleBattle("tag", "username", logger, gen=8)

    battle.parse_request(example_doubles_request)
    mr_rime, klinklang = battle.active_pokemon
    psychic = mr_rime.moves["psychic"]
    slackoff = mr_rime.moves["slackoff"]

    battle.switch("p2b: Tyranitar", "Tyranitar, L50, M", "48/48")
    assert battle.get_possible_showdown_targets(psychic, mr_rime) == [-2, 2]

    battle.switch("p2a: Milotic", "Milotic, L50, F", "48/48")
    assert battle.get_possible_showdown_targets(psychic, mr_rime) == [-2, 1, 2]
    assert battle.get_possible_showdown_targets(slackoff, mr_rime) == [0]
    assert battle.get_possible_showdown_targets(Move("outrage", gen=8), mr_rime) == [0]
    assert battle.get_possible_showdown_targets(psychic, mr_rime, dynamax=True) == [
        1,
        2,
    ]
    assert battle.get_possible_showdown_targets(slackoff, mr_rime, dynamax=True) == [0]

    # Override last request with terastarstorm for Mr. Rime
    terastarstorm = Move("terastarstorm", gen=9)
    battle._available_moves = [[terastarstorm], []]
    assert battle.get_possible_showdown_targets(terastarstorm, mr_rime) == [-2, 1, 2]
    mr_rime.terastallize("stellar")
    assert battle.get_possible_showdown_targets(terastarstorm, mr_rime) == [0]


def test_targetless_request_index_suppresses_static_move_targets(
    example_doubles_request,
):
    request = copy.deepcopy(example_doubles_request)
    request["active"][0]["moves"] = [{"id": "psychic"}]
    logger = MagicMock()
    battle = DoubleBattle("tag", "username", logger, gen=8)

    battle.parse_request(request)
    mr_rime = battle.active_pokemon[0]
    move = battle.available_moves[0][0]

    assert move.id == "psychic"
    assert move.request_index == 1
    assert move.request_target is None
    assert battle.get_possible_showdown_targets(move, mr_rime) == [0]
    assert "/choose move 1" in {order.message for order in battle.valid_orders[0]}


def test_to_showdown_target(example_doubles_request):
    logger = MagicMock()
    battle = DoubleBattle("tag", "username", logger, gen=8)

    battle.parse_request(example_doubles_request)
    mr_rime, klinklang = battle.active_pokemon
    opp1, opp2 = battle.opponent_active_pokemon
    psychic = mr_rime.moves["psychic"]
    slackoff = mr_rime.moves["slackoff"]
    dynamax_psychic = psychic.dynamaxed

    assert battle.to_showdown_target(psychic, klinklang) == -2
    assert battle.to_showdown_target(psychic, opp1) == 0
    assert battle.to_showdown_target(slackoff, mr_rime) == 0
    assert battle.to_showdown_target(slackoff, None) == 0
    assert battle.to_showdown_target(dynamax_psychic, klinklang) == -2


def test_end_illusion():
    logger = MagicMock()
    battle = DoubleBattle("tag", "username", logger, gen=8)
    battle.player_role = "p2"

    battle.switch("p2a: Celebi", "Celebi", "100/100")
    battle.switch("p2b: Ferrothorn", "Ferrothorn, M", "100/100")
    battle.switch("p1a: Pelipper", "Pelipper, F", "100/100")
    battle.switch("p1b: Kingdra", "Kingdra, F", "100/100")

    battle.end_illusion("p2a: Zoroark", "Zoroark, M")
    zoroark = battle.team["p2: Zoroark"]
    celebi = battle.team["p2: Celebi"]
    ferrothorn = battle.team["p2: Ferrothorn"]
    assert zoroark in battle.active_pokemon
    assert ferrothorn in battle.active_pokemon
    assert celebi not in battle.active_pokemon


def test_illusion_our_moves(example_doubles_zoroark_request):
    """
    This is basically the same test as in the single battle test,
    but this time by using nicknames for all our pokemon.
    """
    logger = MagicMock()
    battle = DoubleBattle("tag", "username", logger, gen=9)
    battle.player_role = "p1"

    battle.switch("p1a: Alien", "Deoxys-Defense, L84", "100/100")
    battle.switch("p1b: Plant", "Venusaur, L86, M", "100/100")
    battle.switch("p2a: Poliwrath", "Poliwrath, L88, F", "302/302")
    battle.switch("p2b: Politoed", "Politoed, L88, M", "302/302")

    assert battle.active_pokemon[0].species == "deoxysdefense"
    assert battle.active_pokemon[1].species == "venusaur"

    battle.parse_request(example_doubles_zoroark_request, strict_battle_tracking=True)
    assert battle.active_pokemon[0].species == "zoroarkhisui"
    assert battle.active_pokemon[1].species == "venusaur"

    battle.parse_message(["", "move", "p1a: Alien", "Focus Blast", "p2a: Poliwrath"])
    assert battle.active_pokemon[0].species == "zoroarkhisui"
    assert len(battle._team["p1: Alien"].moves) == 4
    assert "focusblast" not in battle._team["p1: Alien"].moves


def test_one_mon_left_in_double_battles_results_in_available_move_in_the_correct_slot():
    request = {
        "active": [
            {
                "moves": [
                    {
                        "move": "Ally Switch",
                        "id": "allyswitch",
                        "pp": 18,
                        "maxpp": 24,
                        "target": "self",
                        "disabled": False,
                    }
                ]
            },
            {
                "moves": [
                    {
                        "move": "Recover",
                        "id": "recover",
                        "pp": 4,
                        "maxpp": 8,
                        "target": "self",
                        "disabled": False,
                    },
                    {
                        "move": "Haze",
                        "id": "haze",
                        "pp": 46,
                        "maxpp": 48,
                        "target": "all",
                        "disabled": False,
                    },
                ]
            },
        ],
        "side": {
            "name": "DisplayPlayer 1",
            "id": "p1",
            "pokemon": [
                {
                    "ident": "p1: Cresselia",
                    "details": "Cresselia, F",
                    "condition": "0 fnt",
                    "active": True,
                    "stats": {
                        "atk": 145,
                        "def": 350,
                        "spa": 167,
                        "spd": 277,
                        "spe": 206,
                    },
                    "moves": ["allyswitch"],
                    "baseAbility": "levitate",
                    "item": "rockyhelmet",
                    "pokeball": "pokeball",
                    "ability": "levitate",
                    "commanding": False,
                    "reviving": False,
                    "teraType": "Psychic",
                    "terastallized": "",
                },
                {
                    "ident": "p1: Milotic",
                    "details": "Milotic, F",
                    "condition": "386/394",
                    "active": True,
                    "stats": {
                        "atk": 112,
                        "def": 194,
                        "spa": 236,
                        "spd": 383,
                        "spe": 199,
                    },
                    "moves": ["recover", "haze"],
                    "baseAbility": "marvelscale",
                    "item": "leftovers",
                    "pokeball": "pokeball",
                    "ability": "marvelscale",
                    "commanding": False,
                    "reviving": False,
                    "teraType": "Water",
                    "terastallized": "Water",
                },
            ],
        },
        "rqid": 16,
    }

    battle = DoubleBattle("tag", "username", MagicMock(), gen=9)
    battle.parse_message(["", "player", "p1", "username", "102", ""])
    battle.parse_message(["", "player", "p2", "username2", "102", ""])

    battle.parse_message(["", "switch", "p1a: Milotic", "Milotic, F", "394/394"])
    battle.parse_message(["", "switch", "p1b: Cresselia", "Cresselia, F", "444/444"])
    battle.parse_message(["", "switch", "p2a: Vaporeon", "Vaporeon, F", "100/100"])
    battle.parse_message(["", "switch", "p2b: Pelipper", "Pelipper, M", "100/100"])
    battle.parse_message(["", "turn", "1"])

    battle.parse_request(request)
    assert battle.last_request == request

    battle.parse_message(
        ["", "swap", "p1b: Cresselia", "0", "[from] move: Ally Switch"]
    )

    assert battle.available_moves[0] == []
    assert [m.id for m in battle.available_moves[1]] == ["recover", "haze"]
    assert battle.active_pokemon[0] is None
    assert battle.active_pokemon[1].species == "milotic"


def test_gen_and_format(example_doubles_logs):
    battle = DoubleBattle("tag", "username", MagicMock(), gen=8)
    battle.player_role = "p1"

    with pytest.raises(RuntimeError):
        for split_message in example_doubles_logs:
            if split_message[1] == "win":
                battle.won_by(split_message[2])
            elif split_message[1] == "tie":
                battle.tied()
            else:
                battle.parse_message(split_message)

    battle = DoubleBattle("tag", "username", MagicMock(), gen=6)
    battle.player_role = "p1"

    for split_message in example_doubles_logs:
        if split_message[1] == "win":
            battle.won_by(split_message[2])
        elif split_message[1] == "tie":
            battle.tied()
        else:
            battle.parse_message(split_message)

    assert battle.gen == 6
    assert battle.battle_tag == "tag"
    assert battle.format == "gen6doublesou"


def test_parse_message_fixture_matches_defensive_copy(example_doubles_logs):
    original_messages = copy.deepcopy(example_doubles_logs)
    direct_messages = copy.deepcopy(example_doubles_logs)
    defensive_messages = copy.deepcopy(example_doubles_logs)

    direct_battle = DoubleBattle("tag", "username", None, gen=6)
    defensive_battle = DoubleBattle("tag", "username", None, gen=6)
    direct_battle.player_role = "p1"
    defensive_battle.player_role = "p1"

    for direct_message, defensive_message in zip(
        direct_messages, defensive_messages, strict=True
    ):
        if direct_message[1] == "win":
            direct_battle.won_by(direct_message[2])
            defensive_battle.won_by(defensive_message[2])
        elif direct_message[1] == "tie":
            direct_battle.tied()
            defensive_battle.tied()
        else:
            direct_battle.parse_message(direct_message)
            defensive_battle.parse_message(defensive_message[:])

    assert direct_messages == original_messages
    assert defensive_messages == original_messages
    assert direct_battle._replay_data == original_messages
    assert defensive_battle._replay_data == original_messages
    assert pickle.dumps(direct_battle) == pickle.dumps(defensive_battle)

    direct_messages[0].append("caller mutation")
    assert direct_battle._replay_data[0] == original_messages[0]


def test_parse_message_copies_only_mutating_move_events():
    class CopyCountingMessage(list):
        def __init__(self, values):
            super().__init__(values)
            self.full_copy_count = 0

        def __getitem__(self, key):
            if key == slice(None, None, None):
                self.full_copy_count += 1
            return super().__getitem__(key)

    battle = DoubleBattle("tag", "username", None, gen=9)
    battle.player_role = "p1"
    battle.switch("p1a: Pikachu", "Pikachu, L50, F", "100/100")
    battle.switch("p1b: Raichu", "Raichu, L50, F", "100/100")
    battle.switch("p2a: Absol", "Absol, L50, F", "100/100")
    battle.switch("p2b: Eevee", "Eevee, L50, F", "100/100")

    damage_values = ["", "-damage", "p2a: Absol", "50/100"]
    damage_message = CopyCountingMessage(damage_values)
    battle.parse_message(damage_message)

    assert damage_message.full_copy_count == 1
    assert damage_message == damage_values
    assert battle._replay_data[-1] == damage_values
    assert battle.opponent_active_pokemon[0].current_hp == 50

    battle.active_pokemon[0]._add_move("sleeptalk")
    move_values = [
        "",
        "move",
        "p1a: Pikachu",
        "Tackle",
        "p2a: Absol",
        "[from] Sleep Talk",
    ]
    move_message = CopyCountingMessage(move_values)
    battle.parse_message(move_message)

    assert move_message.full_copy_count == 2
    assert move_message == move_values
    assert battle._replay_data[-1] == move_values
    assert "tackle" in battle.active_pokemon[0].moves


def test_pledge_moves():
    battle = DoubleBattle("tag", "username", MagicMock(), gen=8)
    battle.player_role = "p2"

    events = [
        ["", "switch", "p1a: Indeedee", "Indeedee-F, L50, F", "100/100"],
        ["", "switch", "p1b: Hatterene", "Hatterene, L50, F", "100/100"],
        ["", "switch", "p2a: Primarina", "Primarina, L50, F, shiny", "169/169"],
        ["", "switch", "p2b: Decidueye", "Decidueye-Hisui, L50, F, shiny", "171/171"],
        ["", ""],
        ["", "move", "p2b: Decidueye", "Grass Pledge", "p1a: Indeedee"],
        ["", "-waiting", "p2b: Decidueye", "p2a: Primarina"],
        [
            "",
            "move",
            "p2a: Primarina",
            "Water Pledge",
            "p1b: Hatterene",
            "[from]move: Grass Pledge",
        ],
        ["", "-combine"],
        ["", "-damage", "p1b: Hatterene", "0 fnt"],
        ["", "-sidestart", "p1: cloverspsyspamsep", "Grass Pledge"],
    ]

    for event in events:
        battle.parse_message(event)

    assert "grasspledge" not in battle.team["p2: Primarina"].moves
    assert "waterpledge" in battle.team["p2: Primarina"].moves


def test_is_grounded():
    battle = DoubleBattle("tag", "username", MagicMock(), gen=9)
    battle.player_role = "p1"
    furret = Pokemon(gen=9, species="furret")
    battle.team = {"p1: Furret": furret}

    battle.parse_message(["", "switch", "p1a: Furret", "Furret, L50, F", "100/100"])
    assert battle.grounded == [True, True]

    furret._type_2 = PokemonType.FLYING
    assert battle.grounded == [False, True]

    furret._type_2 = None
    furret._ability = "levitate"
    assert battle.grounded == [False, True]

    furret._ability = "frisk"
    assert battle.grounded == [True, True]

    furret._effects = {Effect.MAGNET_RISE: 1}
    assert battle.grounded == [False, True]

    furret._effects = {}
    furret.item = "airballoon"
    assert battle.grounded == [False, True]

    battle._fields = {Field.GRAVITY: 1}
    assert battle.grounded == [True, True]

    furret._ability = "levitate"
    assert battle.grounded == [True, True]

    battle._fields = {}
    assert battle.grounded == [False, True]

    furret.item = "ironball"
    assert battle.grounded == [True, True]
    assert battle.is_grounded(furret)


def test_pressure_tracking_targets_in_doubles():
    battle = DoubleBattle("tag", "username", MagicMock(), gen=9)
    battle.player_role = "p1"
    battle.switch("p1a: Charizard", "Charizard, L50, F", "100/100")
    battle.switch("p1b: Blastoise", "Blastoise, L50, F", "100/100")
    battle.switch("p2a: Absol", "Absol, L50, F", "100/100")
    battle.switch("p2b: Venusaur", "Venusaur, L50, F", "100/100")

    battle.get_pokemon("p2: Absol")._ability = "pressure"

    assert battle._pressure_on("p1a: Charizard", "Tackle", "p2a: Absol")
    assert not battle._pressure_on("p1a: Charizard", "Tackle", "p1b: Blastoise")
    assert battle._pressure_on("p1a: Charizard", "Surf", None)


def test_dondozo_tatsugiri():
    battle = DoubleBattle("tag", "username", MagicMock(), gen=9)
    battle.player_role = "p1"
    dozo = Pokemon(gen=9, species="dondozo")
    battle.team = {"p1: Dondozo": dozo}
    tatsu = Pokemon(gen=9, species="tatsugiri")
    tatsu._ability = "commander"
    battle.team["p1: Tatsugiri"] = tatsu

    battle.parse_message(["", "switch", "p1a: Dondozo", "Dondozo, L50, F", "100/100"])
    battle.parse_message(
        ["", "switch", "p1b: Tatsugiri", "Tatsugiri, L50, F", "100/100"]
    )
    battle.parse_message(
        ["", "-activate", "p1b: Tatsugiri", "ability: Commander", "[of] p1a: Dondozo"]
    )
    assert Effect.COMMANDER in tatsu.effects

    # Add a third back-pokemon so available_switches would normally be non-empty
    flamigo = Pokemon(gen=9, species="flamigo")
    battle.team["p1: Flamigo"] = flamigo

    # Parse a request with Commander active (only 1 active entry for Dondozo)
    battle.parse_request(
        {
            "active": [
                {
                    "moves": [
                        {
                            "move": "Wave Crash",
                            "id": "wavecrash",
                            "pp": 16,
                            "maxpp": 16,
                            "target": "normal",
                            "disabled": False,
                        }
                    ],
                    "trapped": True,
                },
                {
                    "moves": [
                        {
                            "move": "Draco Meteor",
                            "id": "dracometeor",
                            "pp": 8,
                            "maxpp": 8,
                            "target": "normal",
                            "disabled": False,
                        }
                    ],
                    "trapped": True,
                },
            ],
            "side": {
                "name": "username",
                "id": "p1",
                "pokemon": [
                    {
                        "ident": "p1: Dondozo",
                        "details": "Dondozo, L50, F",
                        "condition": "100/100",
                        "active": True,
                        "stats": {
                            "atk": 150,
                            "def": 135,
                            "spa": 65,
                            "spd": 85,
                            "spe": 55,
                        },
                        "moves": ["wavecrash"],
                        "baseAbility": "unaware",
                        "item": "",
                        "pokeball": "pokeball",
                        "ability": "unaware",
                        "commanding": False,
                        "reviving": False,
                        "teraType": "Water",
                        "terastallized": "",
                    },
                    {
                        "ident": "p1: Tatsugiri",
                        "details": "Tatsugiri, L50, M",
                        "condition": "100/100",
                        "active": True,
                        "stats": {
                            "atk": 40,
                            "def": 62,
                            "spa": 120,
                            "spd": 75,
                            "spe": 82,
                        },
                        "moves": ["muddywater", "dracometeor"],
                        "baseAbility": "commander",
                        "item": "",
                        "pokeball": "pokeball",
                        "ability": "commander",
                        "commanding": True,
                        "reviving": False,
                        "teraType": "Water",
                        "terastallized": "",
                    },
                    {
                        "ident": "p1: Flamigo",
                        "details": "Flamigo, L50, F",
                        "condition": "100/100",
                        "active": False,
                        "stats": {
                            "atk": 115,
                            "def": 74,
                            "spa": 75,
                            "spd": 64,
                            "spe": 90,
                        },
                        "moves": ["closecombat"],
                        "baseAbility": "costar",
                        "item": "",
                        "pokeball": "pokeball",
                        "ability": "costar",
                        "commanding": False,
                        "reviving": False,
                        "teraType": "Fighting",
                        "terastallized": "",
                    },
                ],
            },
            "rqid": 2,
        }
    )

    # Dondozo slot (0) should have moves and switches
    assert len(battle.available_moves[0]) == 1
    assert battle.available_switches[0] == []

    # Request-side commanding state suppresses Tatsugiri's choices.
    assert battle.available_moves[1] == []
    assert battle.available_switches[1] == []

    assert len(battle.valid_orders[1]) == 1
    assert isinstance(battle.valid_orders[1][0], SkippedBattleOrder)

    battle.parse_message(["", "faint", "p1a: Dondozo"])
    assert Effect.COMMANDER not in tatsu.effects


def test_doubles_request_maps_active_entries_by_field_slot_not_team_order():
    battle = DoubleBattle("tag", "username", MagicMock(), gen=9)
    battle.player_role = "p1"
    battle.parse_message(
        ["", "switch", "p1a: Tatsugiri", "Tatsugiri-Droopy, L50, F", "100/100"]
    )
    battle.parse_message(["", "switch", "p1b: Dondozo", "Dondozo, L50, F", "100/100"])

    battle.parse_request(
        {
            "active": [
                {
                    "moves": [
                        {
                            "move": "Protect",
                            "id": "protect",
                            "pp": 16,
                            "maxpp": 16,
                            "target": "self",
                            "disabled": False,
                        }
                    ],
                    "trapped": True,
                },
                {
                    "moves": [
                        {
                            "move": "Protect",
                            "id": "protect",
                            "pp": 16,
                            "maxpp": 16,
                            "target": "self",
                            "disabled": False,
                        }
                    ],
                    "trapped": True,
                },
            ],
            "side": {
                "name": "username",
                "id": "p1",
                "pokemon": [
                    {
                        "ident": "p1: Dondozo",
                        "details": "Dondozo, L50, F",
                        "condition": "100/100",
                        "active": True,
                        "stats": {
                            "atk": 150,
                            "def": 135,
                            "spa": 65,
                            "spd": 85,
                            "spe": 55,
                        },
                        "moves": ["protect"],
                        "baseAbility": "unaware",
                        "ability": "unaware",
                        "item": "",
                        "pokeball": "pokeball",
                    },
                    {
                        "ident": "p1: Tatsugiri",
                        "details": "Tatsugiri-Droopy, L50, F",
                        "condition": "100/100",
                        "active": True,
                        "stats": {
                            "atk": 40,
                            "def": 62,
                            "spa": 120,
                            "spd": 75,
                            "spe": 82,
                        },
                        "moves": ["protect"],
                        "baseAbility": "commander",
                        "ability": "commander",
                        "item": "",
                        "pokeball": "pokeball",
                    },
                ],
            },
            "rqid": 11,
        }
    )

    assert [pokemon.species for pokemon in battle.active_pokemon] == [
        "tatsugiridroopy",
        "dondozo",
    ]
    assert [move.id for move in battle.available_moves[0]] == ["protect"]
    assert [move.id for move in battle.available_moves[1]] == ["protect"]


def test_dondozo_tatsugiri_switch_out():
    battle = DoubleBattle("tag", "username", MagicMock(), gen=9)
    battle.player_role = "p1"
    dozo = Pokemon(gen=9, species="dondozo")
    battle.team = {"p1: Dondozo": dozo}
    tatsu = Pokemon(gen=9, species="tatsugiri")
    tatsu._ability = "commander"
    battle.team["p1: Tatsugiri"] = tatsu
    urshifu = Pokemon(gen=9, species="urshifu")
    battle.team["p1: Urshifu"] = urshifu

    # Start with Dondozo at p1a, Urshifu at p1b
    battle.parse_message(["", "switch", "p1a: Dondozo", "Dondozo, L50, M", "100/100"])
    battle.parse_message(["", "switch", "p1b: Urshifu", "Urshifu, L50, M", "100/100"])

    # Turn 1: Tatsugiri switches in at p1b, Commander activates
    battle.parse_message(
        ["", "switch", "p1b: Tatsugiri", "Tatsugiri, L50, M", "100/100"]
    )
    battle.parse_message(
        ["", "-activate", "p1b: Tatsugiri", "ability: Commander", "[of] p1a: Dondozo"]
    )
    assert Effect.COMMANDER in tatsu.effects

    # Dondozo switches out at p1a, replaced by Flutter Mane
    flutter = Pokemon(gen=9, species="fluttermane")
    battle.team["p1: Flutter Mane"] = flutter
    battle.parse_message(
        ["", "switch", "p1a: Flutter Mane", "Flutter Mane, L50", "100/100"]
    )

    # Commander effect should be cleared from Tatsugiri
    assert Effect.COMMANDER not in tatsu.effects


def test_symbiosis():
    battle = DoubleBattle("tag", "username", MagicMock(), gen=9)
    battle.player_role = "p1"
    furret = Pokemon(gen=9, species="furret")
    oranguru = Pokemon(gen=9, species="oranguru")
    oranguru._ability = "symbiosis"
    oranguru._item = "choiceband"
    battle.team = {"p1: Furret": furret, "p1: Oranguru": oranguru}

    # https://github.com/smogon/pokemon-showdown/blob/5eee23883897264657c5911d8b37f77472d5eecf/data/mods/gen6/abilities.ts#L99
    battle.parse_message(["", "switch", "p1a: Furret", "Furret, L50, F", "100/100"])
    battle.parse_message(["", "switch", "p1b: Oranguru", "Oranguru, L50, F", "100/100"])
    battle.parse_message(
        [
            "",
            "-activate",
            "p1b: Oranguru",
            "ability: Symbiosis",
            "Choice Band",
            "[of] p1a: Furret",
        ]
    )
    assert furret.item == "choiceband"
    assert oranguru.item is None
