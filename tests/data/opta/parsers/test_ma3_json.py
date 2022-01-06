import os
from datetime import datetime

from pytest import fixture

from socceraction.data.opta.parsers import MA3JSONParser


@fixture()
def ma3json_parser() -> MA3JSONParser:
    path = os.path.join(
        os.path.dirname(__file__),
        os.pardir,
        os.pardir,
        os.pardir,
        "datasets",
        "opta",
        "ma3_bl2020-21-0000000066.json",
    )
    return MA3JSONParser(str(path))


def test_extract_competitions(ma3json_parser: MA3JSONParser) -> None:
    competitions = ma3json_parser.extract_competitions()
    assert len(competitions) == 1
    assert competitions["7u6i088r32wrl84442qxr0gh6"] == {
        "competition_id": "722fdbecxzcq9788l6jqclzlw",
        "season_id": "7u6i088r32wrl84442qxr0gh6",
        "competition_name": "2. Bundesliga",
        "season_name": "2020/2021",
    }


def test_extract_games(ma3json_parser: MA3JSONParser) -> None:
    games = ma3json_parser.extract_games()
    assert len(games) == 1
    assert games["bl2020-21-0000000066"] == {
        "game_id": "bl2020-21-0000000066",
        "season_id": "7u6i088r32wrl84442qxr0gh6",
        "competition_id": "722fdbecxzcq9788l6jqclzlw",
        "game_day": 8,
        "game_date": datetime(2020, 11, 21, 13, 00),
        "home_team_id": "kxpw3rqn4ukt7nqmtjj62lbn",
        "away_team_id": "aojwbjr39s1w2mcd9l2bf2dhk",
        "home_score": 2,
        "away_score": 2,
        "duration": 93,
        "venue": "Wildparkstadion",
    }


def test_extract_teams(ma3json_parser: MA3JSONParser) -> None:
    teams = ma3json_parser.extract_teams()
    assert len(teams) == 2
    assert teams["kxpw3rqn4ukt7nqmtjj62lbn"] == {
        "team_id": "kxpw3rqn4ukt7nqmtjj62lbn",
        "team_name": "Eintracht Braunschweig",
    }
    assert teams["aojwbjr39s1w2mcd9l2bf2dhk"] == {
        "team_id": "aojwbjr39s1w2mcd9l2bf2dhk",
        "team_name": "Karlsruher SC",
    }


def test_extract_players(ma3json_parser: MA3JSONParser) -> None:
    players = ma3json_parser.extract_players()
    assert len(players) == 28
    assert players[("bl2020-21-0000000066", "yuw4a34cpasw5e4vqsg6ex1x")] == {
        "game_id": "bl2020-21-0000000066",
        "player_id": "yuw4a34cpasw5e4vqsg6ex1x",
        "player_name": "D. Diamantakos",
        "team_id": "aojwbjr39s1w2mcd9l2bf2dhk",
        "jersey_number": 9,
        "minutes_played": 36,
        "starting_position": "Substitute",
        "is_starter": False,
    }


def test_extract_events(ma3json_parser: MA3JSONParser) -> None:
    events = ma3json_parser.extract_events()
    assert len(events) == 1955
    assert events[("bl2020-21-0000000066", 1760864446)] == {
        "game_id": "bl2020-21-0000000066",
        "event_id": 1760864446,
        "period_id": 2,
        "team_id": "kxpw3rqn4ukt7nqmtjj62lbn",
        "player_id": "61xxo4zsk6hby0swa756l3wlx",
        "type_id": 1,
        "timestamp": datetime(2016, 2, 20, 13, 14, 21, 606000),
        "minute": 56,
        "second": 40,
        "outcome": False,
        "start_x": 31.8,
        "start_y": 2.6,
        "end_x": 80.0,
        "end_y": 5.6,
        "qualifiers": {
            1: None,
            5: None,
            56: "Right",
            140: "80.0",
            213: "0.0",
            152: None,
            141: "5.6",
            157: None,
            212: "50.7",
            307: "793",
        },
        "assist": False,
        "keypass": False,
    }
