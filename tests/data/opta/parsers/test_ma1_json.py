import os
from datetime import datetime

from pytest import fixture

from socceraction.data.opta.parsers import MA1JSONParser


@fixture()
def ma1json_parser() -> MA1JSONParser:
    path = os.path.join(
        os.path.dirname(__file__),
        os.pardir,
        os.pardir,
        os.pardir,
        "datasets",
        "opta",
        "ma1_408bfjw6uz5k19zk4am50ykmh.json",
    )
    return MA1JSONParser(str(path))


def test_extract_competitions(ma1json_parser: MA1JSONParser) -> None:
    competitions = ma1json_parser.extract_competitions()
    assert len(competitions) == 1
    assert competitions["408bfjw6uz5k19zk4am50ykmh"] == {
        "competition_id": "722fdbecxzcq9788l6jqclzlw",
        "season_id": "408bfjw6uz5k19zk4am50ykmh",
        "competition_name": "2. Bundesliga",
        "season_name": "2015/2016",
    }


def test_extract_games(ma1json_parser: MA1JSONParser) -> None:
    games = ma1json_parser.extract_games()
    assert len(games) == 1
    assert games["bsu6pjne1eqz2hs8r3685vbhl"] == {
        "game_id": "bsu6pjne1eqz2hs8r3685vbhl",
        "season_id": "408bfjw6uz5k19zk4am50ykmh",
        "competition_id": "722fdbecxzcq9788l6jqclzlw",
        "game_day": 22,
        "game_date": datetime(2016, 2, 20, 12, 0),
        "home_team_id": "aojwbjr39s1w2mcd9l2bf2dhk",
        "away_team_id": "kxpw3rqn4ukt7nqmtjj62lbn",
        "venue": "BBBank Wildpark",
        "away_score": 2,
        "home_score": 2,
        "duration": 93,
        "attendance": 12746,
        "referee": "Robert Kampka",
    }


def test_extract_teams(ma1json_parser: MA1JSONParser) -> None:
    teams = ma1json_parser.extract_teams()
    assert len(teams) == 2
    assert teams["aojwbjr39s1w2mcd9l2bf2dhk"] == {
        "team_id": "aojwbjr39s1w2mcd9l2bf2dhk",
        "team_name": "Karlsruher SC",
    }


def test_extract_players(ma1json_parser: MA1JSONParser) -> None:
    players = ma1json_parser.extract_players()
    assert len(players) == 36
    assert players[("bsu6pjne1eqz2hs8r3685vbhl", "b40xhpgxf8cvruo6vumzu3u1h")] == {
        "game_id": "bsu6pjne1eqz2hs8r3685vbhl",
        "player_id": "b40xhpgxf8cvruo6vumzu3u1h",
        "player_name": "Enrico Valentini",
        "team_id": 'aojwbjr39s1w2mcd9l2bf2dhk',
        "jersey_number": 22,
        "minutes_played": 93,
        "starting_position": "Defender",
        "is_starter": True,
    }
    assert (
        players[("bsu6pjne1eqz2hs8r3685vbhl", "49797zk0b4wmp4tevwmaeeh91")]["minutes_played"] == 57
    )
    assert players[("bsu6pjne1eqz2hs8r3685vbhl", "yuw4a34cpasw5e4vqsg6ex1x")][
        "minutes_played"
    ] == (93 - 57)
