import os
from datetime import datetime

import pandas as pd
from pytest import fixture
from socceraction.data.opta import (
    OptaGameSchema,
    OptaPlayerSchema,
    OptaTeamSchema,
)
from socceraction.data.opta.parsers import F9JSONParser


@fixture()
def f9json_parser() -> F9JSONParser:
    path = os.path.join(
        os.path.dirname(__file__),
        os.pardir,
        os.pardir,
        os.pardir,
        "datasets",
        "opta",
        "match-2017-8-918893.json",
    )
    return F9JSONParser(str(path))


def test_extract_games(f9json_parser: F9JSONParser) -> None:
    games = f9json_parser.extract_games()
    assert len(games) == 1
    assert games[918893] == {
        "game_id": 918893,
        "season_id": 2017,
        "competition_id": 8,
        "game_day": 1,
        "game_date": datetime(2017, 8, 11, 18, 45),
        "home_team_id": 3,
        "away_team_id": 13,
        "home_score": 4,
        "away_score": 3,
        "attendance": 59387,
        "duration": 96,
        "referee": "Mike Dean",
        "venue": None,
        "home_manager": None,
        "away_manager": None,
    }
    OptaGameSchema.validate(pd.DataFrame.from_dict(games, orient="index"))


def test_extract_teams(f9json_parser: F9JSONParser) -> None:
    teams = f9json_parser.extract_teams()
    assert len(teams) == 2
    assert teams[3] == {
        "team_id": 3,
        "team_name": "Arsenal",
    }
    assert teams[13] == {
        "team_id": 13,
        "team_name": "Leicester City",
    }
    OptaTeamSchema.validate(pd.DataFrame.from_dict(teams, orient="index"))


def test_extract_players(f9json_parser: F9JSONParser) -> None:
    players = f9json_parser.extract_players()
    assert len(players) == 27
    assert players[(918893, 11334)] == {
        "game_id": 918893,
        "player_id": 11334,
        "player_name": "Petr Cech",
        "team_id": 3,
        "jersey_number": 33,
        "minutes_played": 96,
        "starting_position": "Goalkeeper",
        "is_starter": True,
    }
    OptaPlayerSchema.validate(pd.DataFrame.from_dict(players, orient="index"))
