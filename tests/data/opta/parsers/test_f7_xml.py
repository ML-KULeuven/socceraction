import os
from datetime import datetime

import pandas as pd
from pytest import fixture
from socceraction.data.opta import (
    OptaCompetitionSchema,
    OptaGameSchema,
    OptaPlayerSchema,
    OptaTeamSchema,
)
from socceraction.data.opta.parsers import F7XMLParser


@fixture()
def f7xml_parser() -> F7XMLParser:
    path = os.path.join(
        os.path.dirname(__file__),
        os.pardir,
        os.pardir,
        os.pardir,
        "datasets",
        "opta",
        "f7-23-2018-1009316-matchresults.xml",
    )
    return F7XMLParser(str(path))


def test_extract_competitions(f7xml_parser: F7XMLParser) -> None:
    competitions = f7xml_parser.extract_competitions()
    assert len(competitions) == 1
    assert competitions[(23, 2018)] == {
        "competition_id": 23,
        "season_id": 2018,
        "competition_name": "Spanish La Liga",
        "season_name": "Season 2018/2019",
    }
    OptaCompetitionSchema.validate(pd.DataFrame.from_dict(competitions, orient="index"))


def test_extract_games(f7xml_parser: F7XMLParser) -> None:
    games = f7xml_parser.extract_games()
    assert len(games) == 1
    assert games[1009316] == {
        "game_id": 1009316,
        "season_id": 2018,
        "competition_id": 23,
        "game_day": 1,
        "game_date": datetime(2018, 8, 20, 21, 0),
        "home_team_id": 174,
        "away_team_id": 957,
        "home_score": 2,
        "away_score": 1,
        "duration": 96,
        "referee": "Adrián Cordero Vega",
        "venue": "San Mamés",
        "attendance": 38575,
        "home_manager": "Eduardo Berizzo",
        "away_manager": "Mauricio Pellegrino",
    }
    OptaGameSchema.validate(pd.DataFrame.from_dict(games, orient="index"))


def test_extract_teams(f7xml_parser: F7XMLParser) -> None:
    teams = f7xml_parser.extract_teams()
    assert len(teams) == 2
    assert teams[957] == {
        "team_id": 957,
        "team_name": "Leganés",
    }
    assert teams[174] == {
        "team_id": 174,
        "team_name": "Athletic Club",
    }
    OptaTeamSchema.validate(pd.DataFrame.from_dict(teams, orient="index"))


def test_extract_players(f7xml_parser: F7XMLParser) -> None:
    players = f7xml_parser.extract_players()
    assert len(players) == 36
    assert players[(1009316, 242831)] == {
        "game_id": 1009316,
        "team_id": 174,
        "player_id": 242831,
        "player_name": "Peru Nolaskoain",
        "is_starter": True,
        "minutes_played": 96,
        "jersey_number": 31,
        "starting_position": "Defender",
    }
    OptaPlayerSchema.validate(pd.DataFrame.from_dict(players, orient="index"))
