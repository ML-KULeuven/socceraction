import os
from datetime import datetime

import pandas as pd
from pytest import fixture
from socceraction.data.providers.opta import OptaCompetitionSchema, OptaGameSchema
from socceraction.data.providers.opta.parsers import F1JSONParser


@fixture()
def f1json_parser() -> F1JSONParser:
    path = os.path.join(
        os.path.dirname(__file__),
        os.pardir,
        os.pardir,
        os.pardir,
        "datasets",
        "opta",
        "tournament-2017-8.json",
    )
    return F1JSONParser(str(path))


def test_extract_competitions(f1json_parser: F1JSONParser) -> None:
    competitions = f1json_parser.extract_competitions()
    assert len(competitions) == 1
    assert competitions[(8, 2017)] == {
        "competition_id": 8,
        "season_id": 2017,
        "competition_name": "English Premier League",
        "season_name": "2017",
    }
    OptaCompetitionSchema.validate(pd.DataFrame.from_dict(competitions, orient="index"))


def test_extract_games(f1json_parser: F1JSONParser) -> None:
    games = f1json_parser.extract_games()
    assert len(games) == 1
    assert games[918893] == {
        "game_id": 918893,
        "season_id": 2017,
        "competition_id": 8,
        "game_day": 1,
        "game_date": datetime(2017, 8, 11, 19, 45),
        "home_team_id": 3,
        "away_team_id": 13,
        "home_score": 4,
        "away_score": 3,
    }
    OptaGameSchema.validate(pd.DataFrame.from_dict(games, orient="index"))
