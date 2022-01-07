import os
from datetime import datetime

import pandas as pd
from pytest import fixture

from socceraction.data.opta import OptaEventSchema, OptaGameSchema
from socceraction.data.opta.parsers import F24JSONParser


@fixture()
def f24json_parser() -> F24JSONParser:
    path = os.path.join(
        os.path.dirname(__file__),
        os.pardir,
        os.pardir,
        os.pardir,
        "datasets",
        "opta",
        "match-2017-8-918893.json",
    )
    return F24JSONParser(str(path))


def test_extract_games(f24json_parser: F24JSONParser) -> None:
    games = f24json_parser.extract_games()
    assert len(games) == 1
    assert games[918893] == {
        "game_id": 918893,
        "season_id": 2017,
        "competition_id": 8,
        "game_day": 1,
        "game_date": datetime(2017, 8, 11, 18, 45, 0, 0),
        "home_team_id": 3,
        "away_team_id": 13,
    }
    OptaGameSchema.validate(pd.DataFrame.from_dict(games, orient="index"))


def test_extract_events(f24json_parser: F24JSONParser) -> None:
    events = f24json_parser.extract_events()
    assert len(events) == 1785
    assert events[(918893, 1815408644)] == {
        "game_id": 918893,
        "event_id": 1815408644,
        "period_id": 2,
        "team_id": 3,
        "player_id": 41792,
        "type_id": 5,
        "timestamp": datetime(2017, 8, 11, 20, 38, 49, 0),
        "minute": 94,
        "second": 57,
        "outcome": False,
        "start_x": 101.1,
        "start_y": 44.4,
        "end_x": 101.1,
        "end_y": 44.4,
        "qualifiers": {
            233: "690",
            56: "Center",
        },
        "assist": False,
        "keypass": False,
    }
    df = pd.DataFrame.from_dict(events, orient="index")
    df["type_name"] = "Added later"
    OptaEventSchema.validate(df)
