import os
from datetime import datetime

import pandas as pd
from pytest import fixture
from socceraction.data.opta import OptaEventSchema, OptaGameSchema
from socceraction.data.opta.parsers import F24XMLParser


@fixture()
def f24xml_parser() -> F24XMLParser:
    path = os.path.join(
        os.path.dirname(__file__),
        os.pardir,
        os.pardir,
        os.pardir,
        "datasets",
        "opta",
        "f24-23-2018-1009316-eventdetails.xml",
    )
    return F24XMLParser(str(path))


def test_extract_games(f24xml_parser: F24XMLParser) -> None:
    games = f24xml_parser.extract_games()
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
    }
    OptaGameSchema.validate(pd.DataFrame.from_dict(games, orient="index"))


def test_extract_events(f24xml_parser: F24XMLParser) -> None:
    events = f24xml_parser.extract_events()
    assert len(events) == 1665
    assert events[(1009316, 2097423126)] == {
        "game_id": 1009316,
        "event_id": 2097423126,
        "period_id": 2,
        "team_id": 174,
        "player_id": 197319,
        "type_id": 1,
        "timestamp": datetime(2018, 8, 20, 22, 51, 28, 259000),
        "minute": 94,
        "second": 50,
        "outcome": False,
        "start_x": 46.4,
        "start_y": 37.1,
        "end_x": 74.4,
        "end_y": 8.9,
        "qualifiers": {
            1: None,
            213: "5.7",
            212: "35.1",
            152: None,
            5: None,
            155: None,
            56: "Right",
            140: "74.4",
            141: "8.9",
        },
        "assist": False,
        "keypass": False,
    }
    df = pd.DataFrame.from_dict(events, orient="index")
    df["type_name"] = "Added later"
    OptaEventSchema.validate(df)
