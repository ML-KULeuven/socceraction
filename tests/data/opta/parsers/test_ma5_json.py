import os
from datetime import datetime

import pandas as pd
from pytest import fixture

from socceraction.data.opta import OptaGameSchema
from socceraction.data.opta.parsers import MA5JSONParser


@fixture()
def ma5json_parser() -> MA5JSONParser:
    path = os.path.join(
        os.path.dirname(__file__),
        os.pardir,
        os.pardir,
        os.pardir,
        "datasets",
        "opta",
        "ma5_drnl1etmyzjz088y0bb4kh4pg.json",
    )
    return MA5JSONParser(str(path))


def test_extract_games(ma5json_parser: MA5JSONParser) -> None:
    games = ma5json_parser.extract_games()
    assert len(games) == 1
    assert games["drnl1etmyzjz088y0bb4kh4pg"] == {
        "game_id": "drnl1etmyzjz088y0bb4kh4pg",
        "season_id": "8l3o9v8n8tva0bb2cds2dhatw",
        "competition_id": "2kwbbcootiqqgmrzs6o5inle5",
        "game_day": 1,
        "game_date": datetime(2021, 8, 13, 19, 0),
        "home_team_id": "7yx5dqhhphyvfisohikodajhv",
        "away_team_id": "4dsgumo7d4zupm2ugsvm4zm4d",
        "away_possession": 64.7,
        "home_possession": 35.3,
    }
    OptaGameSchema.validate(pd.DataFrame.from_dict(games, orient="index"))
