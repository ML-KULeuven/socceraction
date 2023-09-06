import os

import pandas as pd
from pytest import fixture

from socceraction.data.opta import OptaPlayerSchema
from socceraction.data.opta.parsers import MA12JSONParser


@fixture()
def ma12json_parser() -> MA12JSONParser:
    path = os.path.join(
        os.path.dirname(__file__),
        os.pardir,
        os.pardir,
        os.pardir,
        "datasets",
        "opta",
        "ma12_drnl1etmyzjz088y0bb4kh4pg.json",
    )
    return MA12JSONParser(str(path))


def test_extract_players(ma12json_parser: MA12JSONParser) -> None:
    players = ma12json_parser.extract_players()
    assert len(players) == 40
    assert players[("drnl1etmyzjz088y0bb4kh4pg", "4iijb6llnz28unsz4rirr3umt")] == {
        "game_id": "drnl1etmyzjz088y0bb4kh4pg",
        "player_id": "4iijb6llnz28unsz4rirr3umt",
        "player_name": "David Raya",
        "team_id": "7yx5dqhhphyvfisohikodajhv",
        "jersey_number": 1,
        "minutes_played": 90,
        "starting_position": "Goalkeeper",
        "is_starter": True,
        "position_side": "Centre",
        "xG_non_penalty": 0.0,
    }
    # red card
    assert (
        players[("drnl1etmyzjz088y0bb4kh4pg", "ds1wjqejslhsbcvzaufxubey2")]["minutes_played"] == 10
    )
    assert (
        players[("drnl1etmyzjz088y0bb4kh4pg", "2o5etqath4k9qaliho4op0hd5")]["minutes_played"] == 0
    )
    OptaPlayerSchema.validate(pd.DataFrame.from_dict(players, orient="index"))
