import os

import pandas as pd
import pytest
import socceraction.spadl.config as spadlcfg
from socceraction.data.opta import OptaLoader
from socceraction.data.opta import parsers as optaparsers
from socceraction.data.opta.loader import _extract_ids_from_path
from socceraction.spadl import SPADLSchema
from socceraction.spadl import opta as opta


class TestSpadlConvertor:
    def setup_method(self) -> None:
        data_dir = os.path.join(os.path.dirname(__file__), os.pardir, "datasets", "opta")

        loader = OptaLoader(
            root=data_dir,
            parser="xml",
            feeds={
                "f7": "f7-{competition_id}-{season_id}-{game_id}-matchresults.xml",
                "f24": "f24-{competition_id}-{season_id}-{game_id}-eventdetails.xml",
            },
        )

        self.events = loader.events(1009316)
        self.actions = opta.convert_to_actions(self.events, 174)

    def test_convert_to_actions(self) -> None:
        assert len(self.actions) > 0
        SPADLSchema.validate(self.actions)
        assert (self.actions.game_id == 1009316).all()
        assert ((self.actions.team_id == 174) | (self.actions.team_id == 957)).all()

    def test_convert_goalkick(self) -> None:
        event = pd.DataFrame(
            [
                {
                    "game_id": 318175,
                    "event_id": 1619686768,
                    "type_id": 1,
                    "period_id": 1,
                    "minute": 2,
                    "second": 14,
                    "timestamp": "2010-01-27 19:47:14",
                    "player_id": 8786,
                    "team_id": 157,
                    "outcome": False,
                    "start_x": 5.0,
                    "start_y": 37.0,
                    "end_x": 73.0,
                    "end_y": 18.7,
                    "assist": False,
                    "keypass": False,
                    "qualifiers": {
                        56: "Right",
                        141: "18.7",
                        124: True,
                        140: "73.0",
                        1: True,
                    },
                    "type_name": "pass",
                }
            ]
        )
        action = opta.convert_to_actions(event, 0).iloc[0]
        assert action["type_id"] == spadlcfg.actiontypes.index("goalkick")

    def test_convert_own_goal(self) -> None:
        event = pd.DataFrame(
            [
                {
                    "game_id": 318175,
                    "event_id": 1619686768,
                    "type_id": 16,
                    "period_id": 1,
                    "minute": 2,
                    "second": 14,
                    "timestamp": "2010-01-27 19:47:14",
                    "player_id": 8786,
                    "team_id": 157,
                    "outcome": 1,
                    "start_x": 5.0,
                    "start_y": 37.0,
                    "end_x": 73.0,
                    "end_y": 18.7,
                    "assist": False,
                    "keypass": False,
                    "qualifiers": {28: True},
                    "type_name": "goal",
                }
            ]
        )
        action = opta.convert_to_actions(event, 0).iloc[0]
        assert action["type_id"] == spadlcfg.actiontypes.index("bad_touch")
        assert action["result_id"] == spadlcfg.results.index("owngoal")

    def test_fix_deflected_passes(self) -> None:
        # for a deflected pass, the end coordinates and result should be fixed
        deflected_pass = self.actions.loc[self.actions.original_event_id == 2016736289].iloc[0]
        assert deflected_pass["result_id"] == spadlcfg.results.index("success")
        assert deflected_pass["end_x"] == (100 - 70.6) / 100 * spadlcfg.field_length
        assert deflected_pass["end_y"] == (100 - 72.6) / 100 * spadlcfg.field_width
        # other actions that are followed by a ball touch event should not be changed
        tackle = self.actions.loc[self.actions.original_event_id == 1820711400].iloc[0]
        assert tackle["result_id"] == spadlcfg.results.index("fail")


def test_extract_lineups_f7xml() -> None:
    data_dir = os.path.join(os.path.dirname(__file__), os.pardir, "datasets", "opta")
    parser = optaparsers.F7XMLParser(os.path.join(data_dir, "f7-23-2018-1009316-matchresults.xml"))
    lineups = parser.extract_lineups()
    for _, lineup in lineups.items():
        # each team should have 11 starters
        assert sum(p["is_starter"] for p in lineup["players"].values()) == 11
        # the summed match time of all players should equal the total time available
        assert sum(p["minutes_played"] for p in lineup["players"].values()) == 11 * 96


def test_extract_lineups_f9json() -> None:
    data_dir = os.path.join(os.path.dirname(__file__), os.pardir, "datasets", "opta")
    parser = optaparsers.F9JSONParser(os.path.join(data_dir, "match-2017-8-918893.json"))
    lineups = parser.extract_lineups()
    for _, lineup in lineups.items():
        print([p["minutes_played"] for p in lineup["players"].values()])
        # each team should have 11 starters
        assert sum(p["is_starter"] for p in lineup["players"].values()) == 11
        # the summed match time of all players should equal the total time available
        assert sum(p["minutes_played"] for p in lineup["players"].values()) == 11 * 96


def test_extract_ids_from_path() -> None:
    glob_pattern = "{competition_id}-{season_id}/{game_id}.json"
    ffp = "blah/blah/blah/1-2021/1234.json"
    ids = _extract_ids_from_path(ffp, glob_pattern)
    assert ids["competition_id"] == 1
    assert ids["season_id"] == 2021
    assert ids["game_id"] == 1234
    ffp = "blah/blah/blah/1kldfa78394kdf-2021/1234.json"
    ids = _extract_ids_from_path(ffp, glob_pattern)
    assert ids["competition_id"] == "1kldfa78394kdf"
    assert ids["season_id"] == 2021
    assert ids["game_id"] == 1234
    ffp = "blah/blah/blah/EPL-2021/1234.json"
    ids = _extract_ids_from_path(ffp, glob_pattern)
    assert ids["competition_id"] == "EPL"
    assert ids["season_id"] == 2021
    assert ids["game_id"] == 1234


def test_extract_ids_from_path_with_incorrect_pattern() -> None:
    glob_pattern = "{competition_id}-{season_id}/{game_id}.json"
    ffp = "blah/blah/blah/1/2021/g1234.json"
    with pytest.raises(ValueError):
        _extract_ids_from_path(ffp, glob_pattern)
