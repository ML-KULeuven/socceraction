import os

import pandas as pd
import pytest

import socceraction.spadl.config as spadlcfg
from socceraction.spadl import opta as opta
from socceraction.spadl.base import SPADLSchema
from socceraction.spadl.opta import (
    OptaCompetitionSchema,
    OptaEventSchema,
    OptaGameSchema,
    OptaPlayerSchema,
    OptaTeamSchema,
)


class TestJSONOptaLoader:
    def setup_method(self):
        data_dir = os.path.join(os.path.dirname(__file__), os.pardir, "data", "opta")
        self.loader = opta.OptaLoader(
            root=data_dir,
            parser="json",
            feeds={
                "f1": "tournament-{season_id}-{competition_id}.json",
                "f9": "match-{season_id}-{competition_id}-{game_id}.json",
                "f24": "match-{season_id}-{competition_id}-{game_id}.json",
            },
        )

    def test_competitions(self):
        df_competitions = self.loader.competitions()
        assert len(df_competitions) > 0
        OptaCompetitionSchema.validate(df_competitions)

    def test_games(self):
        df_games = self.loader.games("8", "2017")
        assert len(df_games) == 1
        OptaGameSchema.validate(df_games)

    def test_teams(self):
        df_teams = self.loader.teams("918893")
        assert len(df_teams) == 2
        OptaTeamSchema.validate(df_teams)

    def test_players(self):
        df_players = self.loader.players("918893")
        assert len(df_players) == 27
        OptaPlayerSchema.validate(df_players)

    def test_events(self):
        df_events = self.loader.events("918893")
        assert len(df_events) > 0
        OptaEventSchema.validate(df_events)


class TestXMLOptaLoader:
    def setup_method(self):
        data_dir = os.path.join(os.path.dirname(__file__), os.pardir, "data", "opta")

        self.loader = opta.OptaLoader(
            root=data_dir,
            parser="xml",
            feeds={
                "f7": "f7-{competition_id}-{season_id}-{game_id}-matchresults.xml",
                "f24": "f24-{competition_id}-{season_id}-{game_id}-eventdetails.xml",
            },
        )

    def test_competitions(self):
        df_competitions = self.loader.competitions()
        assert len(df_competitions) > 0
        OptaCompetitionSchema.validate(df_competitions)

    def test_games(self):
        df_games = self.loader.games("23", "2018")
        assert len(df_games) == 1
        OptaGameSchema.validate(df_games)

    def test_teams(self):
        df_teams = self.loader.teams("1009316")
        assert len(df_teams) == 2
        OptaTeamSchema.validate(df_teams)

    def test_players(self):
        df_players = self.loader.players("1009316")
        assert len(df_players) == 36
        OptaPlayerSchema.validate(df_players)

    def test_events(self):
        df_events = self.loader.events("1009316")
        assert len(df_events) > 0
        OptaEventSchema.validate(df_events)


class TestWhoscoredLoader:
    def setup_method(self):
        data_dir = os.path.join(os.path.dirname(__file__), os.pardir, "data", "whoscored")

        self.loader = opta.OptaLoader(
            root=data_dir,
            parser="whoscored",
            feeds={"whoscored": "{game_id}.json"},
        )

    def test_competitions(self):
        df_competitions = self.loader.competitions()
        assert len(df_competitions) == 0

    def test_games(self):
        df_games = self.loader.games("23", "2018")
        assert len(df_games) == 1
        OptaGameSchema.validate(df_games)

    def test_teams(self):
        df_teams = self.loader.teams("1005916")
        assert len(df_teams) == 2
        OptaTeamSchema.validate(df_teams)

    def test_players(self):
        df_players = self.loader.players("1005916")
        assert len(df_players) == 44
        OptaPlayerSchema.validate(df_players)

    def test_events(self):
        df_events = self.loader.events("1005916")
        assert len(df_events) > 0
        OptaEventSchema.validate(df_events)


class TestSpadlConvertor:
    def setup_method(self):
        data_dir = os.path.join(os.path.dirname(__file__), os.pardir, "data", "opta")

        loader = opta.OptaLoader(
            root=data_dir,
            parser="xml",
            feeds={
                "f7": "f7-{competition_id}-{season_id}-{game_id}-matchresults.xml",
                "f24": "f24-{competition_id}-{season_id}-{game_id}-eventdetails.xml",
            },
        )

        self.events = loader.events("1009316")

    def test_convert_to_actions(self):
        df_actions = opta.convert_to_actions(self.events, 174)
        assert len(df_actions) > 0
        SPADLSchema.validate(df_actions)
        assert (df_actions.game_id == "1009316").all()
        assert ((df_actions.team_id == "174") | (df_actions.team_id == "957")).all()

    def test_convert_goalkick(self):
        event = pd.DataFrame(
            [
                {
                    "game_id": "318175",
                    "event_id": 1619686768,
                    "type_id": 1,
                    "period_id": 1,
                    "minute": 2,
                    "second": 14,
                    "timestamp": "2010-01-27 19:47:14",
                    "player_id": "8786",
                    "team_id": "157",
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
        action = opta.convert_to_actions(event, "0").iloc[0]
        assert action["type_id"] == spadlcfg.actiontypes.index("goalkick")


def test_extract_lineups_f7xml():
    data_dir = os.path.join(os.path.dirname(__file__), os.pardir, "data", "opta")
    parser = opta._F7XMLParser(os.path.join(data_dir, "f7-23-2018-1009316-matchresults.xml"))
    lineups = parser.extract_lineups()
    for _, lineup in lineups.items():
        # each team should have 11 starters
        assert sum([p["is_starter"] for p in lineup["players"].values()]) == 11
        # the summed match time of all players should equal the total time available
        assert sum([p["minutes_played"] for p in lineup["players"].values()]) == 11 * 96


def test_extract_lineups_f9json():
    data_dir = os.path.join(os.path.dirname(__file__), os.pardir, "data", "opta")
    parser = opta._F9JSONParser(os.path.join(data_dir, "match-2017-8-918893.json"))
    lineups = parser.extract_lineups()
    for _, lineup in lineups.items():
        print([p["minutes_played"] for p in lineup["players"].values()])
        # each team should have 11 starters
        assert sum([p["is_starter"] for p in lineup["players"].values()]) == 11
        # the summed match time of all players should equal the total time available
        assert sum([p["minutes_played"] for p in lineup["players"].values()]) == 11 * 96


def test_extract_ids_from_path():
    glob_pattern = "{competition_id}-{season_id}/{game_id}.json"
    ffp = "blah/blah/blah/1-2021/1234.json"
    ids = opta._extract_ids_from_path(ffp, glob_pattern)
    assert ids["competition_id"] == "1"
    assert ids["season_id"] == "2021"
    assert ids["game_id"] == "1234"


def test_extract_ids_from_path_with_incorrect_pattern():
    glob_pattern = "{competition_id}-{season_id}/{game_id}.json"
    ffp = "blah/blah/blah/1-2021/g_1234.json"
    with pytest.raises(ValueError):
        opta._extract_ids_from_path(ffp, glob_pattern)
