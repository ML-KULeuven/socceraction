import os
import sys

import pytest
from py.path import local
from socceraction.data import opta as opta
from socceraction.data.opta import (
    OptaCompetitionSchema,
    OptaEventSchema,
    OptaGameSchema,
    OptaPlayerSchema,
    OptaTeamSchema,
)


def test_create_opta_json_loader(tmpdir: local) -> None:
    """It should be able to parse F1, f9 and F24 JSON feeds."""
    feeds = {
        "f1": "f1-{competition_id}-{season_id}-{game_id}.json",
        "f9": "f9-{competition_id}-{season_id}-{game_id}.json",
        "f24": "f24-{competition_id}-{season_id}-{game_id}.json",
    }
    loader = opta.OptaLoader(root=str(tmpdir), parser="json", feeds=feeds)
    assert loader.parsers == {
        "f1": opta.parsers.F1JSONParser,
        "f9": opta.parsers.F9JSONParser,
        "f24": opta.parsers.F24JSONParser,
    }


def test_create_opta_xml_loader(tmpdir: local) -> None:
    """It should be able to parse F7 and F24 XML feeds."""
    feeds = {
        "f7": "f7-{competition_id}-{season_id}-{game_id}.xml",
        "f24": "f24-{competition_id}-{season_id}-{game_id}.xml",
    }
    loader = opta.OptaLoader(root=str(tmpdir), parser="xml", feeds=feeds)
    assert loader.parsers == {
        "f7": opta.parsers.F7XMLParser,
        "f24": opta.parsers.F24XMLParser,
    }


def test_create_statsperform_loader(tmpdir: local) -> None:
    """It should be able to parse MA1 and MA3 StatsPerfrom feeds."""
    feeds = {
        "ma1": "ma1-{competition_id}-{season_id}-{game_id}.json",
        "ma3": "ma3-{competition_id}-{season_id}-{game_id}.json",
    }
    loader = opta.OptaLoader(root=str(tmpdir), parser="statsperform", feeds=feeds)
    assert loader.parsers == {
        "ma1": opta.parsers.MA1JSONParser,
        "ma3": opta.parsers.MA3JSONParser,
    }


def test_create_whoscored_loader(tmpdir: local) -> None:
    """It should be able to parse WhoScored feeds."""
    feeds = {
        "whoscored": "{competition_id}-{season_id}-{game_id}.json",
    }
    loader = opta.OptaLoader(root=str(tmpdir), parser="whoscored", feeds=feeds)
    assert loader.parsers == {
        "whoscored": opta.parsers.WhoScoredParser,
    }


def test_create_custom_loader(tmpdir: local) -> None:
    """It should support a custom feed and parser."""
    feeds = {
        "myfeed": "{competition_id}-{season_id}-{game_id}.json",
    }
    parser = {
        "myfeed": opta.parsers.base.OptaParser,
    }
    loader = opta.OptaLoader(root=str(tmpdir), parser=parser, feeds=feeds)
    assert loader.parsers == {
        "myfeed": opta.parsers.base.OptaParser,
    }


def test_create_loader_with_unsupported_feed(tmpdir: local) -> None:
    """It should warn if a feed is not supported."""
    feeds = {
        "f0": "f0-{competition_id}-{season_id}-{game_id}.json",
    }
    with pytest.warns(
        UserWarning, match="No parser available for f0 feeds. This feed is ignored."
    ):
        loader = opta.OptaLoader(root=str(tmpdir), parser="json", feeds=feeds)
    assert loader.parsers == {}


def test_create_invalid_loader(tmpdir: local) -> None:
    """It should raise an error if the parser is not supported."""
    feeds = {
        "myfeed": "{competition_id}-{season_id}-{game_id}.json",
    }
    with pytest.raises(ValueError):
        opta.OptaLoader(root=str(tmpdir), parser="wrong", feeds=feeds)


def test_universal_feeds(tmpdir: local) -> None:
    """It should replace forward slashes in glob patterns on Windows."""
    feeds = {
        "myfeed": "{competition_id}/{season_id}/{game_id}.json",
    }
    parser = {
        "myfeed": opta.parsers.base.OptaParser,
    }
    loader = opta.OptaLoader(root=str(tmpdir), parser=parser, feeds=feeds)

    if "win" in sys.platform:
        assert loader.feeds["myfeed"] == "{competition_id}\\{season_id}\\{game_id}.json"

    elif "linux" in sys.platform:
        assert loader.feeds["myfeed"] == "{competition_id}/{season_id}/{game_id}.json"


def test_deepupdate() -> None:
    """It should update a dict with another dict."""
    # list
    t1 = {"name": "ferry", "hobbies": ["programming", "sci-fi"]}
    opta.loader._deepupdate(t1, {"hobbies": ["gaming"], "jobs": ["student"]})
    assert t1 == {
        "name": "ferry",
        "hobbies": ["programming", "sci-fi", "gaming"],
        "jobs": ["student"],
    }
    # set
    t2 = {"name": "ferry", "hobbies": {"programming", "sci-fi"}}
    opta.loader._deepupdate(t2, {"hobbies": {"gaming"}, "jobs": {"student"}})
    assert t2 == {
        "name": "ferry",
        "hobbies": {"programming", "sci-fi", "gaming"},
        "jobs": {"student"},
    }
    # dict
    t3 = {"name": "ferry", "hobbies": {"programming": True, "sci-fi": True}}
    opta.loader._deepupdate(t3, {"hobbies": {"gaming": True}})
    assert t3 == {
        "name": "ferry",
        "hobbies": {"programming": True, "sci-fi": True, "gaming": True},
    }
    # value
    t4 = {"name": "ferry", "hobby": "programming"}
    opta.loader._deepupdate(t4, {"hobby": "gaming"})
    assert t4 == {"name": "ferry", "hobby": "gaming"}


def test_extract_ids_from_path() -> None:
    feeds = {
        "f1": "f1-{competition_id}-{season_id}.json",
        "f9": "f9-{competition_id}-{season_id}-{game_id}.json",
        "f24": "f24-{competition_id}-{season_id}-{game_id}.json",
    }
    assert opta.loader._extract_ids_from_path("./f24-23-2018-1.json", feeds["f24"]) == {
        "competition_id": 23,
        "season_id": 2018,
        "game_id": 1,
    }
    with pytest.raises(
        ValueError,
        match=f"The filepath ./f24-23-2018.json does not match the format {feeds['f24']}.",
    ):
        opta.loader._extract_ids_from_path("./f24-23-2018.json", feeds["f24"])
    with pytest.raises(
        ValueError,
        match=f"The filepath ./f24-23-2018_1.json does not match the format {feeds['f24']}.",
    ):
        opta.loader._extract_ids_from_path("./f24-23-2018_1.json", feeds["f24"])
    assert opta.loader._extract_ids_from_path(
        "./f24-Brasileirão-2324-1716682.json", feeds["f24"]
    ) == {
        "competition_id": "Brasileirão",
        "season_id": 2324,
        "game_id": 1716682,
    }


class TestJSONOptaLoader:
    def setup_method(self) -> None:
        data_dir = os.path.join(os.path.dirname(__file__), os.pardir, "datasets", "opta")
        self.loader = opta.OptaLoader(
            root=data_dir,
            parser="json",
            feeds={
                "f1": "tournament-{season_id}-{competition_id}.json",
                "f9": "match-{season_id}-{competition_id}-{game_id}.json",
                "f24": "match-{season_id}-{competition_id}-{game_id}.json",
            },
        )

    def test_competitions(self) -> None:
        df_competitions = self.loader.competitions()
        assert len(df_competitions) > 0
        OptaCompetitionSchema.validate(df_competitions)

    def test_games(self) -> None:
        df_games = self.loader.games(8, 2017)
        assert len(df_games) == 1
        OptaGameSchema.validate(df_games)

    def test_teams(self) -> None:
        df_teams = self.loader.teams(918893)
        assert len(df_teams) == 2
        OptaTeamSchema.validate(df_teams)

    def test_players(self) -> None:
        df_players = self.loader.players(918893)
        assert len(df_players) == 27
        OptaPlayerSchema.validate(df_players)

    def test_events(self) -> None:
        df_events = self.loader.events(918893)
        assert len(df_events) > 0
        OptaEventSchema.validate(df_events)


class TestXMLOptaLoader:
    def setup_method(self) -> None:
        data_dir = os.path.join(os.path.dirname(__file__), os.pardir, "datasets", "opta")

        self.loader = opta.OptaLoader(
            root=data_dir,
            parser="xml",
            feeds={
                "f7": "f7-{competition_id}-{season_id}-{game_id}-matchresults.xml",
                "f24": "f24-{competition_id}-{season_id}-{game_id}-eventdetails.xml",
            },
        )

    def test_competitions(self) -> None:
        df_competitions = self.loader.competitions()
        assert len(df_competitions) > 0
        OptaCompetitionSchema.validate(df_competitions)

    def test_games(self) -> None:
        df_games = self.loader.games(23, 2018)
        assert len(df_games) == 1
        OptaGameSchema.validate(df_games)

    def test_teams(self) -> None:
        df_teams = self.loader.teams(1009316)
        assert len(df_teams) == 2
        OptaTeamSchema.validate(df_teams)

    def test_players(self) -> None:
        df_players = self.loader.players(1009316)
        assert len(df_players) == 36
        OptaPlayerSchema.validate(df_players)

    def test_events(self) -> None:
        df_events = self.loader.events(1009316)
        assert len(df_events) > 0
        OptaEventSchema.validate(df_events)


class TestWhoscoredLoader:
    def setup_method(self) -> None:
        data_dir = os.path.join(os.path.dirname(__file__), os.pardir, "datasets", "whoscored")

        self.loader = opta.OptaLoader(
            root=data_dir,
            parser="whoscored",
            feeds={"whoscored": "{game_id}.json"},
        )

    # def test_competitions(self) -> None:
    #     df_competitions = self.loader.competitions()
    #     assert len(df_competitions) == 0

    def test_games(self) -> None:
        df_games = self.loader.games(23, 2018)
        assert len(df_games) == 1
        OptaGameSchema.validate(df_games)

    def test_teams(self) -> None:
        df_teams = self.loader.teams(1005916)
        assert len(df_teams) == 2
        OptaTeamSchema.validate(df_teams)

    def test_players(self) -> None:
        df_players = self.loader.players(1005916)
        assert len(df_players) == 44
        OptaPlayerSchema.validate(df_players)

    def test_events(self) -> None:
        df_events = self.loader.events(1005916)
        assert len(df_events) > 0
        OptaEventSchema.validate(df_events)
