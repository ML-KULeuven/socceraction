import json
import os
import shutil
import sys
import unittest
from importlib import reload

import pytest
from py.path import local
from pytest import fixture

import socceraction.data.statsbomb as sb
from socceraction.data.base import ParseError
from socceraction.data.statsbomb import (
    StatsBombCompetitionSchema,
    StatsBombEventSchema,
    StatsBombGameSchema,
    StatsBombPlayerSchema,
    StatsBombTeamSchema,
)


@fixture(scope="module", params=["local", "remote"])
def SBL(request) -> sb.StatsBombLoader:  # type: ignore
    """Create a StatsBombLoader instance."""
    data_dir = os.path.join(os.path.dirname(__file__), os.pardir, "datasets", "statsbomb", "raw")
    return sb.StatsBombLoader(getter=request.param, root=data_dir)


# Test init ##################################################################


def test_load_remote() -> None:
    """It can load remote data."""
    SBL = sb.StatsBombLoader(getter="remote")
    assert SBL._creds is not None


def test_load_local() -> None:
    """It can load local data."""
    data_dir = os.path.join(os.path.dirname(__file__), os.pardir, "datasets", "statsbomb", "raw")
    SBL = sb.StatsBombLoader(getter="local", root=str(data_dir))
    assert SBL._root is not None


def test_load_invalid_source() -> None:
    """It raises an error if the source is not ``remote`` or ``local``."""
    with pytest.raises(ValueError):
        sb.StatsBombLoader(getter="foo")


def test_load_local_missing_root() -> None:
    """It raises an error if the root is not provided when loading local data."""
    with pytest.raises(ValueError):
        sb.StatsBombLoader(getter="local")


class TestWithoutStatsBombPy(unittest.TestCase):
    def setUp(self) -> None:
        self._temp_sbpy = sys.modules.get("statsbombpy")
        sys.modules["statsbombpy"] = None  # type: ignore
        reload(sys.modules["socceraction.data.statsbomb.loader"])

    def tearDown(self) -> None:
        sys.modules["statsbombpy"] = self._temp_sbpy  # type: ignore
        reload(sys.modules["socceraction.data.statsbomb.loader"])

    def tests_load_without_statsbombpy(self) -> None:
        """It raises an error upon initialization of a remote loader if statsbombpy is not installed."""
        with pytest.raises(ImportError):
            sb.StatsBombLoader(getter="remote")


# Test competitions ##########################################################


def test_competitions(SBL: sb.StatsBombLoader) -> None:
    """It loads a DataFrame with available competitions."""
    df_competitions = SBL.competitions()
    assert len(df_competitions) > 0
    StatsBombCompetitionSchema.validate(df_competitions)


def test_no_competitions(tmpdir: local) -> None:
    """It returns an empty DataFrame when no competitions are available."""
    p = tmpdir.join("competitions.json")
    p.write(json.dumps([]))
    SBL = sb.StatsBombLoader(root=str(tmpdir), getter="local")
    df_competitions = SBL.competitions()
    assert len(df_competitions) == 0
    StatsBombCompetitionSchema.validate(df_competitions)


def test_invalid_competitions(tmpdir: local) -> None:
    """It raises an error if the competitions.json file is invalid."""
    p = tmpdir.join("competitions.json")
    p.write(json.dumps({"this is wrong": 1}))
    SBL = sb.StatsBombLoader(root=str(tmpdir), getter="local")
    with pytest.raises(ParseError):
        SBL.competitions()


# Test games #################################################################


def test_games(SBL: sb.StatsBombLoader) -> None:
    """It loads a DataFrame with available competitions."""
    df_games = SBL.games(43, 3)  # World Cup, 2018
    assert len(df_games) == 64
    StatsBombGameSchema.validate(df_games)


def test_no_games(tmpdir: local) -> None:
    """It returns an empty DataFrame when no games are available."""
    p = tmpdir.mkdir("matches").mkdir("11").join("1.json")
    p.write(json.dumps([]))
    SBL = sb.StatsBombLoader(root=str(tmpdir), getter="local")
    df_games = SBL.games(11, 1)
    assert len(df_games) == 0
    StatsBombGameSchema.validate(df_games)


def test_invalid_games(tmpdir: local) -> None:
    """It raises an error if the json file is invalid."""
    p = tmpdir.mkdir("matches").mkdir("11").join("1.json")
    p.write(json.dumps({"this is wrong": 1}))
    SBL = sb.StatsBombLoader(root=str(tmpdir), getter="local")
    with pytest.raises(ParseError):
        SBL.games(11, 1)


# Test teams #################################################################


def test_teams(SBL: sb.StatsBombLoader) -> None:
    """It loads a DataFrame with both teams that participated in a game."""
    df_teams = SBL.teams(7584)
    assert len(df_teams) == 2
    StatsBombTeamSchema.validate(df_teams)


def test_no_teams(tmpdir: local) -> None:
    """It raises an error when no lineups are available for each team."""
    p = tmpdir.mkdir("lineups").join("7584.json")
    p.write(json.dumps([]))
    SBL = sb.StatsBombLoader(root=str(tmpdir), getter="local")
    with pytest.raises(ParseError):
        SBL.teams(7584)


def test_invalid_teams(tmpdir: local) -> None:
    """It raises an error if the json file is invalid."""
    p = tmpdir.mkdir("lineups").join("7584.json")
    p.write(json.dumps({"this is wrong": 1}))
    SBL = sb.StatsBombLoader(root=str(tmpdir), getter="local")
    with pytest.raises(ParseError):
        SBL.teams(7584)


# Test player ################################################################


def test_players(SBL: sb.StatsBombLoader) -> None:
    """It loads a DataFrame with all players that participated in a game."""
    df_players = SBL.players(7584)
    assert len(df_players) == 26
    StatsBombPlayerSchema.validate(df_players)


def test_no_players(tmpdir: local) -> None:
    """It raises an error when no lineups are available for both teams."""
    p = tmpdir.mkdir("lineups").join("7584.json")
    p.write(json.dumps([]))
    SBL = sb.StatsBombLoader(root=str(tmpdir), getter="local")
    with pytest.raises(ParseError):
        SBL.players(7584)


def test_invalid_players(tmpdir: local) -> None:
    """It raises an error if the json file is invalid."""
    p = tmpdir.mkdir("lineups").join("7584.json")
    p.write(json.dumps({"this is wrong": 1}))
    SBL = sb.StatsBombLoader(root=str(tmpdir), getter="local")
    with pytest.raises(ParseError):
        SBL.players(7584)


# Test events ################################################################


def test_events(SBL: sb.StatsBombLoader) -> None:
    """It loads a DataFrame with all events during a game."""
    df_events = SBL.events(7584)
    assert len(df_events) > 0
    StatsBombEventSchema.validate(df_events)


def test_no_events(tmpdir: local) -> None:
    """It returns an empty DataFrame when no events are available."""
    p = tmpdir.mkdir("events").join("7584.json")
    p.write(json.dumps([]))
    SBL = sb.StatsBombLoader(root=str(tmpdir), getter="local")
    df_events = SBL.events(7584)
    assert len(df_events) == 0
    StatsBombEventSchema.validate(df_events)


def test_invalid_events(tmpdir: local) -> None:
    """It raises an error if the json file is invalid."""
    p = tmpdir.mkdir("events").join("7584.json")
    p.write(json.dumps({"this is wrong": 1}))
    SBL = sb.StatsBombLoader(root=str(tmpdir), getter="local")
    with pytest.raises(ParseError):
        SBL.events(7584)


# Test 360 frames ##############################################################


def test_frames(SBL: sb.StatsBombLoader) -> None:
    """It loads a DataFrame with all 360 frames recorded during a game."""
    df_frames = SBL.events(3788741, load_360=True)
    assert len(df_frames) > 0
    StatsBombEventSchema.validate(df_frames)
    assert "visible_area_360" in df_frames.columns
    assert "freeze_frame_360" in df_frames.columns


def test_no_frames_empty(tmpdir: local) -> None:
    """It just returns the events DataFrame when no 360 frames are available."""
    tmpdir.mkdir("events")
    datadir = os.path.join(os.path.dirname(__file__), os.pardir, "datasets", "statsbomb", "raw")
    shutil.copy(
        os.path.join(datadir, "events/7584.json"), os.path.join(tmpdir, "events/7584.json")
    )
    p = tmpdir.mkdir("three-sixty").join("7584.json")
    p.write(json.dumps([]))
    SBL = sb.StatsBombLoader(root=str(tmpdir), getter="local")
    df_frames = SBL.events(7584, load_360=True)
    assert len(df_frames) > 0
    assert "visible_area_360" in df_frames.columns
    assert "freeze_frame_360" in df_frames.columns
    StatsBombEventSchema.validate(df_frames)


def test_invalid_frames(tmpdir: local) -> None:
    """It raises an error if the json file is invalid."""
    tmpdir.mkdir("events")
    datadir = os.path.join(os.path.dirname(__file__), os.pardir, "datasets", "statsbomb", "raw")
    shutil.copy(
        os.path.join(datadir, "events/7584.json"), os.path.join(tmpdir, "events/7584.json")
    )
    p = tmpdir.mkdir("three-sixty").join("7584.json")
    p.write(json.dumps({"this is wrong": 1}))
    SBL = sb.StatsBombLoader(root=str(tmpdir), getter="local")
    with pytest.raises(ParseError):
        SBL.events(7584, load_360=True)


# Test extract_player_games ##################################################


def test_extract_player_games(SBL: sb.StatsBombLoader) -> None:
    df_events = SBL.events(7584)
    df_player_games = sb.extract_player_games(df_events)
    assert len(df_player_games) == 26
    assert len(df_player_games.player_name.unique()) == 26
    assert set(df_player_games.team_name) == {"Belgium", "Japan"}
    assert df_player_games.minutes_played.sum() == 22 * 96


def test_minutes_played(SBL: sb.StatsBombLoader) -> None:
    # Injury time should be added
    df_players = SBL.players(7584).set_index("player_id")
    assert df_players.at[5630, "minutes_played"] == 64 + 1
    assert df_players.at[3296, "minutes_played"] == 96 - (64 + 1)
    # Penalty shoot-outs should no be added
    df_players = SBL.players(7581).set_index("player_id")
    assert df_players.minutes_played.sum() / 22 == 127
    # COL - JAP: red card in '2
    df_players = SBL.players(7541).set_index("player_id")
    assert df_players.at[5685, "minutes_played"] == 2
    # GER - SWE: double yellow card in '80 + 2' injury time
    df_players = SBL.players(7551).set_index("player_id")
    assert df_players.at[5578, "minutes_played"] == 82
