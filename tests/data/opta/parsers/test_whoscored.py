import json
import os
from datetime import datetime

import pandas as pd
import pytest
from py.path import local
from pytest import fixture

from socceraction.data.base import MissingDataError
from socceraction.data.opta import (
    OptaEventSchema,
    OptaGameSchema,
    OptaPlayerSchema,
    OptaTeamSchema,
)
from socceraction.data.opta.parsers import WhoScoredParser


@fixture()
def whoscored_parser() -> WhoScoredParser:
    path = os.path.join(
        os.path.dirname(__file__),
        os.pardir,
        os.pardir,
        os.pardir,
        "datasets",
        "whoscored",
        "1005916.json",
    )
    return WhoScoredParser(str(path), competition_id=5, season_id=1516, game_id=1005916)


def test_extract_competition_id(tmpdir: local) -> None:
    path = os.path.join(
        os.path.dirname(__file__),
        os.pardir,
        os.pardir,
        os.pardir,
        "datasets",
        "whoscored",
        "1005916.json",
    )
    # Read from parameter is the default
    parser = WhoScoredParser(path, competition_id=1234, season_id=1516, game_id=1005916)
    assert parser.competition_id == 1234
    # Read from stream
    parser = WhoScoredParser(path, competition_id=None, season_id=1516, game_id=1005916)
    assert parser.competition_id == 5
    # Raise error when not in stream
    p = tmpdir.join("1005916.json")
    p.write(json.dumps({}))
    with pytest.raises(MissingDataError):
        WhoScoredParser(str(p), competition_id=None, season_id=1516, game_id=1005916)


def test_extract_season_id(tmpdir: local) -> None:
    path = os.path.join(
        os.path.dirname(__file__),
        os.pardir,
        os.pardir,
        os.pardir,
        "datasets",
        "whoscored",
        "1005916.json",
    )
    # Read from parameter is the default
    parser = WhoScoredParser(path, competition_id=5, season_id=1234, game_id=1005916)
    assert parser.season_id == 1234
    # Read from stream
    parser = WhoScoredParser(path, competition_id=5, season_id=None, game_id=1005916)
    assert parser.season_id == 1516
    # Raise error when not in stream
    p = tmpdir.join("1005916.json")
    p.write(json.dumps({}))
    with pytest.raises(MissingDataError):
        WhoScoredParser(str(p), competition_id=5, season_id=None, game_id=1005916)


def test_extract_game_id(tmpdir: local) -> None:
    path = os.path.join(
        os.path.dirname(__file__),
        os.pardir,
        os.pardir,
        os.pardir,
        "datasets",
        "whoscored",
        "1005916.json",
    )
    # Read from parameter is the default
    parser = WhoScoredParser(path, competition_id=5, season_id=1516, game_id=1234)
    assert parser.game_id == 1234
    # Read from stream
    parser = WhoScoredParser(path, competition_id=5, season_id=1516, game_id=None)
    assert parser.game_id == 1005916
    # Raise error when not in stream
    p = tmpdir.join("1005916.json")
    p.write(json.dumps({}))
    with pytest.raises(MissingDataError):
        WhoScoredParser(str(p), competition_id=5, season_id=1516, game_id=None)


def test_extract_games(whoscored_parser: WhoScoredParser) -> None:
    games = whoscored_parser.extract_games()
    assert len(games) == 1
    assert games[1005916] == {
        "game_id": 1005916,
        "season_id": 1516,
        "competition_id": 5,
        "game_day": None,
        "game_date": datetime(2015, 8, 23, 19, 45),
        "home_team_id": 272,
        "away_team_id": 267,
        "home_score": 1,
        "away_score": 3,
        "duration": 96,
        "venue": "Carlo Castellani",
        "attendance": 7309,
        "referee": "Maurizio Mariani",
        "home_manager": "Marco Giampaolo",
        "away_manager": "Rolando Maran",
    }
    OptaGameSchema.validate(pd.DataFrame.from_dict(games, orient="index"))


def test_extract_teams(whoscored_parser: WhoScoredParser) -> None:
    teams = whoscored_parser.extract_teams()
    assert len(teams) == 2
    assert teams[272] == {
        "team_id": 272,
        "team_name": "Empoli",
    }
    assert teams[267] == {
        "team_id": 267,
        "team_name": "Chievo",
    }
    OptaTeamSchema.validate(pd.DataFrame.from_dict(teams, orient="index"))


def test_extract_players(whoscored_parser: WhoScoredParser) -> None:
    players = whoscored_parser.extract_players()
    assert len(players) == 21 + 23
    assert players[(1005916, 4444)] == {
        "game_id": 1005916,
        "team_id": 267,
        "player_id": 4444,
        "player_name": "Albano Bizzarri",
        "is_starter": True,
        "minutes_played": 96,
        "jersey_number": 1,
        "starting_position": "GK",
    }
    OptaPlayerSchema.validate(pd.DataFrame.from_dict(players, orient="index"))


def test_extract_events(whoscored_parser: WhoScoredParser) -> None:
    events = whoscored_parser.extract_events()
    assert len(events) == 1562
    assert events[(1005916, 832925173)] == {
        "game_id": 1005916,
        "event_id": 832925173,
        "period_id": 1,
        "team_id": 272,
        "player_id": 128778,
        "type_id": 1,
        "timestamp": datetime(2015, 8, 23, 19, 45, 1),
        "minute": 0,
        "second": 1,
        "outcome": True,
        "start_x": 50.9,
        "start_y": 48.8,
        "end_x": 35.9,
        "end_y": 49.8,
        "qualifiers": {56: "Back", 140: "35.9", 141: "49.8", 212: "15.8", 213: "3.1"},
        "related_player_id": None,
        "goal": False,
        "shot": False,
        "touch": True,
    }
    df = pd.DataFrame.from_dict(events, orient="index")
    df["type_name"] = "Added later"
    OptaEventSchema.validate(df)


def test_extract_substitutions(whoscored_parser: WhoScoredParser) -> None:
    substitutions = whoscored_parser.extract_substitutions()
    assert len(substitutions) == 6
    assert substitutions[(1005916, 294162)] == {
        "game_id": 1005916,
        "team_id": 272,
        "period_id": 2,
        "period_milliseconds": 1693000,
        "player_in_id": 294162,
        "player_out_id": 260588,
    }


def test_extract_positions(whoscored_parser: WhoScoredParser) -> None:
    positions = whoscored_parser.extract_positions()
    assert len(positions) == 88
    assert positions[(1005916, 4444, 0)] == {
        "game_id": 1005916,
        "team_id": 267,
        "player_id": 4444,
        "period_id": 1,
        "period_milliseconds": 0,
        "start_milliseconds": 0,
        "end_milliseconds": 2520000,
        "formation_scheme": "442",
        "player_position": "GK",
        "player_position_x": 0.0,
        "player_position_y": 5.0,
    }


def test_extract_teamgamestats(whoscored_parser: WhoScoredParser) -> None:
    teamgamestats = whoscored_parser.extract_teamgamestats()
    assert len(teamgamestats) == 2
    assert teamgamestats[(1005916, 272)]["game_id"] == 1005916
    assert teamgamestats[(1005916, 272)]["team_id"] == 272
    assert teamgamestats[(1005916, 272)]["side"] == "home"
    assert teamgamestats[(1005916, 272)]["score"] == 1
    assert teamgamestats[(1005916, 272)]["shootout_score"] is None
    assert teamgamestats[(1005916, 272)]["aerials_total"] == 34
    assert teamgamestats[(1005916, 272)]["aerials_won"] == 10
    assert "aerials_success" not in teamgamestats[(1005916, 272)]


def test_extract_playergamestats(whoscored_parser: WhoScoredParser) -> None:
    playergamestats = whoscored_parser.extract_playergamestats()
    assert len(playergamestats) == 21 + 23
    assert playergamestats[(1005916, 90878)]["game_id"] == 1005916
    assert playergamestats[(1005916, 90878)]["team_id"] == 272
    assert playergamestats[(1005916, 90878)]["player_id"] == 90878
    assert playergamestats[(1005916, 90878)]["mvp"] is False
    assert playergamestats[(1005916, 90878)]["minute_start"] == 0
    assert playergamestats[(1005916, 90878)]["minute_end"] == 96
    assert playergamestats[(1005916, 90878)]["minutes_played"] == 96
    assert playergamestats[(1005916, 90878)]["passes_total"] == 47
    assert playergamestats[(1005916, 90878)]["passes_accurate"] == 37
    assert "pass_success" not in playergamestats[(1005916, 90878)]
