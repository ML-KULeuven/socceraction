import os

import pytest
from socceraction.data import wyscout as wy
from socceraction.data.wyscout import (
    WyscoutCompetitionSchema,
    WyscoutEventSchema,
    WyscoutGameSchema,
    WyscoutPlayerSchema,
    WyscoutTeamSchema,
)


class TestPublicWyscoutLoader:
    def setup_method(self) -> None:
        data_dir = os.path.join(
            os.path.dirname(__file__), os.pardir, "datasets", "wyscout_public", "raw"
        )
        self.WSL = wy.PublicWyscoutLoader(root=data_dir, download=False)

    def test_competitions(self) -> None:
        df_competitions = self.WSL.competitions()
        assert len(df_competitions) > 0
        WyscoutCompetitionSchema.validate(df_competitions)

    def test_matches(self) -> None:
        df_matches = self.WSL.games(28, 10078)  # World Cup, 2018
        assert len(df_matches) == 64
        WyscoutGameSchema.validate(df_matches)

    def test_teams(self) -> None:
        df_teams = self.WSL.teams(2058007)
        assert len(df_teams) == 2
        WyscoutTeamSchema.validate(df_teams)

    def test_players(self) -> None:
        df_players = self.WSL.players(2058007)
        assert len(df_players) == 26
        assert df_players.minutes_played.sum() == 22 * 96
        WyscoutPlayerSchema.validate(df_players)

    def test_players_with_missing_id(self) -> None:
        # The substituted player(s) are sometimes missing
        # See https://github.com/ML-KULeuven/socceraction/issues/276
        with pytest.warns(UserWarning):
            self.WSL.players(2576016)

    def test_minutes_played(self) -> None:
        # Injury time should be added
        df_players = self.WSL.players(2058007).set_index("player_id")
        assert df_players.at[122, "minutes_played"] == 66
        assert df_players.at[8249, "minutes_played"] == 96 - 66
        # Penalty shoot-outs should no be added
        df_players = self.WSL.players(2058005).set_index("player_id")
        assert df_players.minutes_played.sum() / 22 == 127
        # COL - JAP: red card in '3
        df_players = self.WSL.players(2057997).set_index("player_id")
        assert df_players.at[26518, "minutes_played"] == 3
        # GER - SWE: double yellow card in '82 + 2' injury time
        df_players = self.WSL.players(2057986).set_index("player_id")
        assert df_players.at[14716, "minutes_played"] == 84

    def test_events(self) -> None:
        df_events = self.WSL.events(2058007)
        assert len(df_events) > 0
        WyscoutEventSchema.validate(df_events)


class TestWyscoutLoader:
    def setup_method(self) -> None:
        data_dir = os.path.join(os.path.dirname(__file__), os.pardir, "datasets", "wyscout_api")
        feeds = {
            "competitions": "competitions.json",
            "seasons": "seasons_{competition_id}.json",
            # "games": "matches_{season_id}.json",
            "events": "events_{game_id}.json",
        }
        self.WSL = wy.WyscoutLoader(root=data_dir, getter="local", feeds=feeds)

    def test_competitions(self) -> None:
        df_competitions = self.WSL.competitions()
        assert len(df_competitions) > 0
        WyscoutCompetitionSchema.validate(df_competitions)

    def test_matches(self) -> None:
        df_matches = self.WSL.games(10, 10174)
        assert len(df_matches) == 1
        WyscoutGameSchema.validate(df_matches)

    def test_teams(self) -> None:
        df_teams = self.WSL.teams(2852835)
        assert len(df_teams) == 2
        WyscoutTeamSchema.validate(df_teams)

    def test_players(self) -> None:
        df_players = self.WSL.players(2852835)
        assert len(df_players) == 30
        WyscoutPlayerSchema.validate(df_players)

    def test_events(self) -> None:
        df_events = self.WSL.events(2852835)
        assert len(df_events) > 0
        WyscoutEventSchema.validate(df_events)
