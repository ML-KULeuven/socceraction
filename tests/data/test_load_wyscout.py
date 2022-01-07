import os

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
            os.path.dirname(__file__), os.pardir, 'datasets', 'wyscout_public', 'raw'
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

    def test_events(self) -> None:
        df_events = self.WSL.events(2058007)
        assert len(df_events) > 0
        WyscoutEventSchema.validate(df_events)


class TestWyscoutLoader:
    def setup_method(self) -> None:
        data_dir = os.path.join(os.path.dirname(__file__), os.pardir, 'datasets', 'wyscout_api')
        feeds = {
            'competitions': 'competitions.json',
            'seasons': 'seasons_{competition_id}.json',
            # "games": "matches_{season_id}.json",
            'events': 'events_{game_id}.json',
        }
        self.WSL = wy.WyscoutLoader(root=data_dir, getter='local', feeds=feeds)

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
