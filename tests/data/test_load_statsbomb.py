import os

import socceraction.data.statsbomb as sb
from socceraction.data.statsbomb import (
    StatsBombCompetitionSchema,
    StatsBombEventSchema,
    StatsBombGameSchema,
    StatsBombPlayerSchema,
    StatsBombTeamSchema,
)


class TestStatsBombLoader:
    def setup_method(self) -> None:
        data_dir = os.path.join(
            os.path.dirname(__file__), os.pardir, 'datasets', 'statsbomb', 'raw'
        )
        self.SBL = sb.StatsBombLoader(root=data_dir, getter='local')

    def test_default_remote(self) -> None:
        SBL = sb.StatsBombLoader()
        assert SBL.root == 'https://raw.githubusercontent.com/statsbomb/open-data/master/data/'

    def test_competitions(self) -> None:
        df_competitions = self.SBL.competitions()
        assert len(df_competitions) > 0
        StatsBombCompetitionSchema.validate(df_competitions)

    def test_games(self) -> None:
        df_games = self.SBL.games(43, 3)  # World Cup, 2018
        assert len(df_games) == 64
        StatsBombGameSchema.validate(df_games)

    def test_teams(self) -> None:
        df_teams = self.SBL.teams(7584)
        assert len(df_teams) == 2
        StatsBombTeamSchema.validate(df_teams)

    def test_players(self) -> None:
        df_players = self.SBL.players(7584)
        assert len(df_players) == 26
        StatsBombPlayerSchema.validate(df_players)

    def test_events(self) -> None:
        df_events = self.SBL.events(7584)
        assert len(df_events) > 0
        StatsBombEventSchema.validate(df_events)

    def test_extract_player_games(self) -> None:
        df_events = self.SBL.events(7584)
        df_player_games = sb.extract_player_games(df_events)
        assert len(df_player_games) == 26
        assert len(df_player_games.player_name.unique()) == 26
        assert set(df_player_games.team_name) == {'Belgium', 'Japan'}
        assert df_player_games.minutes_played.sum() == 22 * 95
