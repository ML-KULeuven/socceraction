import os

from socceraction.data import opta as opta
from socceraction.data.opta import (
    OptaCompetitionSchema,
    OptaEventSchema,
    OptaGameSchema,
    OptaPlayerSchema,
    OptaTeamSchema,
)


class TestJSONOptaLoader:
    def setup_method(self) -> None:
        data_dir = os.path.join(os.path.dirname(__file__), os.pardir, 'datasets', 'opta')
        self.loader = opta.OptaLoader(
            root=data_dir,
            parser='json',
            feeds={
                'f1': 'tournament-{season_id}-{competition_id}.json',
                'f9': 'match-{season_id}-{competition_id}-{game_id}.json',
                'f24': 'match-{season_id}-{competition_id}-{game_id}.json',
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
        data_dir = os.path.join(os.path.dirname(__file__), os.pardir, 'datasets', 'opta')

        self.loader = opta.OptaLoader(
            root=data_dir,
            parser='xml',
            feeds={
                'f7': 'f7-{competition_id}-{season_id}-{game_id}-matchresults.xml',
                'f24': 'f24-{competition_id}-{season_id}-{game_id}-eventdetails.xml',
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
        data_dir = os.path.join(os.path.dirname(__file__), os.pardir, 'datasets', 'whoscored')

        self.loader = opta.OptaLoader(
            root=data_dir,
            parser='whoscored',
            feeds={'whoscored': '{game_id}.json'},
        )

    def test_competitions(self) -> None:
        df_competitions = self.loader.competitions()
        assert len(df_competitions) == 0

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
