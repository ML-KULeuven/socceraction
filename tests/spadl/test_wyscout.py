import os

import pandas as pd

from socceraction.spadl import config as spadl
from socceraction.spadl import wyscout as wy
from socceraction.spadl.base import SPADLSchema
from socceraction.spadl.wyscout import (
    WyscoutCompetitionSchema,
    WyscoutEventSchema,
    WyscoutGameSchema,
    WyscoutPlayerSchema,
    WyscoutTeamSchema,
)


class TestPublicWyscoutLoader:
    def setup_method(self):
        data_dir = os.path.join(
            os.path.dirname(__file__), os.pardir, 'data', 'wyscout_public', 'raw'
        )
        self.WSL = wy.PublicWyscoutLoader(root=data_dir, download=False)

    def test_competitions(self):
        df_competitions = self.WSL.competitions()
        assert len(df_competitions) > 0
        WyscoutCompetitionSchema.validate(df_competitions)

    def test_matches(self):
        df_matches = self.WSL.games(28, 10078)  # World Cup, 2018
        assert len(df_matches) == 64
        WyscoutGameSchema.validate(df_matches)

    def test_teams(self):
        df_teams = self.WSL.teams(2058007)
        assert len(df_teams) == 2
        WyscoutTeamSchema.validate(df_teams)

    def test_players(self):
        df_players = self.WSL.players(2058007)
        assert len(df_players) == 26
        WyscoutPlayerSchema.validate(df_players)

    def test_events(self):
        df_events = self.WSL.events(2058007)
        assert len(df_events) > 0
        WyscoutEventSchema.validate(df_events)


class TestWyscoutLoader:
    def setup_method(self):
        data_dir = os.path.join(os.path.dirname(__file__), os.pardir, 'data', 'wyscout_api')
        feeds = {
            'competitions': 'competitions.json',
            'seasons': 'seasons_{competition_id}.json',
            # "games": "matches_{season_id}.json",
            'events': 'events_{game_id}.json',
        }
        self.WSL = wy.WyscoutLoader(root=data_dir, getter='local', feeds=feeds)

    def test_competitions(self):
        df_competitions = self.WSL.competitions()
        assert len(df_competitions) > 0
        WyscoutCompetitionSchema.validate(df_competitions)

    def test_matches(self):
        df_matches = self.WSL.games(10, 10174)
        assert len(df_matches) == 1
        WyscoutGameSchema.validate(df_matches)

    def test_teams(self):
        df_teams = self.WSL.teams(2852835)
        assert len(df_teams) == 2
        WyscoutTeamSchema.validate(df_teams)

    def test_players(self):
        df_players = self.WSL.players(2852835)
        assert len(df_players) == 30
        WyscoutPlayerSchema.validate(df_players)

    def test_events(self):
        df_events = self.WSL.events(2852835)
        assert len(df_events) > 0
        WyscoutEventSchema.validate(df_events)


class TestSpadlConvertor:
    def setup_method(self):
        data_dir = os.path.join(
            os.path.dirname(__file__), os.pardir, 'data', 'wyscout_public', 'raw'
        )
        WSL = wy.PublicWyscoutLoader(root=data_dir, download=False)
        self.events = WSL.events(2058007)

    def test_convert_to_actions(self):
        df_actions = wy.convert_to_actions(self.events, 5629)
        assert len(df_actions) > 0
        SPADLSchema.validate(df_actions)
        assert (df_actions.game_id == 2058007).all()
        assert ((df_actions.team_id == 5629) | (df_actions.team_id == 12913)).all()

    def test_insert_interception_passes(self):
        event = pd.DataFrame(
            [
                {
                    'type_id': 8,
                    'subtype_name': 'Head pass',
                    'tags': [{'id': 102}, {'id': 1401}, {'id': 1801}],  # own goal
                    'player_id': 38093,
                    'positions': [{'y': 56, 'x': 5}, {'y': 100, 'x': 100}],
                    'game_id': 2499737,
                    'type_name': 'Pass',
                    'team_id': 1610,
                    'period_id': 2,
                    'milliseconds': 2184.793924,
                    'subtype_id': 82,
                    'event_id': 180427412,
                }
            ]
        )
        actions = wy.convert_to_actions(event, 1610)
        assert len(actions) == 2
        assert actions.at[0, 'type_id'] == spadl.actiontypes.index('interception')
        assert actions.at[1, 'type_id'] == spadl.actiontypes.index('pass')
        assert actions.at[0, 'result_id'] == spadl.results.index('success')
        assert actions.at[1, 'result_id'] == spadl.results.index('owngoal')

    def test_convert_own_goal_touches(self):
        """Own goals resulting from bad touch events in the Wyscout event
        streams should be included in the SPADL representation.
        """
        # An own goal from the game between Leicester and Stoke on 24 Feb 2018.
        # Stoke's goalkeeper Jack Butland allows a low cross to bounce off his
        # gloves and into the net:
        event = pd.DataFrame(
            [
                {
                    'type_id': 8,
                    'subtype_name': 'Cross',
                    'tags': [{'id': 402}, {'id': 801}, {'id': 1802}],
                    'player_id': 8013,
                    'positions': [{'y': 89, 'x': 97}, {'y': 0, 'x': 0}],
                    'game_id': 2499994,
                    'type_name': 'Pass',
                    'team_id': 1631,
                    'period_id': 2,
                    'milliseconds': 1496.7290489999993,
                    'subtype_id': 80,
                    'event_id': 230320305,
                },
                {
                    'type_id': 7,
                    'subtype_name': 'Touch',
                    'tags': [{'id': 102}],
                    'player_id': 8094,
                    'positions': [{'y': 50, 'x': 1}, {'y': 100, 'x': 100}],
                    'game_id': 2499994,
                    'type_name': 'Others on the ball',
                    'team_id': 1639,
                    'period_id': 2,
                    'milliseconds': 1497.6330749999993,
                    'subtype_id': 72,
                    'event_id': 230320132,
                },
                {
                    'type_id': 9,
                    'subtype_name': 'Reflexes',
                    'tags': [{'id': 101}, {'id': 1802}],
                    'player_id': 8094,
                    'positions': [{'y': 100, 'x': 100}, {'y': 50, 'x': 1}],
                    'game_id': 2499994,
                    'type_name': 'Save attempt',
                    'team_id': 1639,
                    'period_id': 2,
                    'milliseconds': 1499.980547,
                    'subtype_id': 90,
                    'event_id': 230320135,
                },
            ]
        )
        actions = wy.convert_to_actions(event, 1639)
        # FIXME: It adds a dribble between the bad touch of the goalkeeper and
        # his attempt to save the ball before crossing the line. Not sure
        # whether that is ideal.
        assert len(actions) == 4
        assert actions.at[1, 'type_id'] == spadl.actiontypes.index('bad_touch')
        assert actions.at[1, 'result_id'] == spadl.results.index('owngoal')
