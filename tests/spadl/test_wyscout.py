import os

import pandas as pd
import pytest
from socceraction.spadl import config as spadl
from socceraction.spadl import wyscout as wy


class TestWyscoutLoader:

    def setup_method(self):
        data_dir = os.path.join(os.path.dirname(__file__), os.pardir, "data", "wyscout", "raw")
        self.WSL = wy.PublicWyscoutLoader(root=data_dir, download=False)

    def test_competitions(self):
        df_competitions = self.WSL.competitions()
        assert len(df_competitions) > 0

    def test_matches(self):
        df_matches = self.WSL.matches(28, 10078) # World Cup, 2018
        assert len(df_matches) == 64

    def test_teams(self):
        df_teams = self.WSL.teams(2058007)
        assert len(df_teams) == 2

    def test_players(self):
        df_players = self.WSL.players(2058007)
        assert len(df_players) == 26

    def test_events(self):
        df_events = self.WSL.events(2058007)
        assert len(df_events) > 0


class TestSpadlConvertor():

    def setup_method(self):
        data_dir = os.path.join(os.path.dirname(__file__), os.pardir, "data", "wyscout", "raw")
        WSL = wy.PublicWyscoutLoader(root=data_dir, download=False)
        self.events = WSL.events(2058007)


    def test_insert_interception_passes(self):
        event = pd.DataFrame([{
            'eventId': 8, 
            'subEventName': 'Head pass', 
            'tags': [{'id': 102}, {'id': 1401}, {'id': 1801}],  # own goal
            'playerId': 38093, 
            'positions': [{'y': 56, 'x': 5}, {'y': 100, 'x': 100}], 
            'matchId': 2499737, 
            'eventName': 'Pass', 
            'teamId': 1610, 
            'matchPeriod': '2H', 
            'eventSec': 2184.793924, 
            'subEventId': 82, 
            'id': 180427412
            }])
        actions = wy.convert_actions(event, 1610)
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
        event = pd.DataFrame( [
            {
                'eventId': 8, 
                'subEventName': 'Cross', 
                'tags': [{'id': 402}, {'id': 801}, {'id': 1802}], 
                'playerId': 8013, 
                'positions': [{'y': 89, 'x': 97}, {'y': 0, 'x': 0}], 
                'matchId': 2499994, 
                'eventName': 'Pass', 
                'teamId': 1631, 
                'matchPeriod': '2H', 
                'eventSec': 1496.7290489999993, 
                'subEventId': 80, 
                'id': 230320305 
            }, 
            {
                'eventId': 7, 
                'subEventName': 'Touch', 
                'tags': [{'id': 102}], 
                'playerId': 8094, 
                'positions': [{'y': 50, 'x': 1}, {'y': 100, 'x': 100}], 
                'matchId': 2499994, 
                'eventName': 'Others on the ball', 
                'teamId': 1639, 
                'matchPeriod': '2H', 
                'eventSec': 1497.6330749999993, 
                'subEventId': 72, 
                'id': 230320132
            }, 
            {
                'eventId': 9, 
                'subEventName': 'Reflexes', 
                'tags': [{'id': 101}, {'id': 1802}], 
                'playerId': 8094, 
                'positions': [{'y': 100, 'x': 100}, {'y': 50, 'x': 1}], 
                'matchId': 2499994, 
                'eventName': 'Save attempt', 
                'teamId': 1639, 
                'matchPeriod': '2H', 
                'eventSec': 1499.980547, 
                'subEventId': 90, 
                'id': 230320135
            }])
        actions = wy.convert_actions(event, 1639)
        # FIXME: It adds a dribble between the bad touch of the goalkeeper and
        # his attempt to save the ball before crossing the line. Not sure
        # whether that is ideal.
        assert len(actions) == 4
        assert actions.at[1, 'type_id'] == spadl.actiontypes.index('bad_touch')
        assert actions.at[1, 'result_id'] == spadl.results.index('owngoal')

