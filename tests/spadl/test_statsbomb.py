import os

import pandas as pd
import pytest

from socceraction.spadl import config as spadl
from socceraction.spadl import statsbomb as sb
from socceraction.spadl.base import SPADLSchema
from socceraction.spadl.statsbomb import (
    StatsBombCompetitionSchema,
    StatsBombEventSchema,
    StatsBombGameSchema,
    StatsBombPlayerSchema,
    StatsBombTeamSchema,
)


class TestStatsBombLoader:
    def setup_method(self):
        data_dir = os.path.join(os.path.dirname(__file__), os.pardir, 'data', 'statsbomb', 'raw')
        self.SBL = sb.StatsBombLoader(root=data_dir, getter='local')

    def test_default_remote(self):
        SBL = sb.StatsBombLoader()
        assert SBL.root == sb.StatsBombLoader._free_open_data

    def test_competitions(self):
        df_competitions = self.SBL.competitions()
        assert len(df_competitions) > 0
        StatsBombCompetitionSchema.validate(df_competitions)

    def test_games(self):
        df_games = self.SBL.games(43, 3)  # World Cup, 2018
        assert len(df_games) == 64
        StatsBombGameSchema.validate(df_games)

    def test_teams(self):
        df_teams = self.SBL.teams(7584)
        assert len(df_teams) == 2
        StatsBombTeamSchema.validate(df_teams)

    def test_players(self):
        df_players = self.SBL.players(7584)
        assert len(df_players) == 26
        StatsBombPlayerSchema.validate(df_players)

    def test_events(self):
        df_events = self.SBL.events(7584)
        assert len(df_events) > 0
        StatsBombEventSchema.validate(df_events)


class TestSpadlConvertor:
    def setup_method(self):
        data_dir = os.path.join(os.path.dirname(__file__), os.pardir, 'data', 'statsbomb', 'raw')
        SBL = sb.StatsBombLoader(root=data_dir, getter='local')
        self.events = SBL.events(7584)

    def test_extract_player_games(self):
        df_player_games = sb.extract_player_games(self.events)
        assert len(df_player_games) == 26
        assert len(df_player_games.player_name.unique()) == 26
        assert set(df_player_games.team_name) == {'Belgium', 'Japan'}
        assert df_player_games.minutes_played.sum() == 22 * 95

    def test_convert_to_actions(self):
        df_actions = sb.convert_to_actions(self.events, 782)
        assert len(df_actions) > 0
        SPADLSchema.validate(df_actions)
        assert (df_actions.game_id == 7584).all()
        assert ((df_actions.team_id == 782) | (df_actions.team_id == 778)).all()

    def test_convert_start_location(self):
        event = pd.DataFrame(
            [
                {
                    'event_id': 'a1b55211-a292-4294-887b-5385cc3c5705',
                    'index': 5,
                    'period_id': 1,
                    'timestamp': '00:00:00.920',
                    'minute': 0,
                    'second': 0,
                    'type_id': 30,
                    'type_name': 'Pass',
                    'possession': 2,
                    'possession_team_id': 782,
                    'possession_team_name': 'Belgium',
                    'play_pattern_id': 9,
                    'play_pattern_name': 'From Kick Off',
                    'team_id': 782,
                    'team_name': 'Belgium',
                    'duration': 0.973,
                    'extra': {
                        'pass': {
                            'recipient': {'id': 5642, 'name': 'Axel Witsel'},
                            'length': 12.369317,
                            'angle': 2.896614,
                            'height': {'id': 1, 'name': 'Ground Pass'},
                            'type': {'id': 65, 'name': 'Kick Off'},
                            'body_part': {'id': 40, 'name': 'Right Foot'},
                        }
                    },
                    'related_events': ['051449c5-e183-46f9-965d-1d8f00f017cb'],
                    'player_id': 3289.0,
                    'player_name': 'Romelu Lukaku Menama',
                    'position_id': 23.0,
                    'position_name': 'Center Forward',
                    'location': [61.0, 40.0],
                    'under_pressure': None,
                    'counterpress': None,
                    'game_id': 7584,
                }
            ]
        )
        action = sb.convert_to_actions(event, 782).iloc[0]
        assert action['start_x'] == ((61.0 - 1) / 119) * spadl.field_length
        assert action['start_y'] == 68 - ((40.0 - 1) / 79) * spadl.field_width
        assert action['end_x'] == action['start_x']
        assert action['end_y'] == action['start_y']

    def test_convert_end_location(self):
        event = pd.DataFrame(
            [
                {
                    'event_id': 'a1b55211-a292-4294-887b-5385cc3c5705',
                    'index': 5,
                    'period_id': 1,
                    'timestamp': '00:00:00.920',
                    'minute': 0,
                    'second': 0,
                    'type_id': 30,
                    'type_name': 'Pass',
                    'possession': 2,
                    'possession_team_id': 782,
                    'possession_team_name': 'Belgium',
                    'play_pattern_id': 9,
                    'play_pattern_name': 'From Kick Off',
                    'team_id': 782,
                    'team_name': 'Belgium',
                    'duration': 0.973,
                    'extra': {
                        'pass': {
                            'recipient': {'id': 5642, 'name': 'Axel Witsel'},
                            'length': 12.369317,
                            'angle': 2.896614,
                            'height': {'id': 1, 'name': 'Ground Pass'},
                            'end_location': [49.0, 43.0],
                            'type': {'id': 65, 'name': 'Kick Off'},
                            'body_part': {'id': 40, 'name': 'Right Foot'},
                        }
                    },
                    'related_events': ['051449c5-e183-46f9-965d-1d8f00f017cb'],
                    'player_id': 3289.0,
                    'player_name': 'Romelu Lukaku Menama',
                    'position_id': 23.0,
                    'position_name': 'Center Forward',
                    'location': [61.0, 40.0],
                    'under_pressure': None,
                    'counterpress': None,
                    'game_id': 7584,
                }
            ]
        )
        action = sb.convert_to_actions(event, 782).iloc[0]
        assert action['end_x'] == ((49.0 - 1) / 119) * spadl.field_length
        assert action['end_y'] == 68 - ((43.0 - 1) / 79) * spadl.field_width

    @pytest.mark.parametrize(
        'period,timestamp,minute,second',
        [
            (1, '00:00:00.920', 0, 0),  # FH
            (1, '00:47:09.453', 47, 9),  # FH extra time
            (2, '00:19:51.740', 64, 51),  # SH (starts again at 45 min)
            (2, '00:48:10.733', 93, 10),  # SH extra time
            (3, '00:10:12.188', 100, 12),  # FH of extensions
            (4, '00:13:31.190', 118, 31),  # SH of extensions
            (5, '00:02:37.133', 122, 37),  # Penalties
        ],
    )
    def test_convert_time(self, period, timestamp, minute, second):
        event = pd.DataFrame(
            [
                {
                    'event_id': 'a1b55211-a292-4294-887b-5385cc3c5705',
                    'index': 5,
                    'period_id': period,
                    'timestamp': timestamp,
                    'minute': minute,
                    'second': second,
                    'type_id': 30,
                    'type_name': 'Pass',
                    'possession': 2,
                    'possession_team_id': 782,
                    'possession_team_name': 'Belgium',
                    'play_pattern_id': 9,
                    'play_pattern_name': 'From Kick Off',
                    'team_id': 782,
                    'team_name': 'Belgium',
                    'duration': 0.973,
                    'extra': {
                        'pass': {
                            'recipient': {'id': 5642, 'name': 'Axel Witsel'},
                            'length': 12.369317,
                            'angle': 2.896614,
                            'height': {'id': 1, 'name': 'Ground Pass'},
                            'end_location': [49.0, 43.0],
                            'type': {'id': 65, 'name': 'Kick Off'},
                            'body_part': {'id': 40, 'name': 'Right Foot'},
                        }
                    },
                    'related_events': ['051449c5-e183-46f9-965d-1d8f00f017cb'],
                    'player_id': 3289.0,
                    'player_name': 'Romelu Lukaku Menama',
                    'position_id': 23.0,
                    'position_name': 'Center Forward',
                    'location': [61.0, 40.0],
                    'under_pressure': None,
                    'counterpress': None,
                    'game_id': 7584,
                }
            ]
        )
        action = sb.convert_to_actions(event, 782).iloc[0]
        assert action['period_id'] == period
        assert (
            action['time_seconds']
            == 60 * minute
            - ((period > 1) * 45 * 60)
            - ((period > 2) * 45 * 60)
            - ((period > 3) * 15 * 60)
            - ((period > 4) * 15 * 60)
            + second
        )

    def test_convert_pass(self):
        pass_event = pd.DataFrame(
            [
                {
                    'event_id': 'a1b55211-a292-4294-887b-5385cc3c5705',
                    'index': 5,
                    'period_id': 1,
                    'timestamp': '00:00:00.920',
                    'minute': 0,
                    'second': 0,
                    'type_id': 30,
                    'type_name': 'Pass',
                    'possession': 2,
                    'possession_team_id': 782,
                    'possession_team_name': 'Belgium',
                    'play_pattern_id': 9,
                    'play_pattern_name': 'From Kick Off',
                    'team_id': 782,
                    'team_name': 'Belgium',
                    'duration': 0.973,
                    'extra': {
                        'pass': {
                            'recipient': {'id': 5642, 'name': 'Axel Witsel'},
                            'length': 12.369317,
                            'angle': 2.896614,
                            'height': {'id': 1, 'name': 'Ground Pass'},
                            'end_location': [49.0, 43.0],
                            'type': {'id': 65, 'name': 'Kick Off'},
                            'body_part': {'id': 40, 'name': 'Right Foot'},
                        }
                    },
                    'related_events': ['051449c5-e183-46f9-965d-1d8f00f017cb'],
                    'player_id': 3289.0,
                    'player_name': 'Romelu Lukaku Menama',
                    'position_id': 23.0,
                    'position_name': 'Center Forward',
                    'location': [61.0, 40.0],
                    'under_pressure': None,
                    'counterpress': None,
                    'game_id': 7584,
                }
            ]
        )
        pass_action = sb.convert_to_actions(pass_event, 782).iloc[0]
        assert pass_action['team_id'] == 782
        assert pass_action['player_id'] == 3289
        assert pass_action['type_id'] == spadl.actiontypes.index('pass')
        assert pass_action['result_id'] == spadl.results.index('success')
        assert pass_action['bodypart_id'] == spadl.bodyparts.index('foot')
