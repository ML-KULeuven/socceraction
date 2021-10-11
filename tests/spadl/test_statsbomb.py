import os

import pytest

from socceraction.data.statsbomb import StatsBombLoader
from socceraction.spadl import SPADLSchema
from socceraction.spadl import config as spadl
from socceraction.spadl import statsbomb as sb


class TestSpadlConvertor:
    def setup_method(self) -> None:
        data_dir = os.path.join(
            os.path.dirname(__file__), os.pardir, 'datasets', 'statsbomb', 'raw'
        )
        self.SBL = StatsBombLoader(root=data_dir, getter='local')
        # https://raw.githubusercontent.com/statsbomb/open-data/master/data/events/7584.json
        self.id_bel = 782
        self.events_japbel = self.SBL.events(7584)

    def test_convert_to_actions(self) -> None:
        df_actions = sb.convert_to_actions(self.events_japbel, 782)
        assert len(df_actions) > 0
        SPADLSchema.validate(df_actions)
        assert (df_actions.game_id == 7584).all()
        assert ((df_actions.team_id == 782) | (df_actions.team_id == 778)).all()

    def test_convert_start_location(self) -> None:
        event = self.events_japbel[
            self.events_japbel.event_id == 'a1b55211-a292-4294-887b-5385cc3c5705'
        ]
        action = sb.convert_to_actions(event, self.id_bel).iloc[0]
        assert action['start_x'] == ((61.0 - 1) / 119) * spadl.field_length
        assert action['start_y'] == 68 - ((40.0 - 1) / 79) * spadl.field_width

    def test_convert_end_location(self) -> None:
        event = self.events_japbel[
            self.events_japbel.event_id == 'a1b55211-a292-4294-887b-5385cc3c5705'
        ]
        action = sb.convert_to_actions(event, self.id_bel).iloc[0]
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
    def test_convert_time(self, period: int, timestamp: str, minute: int, second: int) -> None:
        event = self.events_japbel[
            self.events_japbel.event_id == 'a1b55211-a292-4294-887b-5385cc3c5705'
        ].copy()
        event['period_id'] = period
        event['timestamp'] = timestamp
        event['minute'] = minute
        event['second'] = second
        action = sb.convert_to_actions(event, self.id_bel).iloc[0]
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

    def test_convert_pass(self) -> None:
        pass_event = self.events_japbel[
            self.events_japbel.event_id == 'a1b55211-a292-4294-887b-5385cc3c5705'
        ]
        pass_action = sb.convert_to_actions(pass_event, self.id_bel).iloc[0]
        assert pass_action['team_id'] == 782
        assert pass_action['player_id'] == 3289
        assert pass_action['type_id'] == spadl.actiontypes.index('pass')
        assert pass_action['result_id'] == spadl.results.index('success')
        assert pass_action['bodypart_id'] == spadl.bodyparts.index('foot')

    def test_convert_own_goal(self) -> None:
        events_morira = self.SBL.events(7577)
        own_goal_for_event = events_morira[
            events_morira.event_id == '8981bc58-6041-4b78-95c5-ebe9677ca379'
        ]
        own_goal_for_actions = sb.convert_to_actions(own_goal_for_event, 797)
        assert len(own_goal_for_actions) == 0
        own_goal_against_event = events_morira[
            events_morira.event_id == 'cef0fcb6-28d0-49d7-8f93-4a0aef28001a'
        ]
        own_goal_against_actions = sb.convert_to_actions(own_goal_against_event, 797)
        assert len(own_goal_against_actions) == 1
        assert own_goal_against_actions.iloc[0]['type_id'] == spadl.actiontypes.index('bad_touch')
        assert own_goal_against_actions.iloc[0]['result_id'] == spadl.results.index('owngoal')
        assert own_goal_against_actions.iloc[0]['bodypart_id'] == spadl.bodyparts.index('foot')
