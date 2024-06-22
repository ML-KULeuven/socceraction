import os

import pytest
from socceraction.data.statsbomb import StatsBombLoader
from socceraction.spadl import SPADLSchema
from socceraction.spadl import config as spadl
from socceraction.spadl import statsbomb as sb


class TestSpadlConvertor:
    def setup_method(self) -> None:
        data_dir = os.path.join(
            os.path.dirname(__file__), os.pardir, "datasets", "statsbomb", "raw"
        )
        self.SBL = StatsBombLoader(root=data_dir, getter="local")
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
            self.events_japbel.event_id == "5171bb39-0c6c-4a3d-ae1c-756011dc219f"
        ]
        print(event)
        action = sb.convert_to_actions(event, self.id_bel).iloc[0]
        assert action["start_x"] == ((25.0 - 0.5) / 120) * spadl.field_length
        assert action["start_y"] == 68 - ((26.0 - 0.5) / 80) * spadl.field_width

    def test_convert_end_location(self) -> None:
        event = self.events_japbel[
            self.events_japbel.event_id == "5171bb39-0c6c-4a3d-ae1c-756011dc219f"
        ]
        action = sb.convert_to_actions(event, self.id_bel).iloc[0]
        assert action["end_x"] == ((24.0 - 0.5) / 120) * spadl.field_length
        assert action["end_y"] == spadl.field_width - ((28.0 - 0.5) / 80) * spadl.field_width

    def test_convert_start_location_high_fidelity(self) -> None:
        events = self.SBL.events(9912)
        event = events[events.event_id == "60392108-2599-4875-bcc7-48462d530edf"]
        action = sb.convert_to_actions(event, 217).iloc[0]
        assert action["start_x"] == ((64.0 - 0.05) / 120) * spadl.field_length
        assert action["start_y"] == spadl.field_width - ((73.6 - 0.05) / 80) * spadl.field_width

    @pytest.mark.parametrize(
        "period,timestamp,minute,second,spadl_time",
        [
            (1, "00:00:00.920", 0, 0, 0 * 60 + 0.920),  # FH
            (1, "00:47:09.453", 47, 9, 47 * 60 + 9.453),  # FH extra time
            (2, "00:19:51.740", 64, 51, 19 * 60 + 51.740),  # SH (starts again at 45 min)
            (2, "00:48:10.733", 93, 10, 48 * 60 + 10.733),  # SH extra time
            (3, "00:10:12.188", 100, 12, 10 * 60 + 12.188),  # FH of extensions
            (4, "00:13:31.190", 118, 31, 13 * 60 + 31.190),  # SH of extensions
            (5, "00:02:37.133", 122, 37, 2 * 60 + 37.133),  # Penalties
        ],
    )
    def test_convert_time(
        self, period: int, timestamp: str, minute: int, second: int, spadl_time: float
    ) -> None:
        event = self.events_japbel[
            self.events_japbel.event_id == "5171bb39-0c6c-4a3d-ae1c-756011dc219f"
        ].copy()
        event["period_id"] = period
        event["timestamp"] = timestamp
        event["minute"] = minute
        event["second"] = second
        action = sb.convert_to_actions(event, self.id_bel).iloc[0]
        assert action["period_id"] == period
        assert action["time_seconds"] == spadl_time

    def test_convert_pass(self) -> None:
        pass_event = self.events_japbel[
            self.events_japbel.event_id == "0bc3262b-7cdb-4784-b159-4409317165a7"
        ]
        pass_action = sb.convert_to_actions(pass_event, self.id_bel).iloc[0]
        assert pass_action["team_id"] == 782
        assert pass_action["player_id"] == 3101
        assert pass_action["type_id"] == spadl.actiontypes.index("pass")
        assert pass_action["result_id"] == spadl.results.index("success")
        assert pass_action["bodypart_id"] == spadl.bodyparts.index("foot_right")

    def test_convert_own_goal(self) -> None:
        events_morira = self.SBL.events(7577)
        own_goal_for_event = events_morira[
            events_morira.event_id == "467ab65e-af8b-45d2-b372-06ffb5c71332"
        ]
        own_goal_for_actions = sb.convert_to_actions(own_goal_for_event, 797)
        assert len(own_goal_for_actions) == 0
        own_goal_against_event = events_morira[
            events_morira.event_id == "a21c104e-e944-41a2-91ce-700c5f9ae8e5"
        ]
        own_goal_against_actions = sb.convert_to_actions(own_goal_against_event, 797)
        assert len(own_goal_against_actions) == 1
        assert own_goal_against_actions.iloc[0]["type_id"] == spadl.actiontypes.index("bad_touch")
        assert own_goal_against_actions.iloc[0]["result_id"] == spadl.results.index("owngoal")
        assert own_goal_against_actions.iloc[0]["bodypart_id"] == spadl.bodyparts.index("foot")
