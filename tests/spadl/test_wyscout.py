import os

import pandas as pd
from socceraction.data.providers.wyscout import PublicWyscoutLoader
from socceraction.spadl import SPADLSchema
from socceraction.spadl import config as spadl
from socceraction.spadl import wyscout as wy


class TestSpadlConvertor:
    def setup_method(self) -> None:
        data_dir = os.path.join(
            os.path.dirname(__file__), os.pardir, "datasets", "wyscout_public", "raw"
        )
        self.WSL = PublicWyscoutLoader(root=data_dir, download=False)
        self.events = self.WSL.events(2058007)

    def test_convert_to_actions(self) -> None:
        df_actions = wy.convert_to_actions(self.events, 5629)
        assert len(df_actions) > 0
        SPADLSchema.validate(df_actions)
        assert (df_actions.game_id == 2058007).all()
        assert ((df_actions.team_id == 5629) | (df_actions.team_id == 12913)).all()

    def test_insert_interception_passes(self) -> None:
        event = pd.DataFrame(
            [
                {
                    "type_id": 8,
                    "subtype_name": "Head pass",
                    "tags": [{"id": 102}, {"id": 1401}, {"id": 1801}],  # own goal
                    "player_id": 38093,
                    "positions": [{"y": 56, "x": 5}, {"y": 100, "x": 100}],
                    "game_id": 2499737,
                    "type_name": "Pass",
                    "team_id": 1610,
                    "period_id": 2,
                    "milliseconds": 2184.793924,
                    "subtype_id": 82,
                    "event_id": 180427412,
                }
            ]
        )
        actions = wy.convert_to_actions(event, 1610)
        assert len(actions) == 2
        assert actions.at[0, "type_id"] == spadl.actiontypes.index("interception")
        assert actions.at[1, "type_id"] == spadl.actiontypes.index("bad_touch")
        assert actions.at[0, "result_id"] == spadl.results.index("success")
        assert actions.at[1, "result_id"] == spadl.results.index("owngoal")

    def test_convert_own_goal(self) -> None:
        events_morira = self.WSL.events(2057961)
        own_goal_event = events_morira[events_morira.event_id == 258696133]
        own_goal_actions = wy.convert_to_actions(own_goal_event, 16216)
        assert len(own_goal_actions) == 2  # interception + clearance
        assert own_goal_actions.iloc[0]["type_id"] == spadl.actiontypes.index("interception")
        assert own_goal_actions.iloc[0]["result_id"] == spadl.results.index("success")
        assert own_goal_actions.iloc[1]["type_id"] == spadl.actiontypes.index("bad_touch")
        assert own_goal_actions.iloc[1]["result_id"] == spadl.results.index("owngoal")
        assert own_goal_actions.iloc[1]["bodypart_id"] == spadl.bodyparts.index("foot")

    def test_convert_own_goal_touches(self) -> None:
        """Tests conversion of own goals following a bad touch.

        Own goals resulting from bad touch events in the Wyscout event
        streams should be included in the SPADL representation.
        """
        # An own goal from the game between Leicester and Stoke on 24 Feb 2018.
        # Stoke's goalkeeper Jack Butland allows a low cross to bounce off his
        # gloves and into the net:
        event = pd.DataFrame(
            [
                {
                    "type_id": 8,
                    "subtype_name": "Cross",
                    "tags": [{"id": 402}, {"id": 801}, {"id": 1802}],
                    "player_id": 8013,
                    "positions": [{"y": 89, "x": 97}, {"y": 0, "x": 0}],
                    "game_id": 2499994,
                    "type_name": "Pass",
                    "team_id": 1631,
                    "period_id": 2,
                    "milliseconds": 1496.7290489999993,
                    "subtype_id": 80,
                    "event_id": 230320305,
                },
                {
                    "type_id": 7,
                    "subtype_name": "Touch",
                    "tags": [{"id": 102}],
                    "player_id": 8094,
                    "positions": [{"y": 50, "x": 1}, {"y": 100, "x": 100}],
                    "game_id": 2499994,
                    "type_name": "Others on the ball",
                    "team_id": 1639,
                    "period_id": 2,
                    "milliseconds": 1497.6330749999993,
                    "subtype_id": 72,
                    "event_id": 230320132,
                },
                {
                    "type_id": 9,
                    "subtype_name": "Reflexes",
                    "tags": [{"id": 101}, {"id": 1802}],
                    "player_id": 8094,
                    "positions": [{"y": 100, "x": 100}, {"y": 50, "x": 1}],
                    "game_id": 2499994,
                    "type_name": "Save attempt",
                    "team_id": 1639,
                    "period_id": 2,
                    "milliseconds": 1499.980547,
                    "subtype_id": 90,
                    "event_id": 230320135,
                },
            ]
        )
        actions = wy.convert_to_actions(event, 1639)
        # FIXME: It adds a dribble between the bad touch of the goalkeeper and
        # his attempt to save the ball before crossing the line. Not sure
        # whether that is ideal.
        assert len(actions) == 4
        assert actions.at[1, "type_id"] == spadl.actiontypes.index("bad_touch")
        assert actions.at[1, "result_id"] == spadl.results.index("owngoal")

    def test_convert_simulations_precede_by_take_on(self) -> None:
        events = pd.DataFrame(
            [
                {
                    "type_id": 1,
                    "subtype_name": "Ground attacking duel",
                    "tags": [{"id": 503}, {"id": 701}, {"id": 1802}],
                    "player_id": 8327,
                    "positions": [{"y": 48, "x": 82}, {"y": 47, "x": 83}],
                    "game_id": 2576263,
                    "type_name": "Duel",
                    "team_id": 3158,
                    "period_id": 2,
                    "milliseconds": 706.309475 * 1000,
                    "subtype_id": 11,
                    "event_id": 240828365,
                },
                {
                    "type_id": 2,
                    "subtype_name": "Simulation",
                    "tags": [{"id": 1702}],
                    "player_id": 8327,
                    "positions": [{"y": 47, "x": 83}, {"y": 0, "x": 0}],
                    "game_id": 2576263,
                    "type_name": "Foul",
                    "team_id": 3158,
                    "period_id": 2,
                    "milliseconds": 709.1020480000002 * 1000,
                    "subtype_id": 25,
                    "event_id": 240828368,
                },
            ]
        )

        actions = wy.convert_to_actions(events, 3158)

        assert len(actions) == 1
        assert actions.at[0, "type_id"] == spadl.actiontypes.index("take_on")
        assert actions.at[0, "result_id"] == spadl.results.index("fail")

    def test_convert_simulations(self) -> None:
        events = pd.DataFrame(
            [
                {
                    "type_id": 8,
                    "subtype_name": "Cross",
                    "tags": [{"id": 402}, {"id": 801}, {"id": 1801}],
                    "player_id": 20472,
                    "positions": [{"y": 76, "x": 92}, {"y": 92, "x": 98}],
                    "game_id": 2575974,
                    "type_name": "Pass",
                    "team_id": 3173,
                    "period_id": 1,
                    "milliseconds": 1010.5460250000001 * 1000,
                    "subtype_id": 80,
                    "event_id": 182640540,
                },
                {
                    "type_id": 1,
                    "subtype_name": "Ground loose ball duel",
                    "tags": [{"id": 701}, {"id": 1802}],
                    "player_id": 116171,
                    "positions": [{"y": 92, "x": 98}, {"y": 43, "x": 87}],
                    "game_id": 2575974,
                    "type_name": "Duel",
                    "team_id": 3173,
                    "period_id": 1,
                    "milliseconds": 1012.8018770000001 * 1000,
                    "subtype_id": 13,
                    "event_id": 182640541,
                },
                {
                    "type_id": 2,
                    "subtype_name": "Simulation",
                    "tags": [{"id": 1702}],
                    "player_id": 116171,
                    "positions": [{"y": 43, "x": 87}, {"y": 100, "x": 100}],
                    "game_id": 2575974,
                    "type_name": "Foul",
                    "team_id": 3173,
                    "period_id": 1,
                    "milliseconds": 1014.7540220000001 * 1000,
                    "subtype_id": 25,
                    "event_id": 182640542,
                },
            ]
        )

        actions = wy.convert_to_actions(events, 3157)

        assert len(actions) == 3
        assert actions.at[2, "type_id"] == spadl.actiontypes.index("take_on")
        assert actions.at[2, "result_id"] == spadl.results.index("fail")
