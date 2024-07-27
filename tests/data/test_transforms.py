import os

import pytest
from pandas.testing import assert_frame_equal
from socceraction import features as fs
from socceraction import spadl
from socceraction.atomic.spadl import convert_to_atomic
from socceraction.data import transforms as T
from socceraction.data.providers.statsbomb import StatsBombLoader
from socceraction.spadl import statsbomb as sb


class TestTransforms:
    def setup_method(self) -> None:
        # Load data
        # (https://raw.githubusercontent.com/statsbomb/open-data/master/data/events/7584.json)
        data_dir = os.path.join(
            os.path.dirname(__file__), os.pardir, "datasets", "statsbomb", "raw"
        )
        self.SBL = StatsBombLoader(root=data_dir, getter="local")
        self.game = self.SBL.games(competition_id=43, season_id=3).set_index("game_id").loc[7584]
        self.home_team_id, self.away_team_id = self.game[["home_team_id", "away_team_id"]]
        self.events = self.SBL.events(game_id=7584)
        # Convert events to actions
        self.actions = sb.convert_to_actions(self.events, self.home_team_id)
        # Convert actions to atomic actions
        self.atomic_actions = convert_to_atomic(self.actions)

    def test_events_to_actions(self) -> None:
        to_actions = T.StatsBombEventsToActions()
        df_actions = to_actions(self.game, self.events)
        assert_frame_equal(df_actions, self.actions)

    def test_actions_to_atomic(self) -> None:
        to_atomic = T.ActionsToAtomic()
        df_atomic_actions = to_atomic(self.game, self.actions)
        assert_frame_equal(df_atomic_actions, self.atomic_actions)

    def test_actions_to_features_from_generators(self) -> None:
        to_features = T.ActionsToFeatures(xfns=[fs.startlocation, fs.endlocation])
        df_features = to_features(self.game, self.actions)
        df_features_expected = spadl.utils.play_left_to_right(
            self.actions,
            self.home_team_id,
        )[
            [
                "game_id",
                "action_id",
                "original_event_id",
                "start_x",
                "start_y",
                "end_x",
                "end_y",
            ]
        ]
        assert_frame_equal(df_features, df_features_expected, check_like=True)

    def test_actions_to_features_from_strings(self) -> None:
        to_features = T.ActionsToFeatures(xfns=["start_x", "end_x"])
        df_features = to_features(self.game, self.actions)
        df_features_expected = spadl.utils.play_left_to_right(
            self.actions,
            self.home_team_id,
        )[
            [
                "game_id",
                "action_id",
                "original_event_id",
                "start_x",
                "end_x",
            ]
        ]
        assert_frame_equal(df_features, df_features_expected, check_like=True)

    def test_actions_to_features_unsupported_ftype(self) -> None:
        to_features = T.ActionsToFeatures(xfns=["statsbomb_xg"])
        with pytest.raises(Exception) as exc_info:
            to_features(self.game, self.actions)
        assert type(exc_info.value.__cause__) is ValueError
