import os
from typing import cast

import pandas as pd
import pytest
from pandas.testing import assert_frame_equal
from socceraction import features as fs
from socceraction import spadl
from socceraction.atomic.spadl import convert_to_atomic
from socceraction.data import transforms as T
from socceraction.data.providers.statsbomb import StatsBombLoader
from socceraction.features.utils import feature_generator
from socceraction.spadl import statsbomb as sb


class TestTransforms:
    SBL: StatsBombLoader
    game: pd.Series
    home_team_id: int
    away_team_id: int
    events: pd.DataFrame
    actions: pd.DataFrame
    atomic_actions: pd.DataFrame

    def setup_method(self) -> None:
        # Load data
        # (https://raw.githubusercontent.com/statsbomb/open-data/master/data/events/7584.json)
        data_dir = os.path.join(
            os.path.dirname(__file__), os.pardir, "datasets", "statsbomb", "raw"
        )
        self.SBL = StatsBombLoader(root=data_dir, getter="local")
        self.game = self.SBL.games(competition_id=43, season_id=3).set_index("game_id").loc[7584]
        self.home_team_id = int(self.game["home_team_id"])
        self.away_team_id = int(self.game["away_team_id"])
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
        df_ltr = cast(
            pd.DataFrame, spadl.utils.play_left_to_right(self.actions, self.home_team_id)
        )
        df_features_expected = df_ltr[
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
        df_ltr = cast(
            pd.DataFrame, spadl.utils.play_left_to_right(self.actions, self.home_team_id)
        )
        df_features_expected = df_ltr[
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
        to_features = T.ActionsToFeatures(xfns=["shot_statsbomb_xg"])
        with pytest.raises(Exception) as exc_info:
            to_features(self.game, self.actions)
        assert type(exc_info.value.__cause__) is ValueError

    def test_actions_to_features_subset_generator(self) -> None:
        @feature_generator("actions", features=["subset_action_feat"])
        def subset_fn(actions: pd.DataFrame, mask: pd.Series) -> pd.DataFrame:
            subset = actions.iloc[:10]
            return pd.DataFrame({"subset_action_feat": [1.0] * len(subset)}, index=subset.index)

        to_features = T.ActionsToFeatures(xfns=[subset_fn])
        df_features = to_features(self.game, self.actions)
        assert len(df_features) == len(self.actions)
        assert "subset_action_feat" in df_features.columns
        assert df_features["subset_action_feat"].notna().sum() == 10
        assert df_features["subset_action_feat"].isna().sum() == len(self.actions) - 10

    def test_statsbomb_events_to_features_subset_generator(self) -> None:
        @feature_generator("events", features=["pass_length"])
        def subset_events_fn(events: pd.DataFrame, mask: pd.Series) -> pd.DataFrame:
            is_pass = events["type_name"] == "Pass"
            subset = events.loc[is_pass]
            lengths = subset["extra"].apply(lambda x: x.get("pass", {}).get("length", 0.0))
            return pd.DataFrame({"pass_length": lengths}, index=subset.index)

        to_features = T.StatsBombEventsToFeatures(xfns=[subset_events_fn])
        df_features = to_features(self.game, self.events)
        assert len(df_features) == len(self.events)
        assert "pass_length" in df_features.columns
        assert df_features["pass_length"].notna().sum() > 0
        assert df_features["pass_length"].isna().sum() > 0

    def test_statsbomb_events_to_features_with_mask(self) -> None:
        @feature_generator("events", features=["pass_length"])
        def subset_events_fn(events: pd.DataFrame, mask: pd.Series) -> pd.DataFrame:
            is_pass = events["type_name"] == "Pass"
            subset = events.loc[is_pass & mask]
            lengths = subset["extra"].apply(lambda x: x.get("pass", {}).get("length", 0.0))
            return pd.DataFrame({"pass_length": lengths}, index=subset.index)

        to_features = T.StatsBombEventsToFeatures(
            xfns=[subset_events_fn],
            mask_fn=lambda events: events["team_id"] == self.home_team_id,
        )
        df_features = to_features(self.game, self.events)
        expected_len = (self.events["team_id"] == self.home_team_id).sum()
        assert len(df_features) == expected_len
        assert "pass_length" in df_features.columns
