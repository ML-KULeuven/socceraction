import math
from pandera.typing import DataFrame

import pytest

import socceraction.attributes as fs
import socceraction.spadl as spadl
from socceraction.spadl import SPADLSchema


def test_actiontype(spadl_actions: DataFrame[SPADLSchema]) -> None:
    ltr_actions = spadl.play_left_to_right(spadl_actions, 782)
    out = fs.actiontype(ltr_actions)
    assert out.shape == (len(spadl_actions), 1)


def test_actiontype_onehot(spadl_actions: DataFrame[SPADLSchema]) -> None:
    ltr_actions = spadl.play_left_to_right(spadl_actions, 782)
    out = fs.actiontype_onehot(ltr_actions)
    assert out.shape == (len(spadl_actions), len(spadl.config.actiontypes))


def test_result(spadl_actions: DataFrame[SPADLSchema]) -> None:
    ltr_actions = spadl.play_left_to_right(spadl_actions, 782)
    out = fs.result(ltr_actions)
    assert out.shape == (len(spadl_actions), 1)


def test_result_onehot(spadl_actions: DataFrame[SPADLSchema]) -> None:
    ltr_actions = spadl.play_left_to_right(spadl_actions, 782)
    out = fs.result_onehot(ltr_actions)
    assert out.shape == (len(spadl_actions), len(spadl.config.results))


def test_actiontype_result_onehot(spadl_actions: DataFrame[SPADLSchema]) -> None:
    ltr_actions = spadl.play_left_to_right(spadl_actions, 782)
    out = fs.actiontype_result_onehot(ltr_actions)
    assert out.shape == (
        len(spadl_actions),
        len(spadl.config.actiontypes) * len(spadl.config.results),
    )


def test_bodypart(spadl_actions: DataFrame[SPADLSchema]) -> None:
    ltr_actions = spadl.play_left_to_right(spadl_actions, 782)
    out = fs.bodypart(ltr_actions)
    assert out.shape == (len(spadl_actions), 1)


def test_bodypart_onehot(spadl_actions: DataFrame[SPADLSchema]) -> None:
    ltr_actions = spadl.play_left_to_right(spadl_actions, 782)
    out = fs.bodypart_onehot(ltr_actions)
    assert out.shape == (len(spadl_actions), 4)


def test_player_possession_time(spadl_actions: DataFrame[SPADLSchema]) -> None:
    out = fs.player_possession_time(spadl_actions)
    assert out.shape == (len(spadl_actions), 1)
    assert "player_possession_time" in out.columns
    assert out.loc[10, "player_possession_time"] == pytest.approx(0.359999, 1e-4)
    assert out.loc[11, "player_possession_time"] == 0.0
    assert out.loc[12, "player_possession_time"] == pytest.approx(1.519999, 1e-4)

def test_shot_angle(shot):
    # Test output feature names
    df = fs.shot_angle(shot, mask=[True])
    assert df.columns.tolist() == ["angle_shot"]
    assert len(df) == 1
    # Ball on goalline in center of goal
    shot.loc[:, ["start_x", "start_y"]] = [105, 34]
    df = fs.shot_angle(shot, mask=[True])
    assert df.loc[0, "angle_shot"] == 0
    # Ball on goalline next to goal
    shot.loc[:, ["start_x", "start_y"]] = [105, 0]
    df = fs.shot_angle(shot, mask=[True])
    assert df.loc[0, "angle_shot"] == math.pi / 2
    # Constant as ball moves away from goal
    shot.loc[:, ["start_x", "start_y"]] = [105 - 10, 34]
    df1 = fs.shot_angle(shot, mask=[True])
    shot.loc[:, ["start_x", "start_y"]] = [105 - 11, 34]
    df2 = fs.shot_angle(shot, mask=[True])
    assert df1.loc[0, "angle_shot"] == df2.loc[0, "angle_shot"]
    # Is the same left and right from the goal
    shot.loc[:, ["start_x", "start_y"]] = [105 - 11, 34 - 5]
    df1 = fs.shot_angle(shot, mask=[True])
    shot.loc[:, ["start_x", "start_y"]] = [105 - 11, 34 + 5]
    df2 = fs.shot_angle(shot, mask=[True])
    assert df1.loc[0, "angle_shot"] == df2.loc[0, "angle_shot"]


def test_shot_visible_angle(shot):
    # Test output feature names
    df = fs.shot_visible_angle(shot, mask=[True])
    assert df.columns.tolist() == ["visible_angle_shot"]
    assert len(df) == 1
    # Ball on goalline in center of goal
    shot.loc[:, ["start_x", "start_y"]] = [105, 34]
    df = fs.shot_visible_angle(shot, mask=[True])
    assert df.loc[0, "visible_angle_shot"] == math.pi
    # Ball on goalline next to goal
    shot.loc[:, ["start_x", "start_y"]] = [105, 0]
    df = fs.shot_visible_angle(shot, mask=[True])
    assert df.loc[0, "visible_angle_shot"] == 0
    # Decreases as ball moves away from goal
    shot.loc[:, ["start_x", "start_y"]] = [105 - 10, 34]
    df1 = fs.shot_visible_angle(shot, mask=[True])
    shot.loc[:, ["start_x", "start_y"]] = [105 - 11, 34]
    df2 = fs.shot_visible_angle(shot, mask=[True])
    assert df1.loc[0, "visible_angle_shot"] > df2.loc[0, "visible_angle_shot"]
    # Is the same left and right from the goal
    shot.loc[:, ["start_x", "start_y"]] = [105 - 11, 34 - 5]
    df1 = fs.shot_visible_angle(shot, mask=[True])
    shot.loc[:, ["start_x", "start_y"]] = [105 - 11, 34 + 5]
    df2 = fs.shot_visible_angle(shot, mask=[True])
    assert df1.loc[0, "visible_angle_shot"] == df2.loc[0, "visible_angle_shot"]


def test_shot_relative_angle(shot):
    # Test output feature names
    df = fs.shot_relative_angle(shot, mask=[True])
    assert df.columns.tolist() == ["relative_angle_shot"]
    assert len(df) == 1
    # Ball on goalline in center of goal
    shot.loc[:, ["start_x", "start_y"]] = [105, 34]
    df = fs.shot_relative_angle(shot, mask=[True])
    assert df.loc[0, "relative_angle_shot"] == 1
    # Ball on goalline next to goal
    shot.loc[:, ["start_x", "start_y"]] = [105, 0]
    df = fs.shot_relative_angle(shot, mask=[True])
    assert df.loc[0, "relative_angle_shot"] == 0
    shot.loc[:, ["start_x", "start_y"]] = [105, 68]
    df = fs.shot_relative_angle(shot, mask=[True])
    assert df.loc[0, "relative_angle_shot"] == 0
    # Decreases as ball moves away from goal
    shot.loc[:, ["start_x", "start_y"]] = [100, 20]
    df1 = fs.shot_relative_angle(shot, mask=[True])
    shot.loc[:, ["start_x", "start_y"]] = [100, 10]
    df2 = fs.shot_relative_angle(shot, mask=[True])
    assert df1.loc[0, "relative_angle_shot"] > df2.loc[0, "relative_angle_shot"]
    # Is the same left and right from the goal
    shot.loc[:, ["start_x", "start_y"]] = [105 - 11, 34 - 5]
    df1 = fs.shot_relative_angle(shot, mask=[True])
    shot.loc[:, ["start_x", "start_y"]] = [105 - 11, 34 + 5]
    df2 = fs.shot_relative_angle(shot, mask=[True])
    assert df1.loc[0, "relative_angle_shot"] == pytest.approx(df2.loc[0, "relative_angle_shot"])


def test_shot_dist(shot):
    # Test output feature names
    df = fs.shot_dist(shot, mask=[True])
    assert df.columns.tolist() == ["dist_shot"]
    assert len(df) == 1
    # Ball on goalline in center of goal
    shot.loc[:, ["start_x", "start_y"]] = [105, 34]
    df = fs.shot_dist(shot, mask=[True])
    assert df.loc[0, "dist_shot"] == 0
    # Ball on goalline next to goal
    shot.loc[:, ["start_x", "start_y"]] = [105, 0]
    df = fs.shot_dist(shot, mask=[True])
    assert df.loc[0, "dist_shot"] == 34
    # Ball on penalty spot
    shot.loc[:, ["start_x", "start_y"]] = [105 - 11, 34]
    df = fs.shot_dist(shot, mask=[True])
    assert df.loc[0, "dist_shot"] == 11


def test_shot_location(shot):
    # Test output feature names
    df = fs.shot_location(shot, mask=[True])
    assert df.columns.tolist() == ["dx_shot", "dy_shot"]
    assert len(df) == 1
    # Test feature values
    assert df.loc[0, "dx_shot"] == 11
    assert df.loc[0, "dy_shot"] == 0
