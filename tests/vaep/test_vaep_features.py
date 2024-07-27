import socceraction.spadl as spadl
from pandas import testing as tm
from pandera.typing import DataFrame
from socceraction.spadl import SPADLSchema
from socceraction.vaep import features as fs

xfns = [
    fs.actiontype,
    fs.actiontype_onehot,
    fs.result,
    fs.result_onehot,
    fs.actiontype_result_onehot,
    fs.bodypart,
    fs.bodypart_detailed,
    fs.bodypart_onehot,
    fs.bodypart_detailed_onehot,
    fs.time,
    fs.startlocation,
    fs.endlocation,
    fs.startpolar,
    fs.endpolar,
    fs.movement,
    fs.team,
    fs.time_delta,
    fs.space_delta,
    fs.goalscore,
]


def test_same_index(spadl_actions: DataFrame[SPADLSchema]) -> None:
    """The feature generators should not change the index of the input dataframe."""
    spadl_actions = spadl_actions.set_index(spadl_actions.index + 10)
    game_actions_with_names = spadl.add_names(spadl_actions)
    gamestates = fs.gamestates(game_actions_with_names, 3)
    gamestates = fs.play_left_to_right(gamestates, 782)
    for fn in xfns:
        features = fn(gamestates)
        tm.assert_index_equal(features.index, spadl_actions.index)


def test_time(spadl_actions: DataFrame[SPADLSchema]) -> None:
    gamestates = spadl.to_gamestates(spadl_actions)
    out = fs.time(gamestates)
    assert out.shape == (len(spadl_actions), 9)
    assert out.loc[0, "period_id_a0"] == 1
    assert out.loc[0, "time_seconds_a0"] == 0.533
    assert out.loc[0, "time_seconds_overall_a0"] == 0.533
    assert out.loc[200, "period_id_a0"] == 2
    assert out.loc[200, "time_seconds_a0"] == 0.671
    assert out.loc[200, "time_seconds_overall_a0"] == 0.671 + 45 * 60


def test_player_possession_time(spadl_actions: DataFrame[SPADLSchema]) -> None:
    gamestates = spadl.to_gamestates(spadl_actions)
    out = fs.player_possession_time(gamestates)
    assert out.shape == (len(spadl_actions), len(gamestates))
    assert "player_possession_time_a0" in out.columns
    assert out.loc[0, "player_possession_time_a0"] == 0.0
    assert out.loc[1, "player_possession_time_a0"] == 0.0
    assert out.loc[2, "player_possession_time_a0"] == 0.881


def test_time_delta(spadl_actions: DataFrame[SPADLSchema]) -> None:
    gamestates = spadl.to_gamestates(spadl_actions)
    out = fs.time_delta(gamestates)
    assert out.shape == (len(spadl_actions), 2)
    # Start of H1
    print(out)
    assert out.loc[0, "time_delta_1"] == 0.0
    assert out.loc[0, "time_delta_2"] == 0.0
    assert out.loc[1, "time_delta_1"] == 0.719
    assert out.loc[1, "time_delta_2"] == 0.719
    assert out.loc[2, "time_delta_1"] == 0.881
    assert out.loc[2, "time_delta_2"] == 1.6
    # Start of H2
    assert out.loc[200, "time_delta_1"] == 0.0
    assert out.loc[200, "time_delta_2"] == 0.0
    assert out.loc[201, "time_delta_1"] == 1.32
    assert out.loc[201, "time_delta_2"] == 1.32
