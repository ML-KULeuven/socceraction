from pandas import testing as tm
from pandera.typing import DataFrame

import socceraction.spadl as spadlcfg
import socceraction.spadl as spadl
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
    spadl_actions.index += 10
    game_actions_with_names = spadlcfg.add_names(spadl_actions)
    gamestates = fs.gamestates(game_actions_with_names, 3)
    gamestates = fs.play_left_to_right(gamestates, 782)
    for fn in xfns:
        features = fn(gamestates)
        tm.assert_index_equal(features.index, spadl_actions.index)


def test_actiontype(spadl_actions: DataFrame[SPADLSchema]) -> None:
    gamestates = fs.gamestates(spadl_actions)
    ltr_gamestates = fs.play_left_to_right(gamestates, 782)
    out = fs.actiontype(ltr_gamestates)
    assert out.shape == (len(spadl_actions), 3)


def test_actiontype_onehot(spadl_actions: DataFrame[SPADLSchema]) -> None:
    gamestates = fs.gamestates(spadl_actions)
    ltr_gamestates = fs.play_left_to_right(gamestates, 782)
    out = fs.actiontype_onehot(ltr_gamestates)
    assert out.shape == (len(spadl_actions), len(spadl.config.actiontypes) * 3)


def test_result(spadl_actions: DataFrame[SPADLSchema]) -> None:
    gamestates = fs.gamestates(spadl_actions)
    ltr_gamestates = fs.play_left_to_right(gamestates, 782)
    out = fs.result(ltr_gamestates)
    assert out.shape == (len(spadl_actions), 3)


def test_result_onehot(spadl_actions: DataFrame[SPADLSchema]) -> None:
    gamestates = fs.gamestates(spadl_actions)
    ltr_gamestates = fs.play_left_to_right(gamestates, 782)
    out = fs.result_onehot(ltr_gamestates)
    assert out.shape == (len(spadl_actions), len(spadl.config.results) * 3)


def test_actiontype_result_onehot(spadl_actions: DataFrame[SPADLSchema]) -> None:
    gamestates = fs.gamestates(spadl_actions)
    ltr_gamestates = fs.play_left_to_right(gamestates, 782)
    out = fs.actiontype_result_onehot(ltr_gamestates)
    assert out.shape == (
        len(spadl_actions),
        len(spadl.config.actiontypes) * len(spadl.config.results) * 3,
    )


def test_bodypart(spadl_actions: DataFrame[SPADLSchema]) -> None:
    gamestates = fs.gamestates(spadl_actions)
    ltr_gamestates = fs.play_left_to_right(gamestates, 782)
    out = fs.bodypart(ltr_gamestates)
    assert out.shape == (len(spadl_actions), 3)


def test_bodypart_onehot(spadl_actions: DataFrame[SPADLSchema]) -> None:
    gamestates = fs.gamestates(spadl_actions)
    ltr_gamestates = fs.play_left_to_right(gamestates, 782)
    out = fs.bodypart_onehot(ltr_gamestates)
    assert out.shape == (len(spadl_actions), 4 * 3)


def test_player_possession_time(spadl_actions: DataFrame[SPADLSchema]) -> None:
    gamestates = fs.gamestates(spadl_actions)
    out = fs.player_possession_time(gamestates)
    assert out.shape == (len(spadl_actions), len(gamestates))
    assert "player_possession_time_a0" in out.columns
    assert out.loc[10, "player_possession_time_a0"] == 0.0
    assert out.loc[11, "player_possession_time_a0"] == 0.0
    assert out.loc[12, "player_possession_time_a0"] == 0.881
