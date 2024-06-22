from pandas import testing as tm
from pandera.typing import DataFrame

import socceraction.spadl as spadlcfg
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
