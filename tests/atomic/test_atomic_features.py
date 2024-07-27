import socceraction.atomic.spadl as spadl
from pandas import testing as tm
from pandera.typing import DataFrame
from socceraction.atomic.spadl import AtomicSPADLSchema
from socceraction.atomic.vaep import features as fs

xfns = [
    fs.actiontype,
    fs.actiontype_onehot,
    fs.bodypart,
    fs.bodypart_detailed,
    fs.bodypart_onehot,
    fs.bodypart_detailed_onehot,
    fs.team,
    fs.time,
    fs.time_delta,
    fs.location,
    fs.polar,
    fs.movement_polar,
    fs.direction,
    fs.goalscore,
]


def test_same_index(atomic_spadl_actions: DataFrame[AtomicSPADLSchema]) -> None:
    """The feature generators should not change the index of the input dataframe."""
    atomic_spadl_actions.index += 10
    game_actions_with_names = spadl.add_names(atomic_spadl_actions)
    gamestates = spadl.to_gamestates(game_actions_with_names, 3)
    gamestates = fs.play_left_to_right(gamestates, 782)
    for fn in xfns:
        features = fn(gamestates)
        tm.assert_index_equal(features.index, atomic_spadl_actions.index)
