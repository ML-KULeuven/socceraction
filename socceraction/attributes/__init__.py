"""A collection of attribute (i.e., feature and label) generators.

There are three types of generators:

gamestates
   Generators which calculate a set of attributes based on a SPADL action and
   the N previous actions (i.e., action context). The input is a list of
   gamestates. Internally each game state is represented as a list of SPADL
   action dataframes :math:`[a_0, a_1, ...]` where each row in the :math:`a_i`
   dataframe contains the previous action of the action in the same row in the
   :math:`a_{i-1}` dataframe. :math:`a_0` is the action for which the attributes
   are calculated.

actions
   Generators which calculate a set of attributes based on the current and
   all preceding actions. The input is a :class:`pandas.DataFrame` of actions
   in SPADL format and a boolean mask to select the actions for which attributes
   should be computed.

events
   Generators which calculate a set of attributes based on the original
   event data. These generators are provider-specific. The input is
   a :class:`pandas.DataFrame` of events and a series with event IDs to select
   the actions for which attributes should be computed.

The types are specified using the ``ftype`` decorator. Only functions, which
have a parameter called "ftype" are seen by socceraction as a generator. Others
will not be calculated.

As the "gamestates" and "actions" generators compute attributes from
SPADL actions, they work for all data providers that are
supported by the socceraction library.
"""

from .spadl.actiontype import (
    actiontype,
    actiontype_onehot,
)
from .spadl.result import (
    result,
    result_onehot,
    actiontype_result_onehot,
)
from .spadl.bodypart import (
    bodypart,
    bodypart_detailed,
    bodypart_onehot,
    bodypart_detailed_onehot,
)
from .spadl.location import (
    startlocation,
    endlocation,
    startpolar,
    endpolar,
    movement,
)
from .spadl.sequence import (
    time_delta,
    space_delta,
    speed,
)
from .spadl.contextual import (
    time,
    goalscore
)
from .spadl.possession import (
    player_possession_time,
    team
)
from .spadl.shot import (
    goal_from_shot,
    shot_dist,
    shot_location,
    shot_angle,
    shot_visible_angle,
    shot_relative_angle,
    post_dribble,
    assist_type,
    fastbreak,
    rebound,
    caley_grid,
)
from .event.statsbomb import (
    statsbomb_open_goal,
    statsbomb_first_touch,
    statsbomb_free_projection,
    statsbomb_goalkeeper_position,
    statsbomb_defenders_position,
    statsbomb_assist,
    statsbomb_counterattack,
    statsbomb_shot_impact_height,
)

__all__ = [
    "actiontype",
    "actiontype_onehot",
    "result",
    "result_onehot",
    "actiontype_result_onehot",
    "bodypart",
    "bodypart_detailed",
    "bodypart_onehot",
    "bodypart_detailed_onehot",
    "startlocation",
    "endlocation",
    "startpolar",
    "endpolar",
    "movement",
    "time_delta",
    "space_delta",
    "speed",
    "player_possession_time",
    "team",
    "time",
    "goalscore",
    "goal_from_shot",
    "shot_dist",
    "shot_location",
    "shot_angle",
    "shot_visible_angle",
    "shot_relative_angle",
    "post_dribble",
    "assist_type",
    "fastbreak",
    "rebound",
    "caley_grid",
    "statsbomb_open_goal",
    "statsbomb_first_touch",
    "statsbomb_free_projection",
    "statsbomb_goalkeeper_position",
    "statsbomb_defenders_position",
    "statsbomb_assist",
    "statsbomb_counterattack",
    "statsbomb_shot_impact_height",
]
