"""Implements the feature tranformers of the VAEP framework."""

import socceraction.atomic.spadl as atomicspadl
from socceraction.atomic.attributes import (
    actiontype,
    actiontype_onehot,
    bodypart,
    bodypart_detailed,
    bodypart_detailed_onehot,
    bodypart_onehot,
    direction,
    goalscore,
    location,
    movement_polar,
    player_possession_time,
    polar,
    speed,
    team,
    time,
    time_delta,
)
from socceraction.atomic.attributes.utils import feature_column_names, simple
from socceraction.types import Actions, GameStates
from socceraction.utils import deprecated

__all__ = [
    'feature_column_names',
    'play_left_to_right',
    'gamestates',
    'actiontype',
    'actiontype_onehot',
    'bodypart',
    'bodypart_detailed',
    'bodypart_onehot',
    'bodypart_detailed_onehot',
    'team',
    'time',
    'time_delta',
    'speed',
    'location',
    'polar',
    'movement_polar',
    'direction',
    'goalscore',
    'player_possession_time',
]

actiontype = simple(actiontype)
actiontype_onehot = simple(actiontype_onehot)
bodypart = simple(bodypart)
bodypart_detailed = simple(bodypart_detailed)
bodypart_onehot = simple(bodypart_onehot)
bodypart_detailed_onehot = simple(bodypart_detailed_onehot)
time = simple(time)
player_possession_time = simple(player_possession_time)
location = simple(location)
polar = simple(polar)
movement_polar = simple(movement_polar)
direction = simple(direction)


@deprecated('Use socceraction.atomic_spadl.play_left_to_right instead.')
def play_left_to_right(gamestates: GameStates, home_team_id: int) -> GameStates:
    """See socceraction.atomic_spadl.play_left_to_right."""
    return atomicspadl.play_left_to_right(gamestates, home_team_id)


@deprecated('Use socceraction.atomic_spadl.to_gamestates instead.')
def gamestates(actions: Actions, nb_prev_actions: int = 3) -> GameStates:
    """See socceraction.atomic_spadl.to_gamestates."""
    return atomicspadl.to_gamestates(actions, nb_prev_actions)
