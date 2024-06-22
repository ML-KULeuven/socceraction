"""Implements the feature tranformers of the VAEP framework."""
from socceraction import spadl
from socceraction.attributes import (
    actiontype,
    actiontype_onehot,
    actiontype_result_onehot,
    bodypart,
    bodypart_detailed,
    bodypart_detailed_onehot,
    bodypart_onehot,
    endlocation,
    endpolar,
    goalscore,
    movement,
    player_possession_time,
    result,
    result_onehot,
    space_delta,
    speed,
    startlocation,
    startpolar,
    team,
    time,
    time_delta,
)
from socceraction.attributes.utils import feature_column_names, simple
from socceraction.types import Actions, GameStates
from socceraction.utils import deprecated

actiontype = simple(actiontype)
actiontype_onehot = simple(actiontype_onehot)
result = simple(result)
result_onehot = simple(result_onehot)
actiontype_result_onehot = simple(actiontype_result_onehot)
bodypart = simple(bodypart)
bodypart_detailed = simple(bodypart_detailed)
bodypart_onehot = simple(bodypart_onehot)
bodypart_detailed_onehot = simple(bodypart_detailed_onehot)
time = simple(time)
startlocation = simple(startlocation)
endlocation = simple(endlocation)
startpolar = simple(startpolar)
endpolar = simple(endpolar)
movement = simple(movement)
player_possession_time = simple(player_possession_time)


@deprecated('Use socceraction.spadl.to_gamestates instead.')
def gamestates(actions: Actions, nb_prev_actions: int = 3) -> GameStates:
    """See socceraction.spadl.to_gamestates."""
    return spadl.to_gamestates(actions, nb_prev_actions)


@deprecated('Use socceraction.spadl.play_left_to_right instead.')
def play_left_to_right(gamestates: GameStates, home_team_id: int) -> GameStates:
    """See socceraction.spadl.play_left_to_right."""
    return spadl.play_left_to_right(gamestates, home_team_id)


__all__ = [
    'actiontype',
    'actiontype_onehot',
    'result',
    'result_onehot',
    'actiontype_result_onehot',
    'bodypart',
    'bodypart_detailed',
    'bodypart_onehot',
    'bodypart_detailed_onehot',
    'time',
    'startlocation',
    'endlocation',
    'startpolar',
    'endpolar',
    'movement',
    'player_possession_time',
    'team',
    'time_delta',
    'space_delta',
    'speed',
    'goalscore',
    'simple',
    'feature_column_names',
    'gamestates',
    'play_left_to_right',
]
