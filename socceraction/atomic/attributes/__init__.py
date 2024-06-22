"""A collection of attribute (i.e., feature and label) generators.
"""
from .spadl import (
    actiontype_onehot,
    location,
    polar,
    movement_polar,
    direction,
    goalscore,
)
from socceraction.attributes import (
    actiontype,
    bodypart,
    bodypart_detailed,
    bodypart_detailed_onehot,
    bodypart_onehot,
    player_possession_time,
    speed,
    team,
    time,
    time_delta,
)

__all__ = [
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
