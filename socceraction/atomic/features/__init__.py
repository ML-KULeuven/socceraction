"""A collection of feature generators."""

from socceraction.features import (
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

from .spadl import (
    actiontype_onehot,
    direction,
    goalscore,
    location,
    movement_polar,
    polar,
)

__all__ = [
    "actiontype",
    "actiontype_onehot",
    "bodypart",
    "bodypart_detailed",
    "bodypart_onehot",
    "bodypart_detailed_onehot",
    "team",
    "time",
    "time_delta",
    "speed",
    "location",
    "polar",
    "movement_polar",
    "direction",
    "goalscore",
    "player_possession_time",
]
