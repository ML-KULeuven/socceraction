import socceraction.spadl.config as spadl
import socceraction.atomic.spadl as atomicspadl

import pandas as pd
import numpy as np
from socceraction.vaep.features import (
    gamestates,
    simple,
    actiontype,
    bodypart,
    bodypart_onehot,
    time,
    team,
    time_delta
)

_spadlcolumns = [
    "game_id",
    "period_id",
    "time_seconds",
    "timestamp",
    "team_id",
    "player_id",
    "x",
    "y",
    "dx",
    "dy",
    "bodypart_id",
    "bodypart_name",
    "type_id",
    "type_name",
]

_dummy_actions = pd.DataFrame(np.zeros((10, len(_spadlcolumns))), columns=_spadlcolumns)
for c in _spadlcolumns:
    if "name" in c:
        _dummy_actions[c] = _dummy_actions[c].astype(str)

def feature_column_names(fs, nb_prev_actions=3):
    gs = gamestates(_dummy_actions, nb_prev_actions)
    return list(pd.concat([f(gs) for f in fs], axis=1).columns)


def play_left_to_right(gamestates, home_team_id):
    a0 = gamestates[0]
    away_idx = a0.team_id != home_team_id
    for actions in gamestates:
        actions.loc[away_idx,"x"] = spadl.field_length - actions[away_idx]["x"].values
        actions.loc[away_idx,"y"] = spadl.field_width - actions[away_idx]["y"].values
        actions.loc[away_idx,"dx"] = - actions[away_idx]["dx"].values
        actions.loc[away_idx,"dy"] = - actions[away_idx]["dy"].values
    return gamestates


@simple
def actiontype_onehot(actions):
    X = pd.DataFrame()
    for type_name in atomicspadl.actiontypes:
        col = "type_" + type_name
        X[col] = actions["type_name"] == type_name
    return X


@simple
def location(actions):
    return actions[["x","y"]]

_goal_x = spadl.field_length
_goal_y = spadl.field_width / 2

@simple
def polar(actions):
    polardf = pd.DataFrame()
    dx = abs(_goal_x - actions["x"])
    dy = abs(_goal_y - actions["y"])
    polardf["dist_to_goal"] = np.sqrt(dx ** 2 + dy ** 2)
    with np.errstate(divide="ignore", invalid="ignore"):
        polardf["angle_to_goal"] = np.nan_to_num(np.arctan(dy / dx))
    return polardf

@simple
def movement_polar(actions):
    mov = pd.DataFrame()
    mov["mov_d"] = np.sqrt(actions.dx ** 2 + actions.dy ** 2)
    with np.errstate(divide="ignore", invalid="ignore"):
        mov["mov_angle"] = np.arctan2(actions.dy,actions.dx)
        mov.loc[actions.dy == 0,"mov_angle"] = 0 # fix float errors
    return mov

@simple
def direction(actions):
    mov = pd.DataFrame()
    totald = np.sqrt(actions.dx ** 2 + actions.dy ** 2)
    for d in ["dx","dy"]:
        # we don't want to give away the end location,
        # just the direction of the ball
        # We also don't want to divide by zero
        mov[d] = actions[d].mask(totald > 0, actions[d] / totald)

    return mov



def goalscore(gamestates):
    """
    This function determines the nr of goals scored by each team after the 
    action
    """
    actions = gamestates[0]
    teamA = actions["team_id"].values[0]
    goals = actions["type_name"].str.contains("goal")
    owngoals = actions["type_name"].str.contains("owngoal")

    teamisA = actions["team_id"] == teamA
    teamisB = ~teamisA
    goalsteamA = (goals & teamisA) | (owngoals & teamisB)
    goalsteamB = (goals & teamisB) | (owngoals & teamisA)
    goalscoreteamA = goalsteamA.cumsum() - goalsteamA
    goalscoreteamB = goalsteamB.cumsum() - goalsteamB

    scoredf = pd.DataFrame()
    scoredf["goalscore_team"] = (goalscoreteamA * teamisA) + (goalscoreteamB * teamisB)
    scoredf["goalscore_opponent"] = (goalscoreteamB * teamisA) + (
        goalscoreteamA * teamisB
    )
    scoredf["goalscore_diff"] = (
        scoredf["goalscore_team"] - scoredf["goalscore_opponent"]
    )
    return scoredf