"""Implements the feature tranformers for atomic-SPADL."""

import numpy as np
import pandas as pd

import socceraction.atomic.spadl.config as atomicspadl
from socceraction.types import AtomicSPADLActions, Features, GameStates, Mask
from socceraction.attributes.utils import ftype

@ftype("actions")
def actiontype_onehot(actions: AtomicSPADLActions, mask: Mask) -> Features:
    """Get the one-hot-encoded type of each action.

    Parameters
    ----------
    actions : Actions
        The actions of a game.

    Returns
    -------
    Features
        A one-hot encoding of each action's type.
    """
    X = {}
    for type_id, type_name in enumerate(atomicspadl.actiontypes):
        col = 'actiontype_' + type_name
        X[col] = actions['type_id'] == type_id
    return pd.DataFrame(X, index=actions.index).loc[mask]


@ftype("actions")
def location(actions: AtomicSPADLActions, mask: Mask) -> Features:
    """Get the location where each action started.

    Parameters
    ----------
    actions : Actions
        The actions of a game.

    Returns
    -------
    Features
        The 'x' and 'y' location of each action.
    """
    return actions.loc[mask, ['x', 'y']]


_goal_x = atomicspadl.field_length
_goal_y = atomicspadl.field_width / 2


@ftype("actions")
def polar(actions: AtomicSPADLActions, mask: Mask) -> Features:
    """Get the polar coordinates of each action's start location.

    The center of the opponent's goal is used as the origin.

    Parameters
    ----------
    actions : Actions
        The actions of a game.

    Returns
    -------
    Features
        The 'dist_to_goal' and 'angle_to_goal' of each action.
    """
    polardf = pd.DataFrame(index=actions.index)
    dx = (_goal_x - actions['x']).abs().values
    dy = (_goal_y - actions['y']).abs().values
    polardf['dist_to_goal'] = np.sqrt(dx**2 + dy**2)
    with np.errstate(divide='ignore', invalid='ignore'):
        polardf['angle_to_goal'] = np.nan_to_num(np.arctan(dy / dx))
    return polardf.loc[mask]


@ftype("actions")
def movement_polar(actions: AtomicSPADLActions, mask: Mask) -> Features:
    """Get the distance covered and direction of each action.

    Parameters
    ----------
    actions : Actions
        The actions of a game.

    Returns
    -------
    Features
        The distance covered ('mov_d') and direction ('mov_angle') of each action.
    """
    mov = pd.DataFrame(index=actions.index)
    mov['mov_d'] = np.sqrt(actions.dx**2 + actions.dy**2)
    with np.errstate(divide='ignore', invalid='ignore'):
        mov['mov_angle'] = np.arctan2(actions.dy, actions.dx)
        mov.loc[actions.dy == 0, 'mov_angle'] = 0  # fix float errors
    return mov.loc[mask]


@ftype("actions")
def direction(actions: AtomicSPADLActions, mask: Mask) -> Features:
    """Get the direction of the action as components of the unit vector.

    Parameters
    ----------
    actions : Actions
        The actions of a game.

    Returns
    -------
    Features
        The x-component ('dx') and y-compoment ('mov_angle') of the unit
        vector of each action.
    """
    mov = pd.DataFrame(index=actions.index)
    totald = np.sqrt(actions.dx**2 + actions.dy**2)
    for d in ['dx', 'dy']:
        # we don't want to give away the end location,
        # just the direction of the ball
        # We also don't want to divide by zero
        mov[d] = actions[d].mask(totald > 0, actions[d] / totald)

    return mov.loc[mask]


@ftype("gamestates")
def goalscore(gamestates: GameStates, mask: Mask) -> Features:
    """Get the number of goals scored by each team after the action.

    Parameters
    ----------
    gamestates : GameStates
        The gamestates of a game.

    Returns
    -------
    Features
        The number of goals scored by the team performing the last action of the
        game state ('goalscore_team'), by the opponent ('goalscore_opponent'),
        and the goal difference between both teams ('goalscore_diff').
    """
    actions = gamestates[0]
    teamA = actions['team_id'].values[0]
    goals = actions.type_name == 'goal'
    owngoals = actions['type_name'].str.contains('owngoal')

    teamisA = actions['team_id'] == teamA
    teamisB = ~teamisA
    goalsteamA = (goals & teamisA) | (owngoals & teamisB)
    goalsteamB = (goals & teamisB) | (owngoals & teamisA)
    goalscoreteamA = goalsteamA.cumsum() - goalsteamA
    goalscoreteamB = goalsteamB.cumsum() - goalsteamB

    scoredf = pd.DataFrame(index=actions.index)
    scoredf['goalscore_team'] = (goalscoreteamA * teamisA) + (goalscoreteamB * teamisB)
    scoredf['goalscore_opponent'] = (goalscoreteamB * teamisA) + (goalscoreteamA * teamisB)
    scoredf['goalscore_diff'] = scoredf['goalscore_team'] - scoredf['goalscore_opponent']
    return scoredf.loc[mask]
