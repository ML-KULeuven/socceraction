import pandas as pd
from socceraction.spadl import config as spadlcfg
from socceraction.types import Features, GameStates, Mask, SPADLActions

from ..utils import ftype


@ftype("actions")
def player_possession_time(actions: SPADLActions, mask: Mask) -> Features:
    """Get the time (sec) a player was in ball possession before attempting the action.

    We only look at the dribble preceding the action and reset the possession
    time after a defensive interception attempt or a take-on.

    Parameters
    ----------
    actions : SPADLActions
        The actions of a game.
    mask : Mask
        A boolean mask to filter actions.

    Returns
    -------
    Features
        The 'player_possession_time' of each action.
    """
    cur_action = actions.loc[mask, ["period_id", "time_seconds", "player_id", "type_id"]]
    prev_action = (
        actions.copy().shift(1).loc[mask, ["period_id", "time_seconds", "player_id", "type_id"]]
    )
    df = cur_action.join(prev_action, rsuffix="_prev")
    same_player = df.player_id == df.player_id_prev
    same_period = df.period_id == df.period_id_prev
    prev_dribble = df.type_id_prev == spadlcfg.actiontypes.index("dribble")
    mask = same_period & same_player & prev_dribble
    df.loc[mask, "player_possession_time"] = (
        df.loc[mask, "time_seconds"] - df.loc[mask, "time_seconds_prev"]
    )
    return df[["player_possession_time"]].fillna(0.0)


@ftype("gamestates")
def team(gamestates: GameStates, mask: Mask) -> Features:
    """Check whether the possession changed during the game state.

    For each action in the game state, True if the team that performed the
    action is the same team that performed the last action of the game state;
    otherwise False.

    Parameters
    ----------
    gamestates : GameStates
        The game states of a game.
    mask : Mask
        A boolean mask to filter game states.

    Returns
    -------
    Features
        A dataframe with a column 'team_ai' for each <nb_prev_actions> indicating
        whether the team that performed action a0 is in possession.
    """
    a0 = gamestates[0]
    teamdf = pd.DataFrame(index=a0.index)
    for i, a in enumerate(gamestates[1:]):
        teamdf["team_" + (str(i + 1))] = a.loc[mask].team_id == a0.team_id
    return teamdf.loc[mask]
