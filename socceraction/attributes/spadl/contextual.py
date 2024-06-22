import pandas as pd

import socceraction.spadl.config as spadlcfg
from socceraction.types import Actions, Features, GameStates, Mask
from ..utils import ftype


@ftype("actions")
def time(actions: Actions, mask: Mask) -> Features:
    """Get the time when each action was performed.

    This generates the following features:
        :period_id:
            The ID of the period.
        :time_seconds:
            Seconds since the start of the period.
        :time_seconds_overall:
            Seconds since the start of the game. Stoppage time during previous
            periods is ignored.

    Parameters
    ----------
    actions : Actions
        The actions of a game.
    mask : Mask
        A boolean mask to filter actions.

    Returns
    -------
    Features
        The 'period_id', 'time_seconds' and 'time_seconds_overall' when each
        action was performed.
    """
    timedf = actions.loc[mask, ['period_id', 'time_seconds']].copy()
    timedf['time_seconds_overall'] = ((timedf.period_id - 1) * 45 * 60) + timedf.time_seconds
    return timedf


@ftype("gamestates")
def goalscore(gamestates: GameStates, mask: Mask) -> Features:
    """Get the number of goals scored by each team after the action.

    Parameters
    ----------
    gamestates : GameStates
        The gamestates of a game.
    mask : Mask
        A boolean mask to filter gamestates.

    Returns
    -------
    Features
        The number of goals scored by the team performing the last action of the
        game state ('goalscore_team'), by the opponent ('goalscore_opponent'),
        and the goal difference between both teams ('goalscore_diff').
    """
    actions = gamestates[0]
    teamA = actions['team_id'].values[0]
    goals = actions['type_name'].str.contains('shot') & (
        actions['result_id'] == spadlcfg.results.index('success')
    )
    owngoals = actions['type_name'].str.contains('shot') & (
        actions['result_id'] == spadlcfg.results.index('owngoal')
    )
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
