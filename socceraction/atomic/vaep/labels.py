# -*- coding: utf-8 -*-
"""Implements the label tranformers of the Atomic-VAEP framework."""
import pandas as pd
from pandera.typing import DataFrame

import socceraction.atomic.spadl as atomicspadl
from socceraction.atomic.spadl import AtomicSPADLSchema


def scores(actions: DataFrame[AtomicSPADLSchema], nr_actions: int = 10) -> DataFrame:
    """Determine whether a goal was scored by the team possessing the ball within the next x actions.

    Parameters
    ----------
    actions : pd.DataFrame
        The actions of a game.
    nr_actions : int (default = 10)
        Number of actions after the current action to consider.

    Returns
    -------
    pd.DataFrame
        A dataframe with a column 'scores' and a row for each action set to
        True if a goal was scored by the team possessing the ball within the
        next x actions; otherwise False.
    """
    # merging goals, owngoals and team_ids
    goals = actions['type_id'] == atomicspadl.actiontypes.index('goal')
    owngoals = actions['type_id'] == atomicspadl.actiontypes.index('owngoal')
    y = pd.concat([goals, owngoals, actions['team_id']], axis=1)
    y.columns = ['goal', 'owngoal', 'team_id']

    # adding future results
    for i in range(1, nr_actions):
        for c in ['team_id', 'goal', 'owngoal']:
            shifted = y[c].shift(-i)
            shifted[-i:] = y[c][len(y) - 1]
            y['%s+%d' % (c, i)] = shifted

    res = y['goal']
    for i in range(1, nr_actions):
        gi = y['goal+%d' % i] & (y['team_id+%d' % i] == y['team_id'])
        ogi = y['owngoal+%d' % i] & (y['team_id+%d' % i] != y['team_id'])
        res = res | gi | ogi

    return pd.DataFrame(res, columns=['scores'])


def concedes(actions: DataFrame[AtomicSPADLSchema], nr_actions: int = 10) -> DataFrame:
    """Determine whether a goal was conceded by the team possessing the ball within the next x actions.

    Parameters
    ----------
    actions : pd.DataFrame
        The actions of a game.
    nr_actions : int (default = 10)
        Number of actions after the current action to consider.

    Returns
    -------
    pd.DataFrame
        A dataframe with a column 'concedes' and a row for each action set to
        True if a goal was conceded by the team possessing the ball within the
        next x actions; otherwise False.
    """
    # merging goals, owngoals and team_ids
    goals = actions['type_id'] == atomicspadl.actiontypes.index('goal')
    owngoals = actions['type_id'] == atomicspadl.actiontypes.index('owngoal')
    y = pd.concat([goals, owngoals, actions['team_id']], axis=1)
    y.columns = ['goal', 'owngoal', 'team_id']

    # adding future results
    for i in range(1, nr_actions):
        for c in ['team_id', 'goal', 'owngoal']:
            shifted = y[c].shift(-i)
            shifted[-i:] = y[c][len(y) - 1]
            y['%s+%d' % (c, i)] = shifted

    res = y['owngoal']
    for i in range(1, nr_actions):
        gi = y['goal+%d' % i] & (y['team_id+%d' % i] != y['team_id'])
        ogi = y['owngoal+%d' % i] & (y['team_id+%d' % i] == y['team_id'])
        res = res | gi | ogi

    return pd.DataFrame(res, columns=['concedes'])


def goal_from_shot(actions: DataFrame[AtomicSPADLSchema]) -> DataFrame:
    """Determine whether a goal was scored from the current action.

    This label can be use to train an xG model.

    Parameters
    ----------
    actions : pd.DataFrame
        The actions of a game.

    Returns
    -------
    pd.DataFrame
        A dataframe with a column 'goal' and a row for each action set to
        True if a goal was scored from the current action; otherwise False.
    """
    goals = (actions['type_id'] == atomicspadl.actiontypes.index('shot')) & (
        actions['type_id'] == atomicspadl.actiontypes.index('goal')
    )

    return pd.DataFrame(goals, columns=['goal'])
