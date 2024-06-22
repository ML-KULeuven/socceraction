"""Utility functions for working with Atomic-SPADL dataframes."""
from typing import Union, cast

import pandas as pd
from pandera.typing import DataFrame

from socceraction.spadl.utils import to_gamestates

from . import config as spadlconfig
from .schema import AtomicSPADLSchema


def add_names(actions: DataFrame[AtomicSPADLSchema]) -> DataFrame[AtomicSPADLSchema]:
    """Add the type name, result name and bodypart name to an Atomic-SPADL dataframe.

    Parameters
    ----------
    actions : pd.DataFrame
        An Atomic-SPADL dataframe.

    Returns
    -------
    pd.DataFrame
        The original dataframe with a 'type_name', 'result_name' and
        'bodypart_name' appended.
    """
    return cast(
        DataFrame[AtomicSPADLSchema],
        actions.drop(columns=['type_name', 'bodypart_name'], errors='ignore')
        .merge(spadlconfig.actiontypes_df(), how='left')
        .merge(spadlconfig.bodyparts_df(), how='left')
        .set_index(actions.index),
    )


def play_left_to_right(
    actions: Union[DataFrame[AtomicSPADLSchema], list[DataFrame[AtomicSPADLSchema]]],
    home_team_id: int,
) -> Union[DataFrame[AtomicSPADLSchema], list[DataFrame[AtomicSPADLSchema]]]:
    """Perform all action in the same playing direction.

    When a dataframe of atomic spadl actions is given, this changes the start and end
    location of each action, such that all actions are performed as if the
    team that executes the action plays from left to right.

    When gamestates are given, this changes the start and end location of each
    action in a gamestate, such that all actions are performed as if the team
    that performs the first action in the gamestate plays from left to right.

    Parameters
    ----------
    actions : pd.DataFrame or list(pd.DataFrame)
        The SPADL actions or gamestates of a game.
    home_team_id : int
        The ID of the home team.

    Raises
    ------
    ValueError
        If the input is not a DataFrame or a list of DataFrames.

    Returns
    -------
    list(pd.DataFrame)
        All actions performed left to right.
    """
    if isinstance(actions, list):
        a0 = actions[0]
        away_idx = a0.team_id != home_team_id
        for a in actions:
            a.loc[away_idx, 'x'] = spadlconfig.field_length - a[away_idx]['x'].values
            a.loc[away_idx, 'y'] = spadlconfig.field_width - a[away_idx]['y'].values
            a.loc[away_idx, 'dx'] = -a[away_idx]['dx'].values
            a.loc[away_idx, 'dy'] = -a[away_idx]['dy'].values
        return actions
    elif isinstance(actions, pd.DataFrame):
        ltr_actions = actions.copy()
        away_idx = actions.team_id != home_team_id
        ltr_actions.loc[away_idx, 'x'] = spadlconfig.field_length - actions[away_idx]['x'].values
        ltr_actions.loc[away_idx, 'y'] = spadlconfig.field_width - actions[away_idx]['y'].values
        ltr_actions.loc[away_idx, 'dx'] = -actions[away_idx]['dx'].values
        ltr_actions.loc[away_idx, 'dy'] = -actions[away_idx]['dy'].values
        return ltr_actions
    else:
        raise ValueError("Input must be a DataFrame or a list of DataFrames.")


__all__ = [
    'add_names',
    'play_left_to_right',
    'to_gamestates',
]
