"""Utility functions for working with Atomic-SPADL dataframes."""
from pandera.typing import DataFrame

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
    return (
        actions.drop(columns=['type_name', 'bodypart_name'], errors='ignore')
        .merge(spadlconfig.actiontypes_df(), how='left')
        .merge(spadlconfig.bodyparts_df(), how='left')
        .pipe(DataFrame[AtomicSPADLSchema])
    )


def play_left_to_right(
    actions: DataFrame[AtomicSPADLSchema], home_team_id: int
) -> DataFrame[AtomicSPADLSchema]:
    """Perform all action in the same playing direction.

    This changes the location of each action, such that all actions
    are performed as if the team plays from left to right.

    Parameters
    ----------
    actions : pd.DataFrame
        The SPADL actins of a game.
    home_team_id : int
        The ID of the home team.

    Returns
    -------
    list(pd.DataFrame)
        All actions performed left to right.
    """
    ltr_actions = actions.copy()
    away_idx = actions.team_id != home_team_id
    ltr_actions.loc[away_idx, 'x'] = spadlconfig.field_length - actions[away_idx]['x'].values
    ltr_actions.loc[away_idx, 'y'] = spadlconfig.field_width - actions[away_idx]['y'].values
    ltr_actions.loc[away_idx, 'dx'] = -actions[away_idx]['dx'].values
    ltr_actions.loc[away_idx, 'dy'] = -actions[away_idx]['dy'].values
    return ltr_actions
