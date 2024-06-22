"""Utility functions for working with SPADL dataframes."""

from typing import cast

from pandera.typing import DataFrame

from . import config as spadlconfig
from .schema import SPADLSchema


def add_names(actions: DataFrame[SPADLSchema]) -> DataFrame[SPADLSchema]:
    """Add the type name, result name and bodypart name to a SPADL dataframe.

    Parameters
    ----------
    actions : pd.DataFrame
        A SPADL dataframe.

    Returns
    -------
    pd.DataFrame
        The original dataframe with a 'type_name', 'result_name' and
        'bodypart_name' appended.
    """
    return cast(
        DataFrame[SPADLSchema],
        actions.drop(columns=["type_name", "result_name", "bodypart_name"], errors="ignore")
        .merge(spadlconfig.actiontypes_df(), how="left")
        .merge(spadlconfig.results_df(), how="left")
        .merge(spadlconfig.bodyparts_df(), how="left")
        .set_index(actions.index),
    )


def play_left_to_right(
    actions: DataFrame[SPADLSchema], home_team_id: int
) -> DataFrame[SPADLSchema]:
    """Perform all action in the same playing direction.

    This changes the start and end location of each action, such that all actions
    are performed as if the team that executes the action plays from left to
    right.

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

    See Also
    --------
    socceraction.vaep.features.play_left_to_right : For transforming gamestates.
    """
    ltr_actions = actions.copy()
    away_idx = actions.team_id != home_team_id
    for col in ["start_x", "end_x"]:
        ltr_actions.loc[away_idx, col] = spadlconfig.field_length - actions[away_idx][col].values
    for col in ["start_y", "end_y"]:
        ltr_actions.loc[away_idx, col] = spadlconfig.field_width - actions[away_idx][col].values
    return ltr_actions
