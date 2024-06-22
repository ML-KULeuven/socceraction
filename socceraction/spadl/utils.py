"""Utility functions for working with SPADL dataframes."""
from typing import Union, cast

import pandas as pd
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
    actions: Union[DataFrame[SPADLSchema], list[DataFrame[SPADLSchema]]], home_team_id: int
) -> Union[DataFrame[SPADLSchema], list[DataFrame[SPADLSchema]]]:
    """Perform all action (in a gamestate) in the same playing direction.

    When a dataframe of spadl actions is given, this changes the start and end
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

    Returns
    -------
    pd.DataFrame or list(pd.DataFrame)
        All actions performed left to right.

    Raises
    ------
    ValueError
        If the input is not a DataFrame or a list of DataFrames.
    """
    if isinstance(actions, pd.DataFrame):
        ltr_actions = actions.copy()
        away_idx = actions.team_id != home_team_id
        for col in ["start_x", "end_x"]:
            ltr_actions.loc[away_idx, col] = (
                spadlconfig.field_length - actions[away_idx][col].values
            )
        for col in ["start_y", "end_y"]:
            ltr_actions.loc[away_idx, col] = (
                spadlconfig.field_width - actions[away_idx][col].values
            )
        return ltr_actions
    elif isinstance(actions, list):
        a0 = actions[0]
        away_idx = a0.team_id != home_team_id
        for a in actions:
            for col in ['start_x', 'end_x']:
                a.loc[away_idx, col] = spadlconfig.field_length - a[away_idx][col].values
            for col in ['start_y', 'end_y']:
                a.loc[away_idx, col] = spadlconfig.field_width - a[away_idx][col].values
        return actions
    else:
        raise ValueError('The input should be a DataFrame or a list of DataFrames.')


def to_gamestates(
    actions: DataFrame[SPADLSchema], nb_prev_actions: int = 3
) -> list[DataFrame[SPADLSchema]]:
    r"""Convert a dataframe of actions to gamestates.

    Each gamestate is represented as the <nb_prev_actions> previous actions.

    The list of gamestates is internally represented as a list of actions
    dataframes :math:`[a_0,a_1,\ldots]` where each row in the a_i dataframe contains the
    previous action of the action in the same row in the :math:`a_{i-1}` dataframe.

    Parameters
    ----------
    actions : Actions
        A DataFrame with the actions of a game.
    nb_prev_actions : int, default=3  # noqa: DAR103
        The number of previous actions included in the game state.

    Raises
    ------
    ValueError
        If the number of actions is smaller 1.

    Returns
    -------
    GameStates
         The <nb_prev_actions> previous actions for each action.
    """
    if nb_prev_actions < 1:
        raise ValueError('The game state should include at least one preceding action.')
    states = [actions]
    for i in range(1, nb_prev_actions):
        prev_actions = actions.copy().shift(i, fill_value=0)
        prev_actions.iloc[:i] = pd.concat([actions[:1]] * i, ignore_index=True)
        states.append(prev_actions)  # type: ignore
    return states
