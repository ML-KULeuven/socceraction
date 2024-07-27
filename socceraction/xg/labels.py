"""Implements the label tranformers for an xG model."""

import pandas as pd

import socceraction.spadl.config as spadl
from socceraction.features.utils import feature_generator
from socceraction.types import Features, Mask, SPADLActions


@feature_generator("actions", features=["goal_from_shot"])
def goal_from_shot(actions: SPADLActions, mask: Mask) -> Features:
    """Determine whether a goal was scored from the current action.

    This label can be used to train an xG model.

    Parameters
    ----------
    actions : SPADLActions
        The actions of a game in SPADL format.
    mask : Mask
        A boolean mask to select the shots.

    Returns
    -------
    Features
        A dataframe with a column 'goal' and a row for each shot set to
        True if a goal was scored from the current shot; otherwise False.
    """
    shots = actions.loc[mask]
    goaldf = pd.DataFrame(index=shots.index)
    goaldf["goal_from_shot"] = shots["result_id"] == spadl.results.index("success")
    return goaldf
