"""Feature generators for the action type of each action."""

import pandas as pd
import socceraction.spadl.config as spadlcfg
from socceraction.types import Features, Mask, SPADLActions

from ..utils import feature_generator


@feature_generator("actions", features=["actiontype"])
def actiontype(actions: SPADLActions, mask: Mask) -> Features:
    """Get the type of each action.

    Parameters
    ----------
    actions : SPADLActions
        The actions of a game.
    mask : Mask
        A boolean mask to filter actions.

    Returns
    -------
    Features
        The 'type_id' of each action.
    """
    X = pd.DataFrame(index=actions.index)
    X["actiontype"] = pd.Categorical(
        actions["type_id"].replace(spadlcfg.actiontypes_df().type_name.to_dict()),
        categories=spadlcfg.actiontypes,
        ordered=False,
    )
    return X.loc[mask]


@feature_generator("actions", features=[f"actiontype_{t}" for t in spadlcfg.actiontypes])
def actiontype_onehot(actions: SPADLActions, mask: Mask) -> Features:
    """Get the one-hot-encoded type of each action.

    Parameters
    ----------
    actions : SPADLActions
        The actions of a game.
    mask : Mask
        A boolean mask to filter actions.

    Returns
    -------
    Features
        A one-hot encoding of each action's type.
    """
    X = {}
    for type_id, type_name in enumerate(spadlcfg.actiontypes):
        col = "actiontype_" + type_name
        X[col] = actions["type_id"] == type_id
    return pd.DataFrame(X, index=actions.index).loc[mask]
