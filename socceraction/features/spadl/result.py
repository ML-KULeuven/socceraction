"""Feature generators for the result of each action."""

import pandas as pd
import socceraction.spadl.config as spadlcfg
from socceraction.types import Features, Mask, SPADLActions

from ..utils import feature_generator
from .actiontype import actiontype_onehot


@feature_generator("actions", features=["result"])
def result(actions: SPADLActions, mask: Mask) -> Features:
    """Get the result of each action.

    Parameters
    ----------
    actions : SPADLActions
        The actions of a game.
    mask : Mask
        A boolean mask to filter actions.

    Returns
    -------
    Features
        The 'result_id' of each action.
    """
    X = pd.DataFrame(index=actions.index)
    X["result"] = pd.Categorical(
        actions["result_id"].replace(spadlcfg.results_df().result_name.to_dict()),
        categories=spadlcfg.results,
        ordered=False,
    )
    return X.loc[mask]


@feature_generator("actions", features=[f"result_{r}" for r in spadlcfg.results])
def result_onehot(actions: SPADLActions, mask: Mask) -> Features:
    """Get the one-hot-encode result of each action.

    Parameters
    ----------
    actions : SPADLActions
        The actions of a game.
    mask : Mask
        A boolean mask to filter actions.

    Returns
    -------
    Features
        The one-hot encoding of each action's result.
    """
    X = {}
    for result_id, result_name in enumerate(spadlcfg.results):
        col = "result_" + result_name
        X[col] = actions["result_id"] == result_id
    return pd.DataFrame(X, index=actions.index).loc[mask]


@feature_generator(
    "actions",
    features=[f"actiontype_{t}_{r}" for t in spadlcfg.actiontypes for r in spadlcfg.results],
)
def actiontype_result_onehot(actions: SPADLActions, mask: Mask) -> Features:
    """Get a one-hot encoding of the combination between the type and result of each action.

    Parameters
    ----------
    actions : SPADLActions
        The actions of a game.
    mask : Mask
        A boolean mask to filter actions.

    Returns
    -------
    Features
        The one-hot encoding of each action's type and result.
    """
    res = result_onehot(actions)  # type: ignore
    tys = actiontype_onehot(actions)  # type: ignore
    df = {}
    for tyscol in list(tys.columns):
        for rescol in list(res.columns):
            df[tyscol + "_" + rescol] = tys[tyscol] & res[rescol]
    return pd.DataFrame(df, index=actions.index).loc[mask]
