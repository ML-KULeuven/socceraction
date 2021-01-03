# -*- coding: utf-8 -*-
"""Implements the formula of the VAEP framework."""
import pandas as pd  # type: ignore
from pandera.typing import DataFrame, Series

from socceraction.spadl.base import SPADLSchema


def _prev(x: pd.Series) -> pd.Series:
    prev_x = x.shift(1)
    prev_x[:1] = x.values[0]
    return prev_x


_samephase_nb: int = 10


def offensive_value(actions: DataFrame[SPADLSchema], scores: Series, concedes: Series) -> Series:
    r"""Compute the offensive value of each action.

    VAEP defines the *offensive value* of an action as the change in scoring
    probability before and after the action.

    .. math::

      \Delta P_{score}(a_{i}, t) = P^{k}_{score}(S_i, t) - P^{k}_{score}(S_{i-1}, t)

    where :math:`P_{score}(S_i, t)` is the probability that team :math:`t`
    which possesses the ball in state :math:`S_i` will score in the next 10
    actions.

    Parameters
    ----------
    actions : pd.DataFrame
        SPADL action.
    scores : pd.Series
        The probability of scoring from each corresponding game state.
    scores : pd.Series
        The probability of conceding from each corresponding game state.

    Returns
    -------
    pd.Series
        he ffensive value of each action.
    """
    sameteam = _prev(actions.team_id) == actions.team_id
    prev_scores = _prev(scores) * sameteam + _prev(concedes) * (~sameteam)

    # if the previous action was too long ago, the odds of scoring are now 0
    toolong_idx = abs(actions.time_seconds - _prev(actions.time_seconds)) > _samephase_nb
    prev_scores[toolong_idx] = 0

    # if the previous action was a goal, the odds of scoring are now 0
    prevgoal_idx = (_prev(actions.type_name).isin(['shot', 'shot_freekick', 'shot_penalty'])) & (
        _prev(actions.result_name) == 'success'
    )
    prev_scores[prevgoal_idx] = 0

    # fixed odds of scoring when penalty
    penalty_idx = actions.type_name == 'shot_penalty'
    prev_scores[penalty_idx] = 0.792453

    # fixed odds of scoring when corner
    corner_idx = actions.type_name.isin(['corner_crossed', 'corner_short'])
    prev_scores[corner_idx] = 0.046500

    return scores - prev_scores


def defensive_value(actions: DataFrame[SPADLSchema], scores: Series, concedes: Series) -> Series:
    r"""Compute the defensive value of each action.

    VAEP defines the *defensive value* of an action as the change in conceding
    probability.

    .. math::

      \Delta P_{concede}(a_{i}, t) = P^{k}_{concede}(S_i, t) - P^{k}_{concede}(S_{i-1}, t)

    where :math:`P_{concede}(S_i, t)` is the probability that team :math:`t`
    which possesses the ball in state :math:`S_i` will concede in the next 10
    actions.

    Parameters
    ----------
    actions : pd.DataFrame
        SPADL action.
    scores : pd.Series
        The probability of scoring from each corresponding game state.
    scores : pd.Series
        The probability of conceding from each corresponding game state.

    Returns
    -------
    pd.Series
        The defensive value of each action.
    """
    sameteam = _prev(actions.team_id) == actions.team_id
    prev_concedes = _prev(concedes) * sameteam + _prev(scores) * (~sameteam)

    toolong_idx = abs(actions.time_seconds - _prev(actions.time_seconds)) > _samephase_nb
    prev_concedes[toolong_idx] = 0

    # if the previous action was a goal, the odds of conceding are now 0
    prevgoal_idx = (_prev(actions.type_name).isin(['shot', 'shot_freekick', 'shot_penalty'])) & (
        _prev(actions.result_name) == 'success'
    )
    prev_concedes[prevgoal_idx] = 0

    return -(concedes - prev_concedes)


def value(actions: DataFrame[SPADLSchema], Pscores: Series, Pconcedes: Series) -> DataFrame:
    r"""Compute the offensive, defensive and VAEP value of each action.

    The total VAEP value of an action is the difference between that action's
    offensive value and defensive value.

    .. math::

      V_{VAEP}(a_i) = \Delta P_{score}(a_{i}, t) - \Delta P_{concede}(a_{i}, t)

    Parameters
    ----------
    actions : pd.DataFrame
        SPADL action.
    scores : pd.Series
        The probability of scoring from each corresponding game state.
    scores : pd.Series
        The probability of conceding from each corresponding game state.

    Returns
    -------
    pd.DataFrame
        The 'offensive_value', 'defensive_value' and 'vaep_value' of each action.

    See Also
    --------
    :func:`~socceraction.vaep.formula.offensive_value`: The offensive value
    :func:`~socceraction.vaep.formula.defensive_value`: The defensive value
    """
    v = pd.DataFrame()
    v['offensive_value'] = offensive_value(actions, Pscores, Pconcedes)
    v['defensive_value'] = defensive_value(actions, Pscores, Pconcedes)
    v['vaep_value'] = v['offensive_value'] + v['defensive_value']
    return v
