# -*- coding: utf-8 -*-
"""Implements the VAEP framework.

Attributes
----------
xfns_default : list(callable)
    The default VAEP features.

"""
import math
from typing import Any, Callable, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd
from sklearn.exceptions import NotFittedError
from sklearn.metrics import brier_score_loss, roc_auc_score

import socceraction.spadl.config as spadlcfg

from . import features as fs
from . import formula as vaep
from . import labels as lab

try:
    import xgboost
except ImportError:
    xgboost = None
try:
    import catboost
except ImportError:
    catboost = None
try:
    import lightgbm
except ImportError:
    lightgbm = None


xfns_default = [
    fs.actiontype_onehot,
    fs.result_onehot,
    fs.actiontype_result_onehot,
    fs.bodypart_onehot,
    fs.time,
    fs.startlocation,
    fs.endlocation,
    fs.startpolar,
    fs.endpolar,
    fs.movement,
    fs.team,
    fs.time_delta,
    fs.space_delta,
    fs.goalscore,
]


class VAEP:
    """
    An implementation of the VAEP framework [Decroos19]_.

    VAEP (Valuing Actions by Estimating Probabilities) defines the problem of
    valuing a soccer player's contributions within a match as a binary
    classification problem and rates actions by estimating its effect on the
    short-term probablities that a team will both score and concede.

    Parameters
    ----------
    xfns : list
        List of feature transformers (see :mod:`socceraction.vaep.features`)
        used to describe the game states. Uses :attr:`~socceraction.vaep.base.xfns_default`
        if None.
    nb_prev_actions : int, default=3
        Number of previous actions used to decscribe the game state.


    .. [Decroos19] Decroos, Tom, Lotte Bransen, Jan Van Haaren, and Jesse Davis.
        "Actions speak louder than goals: Valuing player actions in soccer." In
        Proceedings of the 25th ACM SIGKDD International Conference on Knowledge
        Discovery & Data Mining, pp. 1851-1861. 2019.
    """

    _spadlcfg = spadlcfg
    _fs = fs
    _lab = lab
    _vaep = vaep

    def __init__(
        self,
        xfns: Optional[List[Callable[[List[pd.DataFrame]], pd.DataFrame]]] = None,
        nb_prev_actions: int = 3,
    ):
        self.__models: Dict[str, Any] = {}
        self.xfns = xfns_default if xfns is None else xfns
        self.yfns = [self._lab.scores, self._lab.concedes]
        self.nb_prev_actions = nb_prev_actions

    def compute_features(self, game: pd.Series, game_actions: pd.DataFrame) -> pd.DataFrame:
        """
        Transform actions to the feature-based representation of game states.

        Parameters
        ----------
        game : pd.Series
            The SPADL representation of a single game.
        game_actions : pd.DataFrame
            The actions performed during `game` in the SPADL representation.

        Returns
        -------
        features : pd.DataFrame
            Returns the feature-based representation of each game state in the game.
        """
        game_actions_with_names = self._spadlcfg.add_names(game_actions)
        gamestates = self._fs.gamestates(game_actions_with_names, self.nb_prev_actions)
        gamestates = self._fs.play_left_to_right(gamestates, game.home_team_id)
        return pd.concat([fn(gamestates) for fn in self.xfns], axis=1)

    def compute_labels(
        self, game: pd.Series, game_actions: pd.DataFrame
    ) -> pd.DataFrame:  # pylint: disable=W0613
        """
        Compute the labels for each game state in the given game.

        Parameters
        ----------
        game : pd.Series
            The SPADL representation of a single game.
        game_actions : pd.DataFrame
            The actions performed during `game` in the SPADL representation.

        Returns
        -------
        labels : pd.DataFrame
            Returns the labels of each game state in the game.
        """
        game_actions_with_names = self._spadlcfg.add_names(game_actions)
        return pd.concat([fn(game_actions_with_names) for fn in self.yfns], axis=1)

    def fit(
        self,
        X: pd.DataFrame,
        y: pd.DataFrame,
        learner: str = 'xgboost',
        val_size: float = 0.25,
        tree_params: Optional[Dict[str, Any]] = None,
        fit_params: Optional[Dict[str, Any]] = None,
    ) -> 'VAEP':
        """
        Fit the model according to the given training data.

        Parameters
        ----------
        X : pd.DataFrame
            Feature representation of the game states.
        y : pd.DataFrame
            Scoring and conceding labels for each game state.
        learner : string, default='xgboost'
            Gradient boosting implementation which should be used to learn the
            model. The supported learners are 'xgboost', 'catboost' and 'lightgbm'.
        val_size : float, default=0.25
            Percentage of the dataset that will be used as the validation set
            for early stopping. When zero, no validation data will be used.
        tree_params : dict
            Parameters passed to the constructor of the learner.
        fit_params : dict
            Parameters passed to the fit method of the learner.

        Returns
        -------
        self
            Fitted VAEP model.

        """
        nb_states = len(X)
        idx = np.random.permutation(nb_states)
        # fmt: off
        train_idx = idx[:math.floor(nb_states * (1 - val_size))]
        val_idx = idx[(math.floor(nb_states * (1 - val_size)) + 1):]
        # fmt: on

        # filter feature columns
        cols = self._fs.feature_column_names(self.xfns, self.nb_prev_actions)
        if not set(cols).issubset(set(X.columns)):
            missing_cols = ' and '.join(set(cols).difference(X.columns))
            raise ValueError('{} are not available in the features dataframe'.format(missing_cols))

        # split train and validation data
        X_train, y_train = X.iloc[train_idx][cols], y.iloc[train_idx]
        X_val, y_val = X.iloc[val_idx][cols], y.iloc[val_idx]

        # train classifiers F(X) = Y
        for col in list(y.columns):
            eval_set = [(X_val, y_val[col])] if val_size > 0 else None
            if learner == 'xgboost':
                self.__models[col] = self._fit_xgboost(
                    X_train, y_train[col], eval_set, tree_params, fit_params
                )
            elif learner == 'catboost':
                self.__models[col] = self._fit_catboost(
                    X_train, y_train[col], eval_set, tree_params, fit_params
                )
            elif learner == 'lightgbm':
                self.__models[col] = self._fit_lightgbm(
                    X_train, y_train[col], eval_set, tree_params, fit_params
                )
            else:
                raise ValueError('A {} learner is not supported'.format(learner))
        return self

    def _fit_xgboost(
        self,
        X: pd.DataFrame,
        y: pd.Series,
        eval_set: Optional[List[Tuple[pd.DataFrame, pd.Series]]] = None,
        tree_params: Optional[Dict[str, Any]] = None,
        fit_params: Optional[Dict[str, Any]] = None,
    ) -> 'xgboost.XGBClassifier':
        if xgboost is None:
            raise ImportError('xgboost is not installed.')
        # Default settings
        if tree_params is None:
            tree_params = dict(n_estimators=100, max_depth=3)
        if fit_params is None:
            fit_params = dict(eval_metric='auc', verbose=True)
        if eval_set is not None:
            val_params = dict(early_stopping_rounds=10, eval_set=eval_set)
            fit_params = {**fit_params, **val_params}
        # Train the model
        model = xgboost.XGBClassifier(**tree_params)
        return model.fit(X, y, **fit_params)

    def _fit_catboost(
        self,
        X: pd.DataFrame,
        y: pd.Series,
        eval_set: Optional[List[Tuple[pd.DataFrame, pd.Series]]] = None,
        tree_params: Optional[Dict[str, Any]] = None,
        fit_params: Optional[Dict[str, Any]] = None,
    ) -> 'catboost.CatBoostClassifier':
        if catboost is None:
            raise ImportError('catboost is not installed.')
        # Default settings
        if tree_params is None:
            tree_params = dict(eval_metric='BrierScore', loss_function='Logloss', iterations=100)
        if fit_params is None:
            is_cat_feature = [c.dtype.name == 'category' for (_, c) in X.iteritems()]
            fit_params = dict(
                cat_features=np.nonzero(is_cat_feature)[0].tolist(),
                verbose=True,
            )
        if eval_set is not None:
            val_params = dict(early_stopping_rounds=10, eval_set=eval_set)
            fit_params = {**fit_params, **val_params}
        # Train the model
        model = catboost.CatBoostClassifier(**tree_params)
        return model.fit(X, y, **fit_params)

    def _fit_lightgbm(
        self,
        X: pd.DataFrame,
        y: pd.Series,
        eval_set: Optional[List[Tuple[pd.DataFrame, pd.Series]]] = None,
        tree_params: Optional[Dict[str, Any]] = None,
        fit_params: Optional[Dict[str, Any]] = None,
    ) -> 'lightgbm.LGBMClassifier':
        if lightgbm is None:
            raise ImportError('lightgbm is not installed.')
        if tree_params is None:
            tree_params = dict(n_estimators=100, max_depth=3)
        if fit_params is None:
            fit_params = dict(eval_metric='auc', verbose=True)
        if eval_set is not None:
            val_params = dict(early_stopping_rounds=10, eval_set=eval_set)
            fit_params = {**fit_params, **val_params}
        # Train the model
        model = lightgbm.LGBMClassifier(**tree_params)
        return model.fit(X, y, **fit_params)

    def _estimate_probabilities(self, X: pd.DataFrame) -> pd.DataFrame:
        # filter feature columns
        cols = self._fs.feature_column_names(self.xfns, self.nb_prev_actions)
        if not set(cols).issubset(set(X.columns)):
            missing_cols = ' and '.join(set(cols).difference(X.columns))
            raise ValueError('{} are not available in the features dataframe'.format(missing_cols))

        Y_hat = pd.DataFrame()
        for col in self.__models:
            Y_hat[col] = [p[1] for p in self.__models[col].predict_proba(X)]
        return Y_hat

    def rate(
        self, game: pd.Series, game_actions: pd.DataFrame, game_states: pd.DataFrame = None
    ) -> pd.DataFrame:
        """
        Compute the VAEP rating for the given game states.

        Parameters
        ----------
        game : pd.Series
            The SPADL representation of a single game.
        game_actions : pd.DataFrame
            The actions performed during `game` in the SPADL representation.
        game_states : pd.DataFrame, default=None
            DataFrame with the game state representation of each action. If
            `None`, these will be computed on-th-fly.

        Returns
        -------
        ratings : pd.DataFrame
            Returns the VAEP rating for each given action, as well as the
            offensive and defensive value of each action.
        """
        if not self.__models:
            raise NotFittedError()

        game_actions_with_names = self._spadlcfg.add_names(game_actions)
        if game_states is None:
            game_states = self.compute_features(game, game_actions)

        y_hat = self._estimate_probabilities(game_states)
        p_scores, p_concedes = y_hat.scores, y_hat.concedes
        vaep_values = self._vaep.value(game_actions_with_names, p_scores, p_concedes)
        return pd.concat([game_actions, vaep_values], axis=1)

    def score(self, X: pd.DataFrame, y: pd.DataFrame) -> Dict[str, Dict[str, float]]:
        """Evaluate the fit of the model on the given test data and labels.

        Parameters
        ----------
        X : pd.DataFrame
            Feature representation of the game states.
        y : pd.DataFrame
            Scoring and conceding labels for each game state.

        Returns
        -------
        score : dict
            The Brier and AUROC scores for both binary classification problems.
        """
        if not self.__models:
            raise NotFittedError()

        y_hat = self._estimate_probabilities(X)

        scores: Dict[str, Dict[str, float]] = {}
        for col in self.__models:
            scores[col] = {}
            scores[col]['brier'] = brier_score_loss(y[col], y_hat[col])
            scores[col]['auroc'] = roc_auc_score(y[col], y_hat[col])

        return scores
