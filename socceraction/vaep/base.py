# -*- coding: utf-8 -*-

import math

import numpy as np
import pandas as pd
import socceraction.spadl.config as _spadlcfg
from sklearn.exceptions import NotFittedError
from sklearn.metrics import brier_score_loss, roc_auc_score

from . import features as fs
from . import formula as _vaep
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
   fs.actiontype,
   fs.actiontype_onehot,
   fs.result,
   fs.result_onehot,
   fs.actiontype_result_onehot,
   fs.bodypart,
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
   fs.goalscore
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
        used to describe the game states.
    nb_prev_actions : int, default=3
        Number of previous actions used to decscribe the game state.


    .. [Decroos19] Decroos, Tom, Lotte Bransen, Jan Van Haaren, and Jesse Davis.
        "Actions speak louder than goals: Valuing player actions in soccer." In
        Proceedings of the 25th ACM SIGKDD International Conference on Knowledge
        Discovery & Data Mining, pp. 1851-1861. 2019.
    """

    def __init__(self, xfns=xfns_default, nb_prev_actions=3):
        self.__models = {}
        self.fs = fs
        self.xfns = xfns
        self.yfns = [lab.scores, lab.concedes]
        self.spadlcfg = _spadlcfg
        self.vaep = _vaep
        self.nb_prev_actions = nb_prev_actions

    def compute_features(self, game, game_actions):
        """
        Transform actions to the feature-based representation of game states.

        Parameter
        ---------
        game : pd.Series
            The SPADL representation of a single game.
        game_actions : pd.DataFrame
            The actions performed during `game` in the SPADL representation.

        Returns
        -------
        features : pd.DataFrame
            Returns the feature-based representation of each game state in the game.
        """
        game_actions_with_names = self.spadlcfg.add_names(game_actions)
        gamestates = self.fs.gamestates(game_actions_with_names, self.nb_prev_actions)
        gamestates = self.fs.play_left_to_right(gamestates, game.home_team_id)
        return pd.concat([fn(gamestates) for fn in self.xfns], axis=1)

    def compute_labels(self, game, game_actions):
        """
        Compute the labels for each game state in the given game. 

        Parameter
        ---------
        game : pd.Series
            The SPADL representation of a single game.
        game_actions : pd.DataFrame
            The actions performed during `game` in the SPADL representation.

        Returns
        -------
        labels : pd.DataFrame
            Returns the labels of each game state in the game.
        """
        game_actions_with_names = self.spadlcfg.add_names(game_actions)
        return pd.concat([fn(game_actions_with_names) for fn in self.yfns], axis=1)

    def fit(self, X, y, learner='xgboost', val_size=.25, tree_params={}, fit_params={}):
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
        train_idx = idx[:math.floor(nb_states * (1-val_size))]
        val_idx = idx[math.floor(nb_states * (1-val_size))+1:]

        X_train, y_train = X.iloc[train_idx], y.iloc[train_idx]
        X_val, y_val = X.iloc[val_idx], y.iloc[val_idx]

        # train classifiers F(X) = Y
        for col in list(y.columns):
            val_params = {}
            if val_size > 0:
                val_params = dict(
                    early_stopping_rounds=10,
                    eval_set=[(X_val, y_val[col])])
 
            if learner == 'xgboost':
                if xgboost is None:
                    raise ImportError("xgboost is not installed.")
                # XGboost
                tree_params_default = dict(
                        n_estimators=100,
                        max_depth=3)
                tree_params = {**tree_params_default, **tree_params}
                fit_params_default = dict(
                    eval_metric="auc",
                    verbose=True)
                fit_params = {**fit_params_default, **val_params, **fit_params}
                model = xgboost.XGBClassifier(**tree_params)
            elif learner == 'catboost':
                if catboost is None:
                    raise ImportError("catboost is not installed.")
                tree_params_default = dict(
                    eval_metric="BrierScore", 
                    loss_function="Logloss",
                    iterations=100)
                tree_params = {**tree_params_default, **tree_params}
                fit_params_default = dict(
                    cat_features=np.nonzero([c.dtype.name == 'category' for (_, c) in X.iteritems()])[0].tolist(),
                    verbose=True)
                fit_params = {**fit_params_default, **val_params, **fit_params}
                model = catboost.CatBoostClassifier(**tree_params)
            elif learner == 'lightgbm':
                if lightgbm is None:
                    raise ImportError("lightgbm is not installed.")
                tree_params_default = dict(
                        n_estimators=100,
                        max_depth=3)
                tree_params = {**tree_params_default, **val_params, **tree_params}
                fit_params_default = dict(
                    eval_metric="auc",
                    verbose=True
                    )
                fit_params = {**fit_params_default, **fit_params}
                model = lightgbm.LGBMClassifier(**tree_params)
            else:
                raise ValueError("A {} learner is not supported".format(learner))

            model.fit(X_train, y_train[col].astype(int), **fit_params)
            self.__models[col] = model
        return self


    def _estimate_probabilities(self, gamestates):
        Y_hat = pd.DataFrame()
        for col in self.__models.keys():
            Y_hat[col] = [p[1] for p in self.__models[col].predict_proba(gamestates)]
        return Y_hat


    def rate(self, game, game_actions, game_states=None):
        """
        Computes the VAEP rating for the given game states.

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

        game_actions_with_names = self.spadlcfg.add_names(game_actions)
        if game_states is None:
            game_states = self.compute_features(game, game_actions)

        y_hat = self._estimate_probabilities(game_states)
        p_scores, p_concedes = y_hat.scores, y_hat.concedes
        vaep_values = self.vaep.value(game_actions_with_names, p_scores, p_concedes)
        return pd.concat([game_actions, vaep_values], axis=1)

    def score(self, X, y):
        """Evaluates the fit of the model on the given test data and labels.

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

        scores = {}
        for col in self.__models.keys():
            scores[col] = {}
            scores[col]['brier'] = brier_score_loss(y[col], y_hat[col])
            scores[col]['auroc'] = roc_auc_score(y[col], y_hat[col])

        return scores
