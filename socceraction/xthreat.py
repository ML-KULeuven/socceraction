# -*- coding: utf-8 -*-
"""Implements the xT framework."""
import json
import os
import warnings
from typing import Callable, List, Optional, Tuple

import numpy as np
import numpy.typing as npt
import pandas as pd
from pandera.typing import DataFrame, Series
from sklearn.exceptions import NotFittedError

import socceraction.spadl.config as spadlconfig
from socceraction.spadl.schema import SPADLSchema

try:
    from scipy.interpolate import interp2d  # type: ignore
except ImportError:  # pragma: no cover
    interp2d = None

M: int = 12
N: int = 16


def _get_cell_indexes(
    x: Series[float], y: Series[float], l: int = N, w: int = M
) -> Tuple[Series[int], Series[int]]:
    xi = x.divide(spadlconfig.field_length).multiply(l)
    yj = y.divide(spadlconfig.field_width).multiply(w)
    xi = xi.astype(int).clip(0, l - 1)
    yj = yj.astype(int).clip(0, w - 1)
    return xi, yj


def _get_flat_indexes(x: Series[float], y: Series[float], l: int = N, w: int = M) -> Series[int]:
    xi, yj = _get_cell_indexes(x, y, l, w)
    return yj.rsub(w - 1).mul(l).add(xi)


def _count(x: Series[float], y: Series[float], l: int = N, w: int = M) -> npt.NDArray[np.int_]:
    """Count the number of actions occurring in each cell of the grid.

    Parameters
    ----------
    x : pd.Series
        The x-coordinates of the actions.
    y : pd.Series
        The y-coordinates of the actions.
    l : int
        Amount of grid cells in the x-dimension of the grid.
    w : int
        Amount of grid cells in the y-dimension of the grid.

    Returns
    -------
    np.ndarray
        A matrix, denoting the amount of actions occurring in each cell. The
        top-left corner is the origin.
    """
    x = x[~np.isnan(x) & ~np.isnan(y)]
    y = y[~np.isnan(x) & ~np.isnan(y)]

    flat_indexes = _get_flat_indexes(x, y, l, w)
    vc = flat_indexes.value_counts(sort=False)
    vector = np.zeros(w * l)
    vector[vc.index] = vc
    return vector.reshape((w, l))


def _safe_divide(a: npt.ArrayLike, b: npt.ArrayLike) -> npt.NDArray[np.float64]:
    return np.divide(a, b, out=np.zeros_like(a), where=b != 0)


def scoring_prob(
    actions: DataFrame[SPADLSchema], l: int = N, w: int = M
) -> npt.NDArray[np.float64]:
    """Compute the probability of scoring when taking a shot for each cell.

    Parameters
    ----------
    actions : pd.DataFrame
        Actions, in SPADL format.
    l : int
        Amount of grid cells in the x-dimension of the grid.
    w : int
        Amount of grid cells in the y-dimension of the grid.

    Returns
    -------
    np.ndarray
        A matrix, denoting the probability of scoring for each cell.
    """
    shot_actions = actions[(actions.type_id == spadlconfig.actiontypes.index('shot'))]
    goals = shot_actions[(shot_actions.result_id == spadlconfig.results.index('success'))]

    shotmatrix = _count(shot_actions.start_x, shot_actions.start_y, l, w)
    goalmatrix = _count(goals.start_x, goals.start_y, l, w)
    return _safe_divide(goalmatrix, shotmatrix)


def get_move_actions(actions: DataFrame[SPADLSchema]) -> DataFrame[SPADLSchema]:
    """Get all ball-progressing actions.

    These include passes, dribbles and crosses. Take-ons are ignored because
    they typically coincide with dribbles and do not move the ball to
    a different cell.

    Parameters
    ----------
    actions : pd.DataFrame
        Actions, in SPADL format.

    Returns
    -------
    pd.DataFrame
        All ball-progressing actions in the input dataframe.
    """
    return actions[
        (actions.type_id == spadlconfig.actiontypes.index('pass'))
        | (actions.type_id == spadlconfig.actiontypes.index('dribble'))
        | (actions.type_id == spadlconfig.actiontypes.index('cross'))
    ]


def get_successful_move_actions(actions: DataFrame[SPADLSchema]) -> DataFrame[SPADLSchema]:
    """Get all successful ball-progressing actions.

    These include successful passes, dribbles and crosses.

    Parameters
    ----------
    actions : pd.DataFrame
        Actions, in SPADL format.

    Returns
    -------
    pd.DataFrame
        All ball-progressing actions in the input dataframe.
    """
    move_actions = get_move_actions(actions)
    return move_actions[(move_actions.result_id == spadlconfig.results.index('success'))]


def action_prob(
    actions: DataFrame[SPADLSchema], l: int = N, w: int = M
) -> Tuple[npt.NDArray[np.float64], npt.NDArray[np.float64]]:
    """Compute the probability of taking an action in each cell of the grid.

    The options are: shooting or moving.

    Parameters
    ----------
    actions : pd.DataFrame
        Actions, in SPADL format.
    l : pd.DataFrame
        Amount of grid cells in the x-dimension of the grid.
    w : pd.DataFrame
        Amount of grid cells in the y-dimension of the grid.

    Returns
    -------
    shotmatrix : np.ndarray
        For each cell the probability of choosing to shoot.
    movematrix : np.ndarray
        For each cell the probability of choosing to move.
    """
    move_actions = get_move_actions(actions)
    shot_actions = actions[(actions.type_id == spadlconfig.actiontypes.index('shot'))]

    movematrix = _count(move_actions.start_x, move_actions.start_y, l, w)
    shotmatrix = _count(shot_actions.start_x, shot_actions.start_y, l, w)
    totalmatrix = movematrix + shotmatrix

    return _safe_divide(shotmatrix, totalmatrix), _safe_divide(movematrix, totalmatrix)


def move_transition_matrix(
    actions: DataFrame[SPADLSchema], l: int = N, w: int = M
) -> npt.NDArray[np.float64]:
    """Compute the move transition matrix from the given actions.

    This is, when a player chooses to move, the probability that he will
    end up in each of the other cells of the grid successfully.

    Parameters
    ----------
    actions : pd.DataFrame
        Actions, in SPADL format.
    l : int
        Amount of grid cells in the x-dimension of the grid.
    w : int
        Amount of grid cells in the y-dimension of the grid.

    Returns
    -------
    np.ndarray
        The transition matrix.
    """
    move_actions = get_move_actions(actions)

    X = pd.DataFrame()
    X['start_cell'] = _get_flat_indexes(move_actions.start_x, move_actions.start_y, l, w)
    X['end_cell'] = _get_flat_indexes(move_actions.end_x, move_actions.end_y, l, w)
    X['result_id'] = move_actions.result_id

    vc = X.start_cell.value_counts(sort=False)
    start_counts = np.zeros(w * l)
    start_counts[vc.index] = vc

    transition_matrix = np.zeros((w * l, w * l))

    for i in range(0, w * l):
        vc2 = X[
            ((X.start_cell == i) & (X.result_id == spadlconfig.results.index('success')))
        ].end_cell.value_counts(sort=False)
        transition_matrix[i, vc2.index] = vc2 / start_counts[i]

    return transition_matrix


class ExpectedThreat:
    """An implementation of the Expected Threat (xT) model.

    The xT model [1]_ can be used to value actions that successfully move
    the ball between two locations on the pitch by computing the difference
    between the long-term probability of scoring on the start and end location
    of an action.

    Parameters
    ----------
    l : int
        Amount of grid cells in the x-dimension of the grid.
    w : int
        Amount of grid cells in the y-dimension of the grid.
    eps : float
       The desired precision to calculate the xT value of a cell. Default is
       5 decimal places of precision (1e-5).

    Attributes
    ----------
    l : int
        Amount of grid cells in the x-dimension of the grid.
    w : int
        Amount of grid cells in the y-dimension of the grid.
    eps : float
       The desired precision to calculate the xT value of a cell. Default is
       5 decimal places of precision (1e-5).
    heatmaps : list(np.ndarray)
        The i-th element corresponds to the xT value surface after i iterations.
    xT : np.ndarray
        The final xT value surface.
    scoring_prob_matrix : np.ndarray, shape(M,N)
        The probability of scoring when taking a shot for each cell.
    shot_prob_matrix : np.ndarray, shape(M,N)
        The probability of choosing to shoot for each cell.
    move_prob_matrix : np.ndarray, shape(M,N)
        The probability of choosing to move for each cell.
    transition_matrix : np.ndarray, shape(M*N,M*N)
        When moving, the probability of moving to each of the other zones.

    References
    ----------
    .. [1] Singh, Karun. "Introducing Expected Threat (xT)." 15 February, 2019.
        https://karun.in/blog/expected-threat.html
    """

    def __init__(self, l: int = N, w: int = M, eps: float = 1e-5) -> None:
        self.l = l
        self.w = w
        self.eps = eps
        self.heatmaps: List[npt.NDArray[np.float64]] = []
        self.xT: npt.NDArray[np.float64] = np.zeros((self.w, self.l))
        self.scoring_prob_matrix: Optional[npt.NDArray[np.float64]] = None
        self.shot_prob_matrix: Optional[npt.NDArray[np.float64]] = None
        self.move_prob_matrix: Optional[npt.NDArray[np.float64]] = None
        self.transition_matrix: Optional[npt.NDArray[np.float64]] = None

    def __solve(
        self,
        p_scoring: npt.NDArray[np.float64],
        p_shot: npt.NDArray[np.float64],
        p_move: npt.NDArray[np.float64],
        transition_matrix: npt.NDArray[np.float64],
    ) -> None:
        """Solves the expected threat equation with dynamic programming.

        Parameters
        ----------
        p_scoring : (np.ndarray, shape(M, N)):
            Probability of scoring at each grid cell, when shooting from that cell.
        p_shot : (np.ndarray, shape(M,N)):
            For each grid cell, the probability of choosing to shoot from there.
        p_move : (np.ndarray, shape(M,N)):
            For each grid cell, the probability of choosing to move from there.
        transition_matrix : (np.ndarray, shape(M*N,M*N)):
            When moving, the probability of moving to each of the other zones.
        """
        gs = p_scoring * p_shot
        diff = 1
        it = 0
        self.heatmaps.append(self.xT.copy())

        while np.any(diff > self.eps):
            total_payoff = np.zeros((self.w, self.l))

            for y in range(0, self.w):
                for x in range(0, self.l):
                    for q in range(0, self.w):
                        for z in range(0, self.l):
                            total_payoff[y, x] += (
                                transition_matrix[self.l * y + x, self.l * q + z] * self.xT[q, z]
                            )

            newxT = gs + (p_move * total_payoff)
            diff = newxT - self.xT
            self.xT = newxT
            self.heatmaps.append(self.xT.copy())
            it += 1

        print('# iterations: ', it)

    def fit(self, actions: DataFrame[SPADLSchema]) -> 'ExpectedThreat':
        """Fits the xT model with the given actions.

        Parameters
        ----------
        actions : pd.DataFrame
            Actions, in SPADL format.

        Returns
        -------
        self
            Fitted xT model.
        """
        self.scoring_prob_matrix = scoring_prob(actions, self.l, self.w)
        self.shot_prob_matrix, self.move_prob_matrix = action_prob(actions, self.l, self.w)
        self.transition_matrix = move_transition_matrix(actions, self.l, self.w)
        self.xT = np.zeros((self.w, self.l))
        self.__solve(
            self.scoring_prob_matrix,
            self.shot_prob_matrix,
            self.move_prob_matrix,
            self.transition_matrix,
        )
        return self

    def interpolator(
        self, kind: str = 'linear'
    ) -> Callable[[npt.NDArray[np.float64], npt.NDArray[np.float64]], npt.NDArray[np.float64]]:
        """Interpolate over the pitch.

        This is a wrapper around :func:`scipy.interpolate.interp2d`.

        Parameters
        ----------
        kind : {'linear', 'cubic', 'quintic'}, optional
            The kind of spline interpolation to use. Default is ‘linear’.

        Returns
        -------
        callable
            A function that interpolates xT values over the pitch.
        """
        if interp2d is None:
            raise ImportError('Interpolation requires scipy to be installed.')

        cell_length = spadlconfig.field_length / self.l
        cell_width = spadlconfig.field_width / self.w

        x = np.arange(0.0, spadlconfig.field_length, cell_length) + 0.5 * cell_length
        y = np.arange(0.0, spadlconfig.field_width, cell_width) + 0.5 * cell_width

        return interp2d(x=x, y=y, z=self.xT, kind=kind, bounds_error=False)

    def predict(
        self, actions: DataFrame[SPADLSchema], use_interpolation: bool = False
    ) -> npt.NDArray[np.float64]:  # pragma: no cover
        """Compute the xT values for the given actions.

        xT should only be used to value actions that move the ball and also
        keep the current team in possession of the ball.

        Parameters
        ----------
        actions : pd.DataFrame
            Actions, in SPADL format.
        use_interpolation : bool
            Indicates whether to use bilinear interpolation when inferring xT
            values. Note that this requires Scipy to be installed (pip install
            scipy).

        Returns
        -------
        np.ndarray
            The xT value for each action.

        .. deprecated:: 1.0.3
               Use :func:`socceraction.xthreat.ExpectedThreat.rate` instead.
        """
        warnings.warn('predict is deprecated, use rate instead', DeprecationWarning)
        return self.rate(actions, use_interpolation)

    def rate(
        self, actions: DataFrame[SPADLSchema], use_interpolation: bool = False
    ) -> npt.NDArray[np.float64]:
        """Compute the xT values for the given actions.

        xT should only be used to value actions that move the ball and also
        keep the current team in possession of the ball. All other actions in
        the given dataframe receive a `NaN` rating.

        Parameters
        ----------
        actions : pd.DataFrame
            Actions, in SPADL format.
        use_interpolation : bool
            Indicates whether to use bilinear interpolation when inferring xT
            values. Note that this requires Scipy to be installed (pip install
            scipy).

        Returns
        -------
        np.ndarray
            The xT value for each action.
        """
        if not np.any(self.xT):
            raise NotFittedError()

        if not use_interpolation:
            l = self.l
            w = self.w
            grid = self.xT
        else:
            # Use interpolation to create a
            # more fine-grained 1050 x 680 grid
            interp = self.interpolator()
            l = int(spadlconfig.field_length * 10)
            w = int(spadlconfig.field_width * 10)
            xs = np.linspace(0, spadlconfig.field_length, l)
            ys = np.linspace(0, spadlconfig.field_width, w)
            grid = interp(xs, ys)

        ratings = np.empty(len(actions))
        ratings[:] = np.NaN

        move_actions = get_successful_move_actions(actions)

        startxc, startyc = _get_cell_indexes(move_actions.start_x, move_actions.start_y, l, w)
        endxc, endyc = _get_cell_indexes(move_actions.end_x, move_actions.end_y, l, w)

        xT_start = grid[startyc.rsub(w - 1), startxc]
        xT_end = grid[endyc.rsub(w - 1), endxc]

        ratings[move_actions.index] = xT_end - xT_start
        return ratings

    def save_model(self, filepath: str, overwrite: bool = True) -> None:
        """Save the xT value surface in JSON format.

        This stores only the xT value surface, which is all you need to compute
        xT values for new data. The value surface can be loaded back with the
        :func:`socceraction.xthreat.load_model` function.

        Pickle the `ExpectedThreat` instance to store the entire model and to
        retain the transition, shot probability, move probability and scoring
        probability matrices.

        Parameters
        ----------
        filepath : String or PathLike
            Path to the file to save the value surface to.
        overwrite :
            Whether to silently overwrite any existing file at the target
            location.
        """
        if not np.any(self.xT):
            raise NotFittedError()

        # If file exists and should not be overwritten:
        if not overwrite and os.path.isfile(filepath):
            raise ValueError(
                'save_xt got overwrite="False", but a file '
                f'({filepath}) exists already. No data was saved.'
            )
        with open(filepath, 'w') as f:
            json.dump(self.xT.tolist(), f)


def load_model(path_or_buf: str) -> ExpectedThreat:
    """Create a model from a pre-computed xT value surface.

    The value surface should be provided as a JSON file containing a 2D
    matrix. Karun Singh provides such a grid at the follwing url:
    https://karun.in/blog/data/open_xt_12x8_v1.json

    Parameters
    ----------
    path_or_buf : a valid JSON str, path object or file-like object
        Any valid string path is acceptable. The string could be a URL. Valid
        URL schemes include http, ftp, s3, and file.

    Returns
    -------
    ExpectedThreat
        An xT model that uses the given value surface to value actions.
    """
    grid = pd.read_json(path_or_buf)
    model = ExpectedThreat()
    model.xT = grid.values
    model.w, model.l = model.xT.shape
    return model
