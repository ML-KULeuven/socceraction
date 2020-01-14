import numpy as np
import pandas as pd
import warnings

import socceraction.spadl.config as spadlcfg

spadl_length = spadlcfg.spadl_length
spadl_width = spadlcfg.spadl_width
M = 12
N = 16


def _get_cell_indexes(x, y, l=N, w=M):
    xmin = 0
    ymin = 0

    xi = (x - xmin) / spadl_length * l
    yj = (y - ymin) / spadl_width * w
    xi = xi.astype(int).clip(0, l - 1)
    yj = yj.astype(int).clip(0, w - 1)
    return xi, yj


def _get_flat_indexes(x, y, l=N, w=M):
    xi, yj = _get_cell_indexes(x, y, l, w)
    return l * (w - 1 - yj) + xi


def _count(x, y, l=N, w=M):
    """ Count the number of actions occurring in each cell of the grid.

    :param x: The x-coordinates of the actions.
    :param y: The y-coordinates of the actions.
    :param l: Amount of grid cells in the x-dimension of the grid.
    :param w: Amount of grid cells in the y-dimension of the grid.
    :return: A matrix, denoting the amount of actions occurring in each cell. The top-left corner is the origin.
    """
    x = x[~np.isnan(x) & ~np.isnan(y)]
    y = y[~np.isnan(x) & ~np.isnan(y)]

    flat_indexes = _get_flat_indexes(x, y, l, w)
    vc = flat_indexes.value_counts(sort=False)
    vector = np.zeros(w * l)
    vector[vc.index] = vc
    return vector.reshape((w, l))


def safe_divide(a, b):
    return np.divide(a, b, out=np.zeros_like(a), where=b != 0)


def scoring_prob(actions, l=N, w=M):
    """ Compute the probability of scoring when taking a shot for each cell.

    :param actions: Actions, in SPADL format.
    :param l: Amount of grid cells in the x-dimension of the grid.
    :param w: Amount of grid cells in the y-dimension of the grid.
    :return: A matrix, denoting the probability of scoring for each cell.
    """
    shot_actions = actions[(actions.type_name == "shot")]
    goals = shot_actions[(shot_actions.result_name == "success")]

    shotmatrix = _count(shot_actions.start_x, shot_actions.start_y, l, w)
    goalmatrix = _count(goals.start_x, goals.start_y, l, w)
    return safe_divide(goalmatrix, shotmatrix)


def get_move_actions(actions):
    return actions[
        (actions.type_name == "pass")
        | (actions.type_name == "dribble")
        | (actions.type_name == "cross")
    ]


def get_successful_move_actions(actions):
    move_actions = get_move_actions(actions)
    return move_actions[move_actions.result_name == "success"]


def action_prob(actions, l=N, w=M):
    """ Compute the probability of taking an action in each cell of the grid. The options are: shooting or moving.

    :param actions: Actions, in SPADL format.
    :param l: Amount of grid cells in the x-dimension of the grid.
    :param w: Amount of grid cells in the y-dimension of the grid.
    :return: 2 matrices, denoting for each cell the probability of choosing to shoot
    and the probability of choosing to move.
    """
    move_actions = get_move_actions(actions)
    shot_actions = actions[(actions.type_name == "shot")]

    movematrix = _count(move_actions.start_x, move_actions.start_y, l, w)
    shotmatrix = _count(shot_actions.start_x, shot_actions.start_y, l, w)
    totalmatrix = movematrix + shotmatrix

    return safe_divide(shotmatrix, totalmatrix), safe_divide(movematrix, totalmatrix)


def move_transition_matrix(actions, l=N, w=M):
    """ Compute the move transition matrix from the given actions.

    This is, when a player chooses to move, the probability that he will
    end up in each of the other cells of the grid successfully.

    :param actions: Actions, in SPADL format.
    :param l: Amount of grid cells in the x-dimension of the grid.
    :param w: Amount of grid cells in the y-dimension of the grid.
    :return: The transition matrix.
    """
    move_actions = get_move_actions(actions)

    X = pd.DataFrame()
    X["start_cell"] = _get_flat_indexes(
        move_actions.start_x, move_actions.start_y, l, w
    )
    X["end_cell"] = _get_flat_indexes(move_actions.end_x, move_actions.end_y, l, w)
    X["result_name"] = move_actions.result_name

    vc = X.start_cell.value_counts(sort=False)
    start_counts = np.zeros(w * l)
    start_counts[vc.index] = vc

    transition_matrix = np.zeros((w * l, w * l))

    for i in range(0, w * l):
        vc2 = X[
            ((X.start_cell == i) & (X.result_name == "success"))
        ].end_cell.value_counts(sort=False)
        transition_matrix[i, vc2.index] = vc2 / start_counts[i]

    return transition_matrix


class ExpectedThreat:
    """An implementation of Karun Singh's Expected Threat model (https://karun.in/blog/expected-threat.html)."""

    def __init__(self, l=N, w=M, eps=1e-5):
        self.l = l
        self.w = w
        self.eps = eps
        self.heatmaps = []
        self.xT = np.zeros((w, l))
        self.scoring_prob_matrix = np.zeros((w, l))
        self.shot_prob_matrix = np.zeros((w, l))
        self.move_prob_matrix = np.zeros((w, l))
        self.transition_matrix = np.zeros((w * l, w * l))

    def __solve(self, p_scoring, p_shot, p_move, transition_matrix):
        """Solves the expected threat equation with dynamic programming.

        :param p_scoring (matrix, shape(M, N)): Probability of scoring at each grid cell, when shooting from that cell.
        :param p_shot (matrix, shape(M,N)): For each grid cell, the probability of choosing to shoot from there.
        :param p_move (matrix, shape(M,N)): For each grid cell, the probability of choosing to move from there.
        :param transition_matrix (matrix, shape(M*N,M*N)): When moving, the probability of moving to each of the other zones.
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
                                transition_matrix[self.l * y + x, self.l * q + z]
                                * self.xT[q, z]
                            )

            newxT = gs + (p_move * total_payoff)
            diff = newxT - self.xT
            self.xT = newxT
            self.heatmaps.append(self.xT.copy())
            it += 1

        print("# iterations: ", it)

    def fit(self, actions):
        """ Fits the xT model with the given actions.

        :param actions: Actions, in SPADL format.
        """
        self.scoring_prob_matrix = scoring_prob(actions, self.l, self.w)
        self.shot_prob_matrix, self.move_prob_matrix = action_prob(
            actions, self.l, self.w
        )
        self.transition_matrix = move_transition_matrix(actions, self.l, self.w)
        self.__solve(
            self.scoring_prob_matrix,
            self.shot_prob_matrix,
            self.move_prob_matrix,
            self.transition_matrix,
        )

    def interpolator(self, kind="linear"):
        from scipy.interpolate import interp2d

        cell_length = spadl_length / self.l
        cell_width = spadl_width / self.w

        x = np.arange(0.0, spadl_length, cell_length) + 0.5 * cell_length
        y = np.arange(0.0, spadl_width, cell_width) + 0.5 * cell_width

        return interp2d(x=x, y=y, z=self.xT, kind=kind, bounds_error=False)

    def predict(self, actions, use_interpolation=True):
        """ Predicts the xT values for the given actions.

        :param actions: Actions, in SPADL format.
        :param use_interpolation: Indicates whether to use bilinear interpolation when inferring xT values.
        :return: Each action, including its xT value.
        """

        if not use_interpolation:
            l = self.l
            w = self.w
            grid = self.xT
        else:
            # Use interpolation to create a
            # more fine-grained 1050 x 680 grid
            interp = self.interpolator()
            l = spadl_length * 10
            w = spadl_width * 10
            xs = np.linspace(0, spadl_length, l)
            ys = np.linspace(0, spadl_width, w)
            grid = interp(xs, ys)

        startxc, startyc = _get_cell_indexes(actions.start_x, actions.start_y, l, w)
        endxc, endyc = _get_cell_indexes(actions.end_x, actions.end_y, l, w)

        xT_start = grid[w - 1 - startyc, startxc]
        xT_end = grid[w - 1 - endyc, endxc]

        return xT_end - xT_start
