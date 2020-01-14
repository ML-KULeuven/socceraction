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
    return np.divide(
        goalmatrix, shotmatrix, out=np.zeros_like(shotmatrix), where=shotmatrix != 0
    )


def action_prob(actions, l=N, w=M):
    """ Compute the probability of taking an action in each cell of the grid. The options are: shooting or moving.

    :param actions: Actions, in SPADL format.
    :param l: Amount of grid cells in the x-dimension of the grid.
    :param w: Amount of grid cells in the y-dimension of the grid.
    :return: 2 matrices, denoting for each cell the probability of choosing to shoot
    and the probability of choosing to move. Summing both results in a probability of 1 for each cell.
    """
    move_actions = actions[
        (
            (
                (actions.type_name == "pass")
                | (actions.type_name == "dribble")
                | (actions.type_name == "cross")
            )
            & (actions.result_name == "success")
        )
    ]
    shot_actions = actions[(actions.type_name == "shot")]

    movematrix = _count(move_actions.start_x, move_actions.start_y, l, w)
    shotmatrix = _count(shot_actions.start_x, shot_actions.start_y, l, w)
    totalmatrix = movematrix + shotmatrix
    return shotmatrix / totalmatrix, movematrix / totalmatrix


def move_transition_matrix(actions, l=N, w=M):
    """ Compute the move transition matrix from the given actions.

    This is, when a player chooses to move, the probability that he will
    end up in each of the other cells of the grid successfully.

    :param actions: Actions, in SPADL format.
    :param l: Amount of grid cells in the x-dimension of the grid.
    :param w: Amount of grid cells in the y-dimension of the grid.
    :return: The transition matrix.
    """
    move_actions = actions[
        (
            (actions.type_name == "pass")
            | (actions.type_name == "dribble")
            | (actions.type_name == "cross")
        )
    ]

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

    def predict(self, actions, use_interpolation=True):
        """ Predicts the xT values for the given actions.

        :param actions: Actions, in SPADL format.
        :param use_interpolation: Indicates whether to use bilinear interpolation when inferring xT values.
        :return: Each action, including its xT value.
        """
        predictions = actions.copy()
        predictions["xT_value"] = np.nan
        mask = (
            (
                (predictions.type_name == "pass")
                | (predictions.type_name == "dribble")
                | (predictions.type_name == "cross")
            )
            & (predictions.result_name == "success")
            & (~predictions.start_x.isna())
            & (~predictions.start_y.isna())
            & (~predictions.end_x.isna())
            & (~predictions.end_y.isna())
        )
        candidates = predictions[mask]

        if not use_interpolation:
            startxc, startyc = _get_cell_indexes(
                candidates.start_x, candidates.start_y, self.l, self.w
            )
            endxc, endyc = _get_cell_indexes(
                candidates.end_x, candidates.end_y, self.l, self.w
            )
            xT_start = self.xT[self.w - 1 - startyc, startxc]
            xT_end = self.xT[self.w - 1 - endyc, endxc]
            xT_diffs = xT_end - xT_start
        else:
            from scipy.interpolate import interp2d

            interp_xs = np.arange(0.0, spadl_length, spadl_length / N) + (
                0.5 * spadl_length / N
            )
            interp_ys = np.arange(0.0, spadl_width, spadl_width / M) + (
                0.5 * spadl_width / M
            )
            # Reverse y-axis for inference
            interp = interp2d(
                x=interp_xs,
                y=interp_ys[::-1],
                z=self.xT,
                kind="linear",
                bounds_error=False,
            )
            xT_diffs = []
            for index, row in candidates.iterrows():
                xT_diffs.append(
                    interp(row["end_x"], row["end_y"])
                    - interp(row["start_x"], row["start_y"])
                )

        predictions.loc[mask, "xT_value"] = xT_diffs
        return predictions

    def visualize_heatmaps(self):
        """ Visualizes the heatmap of each iteration of the model. """
        try:
            import matplotsoccer

            for hm in self.heatmaps:
                matplotsoccer.heatmap(hm)
        except ImportError:
            warnings.warn("Could not import the following package: matplotsoccer.")

    def visualize_surface_plots(self):
        """ Visualizes the surface plot of each iteration of the model.

            See https://plot.ly/python/sliders/ and https://karun.in/blog/expected-threat.html#visualizing-xt
            NOTE: y-axis is mirrored in plotly.
        """
        try:
            import plotly.graph_objects as go

            camera = dict(
                up=dict(x=0, y=0, z=1),
                center=dict(x=0, y=0, z=0),
                eye=dict(x=-2.25, y=-1, z=0.5),
            )

            max_z = np.around(self.xT.max() + 0.05, decimals=1)

            layout = go.Layout(
                title="Expected Threat",
                autosize=True,
                width=500,
                height=500,
                margin=dict(l=65, r=50, b=65, t=90),
                scene=dict(
                    camera=camera,
                    aspectmode="auto",
                    xaxis=dict(),
                    yaxis=dict(),
                    zaxis=dict(autorange=False, range=[0, max_z]),
                ),
            )

            fig = go.Figure(layout=layout)

            for i in self.heatmaps:
                fig.add_trace(go.Surface(z=i))

            # Make last trace visible
            for i in range(len(fig.data) - 1):
                fig.data[i].visible = False
            fig.data[len(fig.data) - 1].visible = True

            # Create and add slider
            steps = []
            for i in range(len(fig.data)):
                step = dict(method="restyle", args=["visible", [False] * len(fig.data)])
                step["args"][1][i] = True  # Toggle i'th trace to "visible"
                steps.append(step)

            sliders = [
                dict(
                    active=(len(fig.data) - 1),
                    currentvalue={"prefix": "Iteration: "},
                    pad={"t": 50},
                    steps=steps,
                )
            ]

            fig.update_layout(sliders=sliders)

            fig.show()

        except ImportError:
            warnings.warn("Could not import the following package: plotly.")
