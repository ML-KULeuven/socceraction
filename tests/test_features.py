import math

import pytest
import socceraction.features as fs
import socceraction.spadl as spadl
from pandera.typing import DataFrame
from socceraction.features.event.statsbomb.shot_blocking import (
    is_inside_triangle_smoothed,
)
from socceraction.spadl import SPADLSchema


def test_actiontype(spadl_actions: DataFrame[SPADLSchema]) -> None:
    ltr_actions = spadl.play_left_to_right(spadl_actions, 782)
    out = fs.actiontype(ltr_actions)
    assert out.shape == (len(spadl_actions), 1)


def test_actiontype_onehot(spadl_actions: DataFrame[SPADLSchema]) -> None:
    ltr_actions = spadl.play_left_to_right(spadl_actions, 782)
    out = fs.actiontype_onehot(ltr_actions)
    assert out.shape == (len(spadl_actions), len(spadl.config.actiontypes))


def test_result(spadl_actions: DataFrame[SPADLSchema]) -> None:
    ltr_actions = spadl.play_left_to_right(spadl_actions, 782)
    out = fs.result(ltr_actions)
    assert out.shape == (len(spadl_actions), 1)


def test_result_onehot(spadl_actions: DataFrame[SPADLSchema]) -> None:
    ltr_actions = spadl.play_left_to_right(spadl_actions, 782)
    out = fs.result_onehot(ltr_actions)
    assert out.shape == (len(spadl_actions), len(spadl.config.results))


def test_actiontype_result_onehot(spadl_actions: DataFrame[SPADLSchema]) -> None:
    ltr_actions = spadl.play_left_to_right(spadl_actions, 782)
    out = fs.actiontype_result_onehot(ltr_actions)
    assert out.shape == (
        len(spadl_actions),
        len(spadl.config.actiontypes) * len(spadl.config.results),
    )


def test_bodypart(spadl_actions: DataFrame[SPADLSchema]) -> None:
    ltr_actions = spadl.play_left_to_right(spadl_actions, 782)
    out = fs.bodypart(ltr_actions)
    assert out.shape == (len(spadl_actions), 1)


def test_bodypart_onehot(spadl_actions: DataFrame[SPADLSchema]) -> None:
    ltr_actions = spadl.play_left_to_right(spadl_actions, 782)
    out = fs.bodypart_onehot(ltr_actions)
    assert out.shape == (len(spadl_actions), 4)


def test_shot_angle(shot: DataFrame[SPADLSchema]) -> None:
    # Test output feature names
    df = fs.shot_angle(shot, mask=[True])
    assert df.columns.tolist() == ["angle_shot"]
    assert len(df) == 1
    # Ball on goalline in center of goal
    shot.loc[:, ["start_x", "start_y"]] = [105, 34]
    df = fs.shot_angle(shot, mask=[True])
    assert df.loc[0, "angle_shot"] == 0
    # Ball on goalline next to goal
    shot.loc[:, ["start_x", "start_y"]] = [105, 0]
    df = fs.shot_angle(shot, mask=[True])
    assert df.loc[0, "angle_shot"] == math.pi / 2
    # Constant as ball moves away from goal
    shot.loc[:, ["start_x", "start_y"]] = [105 - 10, 34]
    df1 = fs.shot_angle(shot, mask=[True])
    shot.loc[:, ["start_x", "start_y"]] = [105 - 11, 34]
    df2 = fs.shot_angle(shot, mask=[True])
    assert df1.loc[0, "angle_shot"] == df2.loc[0, "angle_shot"]
    # Is the same left and right from the goal
    shot.loc[:, ["start_x", "start_y"]] = [105 - 11, 34 - 5]
    df1 = fs.shot_angle(shot, mask=[True])
    shot.loc[:, ["start_x", "start_y"]] = [105 - 11, 34 + 5]
    df2 = fs.shot_angle(shot, mask=[True])
    assert df1.loc[0, "angle_shot"] == df2.loc[0, "angle_shot"]


def test_shot_visible_angle(shot: DataFrame[SPADLSchema]) -> None:
    # Test output feature names
    df = fs.shot_visible_angle(shot, mask=[True])
    assert df.columns.tolist() == ["visible_angle_shot"]
    assert len(df) == 1
    # Ball on goalline in center of goal
    shot.loc[:, ["start_x", "start_y"]] = [105, 34]
    df = fs.shot_visible_angle(shot, mask=[True])
    assert df.loc[0, "visible_angle_shot"] == math.pi
    # Ball on goalline next to goal
    shot.loc[:, ["start_x", "start_y"]] = [105, 0]
    df = fs.shot_visible_angle(shot, mask=[True])
    assert df.loc[0, "visible_angle_shot"] == 0
    # Decreases as ball moves away from goal
    shot.loc[:, ["start_x", "start_y"]] = [105 - 10, 34]
    df1 = fs.shot_visible_angle(shot, mask=[True])
    shot.loc[:, ["start_x", "start_y"]] = [105 - 11, 34]
    df2 = fs.shot_visible_angle(shot, mask=[True])
    assert df1.loc[0, "visible_angle_shot"] > df2.loc[0, "visible_angle_shot"]
    # Is the same left and right from the goal
    shot.loc[:, ["start_x", "start_y"]] = [105 - 11, 34 - 5]
    df1 = fs.shot_visible_angle(shot, mask=[True])
    shot.loc[:, ["start_x", "start_y"]] = [105 - 11, 34 + 5]
    df2 = fs.shot_visible_angle(shot, mask=[True])
    assert df1.loc[0, "visible_angle_shot"] == df2.loc[0, "visible_angle_shot"]


def test_shot_relative_angle(shot: DataFrame[SPADLSchema]) -> None:
    # Test output feature names
    df = fs.shot_relative_angle(shot, mask=[True])
    assert df.columns.tolist() == ["relative_angle_shot"]
    assert len(df) == 1
    # Ball on goalline in center of goal
    shot.loc[:, ["start_x", "start_y"]] = [105, 34]
    df = fs.shot_relative_angle(shot, mask=[True])
    assert df.loc[0, "relative_angle_shot"] == 1
    # Ball on goalline next to goal
    shot.loc[:, ["start_x", "start_y"]] = [105, 0]
    df = fs.shot_relative_angle(shot, mask=[True])
    assert df.loc[0, "relative_angle_shot"] == 0
    shot.loc[:, ["start_x", "start_y"]] = [105, 68]
    df = fs.shot_relative_angle(shot, mask=[True])
    assert df.loc[0, "relative_angle_shot"] == 0
    # Decreases as ball moves away from goal
    shot.loc[:, ["start_x", "start_y"]] = [100, 20]
    df1 = fs.shot_relative_angle(shot, mask=[True])
    shot.loc[:, ["start_x", "start_y"]] = [100, 10]
    df2 = fs.shot_relative_angle(shot, mask=[True])
    assert df1.loc[0, "relative_angle_shot"] > df2.loc[0, "relative_angle_shot"]
    # Is the same left and right from the goal
    shot.loc[:, ["start_x", "start_y"]] = [105 - 11, 34 - 5]
    df1 = fs.shot_relative_angle(shot, mask=[True])
    shot.loc[:, ["start_x", "start_y"]] = [105 - 11, 34 + 5]
    df2 = fs.shot_relative_angle(shot, mask=[True])
    assert df1.loc[0, "relative_angle_shot"] == pytest.approx(df2.loc[0, "relative_angle_shot"])


def test_shot_dist(shot: DataFrame[SPADLSchema]) -> None:
    # Test output feature names
    df = fs.shot_dist(shot, mask=[True])
    assert df.columns.tolist() == ["dist_shot"]
    assert len(df) == 1
    # Ball on goalline in center of goal
    shot.loc[:, ["start_x", "start_y"]] = [105, 34]
    df = fs.shot_dist(shot, mask=[True])
    assert df.loc[0, "dist_shot"] == 0
    # Ball on goalline next to goal
    shot.loc[:, ["start_x", "start_y"]] = [105, 0]
    df = fs.shot_dist(shot, mask=[True])
    assert df.loc[0, "dist_shot"] == 34
    # Ball on penalty spot
    shot.loc[:, ["start_x", "start_y"]] = [105 - 11, 34]
    df = fs.shot_dist(shot, mask=[True])
    assert df.loc[0, "dist_shot"] == 11


def test_shot_location(shot: DataFrame[SPADLSchema]) -> None:
    # Test output feature names
    df = fs.shot_location(shot, mask=[True])
    assert df.columns.tolist() == ["dx_shot", "dy_shot"]
    assert len(df) == 1
    # Test feature values
    assert df.loc[0, "dx_shot"] == 11
    assert df.loc[0, "dy_shot"] == 0


class TestGaussianTriangleIntegral:
    """Test class for the is_inside_triangle_smoothed function."""

    enable_plotting = False

    def _plot_gaussian_and_triangle(
        self,
        mu: tuple[float, float],
        Sigma: tuple[tuple[float, float], tuple[float, float]],
        triangle: tuple[tuple[float, float], tuple[float, float], tuple[float, float]],
        save_path: str | None = None,
    ) -> tuple:
        """
        Create a visualization of a 2D Gaussian distribution with an overlaid triangle.

        This function generates a contour plot of a bivariate normal distribution and
        overlays a triangular region to visualize the integration domain used in
        gaussian_triangle_integral.

        Parameters
        ----------
        mu : array-like or list of float
            Mean vector of the Gaussian distribution [mu_x, mu_y]
        Sigma : 2x2 array-like or list of lists
            Covariance matrix of the Gaussian distribution [[var_x, cov_xy], [cov_xy, var_y]]
        triangle : tuple of three tuples
            Coordinates of the triangle vertices [(x1, y1), (x2, y2), (x3, y3)]
        save_path : str, optional
            If provided, saves the plot to this path instead of displaying it

        Returns
        -------
        matplotlib.figure.Figure
            The figure object containing the plot
        matplotlib.axes.Axes
            The axes object containing the plot elements
        """
        import matplotlib.pyplot as plt
        import numpy as np
        from matplotlib.patches import Polygon

        # Create grid covering ±3 standard deviations from the mean
        x = np.linspace(mu[0] - 3 * np.sqrt(Sigma[0][0]), mu[0] + 3 * np.sqrt(Sigma[0][0]), 100)
        y = np.linspace(mu[1] - 3 * np.sqrt(Sigma[1][1]), mu[1] + 3 * np.sqrt(Sigma[1][1]), 100)
        X, Y = np.meshgrid(x, y)
        pos = np.dstack((X, Y))

        # Compute Gaussian PDF values across the grid
        det_Sigma = np.linalg.det(Sigma)
        inv_Sigma = np.linalg.inv(Sigma)
        constant = 1.0 / (2 * np.pi * np.sqrt(det_Sigma))
        Z = constant * np.exp(-0.5 * np.einsum("...k,kl,...l->...", pos - mu, inv_Sigma, pos - mu))

        # Create plot
        fig, ax = plt.subplots(figsize=(8, 6))
        contour = ax.contourf(X, Y, Z, levels=50, cmap="viridis")

        # Plot triangle as a red outlined polygon
        tri_patch = Polygon(triangle, closed=True, edgecolor="r", facecolor="none", linewidth=2)
        ax.add_patch(tri_patch)

        # Add colorbar and labels
        fig.colorbar(contour, ax=ax, label="Probability Density")
        ax.set_xlabel("X")
        ax.set_ylabel("Y")
        ax.set_title("2D Gaussian Distribution with Integration Triangle")
        ax.set_aspect("equal")

        # Save or show the plot
        if save_path:
            plt.savefig(save_path)
            plt.close(fig)
        else:
            plt.show()

        return fig, ax

    def test_standard_normal_unit_triangle(self) -> None:
        """Test integration of standard normal over unit triangle in first quadrant."""
        mu = (0.0, 0.0)
        Sigma = ((0.01, 0.0), (0.0, 0.01))
        triangle = ((0.0, 0.0), (1.0, 0.0), (0.0, 1.0))

        result = is_inside_triangle_smoothed(mu, Sigma, triangle)

        # Approximately the entire quadrant, should be close to 0.25
        pytest.approx(result, 0.25, abs=0.01)

        if self.enable_plotting:
            self._plot_gaussian_and_triangle(
                mu, Sigma, triangle, save_path="test_plots/standard_normal_unit_triangle.png"
            )

    def test_degenerate_triangle(self) -> None:
        """Test that a degenerate triangle (zero area) returns zero."""
        mu = (0.0, 0.0)
        Sigma = ((1.0, 0.0), (0.0, 1.0))
        triangle = ((0, 0), (0, 0), (0, 0))  # All points same - degenerate

        result = is_inside_triangle_smoothed(mu, Sigma, triangle)
        assert result == 0.0

    def test_large_triangle(self) -> None:
        """Test integration over a triangle that covers most of the Gaussian."""
        mu = (0.0, 0.0)
        Sigma = ((1.0, 0.0), (0.0, 1.0))
        triangle = (
            (-10.0, -10.0),
            (10.0, -10.0),
            (0.0, 10.0),
        )  # Large triangle covering most of the Gaussian

        result = is_inside_triangle_smoothed(mu, Sigma, triangle)

        # Should be close to 1 (but not exactly due to numerical integration limits)
        assert result < 1.0
        pytest.approx(result, 1.0, abs=0.01)

        if self.enable_plotting:
            self._plot_gaussian_and_triangle(
                mu, Sigma, triangle, save_path="test_plots/large_triangle.png"
            )

    def test_shifted_gaussian(self) -> None:
        """Test integration with a Gaussian not centered at origin."""
        mu = (1.0, 1.0)
        Sigma = ((0.05, 0.0), (0.0, 0.05))
        triangle = ((0.0, 0.0), (2.0, 0.0), (0.0, 2.0))

        result = is_inside_triangle_smoothed(mu, Sigma, triangle)

        # Should be positive and less than 1
        pytest.approx(result, 0.5, abs=0.1)

        if self.enable_plotting:
            self._plot_gaussian_and_triangle(
                mu, Sigma, triangle, save_path="test_plots/shifted_gaussian.png"
            )

    def test_correlated_gaussian(self) -> None:
        """Test integration with a correlated Gaussian."""
        mu = (0.0, 0.0)
        Sigma = ((0.10, 0.05), (0.05, 0.10))  # Positive correlation
        triangle = ((-1.0, -1.0), (1.0, -1.0), (0.0, 1.0))

        result = is_inside_triangle_smoothed(mu, Sigma, triangle)

        # Should be positive and less than 1
        assert result > 0
        assert result < 1.0

        if self.enable_plotting:
            self._plot_gaussian_and_triangle(
                mu, Sigma, triangle, save_path="test_plots/correlated_gaussian.png"
            )
