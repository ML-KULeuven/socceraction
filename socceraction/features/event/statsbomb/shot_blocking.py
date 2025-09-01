"""Utilty functions for computing shot blocking-related features."""

import numpy as np
from scipy.integrate import dblquad
from scipy.stats import norm


def right_of_x(
    point: tuple[float, float],
    x0: float,
) -> bool:
    """
    Check if a point is to the right of a vertical line x = x0.

    Parameters
    ----------
    point : tuple of float
        Coordinates of the point (x, y)
    x0 : float
        Vertical cutoff. Checks if x <= x0.

    Returns
    -------
    bool
        True if the point is to the right of the line (x > x0), False otherwise.
    """
    x, _ = point
    return x > x0


def gaussian_mass_right_of_x(
    mu: tuple[float, float],
    Sigma: tuple[tuple[float, float], tuple[float, float]],
    x0: float,
) -> float:
    """
    Check if a point is to the right of a vertical line x = x0.

    Compute the probability mass of a 2D Gaussian distribution
    to the right of a vertical line x = x0 (i.e. integral over {x > x0}).

    Parameters
    ----------
    mu : tuple of float
        Mean vector of the Gaussian distribution [mu_x, mu_y]
    Sigma : 2x2 tuple of tuples
        Covariance matrix [[var_x, cov_xy], [cov_xy, var_y]]
    x0 : float
        Vertical cutoff. Integrates over x <= x0.

    Returns
    -------
    float
        The probability mass of the Gaussian distribution with x > x0
    """
    mu_arr = np.array(mu)
    Sigma_arr = np.array(Sigma)
    mu_x = mu_arr[0]
    sigma_x = np.sqrt(Sigma_arr[0, 0])

    return 1.0 - norm.cdf(x0, loc=mu_x, scale=sigma_x)


def is_inside_triangle(
    point: tuple[float, float],
    triangle: tuple[tuple[float, float], tuple[float, float], tuple[float, float]],
) -> bool:
    """
    Check if a point is inside a triangle defined by three vertices.

    Parameters
    ----------
    point : tuple of float
        Coordinates of the point (x, y)
    triangle : tuple of three tuples
        Coordinates of the triangle vertices ((x1, y1), (x2, y2), (x3, y3))

    Returns
    -------
    bool
        True if the point is inside the triangle or on its edges, False otherwise.
    """
    Dx, Dy = point

    A, B, C = triangle
    Ax, Ay = A
    Bx, By = B
    Cx, Cy = C

    M1 = np.array([[Dx - Bx, Dy - By, 0], [Ax - Bx, Ay - By, 0], [1, 1, 1]])
    M1 = np.linalg.det(M1)

    M2 = np.array([[Dx - Ax, Dy - Ay, 0], [Cx - Ax, Cy - Ay, 0], [1, 1, 1]])
    M2 = np.linalg.det(M2)

    M3 = np.array([[Dx - Cx, Dy - Cy, 0], [Bx - Cx, By - Cy, 0], [1, 1, 1]])
    M3 = np.linalg.det(M3)

    if M1 == 0 or M2 == 0 or M3 == 0:
        # lies on the arms of Triangle
        return True
    if (M1 > 0 and M2 > 0 and M3 > 0) or (M1 < 0 and M2 < 0 and M3 < 0):
        # if products is non 0 check if all of their sign is same
        # lies inside the Triangle
        return True
    # lies outside the Triangle
    return False


def is_inside_triangle_smoothed(
    mu: tuple[float, float],
    Sigma: tuple[tuple[float, float], tuple[float, float]],
    triangle: tuple[tuple[float, float], tuple[float, float], tuple[float, float]],
    epsabs: float = 1e-6,
    epsrel: float = 1e-6,
) -> float:
    """
    Compute the integral of a 2D Gaussian distribution over a triangular region.

    This function calculates the probability mass of a bivariate normal distribution
    that lies within a specified triangular region using numerical integration with
    a change of variables to transform the integral over the triangle into an
    integral over the unit triangle.

    Parameters
    ----------
    mu : array-like or list of float
        Mean vector of the Gaussian distribution [mu_x, mu_y]
    Sigma : 2x2 array-like or list of lists
        Covariance matrix of the Gaussian distribution [[var_x, cov_xy], [cov_xy, var_y]]
    triangle : tuple of three tuples
        Coordinates of the triangle vertices [(x1, y1), (x2, y2), (x3, y3)]
    epsabs : float, optional
        Absolute error tolerance for numerical integration (default: 1e-6)
    epsrel : float, optional
        Relative error tolerance for numerical integration (default: 1e-6)

    Returns
    -------
    float
        The probability mass of the Gaussian distribution within the triangle

    Implementation Details
    ----------------------
    The method uses barycentric coordinates to transform the integral over the triangle
    into an integral over the unit triangle (u ≥ 0, v ≥ 0, u + v ≤ 1). The Jacobian
    determinant of this transformation is constant and equals twice the area of the
    triangle, which is used to scale the final result.

    The Gaussian probability density function is defined as:
        f(x,y) = (1/(2π√|Σ|)) * exp(-0.5 * (x-μ)ᵀ Σ⁻¹ (x-μ))

    The integral is computed numerically using scipy.integrate.dblquad with careful
    handling of the transformation between coordinate systems.
    """
    A, B, C = triangle
    Ax, Ay = A
    Bx, By = B
    Cx, Cy = C

    # Calculate triangle area using the shoelace formula
    area = 0.5 * abs((Bx - Ax) * (Cy - Ay) - (Cx - Ax) * (By - Ay))
    if np.isclose(area, 0):
        return 0.0

    # Precompute Gaussian distribution parameters
    det_Sigma = np.linalg.det(Sigma)
    inv_Sigma = np.linalg.inv(Sigma)
    constant = 1.0 / (2 * np.pi * np.sqrt(det_Sigma))
    mu_arr = np.array(mu)

    def integrand(u: float, v: float) -> float:
        """Evaluate the Gaussian PDF at a point in the triangle.

        The transformation from barycentric coordinates (u,v) to Cartesian coordinates (x,y)
        is given by:
            x = A_x + u*(B_x - A_x) + v*(C_x - A_x)
            y = A_y + u*(B_y - A_y) + v*(C_y - A_y)
        """
        x = Ax + u * (Bx - Ax) + v * (Cx - Ax)
        y = Ay + u * (By - Ay) + v * (Cy - Ay)
        point = np.array([x, y])
        diff = point - mu_arr
        exponent = -0.5 * (diff.T @ inv_Sigma @ diff)
        return constant * np.exp(exponent)

    # Perform numerical integration over the unit triangle
    integral_val, _ = dblquad(  # type: ignore
        integrand,
        0,
        1,  # u limits
        lambda u: 0,  # v lower limit
        lambda u: 1 - u,  # v upper limit (u+v ≤ 1)
        epsabs=epsabs,
        epsrel=epsrel,
    )

    # Scale by Jacobian determinant (2 * area)
    return 2 * area * integral_val


def plot_gaussian_and_triangle(
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


if __name__ == "__main__":
    shooter = [98.0, 40.0]
    defenders = [
        {"location": [118.0, 42.0], "position": "goalkeeper"},
        {"location": [118.0, 38.0], "position": "defender"},
        {"location": [114.0, 39.0], "position": "defender"},
    ]
    left_post = [120.0, 36.0]
    right_post = [120.0, 44.0]

    mu = defenders[0]["location"]
    Sigma = ((1, 0), (0, 1))
    triangle = (shooter, left_post, right_post)
    prob = is_inside_triangle_smoothed(mu, Sigma, triangle)
    print(f"Probability mass inside triangle: {prob}")
    fig, ax = plot_gaussian_and_triangle(mu, Sigma, triangle)
