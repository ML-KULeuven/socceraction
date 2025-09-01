"""Utility functions for computing lateral occlusion across the goal mouth.

Three methods are implemented:
- `occlusion_binary`: geometric, binary (no-smoothing) method with defender
  segments oriented perpendicular to shooter→defender LOS
- `occlusion_numeric`: numerical ray-based (Gaussian lateral kernel)
- `occlusion_analytic`: analytic marginalization assuming isotropic defender uncertainty
"""

from collections.abc import Sequence
from typing import TYPE_CHECKING, Optional, Union

import numpy as np
from scipy.stats import norm

if TYPE_CHECKING:
    import matplotlib.pyplot as plt
    from matplotlib.axes import Axes

# Constants
GOAL_X = 120.0
GOAL_Y_MIN = 36.0
GOAL_Y_MAX = 44.0
GOAL_WIDTH = GOAL_Y_MAX - GOAL_Y_MIN
GOAL_LEFT_POST = np.array([GOAL_X, GOAL_Y_MIN])
GOAL_RIGHT_POST = np.array([GOAL_X, GOAL_Y_MAX])
GOAL_CENTER = np.array([GOAL_X, (GOAL_Y_MAX + GOAL_Y_MIN) / 2])

# Type aliases
Point = Union[Sequence[float], np.ndarray]
Defender = Union[Sequence[float], dict]


def _normalize_defenders(defenders: Sequence[Defender]) -> list[dict]:
    """Normalize defender input to list of dicts with location and position."""
    normalized = []
    for defender in defenders:
        if isinstance(defender, dict):
            loc = tuple(defender["location"])
            pos = defender.get("position")
            normalized.append({"location": loc, "position": pos})
        else:
            loc = tuple(defender)
            normalized.append({"location": loc, "position": None})
    return normalized


def _get_intersection(a1: Point, a2: Point, b1: Point, b2: Point) -> tuple[float, float]:
    """Compute intersection of two lines in Cartesian coordinates.

    Return intersection point of lines through a1->a2 and b1->b2 in Cartesian coords.
    Returns (x, y) or (inf, inf) if parallel.
    """
    a1, a2, b1, b2 = map(np.array, [a1, a2, b1, b2])
    h1 = np.append(a1, 1)
    h2 = np.append(a2, 1)
    h3 = np.append(b1, 1)
    h4 = np.append(b2, 1)

    l1 = np.cross(h1, h2)
    l2 = np.cross(h3, h4)
    x, y, z = np.cross(l1, l2)

    return (x / z, y / z) if not np.isclose(z, 0) else (float("inf"), float("inf"))


def _get_intersection_old(
    a1: tuple[float, float],
    a2: tuple[float, float],
    b1: tuple[float, float],
    b2: tuple[float, float],
) -> tuple[float, float]:
    """Get the point of intersection of the lines passing through a2,a1 and b2,b1.

    Parameters
    ----------
    a1: [float, float]
        A point on the first line.
    a2: [float, float]
        Another point on the first line.
    b1: [float, float]
        A point on the second line.
    b2: [float, float]
        Another point on the second line.

    Returns
    -------
    [float, float]
        The point of intersection of the two lines.
    """
    s = np.vstack([a1, a2, b1, b2])  # s for stacked
    h = np.hstack((s, np.ones((4, 1))))  # h for homogeneous
    l1 = np.cross(h[0], h[1])  # get first line
    l2 = np.cross(h[2], h[3])  # get second line
    x, y, z = np.cross(l1, l2)  # point of intersection
    if z == 0:  # lines are parallel
        return (float("inf"), float("inf"))
    return (x / z, y / z)


def _overlap_old(
    min1: float, max1: float, min2: float, max2: float
) -> tuple[bool, None, None] | tuple[float, float, float]:
    start = max(min1, min2)
    end = min(max1, max2)
    d = end - start
    if d < 0:
        return False, None, None
    else:
        return d, start, end


def _get_blocked_intervals(
    shooter: Point,
    defenders: list[dict],
    goal_posts: tuple[Point, Point],
    goalkeeper_width: float,
    defender_width: float,
) -> list[tuple[float, float]]:
    """Compute blocked intervals on goal line from defenders."""
    shooter_arr = np.asarray(shooter)
    blocked_intervals = []

    for defender in defenders:
        def_loc = np.array(defender["location"])
        pos = defender["position"]
        width = goalkeeper_width if pos and str(pos).lower() == "goalkeeper" else defender_width

        # Calculate defender segment endpoints
        shooter_to_def = def_loc - shooter_arr
        distance = np.linalg.norm(shooter_to_def)
        if distance == 0:
            return [(GOAL_Y_MIN, GOAL_Y_MAX)]

        unit_vector = shooter_to_def / distance
        normal = np.array([-unit_vector[1], unit_vector[0]])
        half_width = width / 2

        p1 = def_loc - half_width * normal
        p2 = def_loc + half_width * normal

        # Check if defender is between shooter and goal
        shooter_to_goal = GOAL_CENTER - shooter_arr
        if np.dot(shooter_to_def, shooter_to_goal) <= 0:
            continue

        # Find intersections with goal line
        _, y1 = _get_intersection(shooter, p1, *goal_posts)
        _, y2 = _get_intersection(shooter, p2, *goal_posts)

        if not all(np.isfinite(y) for y in [y1, y2]):
            continue

        # Get y-coordinates of intersections (goal line is vertical at x=120)
        left_y = max(min(y1, y2), GOAL_Y_MIN)
        right_y = min(max(y1, y2), GOAL_Y_MAX)

        if right_y > left_y:
            blocked_intervals.append((left_y, right_y))

    return blocked_intervals


def _merge_intervals(intervals: list[tuple[float, float]]) -> list[tuple[float, float]]:
    """Merge overlapping intervals."""
    if not intervals:
        return []

    sorted_intervals = sorted(intervals, key=lambda x: x[0])
    merged = [list(sorted_intervals[0])]

    for start, end in sorted_intervals[1:]:
        if start <= merged[-1][1]:
            merged[-1][1] = max(merged[-1][1], end)
        else:
            merged.append([start, end])

    return merged


def occlusion_binary(
    shooter: Point,
    defenders: Sequence[Defender],
    nx: int = 240,
    goalkeeper_width: float = 1.60,
    defender_width: float = 0.80,
) -> tuple[np.ndarray, np.ndarray]:
    """
    Binary (no-smoothing) occlusion via geometric projection.

    Each defender is approximated as a short segment of length `width` centered at
    their location and oriented perpendicular to the shooter->defender LOS. The
    endpoints are projected to the goal line and the interval between those y
    coordinates is marked as blocked. Multiple defenders are unioned.

    Parameters
    ----------
    shooter : array-like, shape (2,)
        Shooter coordinates (x, y).
    defenders : sequence of defenders
        Can be array-like with shape (n_def, 2) or list of dicts with 'location' and optional 'position'
    nx : int, optional
        Number of samples across the goal width
    goalkeeper_width : float, optional
        Width assigned to goalkeepers (meters)
    defender_width : float, optional
        Width for outfield defenders (meters)

    Returns
    -------
    blocked_mask : ndarray, shape (nx,)
        Binary mask (0/1) across goal width where 1 indicates blocked
    ys : ndarray, shape (nx,)
        Y coordinates corresponding to samples across goal width
    """
    norm_defenders = _normalize_defenders(defenders)
    blocked_intervals = _get_blocked_intervals(
        shooter,
        norm_defenders,
        (GOAL_LEFT_POST, GOAL_RIGHT_POST),
        goalkeeper_width,
        defender_width,
    )
    merged = _merge_intervals(blocked_intervals)

    # Create binary mask
    ys = np.linspace(GOAL_Y_MIN, GOAL_Y_MAX, nx)
    blocked_mask = np.zeros_like(ys, dtype=float)

    for start, end in merged:
        mask = (ys >= start) & (ys <= end)
        blocked_mask[mask] = 1.0

    return blocked_mask, ys


def occlusion_numeric(
    shooter: Point,
    defenders: Sequence[Defender],
    nx: int = 240,
    goalkeeper_sigma: float = 0.40,
    defender_sigma: float = 0.80,
) -> tuple[np.ndarray, np.ndarray]:
    """
    Numerical (ray-based) occlusion across the goal width.

    Computes blocking probability for each sampled y coordinate across the goal line
    by applying a lateral Gaussian kernel to the perpendicular distance from each
    defender to the line-of-sight (LOS) joining shooter to that goal y-coordinate.

    Parameters
    ----------
    shooter : array-like, shape (2,)
        Shooter coordinates (x, y)
    defenders : sequence of defenders
        Can be array-like with shape (n_def, 2) or list of dicts with 'location' and optional 'position'
    nx : int, optional
        Number of sample points across the goal width
    goalkeeper_sigma : float
        Lateral kernel width (meters) for goalkeepers (p = exp(-0.5*(perp/sigma)**2)
    defender_sigma : float
        Lateral kernel width (meters) for outfield defenders (p = exp(-0.5*(perp/sigma)**2)

    Returns
    -------
    occlusion : ndarray, shape (nx,)
        Occlusion probabilities (0..1) sampled across the goal width
    ys : ndarray, shape (nx,)
        Y coordinates corresponding to occlusion samples
    """
    shooter_arr = np.asarray(shooter)
    norm_defenders = _normalize_defenders(defenders)

    ys = np.linspace(GOAL_Y_MIN, GOAL_Y_MAX, nx)
    goal_points = np.column_stack([np.full(nx, 120.0), ys])

    # Vectorized computation
    shooter_to_goal = goal_points - shooter_arr
    shooter_to_goal_norm2 = np.sum(shooter_to_goal**2, axis=1)
    shooter_to_goal_norm2[shooter_to_goal_norm2 == 0] = 1e-8

    all_p = []
    for defender in norm_defenders:
        def_loc = np.array(defender["location"])
        pos_name = defender.get("position")
        sigma = (
            goalkeeper_sigma
            if pos_name and str(pos_name).lower() == "goalkeeper"
            else defender_sigma
        )

        shooter_to_def = def_loc - shooter_arr

        # Project defender onto shooter-to-goal vectors
        t = np.dot(shooter_to_goal, shooter_to_def) / shooter_to_goal_norm2
        closest_points = shooter_arr + t[:, np.newaxis] * shooter_to_goal
        perpendicular_dist = np.linalg.norm(def_loc - closest_points, axis=1)

        # Check if defender is between shooter and goal point
        in_between = (t >= 0.0) & (t <= 1.0)
        p = np.exp(-0.5 * (perpendicular_dist / sigma) ** 2) * in_between
        all_p.append(p)

    # Combine probabilities from all defenders
    if not all_p:
        occlusion = np.zeros(nx)
    else:
        combined_free = np.ones(nx)
        for p in all_p:
            combined_free *= 1 - p
        occlusion = 1 - combined_free

    return occlusion, ys


def occlusion_analytic(
    shooter: Point,
    defenders: Sequence[Defender],
    nx: int = 240,
    pos_std: float = 0.2,
    defender_sigma: float = 0.4,
    goalkeeper_sigma: float = 0.8,
) -> tuple[np.ndarray, np.ndarray]:
    """
    Analytic vectorized lateral occlusion assuming isotropic defender positional uncertainty.

    Under an isotropic 2D Gaussian positional uncertainty (variance pos_std**2),
    this function marginalizes the defender position across the perpendicular axis
    in closed form and uses normal CDFs to compute the probability the defender
    projects between shooter and each goal-sample point.

    Parameters
    ----------
    shooter : array-like, shape (2,)
        Shooter coordinates (x, y)
    defenders : sequence of defenders
        Can be array-like with shape (n_def, 2) or list of dicts with 'location' and optional 'position'
    nx : int, optional
        Number of samples across goal width
    pos_std : float
        Defender positional standard deviation (isotropic), meters
    defender_sigma : float
        Lateral kernel sigma for outfield defenders (p(perp) = exp(-0.5*(perp/sigma)**2)
    goalkeeper_sigma : float
        Lateral kernel sigma for goalkeepers (p(perp) = exp(-0.5*(perp/sigma)**2)

    Returns
    -------
    occlusion : ndarray, shape (nx,)
        Expected occlusion probability across the goal width
    ys : ndarray, shape (nx,)
        Y coordinates corresponding to occlusion samples
    """
    shooter_arr = np.asarray(shooter)
    norm_defenders = _normalize_defenders(defenders)

    # Convert to array for vectorized computation
    defenders_arr = np.array([d["location"] for d in norm_defenders])
    is_goalkeeper = [d.get("position") == "goalkeeper" for d in norm_defenders]

    if defenders_arr.ndim == 1:
        defenders_arr = defenders_arr[np.newaxis, :]
        is_goalkeeper = [is_goalkeeper]  # Ensure it's a list even for single defender

    ys = np.linspace(GOAL_Y_MIN, GOAL_Y_MAX, nx)
    goal_points = np.column_stack([np.full(nx, 120.0), ys])

    # Vectorized computation
    shooter_to_goal = goal_points - shooter_arr
    L = np.linalg.norm(shooter_to_goal, axis=1)
    L_safe = np.where(L == 0, 1e-8, L)
    u = shooter_to_goal / L_safe[:, np.newaxis]
    n = np.column_stack([-u[:, 1], u[:, 0]])

    deltas = defenders_arr - shooter_arr
    mu_t = u @ deltas.T
    mu_perp = n @ deltas.T

    var_t = pos_std**2
    var_perp = pos_std**2

    # Create sigma array with appropriate values for each defender
    sigma_arr = np.array([goalkeeper_sigma if gk else defender_sigma for gk in is_goalkeeper])

    # Reshape for broadcasting
    sigma_arr = sigma_arr.reshape(1, -1)

    denom = var_perp + sigma_arr**2
    const_pref = np.sqrt(sigma_arr**2 / denom)
    exp_term = np.exp(-0.5 * mu_perp**2 / denom)

    s = np.sqrt(var_t)
    L_exp = np.tile(L_safe[:, np.newaxis], (1, deltas.shape[0]))

    cdf_upper = norm.cdf((L_exp - mu_t) / s)
    cdf_lower = norm.cdf((0.0 - mu_t) / s)
    prob_between = cdf_upper - cdf_lower

    p = const_pref * exp_term * prob_between
    p = np.clip(p, 0.0, 1.0)

    if p.size == 0:
        occlusion = np.zeros(nx)
    else:
        combined_free = np.prod(1.0 - p, axis=1)
        occlusion = 1.0 - combined_free

    return occlusion, ys


def mean_occlusion_percent(occlusion: np.ndarray) -> float:
    """Compute overall mean occlusion as percentage from a lateral occlusion array."""
    return float(np.mean(occlusion) * 100.0)


def occlusion_gaps(occlusion: np.ndarray, threshold: float = 0.5, min_width: int = 1) -> int:
    """Count the number of gaps in the occlusion profile.

    A "gap" is defined as a disjoint interval where the mean occlusion is
    below a threshold.

    Parameters
    ----------
    occlusion : np.ndarray
        1D array of occlusion values in [0, 1] across the goal mouth.
        Typically produced by a lateral occlusion function.
    threshold : float, default=0.5
        Values below this are considered "open" (low occlusion).
    min_width : int, default=1
        Minimum consecutive width (in samples) for a gap to count.
        Useful to ignore tiny fluctuations.

    Returns
    -------
    int
        Number of disjoint consecutive open gaps.

    Examples
    --------
    >>> occlusion = np.array([0.8, 0.9, 0.3, 0.2, 0.85, 0.1, 0.05, 0.9])
    >>> occlusion_gaps(occlusion, threshold=0.5)
    2
    """
    is_open = occlusion < threshold
    # Find transitions in boolean mask
    diff = np.diff(is_open.astype(int))
    starts = np.where(diff == 1)[0] + 1
    ends = np.where(diff == -1)[0] + 1

    # Handle edge cases where first/last sample is open
    if is_open[0]:
        starts = np.r_[0, starts]
    if is_open[-1]:
        ends = np.r_[ends, len(is_open)]

    # Count only intervals above min_width
    gaps = sum((end - start) >= min_width for start, end in zip(starts, ends))
    return gaps


# ------------------------- PLOTTING HELPERS -------------------------


def plot_occlusion(
    occlusion: np.ndarray,
    ys: np.ndarray,
    goal_height: float = 2.44,
    nz: int = 100,
    ax: Optional["Axes"] = None,
    **imshow_kwargs,  # noqa: ANN003
) -> "Axes":
    """
    Create and plot a heatmap on `ax` by repeating the occlusion across vertical height.

    Parameters
    ----------
    occlusion: ndarray, shape (ny,)
        Occlusion values across goal width.
    ys : ndarray, shape (ny,)
        Y coordinates for occlusion values (goal width coordinates).
    goal_height : float
        Vertical height of goal (meters).
    nz : int, optional
        Number of vertical samples for visualization.
    ax : matplotlib axis
        Axis to plot on.
    imshow_kwargs : dict
        Extra kwargs passed to `ax.imshow` (e.g., cmap, vmin, vmax).

    Returns
    -------
    im : AxesImage
        The image object from imshow.
    """
    if ax is None:
        ax = plt.gca()

    heat = np.tile(occlusion.reshape(1, -1), (nz, 1))

    ax.imshow(
        heat,
        origin="lower",
        extent=[ys[0], ys[-1], 0.0, goal_height],
        aspect="auto",
        **imshow_kwargs,
    )
    return ax


def plot_top_down_view(  # noqa: C901
    shooter: Point,
    defenders: Sequence[Defender],
    defender_width: float = 0.80,
    goalkeeper_width: float = 1.60,
    show_rays: bool = True,
    shade_blocked: bool = True,
    label_players: bool = True,
    segment_color: str = "orange",
    segment_alpha: float = 0.8,
    ray_color: str = "gray",
    blocked_color: str = "red",
    xlim: tuple[float, float] = (80, 125),
    ylim: tuple[float, float] = (30, 50),
    ax: Optional["Axes"] = None,
) -> list[tuple[float, float]]:
    """Plot top-down schematic of shooter, defenders, and blocked goal intervals.

    Parameters
    ----------
    shooter : sequence (2,)
        Shooter coordinates (x, y).
    defenders : sequence of defenders
        Can be array-like with shape (n_def, 2) or list of dicts with 'location' and optional 'position'
    defender_width : float, optional
        Body width for outfield defenders (meters).
    goalkeeper_width : float, optional
        Body width for goalkeepers (meters).
    show_rays : bool
        If True, draw rays from shooter to the two segment endpoints.
    shade_blocked : bool
        If True, shade blocked intervals on the goal line.
    label_players : bool
        If True, annotate shooter and defenders.
    segment_color : str
        Color for defender segments.
    segment_alpha : float
        Alpha for segment drawing.
    ray_color : str
        Color for rays.
    blocked_color : str
        Color for blocked region on goal line.
    xlim, ylim : tuple
        Axis limits for the plot.
    ax : matplotlib.axes.Axes
        Axes to draw on.

    Returns
    -------
    merged_intervals : list of (left_y, right_y)
        Merged blocked intervals along the goal width (y coordinates, in meters).
    """
    if ax is None:
        ax = plt.gca()

    shooter_arr = np.asarray(shooter)
    norm_defenders = _normalize_defenders(defenders)

    def _intersect_line_with_goal(p0: Point, p1: Point, goal_x: float) -> float:
        """Return y coordinate where line p0->p1 crosses x=goal_x, or np.nan if none/infinite/behind."""
        x0, y0 = p0
        x1, y1 = p1
        dx = x1 - x0
        if abs(dx) < 1e-12:
            return np.nan
        t = (goal_x - x0) / dx
        if t <= 0:
            return np.nan
        return y0 + t * (y1 - y0)

    # Draw goal line and posts
    ax.plot([GOAL_X, GOAL_X], [GOAL_Y_MIN, GOAL_Y_MAX], color="k", lw=3)

    # Draw shooter
    ax.scatter([shooter_arr[0]], [shooter_arr[1]], c="red", s=80, zorder=5)
    if label_players:
        ax.annotate(
            "Shooter",
            (shooter_arr[0], shooter_arr[1]),
            xytext=(5, -10),
            textcoords="offset points",
        )

    blocked_intervals = []

    for i, defender in enumerate(norm_defenders):
        def_x, def_y = defender["location"]
        pos_name = defender.get("position")
        width = (
            goalkeeper_width
            if (pos_name and str(pos_name).lower() == "goalkeeper")
            else defender_width
        )

        # Plot defender point
        ax.scatter([def_x], [def_y], c="blue", s=60, zorder=5)
        if label_players:
            ax.annotate(f"D{i + 1}", (def_x, def_y), xytext=(5, 5), textcoords="offset points")

        # Compute LOS unit vector and perpendicular orientation
        shooter_to_def = np.array([def_x - shooter_arr[0], def_y - shooter_arr[1]])
        distance = np.linalg.norm(shooter_to_def)
        if distance < 1e-9:
            # defender on top of shooter -> treat as full-block
            blocked_intervals = [(GOAL_Y_MIN, GOAL_Y_MAX)]
            break

        unit_vector = shooter_to_def / distance
        normal = np.array([-unit_vector[1], unit_vector[0]])  # perpendicular unit vector

        half_width = width / 2
        p1 = (def_x - normal[0] * half_width, def_y - normal[1] * half_width)
        p2 = (def_x + normal[0] * half_width, def_y + normal[1] * half_width)

        # Plot defender segment
        ax.plot(
            [p1[0], p2[0]],
            [p1[1], p2[1]],
            color=segment_color,
            lw=4,
            alpha=segment_alpha,
            zorder=4,
        )

        # Optional rays from shooter to segment endpoints
        if show_rays:
            ax.plot(
                [shooter_arr[0], p1[0]],
                [shooter_arr[1], p1[1]],
                color=ray_color,
                lw=0.8,
                alpha=0.7,
                zorder=2,
            )
            ax.plot(
                [shooter_arr[0], p2[0]],
                [shooter_arr[1], p2[1]],
                color=ray_color,
                lw=0.8,
                alpha=0.7,
                zorder=2,
            )

        # Compute intersections with goal line
        iy1 = _intersect_line_with_goal(shooter_arr, p1, GOAL_X)
        iy2 = _intersect_line_with_goal(shooter_arr, p2, GOAL_X)

        if np.isfinite(iy1) and np.isfinite(iy2):
            left_y = max(min(iy1, iy2), GOAL_Y_MIN)
            right_y = min(max(iy1, iy2), GOAL_Y_MAX)
            if right_y > left_y + 1e-12:
                blocked_intervals.append((left_y, right_y))
                if shade_blocked:
                    ax.plot(
                        [GOAL_X, GOAL_X],
                        [left_y, right_y],
                        color=blocked_color,
                        lw=6,
                        alpha=0.5,
                        zorder=3,
                    )

    # Merge the blocked intervals
    merged_intervals = _merge_intervals(blocked_intervals)

    ax.set_xlim(xlim)
    ax.set_ylim(ylim)
    ax.set_xlabel("X (length of pitch)")
    ax.set_ylabel("Y (width of pitch)")
    ax.set_aspect("equal", adjustable="box")
    ax.set_title("Top-down view of pitch")

    return merged_intervals


if __name__ == "__main__":
    import matplotlib.pyplot as plt

    plt.rcParams["figure.dpi"] = 120

    # Scenario
    shooter = [98.0, 40.0]
    defenders = [
        {"location": [118.0, 42.0], "position": "goalkeeper"},
        {"location": [118.0, 38.0], "position": "defender"},
        # {"location": [114.0, 39.0], "position": "defender"},
    ]
    goal_height = 2.44
    pos_std = 0.40
    ny, nz = 160, 80
    defender_width = 0.80
    goalkeeper_width = 1.60

    # Compute lateral occlusion arrays (1D across width)
    oc_exact, ys_exact = occlusion_binary(
        shooter, defenders, defender_width=defender_width, goalkeeper_width=goalkeeper_width
    )
    oc_num, ys = occlusion_numeric(
        shooter,
        defenders,
        defender_sigma=defender_width / 2.355,
        goalkeeper_sigma=goalkeeper_width / 2.355,
    )
    oc_ana, _ = occlusion_analytic(
        shooter,
        defenders,
        defender_sigma=defender_width / 2.355,
        goalkeeper_sigma=goalkeeper_width / 2.355,
        pos_std=pos_std,
    )

    print(
        f"Numeric mean: {mean_occlusion_percent(oc_num):.2f} %, {occlusion_gaps(oc_num, threshold=0.5, min_width=3)} gaps"
    )
    print(
        f"Analytic mean: {mean_occlusion_percent(oc_ana):.2f} %, {occlusion_gaps(oc_ana, threshold=0.5, min_width=3)} gaps"
    )
    print(
        f"Exact mean  : {mean_occlusion_percent(oc_exact):.2f} %, {occlusion_gaps(oc_exact, threshold=0.5, min_width=3)} gaps"
    )

    # Create visualization
    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(14, 6))

    # Plot top-down view
    merged = plot_top_down_view(
        shooter=shooter,
        defenders=defenders,
        ax=ax1,
    )
    print("Merged blocked intervals:", merged)

    # Plot occlusion profiles
    ax2.plot(ys, oc_exact, label="Binary", linewidth=2)
    ax2.plot(ys, oc_num, label="Numeric", linewidth=2)
    ax2.plot(ys, oc_ana, label="Analytic", linewidth=2)
    ax2.set_xlabel("Y coordinate on goal line")
    ax2.set_ylabel("Occlusion probability")
    ax2.set_title("Occlusion profiles")
    ax2.legend()
    ax2.grid(True, alpha=0.3)

    plt.tight_layout()
    plt.show()
