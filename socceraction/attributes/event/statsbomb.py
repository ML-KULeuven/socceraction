"""StatsBomb-specific attributes."""
import math

import numpy as np
import pandas as pd

from socceraction.spadl import config as spadlcfg
from ..utils import ftype

_spadl_cfg = {
    "length": 105,
    "width": 68,
    "penalty_box_length": 16.5,
    "penalty_box_width": 40.3,
    "six_yard_box_length": 5.5,
    "six_yard_box_width": 18.3,
    "goal_width": 7.32,
    "penalty_spot_distance": 11,
    "goal_length": 2,
    "origin_x": 0,
    "origin_y": 0,
    "circle_radius": 9.15,
}


def _sb_to_spadl(sb_x, sb_y):
    spadl_x = ((sb_x - 1) / 119) * spadlcfg.field_length
    spadl_y = spadlcfg.field_width - ((sb_y - 1) / 79) * spadlcfg.field_width
    return spadl_x, spadl_y


def _get_intersect(a1, a2, b1, b2):
    """Get the point of intersection of the lines passing through a2,a1 and b2,b1.

    Parameters
    ----------
    a1: [x, y] a point on the first line
    a2: [x, y] another point on the first line
    b1: [x, y] a point on the second line
    b2: [x, y] another point on the second line
    """
    s = np.vstack([a1, a2, b1, b2])  # s for stacked
    h = np.hstack((s, np.ones((4, 1))))  # h for homogeneous
    l1 = np.cross(h[0], h[1])  # get first line
    l2 = np.cross(h[2], h[3])  # get second line
    x, y, z = np.cross(l1, l2)  # point of intersection
    if z == 0:  # lines are parallel
        return (float("inf"), float("inf"))
    return (x / z, y / z)


def _overlap(min1, max1, min2, max2):
    start = max(min1, min2)
    end = min(max1, max2)
    d = end - start
    if d < 0:
        return False, None, None
    else:
        return d, start, end


def _is_inside_triangle(point, tri_points):
    Dx, Dy = point

    A, B, C = tri_points
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


@ftype("events")
def statsbomb_open_goal(events, mask):
    """Get whether the shot was taken into an open goal.

    This is derived from the 'open_goal' annotation in StatsBomb's event
    stream data.

    Parameters
    ----------
    events : pd.DataFrame
        The StatsBomb events of a game.
    mask : pd.Series
        A boolean mask to select the shots for which attributes should be
        computed.

    Returns
    -------
    pd.DataFrame
    """
    output = {}
    for idx, shot in events.loc[mask].iterrows():
        if "shot" in shot.extra:
            output[idx] = {"open_goal": "open_goal" in shot.extra['shot']}

    output = pd.DataFrame.from_dict(output, orient="index")
    return output


@ftype("events")
def statsbomb_first_touch(events, mask):
    """Get whether the shot was a first-touch shot.

    This is derived from the 'first_time' annotation in StatsBomb's event
    stream data.

    Parameters
    ----------
    events : pd.DataFrame
        The StatsBomb events of a game.
    mask : pd.Series
        A boolean mask to select the shots for which attributes should be
        computed.

    Returns
    -------
    pd.DataFrame
    """
    output = {}
    for idx, shot in events.loc[mask].iterrows():
        if "shot" in shot.extra:
            output[idx] = {"first_touch": "first_time" in shot.extra['shot']}

    output = pd.DataFrame.from_dict(output, orient="index")
    return output


@ftype("events")
def statsbomb_free_projection(events, mask):
    """Get the free projection area.

    This feature represents the proportion of the goal that is left uncovered by
    the goalkeeper and defenders. To capture the fact that players are not
    static and will react to the shot, we assume that the defending player has
    an effective span of one arm length (80 cm) and the goalkeeper has an
    effect of two arm lengths (160 cm).

    Parameters
    ----------
    events : pd.DataFrame
        The StatsBomb events of a game.
    mask : pd.Series
        A boolean mask to select the shots for which attributes should be
        computed.

    Returns
    -------
    pd.DataFrame
        A dataframe with a column containing the free projection area
        ('free_projection_pct') and the number of gaps in the projection area
        ('free_projection_gaps').

    References
    ----------
    .. [1] Cem Arslan, Pieter Robberechts and Jesse Davis. "Enhancing xG
       Models with Freeze Frame Data". DTAI Sports Analytics Lab, September 2,
       2020. https://dtai.cs.kuleuven.be/sports/blog/enhancing-xg-models-with-freeze-frame-data
    """
    output = {}
    # We have to use StatsBomb coordinates here
    corner1 = [120, 0]
    corner2 = [120, 80]
    goal = [36, 44]
    for idx, shot in events.loc[mask].iterrows():
        if "shot" not in shot.extra or "freeze_frame" not in shot.extra["shot"]:
            # No freeze frame data available for this shot
            continue
        freeze_frame = shot.extra["shot"]["freeze_frame"]
        start_x, start_y = shot.location
        # By default, the entire goal is free
        free_projection = [goal]
        # Now we remove the area blocked by each defending player
        defenders = [t for t in freeze_frame if not t["teammate"]]
        for defender in defenders:
            def_x, def_y = defender["location"]
            # goalkeepers span 1.60m; defenders span 0.80m
            def_position = defender["position"]["name"]
            def_width = (
                160 / 91.44 if def_position == "Goalkeeper" else 80 / 91.44
            )  # convert to yards
            if def_x > start_x:
                _, left_bound_y = _get_intersect(
                    [start_x, start_y], [def_x, def_y - def_width / 2], corner1, corner2
                )
                _, right_bound_y = _get_intersect(
                    [start_x, start_y], [def_x, def_y + def_width / 2], corner1, corner2
                )
                new_free_projection = []
                for projection in free_projection:
                    d, overlap_start, overlap_end = _overlap(
                        left_bound_y, right_bound_y, *projection
                    )
                    if d:
                        new_free_projection.append([projection[0], overlap_start])
                        new_free_projection.append([overlap_end, projection[1]])
                    else:
                        new_free_projection.append(projection)
                free_projection = [p for p in new_free_projection if p[1] - p[0] > 0]
        output[idx] = {
            "free_projection_gaps": len(free_projection),
            "free_projection_pct": np.sum(np.diff(free_projection)) / np.diff(goal)[0],
        }
    output = pd.DataFrame.from_dict(output, orient="index")
    return output


@ftype("events")
def statsbomb_goalkeeper_position(events, mask):
    """Get the goalkeeper's position.

    Parameters
    ----------
    events : pd.DataFrame
        The StatsBomb events of a game.
    mask : pd.Series
        A boolean mask to select the shots for which attributes should be
        computed.

    Returns
    -------
    pd.DataFrame
        A dataframe with a column containing the goalkeeper's x-coordinate
        ('goalkeeper_x'), y-coordinate ('goalkeeper_y'), distance to the ball
        ('goalkeeper_dist_to_ball'), distance to the center of the goal
        ('goalkeeper_dist_to_goal') and the angle to the center of the goal
        ('goalkeeper_angle_to_goal').
    """
    output = {}
    for idx, shot in events.loc[mask].iterrows():
        if "shot" not in shot.extra or "freeze_frame" not in shot.extra["shot"]:
            # No freeze frame data available for this shot
            continue
        freeze_frame = shot.extra["shot"]["freeze_frame"]
        goalkeeper = next(
            (
                t
                for t in freeze_frame
                # Cartesian coordinates
                if not t["teammate"] and t["position"]["name"] == "Goalkeeper"
            ),
            None,
        )
        if goalkeeper is None:
            # The goalkeeper is not included in the freeze_frame
            continue

        # Cartesian coordinates
        goalkeeper_x, goalkeeper_y = _sb_to_spadl(
            goalkeeper["location"][0], goalkeeper["location"][1]
        )

        # Polar coordinates
        dx_gk = spadlcfg.field_length - goalkeeper_x
        dy_gk = spadlcfg.field_width / 2 - goalkeeper_y
        goalkeeper_dist_to_goal = math.sqrt(dx_gk**2 + dy_gk**2)
        goalkeeper_angle_to_goal = math.atan2(dy_gk, dx_gk)  # if dx_gk > 0 else 0

        ball_x, ball_y = _sb_to_spadl(shot["location"][0], shot["location"][1])
        dx_kb = goalkeeper_x - ball_x
        dy_kb = goalkeeper_y - ball_y
        goalkeeper_dist_to_ball = math.sqrt(dx_kb**2 + dy_kb**2)

        output[idx] = {
            "goalkeeper_x": goalkeeper_x,
            "goalkeeper_y": goalkeeper_y,
            "goalkeeper_dist_to_ball": goalkeeper_dist_to_ball,
            "goalkeeper_dist_to_goal": goalkeeper_dist_to_goal,
            "goalkeeper_angle_to_goal": goalkeeper_angle_to_goal,
        }
    output = pd.DataFrame.from_dict(output, orient="index")
    return output


@ftype("events")
def statsbomb_defenders_position(events, mask):
    """Get features describing the position of the defending players.

    The following features are computed:
        - dist_to_defender: The distance to the closest defender.
        - under_pressure: Whether the shot was taken under pressure (StatsBomb
          definition).
        - nb_defenders_in_shot_line: The number of defenders in the visible
          angle to the goal.
        - nb_defenders_behind_ball: The number of defenders behind the ball.
        - one_on_one: Whether the shot was a 1 versus 1 situation. We define
          a 1v1 as a shot for which only the goalkeeper is inside the triangle
          formed by joining the shot location, the right post, and the left
          post. Also, the striker must be higher up the pitch than any other
          defending player apart from the opposition goalkeeper and the shot
          is made with the player's foot. [1]_

    Parameters
    ----------
    events : pd.DataFrame
        The StatsBomb events of a game.
    mask : pd.Series
        A boolean mask to select the shots for which attributes should be
        computed.

    Returns
    -------
    pd.DataFrame

    References
    ----------
    .. [1] Wear, Matthew, et al. "Learning from the Pros: Extracting
       Professional Goalkeeper Technique from Broadcast Footage." arXiv preprint
       arXiv:2202.12259 (2022).
    """
    output = {}
    left_post = (spadlcfg.field_length, spadlcfg.field_width / 2 - _spadl_cfg["goal_width"] / 2)
    right_post = (spadlcfg.field_length, spadlcfg.field_width / 2 + _spadl_cfg["goal_width"] / 2)
    for idx, shot in events.loc[mask].iterrows():
        if "shot" not in shot.extra or "freeze_frame" not in shot.extra["shot"]:
            # No freeze frame data available for this shot
            continue
        freeze_frame = shot.extra["shot"]["freeze_frame"]
        defenders = [t for t in freeze_frame if not t["teammate"]]
        distances = []
        in_shot_line = []
        behind_ball = []
        for defender in defenders:
            if defender["teammate"] or defender["position"]["name"] == "Goalkeeper":
                continue
            defender_x, defender_y = _sb_to_spadl(defender["location"][0], defender["location"][1])
            ball_x, ball_y = _sb_to_spadl(shot["location"][0], shot["location"][1])
            distances.append(math.sqrt((ball_x - defender_x) ** 2 + (ball_y - defender_y) ** 2))
            in_shot_line.append(
                _is_inside_triangle(
                    (defender_x, defender_y), [left_post, (ball_x, ball_y), right_post]
                )
            )
            behind_ball.append(defender_x > ball_x)
        output[idx] = {
            "dist_to_defender": min(distances, default=float("inf")),
            "under_pressure": shot.under_pressure,
            "nb_defenders_in_shot_line": sum(in_shot_line),
            "nb_defenders_behind_ball": sum(behind_ball),
            "one_on_one": (
                sum(behind_ball) == 0
                and sum(in_shot_line) == 0
                and shot.extra["shot"]["body_part"]["name"] in ["Left Foot", "Right Foot"]
            ),
        }
    output = pd.DataFrame.from_dict(output, orient="index")
    output["one_on_one"] = output["one_on_one"].astype('boolean')
    output["under_pressure"] = output["under_pressure"].astype('boolean')
    return output


@ftype("events")
def statsbomb_assist(events, mask):  # noqa: C901
    """Get features describing the assist.

    The following features are computed:
        - end_x_assist: The assisting pass' x-coordinate
        - end_y_assist: The assisting pass' y-coordinate
        - carry_dist: The distance between the end location of the assisting
          pass and the location of the shot.
        - type_assist: The assist type, which is one of 'standard_pass',
          'free_kick', 'corner', 'throw_in', 'cross', 'cut_back' or 'through_ball'.
        - height_assist: The peak height of the assisting pass, which is one of
          'ground', 'low' (under shoulder level) or 'high' (above shoulder
          level).

    Parameters
    ----------
    events : pd.DataFrame
        The StatsBomb events of a game.
    mask : pd.Series
        A boolean mask to select the shots for which attributes should be
        computed.

    Returns
    -------
    pd.DataFrame

    References
    ----------
    .. [1] Wear, Matthew, et al. "Learning from the Pros: Extracting
       Professional Goalkeeper Technique from Broadcast Footage." arXiv preprint
       arXiv:2202.12259 (2022).
    """
    output = {}
    for event_id, shot in events.loc[mask].iterrows():
        if "shot" not in shot.extra or "key_pass_id" not in shot.extra["shot"]:
            # No assist for this shot
            continue
        assist = events.loc[shot.extra["shot"]["key_pass_id"]]
        assist_x, assist_y = _sb_to_spadl(
            assist.extra["pass"]["end_location"][0], assist.extra["pass"]["end_location"][1]
        )
        shot_x, shot_y = _sb_to_spadl(shot["location"][0], shot["location"][1])

        assist_type = "standard_pass"
        assist_height = "ground"
        if "pass" in assist.extra:
            # assist type
            if "cross" in assist.extra["pass"]:
                assist_type = "cross"
            elif "cut_back" in assist.extra["pass"]:
                assist_type = "cut_back"
            elif "technique" in assist.extra["pass"]:
                if assist.extra["pass"]["technique"]["name"] == "Through Ball":
                    assist_type = "through_ball"
            # special pass type
            if "type" in assist.extra["pass"]:
                if assist.extra["pass"]["type"]["name"] == "Free Kick":
                    assist_type = "free_kick"
                elif assist.extra["pass"]["type"]["name"] == "Corner":
                    assist_type = "corner"
                elif assist.extra["pass"]["type"]["name"] == "Throw-in":
                    assist_type = "throw_in"

            # assist height
            if "height" in assist.extra["pass"]:
                m = {
                    "Ground Pass": "ground",
                    "Low Pass": "low",
                    "High Pass": "high",
                }
                assist_height = m[assist.extra["pass"]["height"]["name"]]

        output[event_id] = {
            "end_x_assist": assist_x,
            "end_y_assist": assist_y,
            "carry_dist": math.sqrt((shot_x - assist_x) ** 2 + (shot_y - assist_y) ** 2),
            "type_assist": assist_type,
            "height_assist": assist_height,
        }

    output = pd.DataFrame.from_dict(output, orient="index")
    output["type_assist"] = pd.Categorical(
        output["type_assist"],
        categories=[
            "standard_pass",
            "free_kick",
            "corner",
            "throw_in",
            "cross",
            "cut_back",
            "through_ball",
        ],
        ordered=False,
    )
    output["height_assist"] = pd.Categorical(
        output["height_assist"], categories=["ground", "low", "high"], ordered=True
    )
    return output


@ftype("events")
def statsbomb_counterattack(events, mask):
    """Get whether a shot was from a counterattack.

    This is derived from the 'play_pattern' annotation in StatsBomb's event
    stream data.

    Parameters
    ----------
    events : pd.DataFrame
        The StatsBomb events of a game.
    mask : pd.Series
        A boolean mask to select the shots for which attributes should be
        computed.

    Returns
    -------
    pd.DataFrame
    """
    output = {}
    for idx, shot in events.loc[mask].iterrows():
        output[idx] = {
            "from_counterattack": shot.play_pattern_name == "From Counter",
        }

    output = pd.DataFrame.from_dict(output, orient="index")
    return output


@ftype("events")
def statsbomb_shot_impact_height(events, mask):
    """Get the height of the ball when the shot was taken.

    This is derived from the bodypart and technique that was used to take the
    shot. Possible values are 'ground', 'low' (below shoulder level) and
    'high' (above shoulder level).

    Parameters
    ----------
    events : pd.DataFrame
        The StatsBomb events of a game.
    mask : pd.Series
        A boolean mask to select the shots for which attributes should be
        computed.

    Returns
    -------
    pd.DataFrame
    """
    # The height of the ball when the ball is touched is not included,
    # but we can use body part and technique as a proxy for this
    output = {}
    for idx, shot in events.loc[mask].iterrows():
        if "shot" not in shot.extra or "technique" not in shot.extra["shot"]:
            # No freeze frame data available for this shot
            continue
        height = "ground"
        if shot.extra["shot"]["body_part"]["name"] == "Head":
            if shot.extra["shot"]["technique"]["name"] == "Diving Header":
                height = "low"
            else:
                height = "high"
        elif shot.extra["shot"]["body_part"]["name"] == "Other":
            height = "low"
        elif shot.extra["shot"]["technique"]["name"] == "Half Volley":
            height = "low"
        elif shot.extra["shot"]["technique"]["name"] == "Volley":
            height = "low"
        elif shot.extra["shot"]["technique"]["name"] == "Overhead Kick":
            height = "high"
        output[idx] = {"impact_height": height}

    output = pd.DataFrame.from_dict(output, orient="index")
    output["impact_height"] = pd.Categorical(
        output["impact_height"], categories=["ground", "low", "high"], ordered=True
    )
    return output
