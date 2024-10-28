"""Attributes on SPADL shots."""

import math
from typing import Callable

import numpy as np
import pandas as pd
import socceraction.spadl.config as spadlcfg
from socceraction.types import Features, Mask, SPADLActions

from ..utils import feature_generator
from .location import custom_grid

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


@feature_generator("actions", features=["dist_shot"])
def shot_dist(actions: SPADLActions, mask: Mask) -> Features:
    """Compute the distance to the middle of the goal.

    Parameters
    ----------
    actions : SPADLActions
        The actions of a game in SPADL format.
    mask : Mask
        A boolean mask to select the shots for which attributes should be
        computed.

    Returns
    -------
    Features
        A dataframe with a column for the distance to the middle of the goal
        ('dist_shot').
    """
    shots = actions.loc[mask]
    distdf = pd.DataFrame(index=shots.index)
    dx = (spadlcfg.field_length - shots["start_x"]).values
    dy = (spadlcfg.field_width / 2 - shots["start_y"]).values
    distdf["dist_shot"] = np.sqrt(dx**2 + dy**2)
    return distdf


@feature_generator("actions", features=["dx_shot", "dy_shot"])
def shot_location(actions: SPADLActions, mask: Mask) -> Features:
    """Compute the distance to the mid line and goal line.

    This corresponds to the absolute x- and y-coordinates of the shot with the
    origin at the center of the goal.

    Parameters
    ----------
    actions : SPADLActions
        The actions of a game in SPADL format.
    mask : Mask
        A boolean mask to select the shots for which attributes should be
        computed.

    Returns
    -------
    Features
        A dataframe with a column for the distance to the mid line ('dy_shot')
        and a column for the distance to the goal line ('dx_shot').
    """
    shots = actions.loc[mask]
    locationdf = pd.DataFrame(index=shots.index)
    locationdf["dx_shot"] = spadlcfg.field_length - shots["start_x"]
    locationdf["dy_shot"] = (spadlcfg.field_width / 2 - shots["start_y"]).abs()
    return locationdf


@feature_generator("actions", features=["angle_shot"])
def shot_angle(actions: SPADLActions, mask: Mask) -> Features:
    """Compute the shot to the middle of the goal.

    This corresponds to the angle in a polar coordinate system with the origin
    at the center of the goal.

    Parameters
    ----------
    actions : SPADLActions
        The actions of a game in SPADL format.
    mask : Mask
        A boolean mask to select the shots for which attributes should be
        computed.

    Returns
    -------
    Features
        A dataframe with a column for the angle to the middle of the goal
        ('angle_shot').
    """
    shots = actions.loc[mask]
    polardf = pd.DataFrame(index=shots.index)
    dx = (spadlcfg.field_length - shots["start_x"]).abs().values
    dy = (spadlcfg.field_width / 2 - shots["start_y"]).abs().values
    with np.errstate(divide="ignore", invalid="ignore"):
        polardf["angle_shot"] = np.nan_to_num(np.arctan(dy / dx))
    return polardf


@feature_generator("actions", features=["visible_angle_shot"])
def shot_visible_angle(actions: SPADLActions, mask: Mask) -> Features:
    """Compute the angle formed between the shot location and the two goal posts.

    Parameters
    ----------
    actions : SPADLActions
        The actions of a game in SPADL format.
    mask : Mask
        A boolean mask to select the shots for which attributes should be
        computed.

    Returns
    -------
    Features
        A dataframe with a column for the angle formed between the shot location
        and the two goal posts ('visible_angle_shot').

    References
    ----------
    .. [1] Sumpter, David. "The Geometry of Shooting", 8 January 2017,
       https://medium.com/@Soccermatics/the-geometry-of-shooting-ae7a67fdf760,
    """
    shots = actions.loc[mask]
    dx = spadlcfg.field_length - shots["start_x"]
    dy = spadlcfg.field_width / 2 - shots["start_y"]
    angledf = pd.DataFrame(index=shots.index)
    angledf["visible_angle_shot"] = np.arctan(
        _spadl_cfg["goal_width"] * dx / (dx**2 + dy**2 - (_spadl_cfg["goal_width"] / 2) ** 2)
    )
    angledf.loc[angledf["visible_angle_shot"] < 0, "visible_angle_shot"] += np.pi
    angledf.loc[(shots["start_x"] >= spadlcfg.field_length), "visible_angle_shot"] = 0
    # Ball is on the goal line
    angledf.loc[
        (shots["start_x"] == spadlcfg.field_length)
        & (
            shots["start_y"].between(
                spadlcfg.field_width / 2 - _spadl_cfg["goal_width"] / 2,
                spadlcfg.field_width / 2 + _spadl_cfg["goal_width"] / 2,
            )
        ),
        "visible_angle_shot",
    ] = np.pi
    return angledf


@feature_generator("actions", features=["relative_angle_shot"])
def shot_relative_angle(actions: SPADLActions, mask: Mask) -> Features:
    """Compute the relative angle to goal.

    If a player is in a central position, the angle is 1. If a player is wide
    of the posts, this feature takes the angle from the shot's location to the
    nearest post. For example, if a player is at a 45-degree angle to the
    nearest post, the angle is 0.5.

    Parameters
    ----------
    actions : SPADLActions
        The actions of a game in SPADL format.
    mask : Mask
        A boolean mask to select the shots for which attributes should be
        computed.

    Returns
    -------
    Features
        A dataframe with a column for the relative angle to the goal
        ('relative_angle_shot').

    References
    ----------
    .. [1] Caley, Micheal. "Premier League Projections and New Expected Goals"
       Cartilage Freecaptain SBNation, 2015,
       https://cartilagefreecaptain.sbnation.com/2015/10/19/9295905/premier-leagueprojections-and-new-expected-goals
    """
    angledf = actions.loc[mask, ["start_x", "start_y"]].copy()
    angledf["dx"] = (spadlcfg.field_length - angledf["start_x"]).abs().values
    left_post = spadlcfg.field_width / 2 + _spadl_cfg["goal_width"] / 2
    right_post = spadlcfg.field_width / 2 - _spadl_cfg["goal_width"] / 2
    angledf.loc[angledf.start_y > left_post, "dy"] = (left_post - angledf["start_y"]).abs().values
    angledf.loc[angledf.start_y < right_post, "dy"] = (
        (right_post - angledf["start_y"]).abs().values
    )
    is_center = (angledf.start_y <= left_post) & (angledf.start_y >= right_post)
    with np.errstate(divide="ignore", invalid="ignore"):
        angledf.loc[~is_center, "relative_angle_shot"] = 1 - (
            np.nan_to_num(np.arctan(angledf.dy / angledf.dx)) / (math.pi / 2)
        )
    angledf.loc[is_center, "relative_angle_shot"] = 1.0
    return angledf[["relative_angle_shot"]]


@feature_generator("actions", features=["post_dribble", "carry_length"])
def post_dribble(actions: SPADLActions, mask: Mask) -> Features:
    """Compute features describing the dribble before the shot.

    Computes the following features:
        - post_dribble: whether the shot follows a previous attempt to beat
          a player
        - carry_length: The distance between the end location of the assisting
          pass and the location of the shot

    Parameters
    ----------
    actions : SPADLActions
        The actions of a game in SPADL format.
    mask : Mask
        A boolean mask to select the shots for which attributes should be
        computed.

    Returns
    -------
    Features
        A dataframe with a boolean column containing whether the shot was
        preceded by a take-on attempt of the shot-taker ('post_dribble') and
        the distance between the end location of the assisting pass and the
        location of the shot ('carry_length').
    """
    df = {}
    for idx in actions.loc[mask].index:
        carry_length = 0
        maybe_carry = actions.loc[:idx].iloc[-1]
        if maybe_carry.type_name == "dribble":
            dx = maybe_carry.end_x - maybe_carry.start_x
            dy = maybe_carry.end_y - maybe_carry.start_y
            carry_length = math.sqrt(dx**2 + dy**2)
        df[idx] = {"carry_length": carry_length}
    return pd.DataFrame.from_dict(df, orient="index")


@feature_generator("actions", features=["type_assist"])
def assist_type(actions: SPADLActions, mask: Mask) -> Features:
    """Return the assist type.

    One of a long ball, cross, through ball, danger-zone pass,
    and pull-back
    The assist type, which is one of pass, recovery, clearance, direct,
    or rebound

    Parameters
    ----------
    actions : SPADLActions
        The actions of a game in SPADL format.
    mask : Mask
        A boolean mask to select the shots for which attributes should be
        computed.

    Returns
    -------
    Features
        A dataframe with a column containing the assist type of each shot
        ('type_assist').
    """
    df = {}
    for idx, shot in actions.loc[mask].iterrows():
        assist = None
        for _, maybe_assist in actions.loc[:idx].iloc[::-1].iterrows():
            if (
                maybe_assist.type_name.isin(
                    [
                        "pass",
                        "cross",
                        "throw_in",
                        "freekick_crossed",
                        "freekick_short",
                        "corner_crossed",
                        "corner_short",
                        "goalkick",
                    ]
                )
                and maybe_assist.team_id == shot.team_id
                and maybe_assist.period_id == shot.period_id
                and maybe_assist.result_name == "success"
            ):
                assist = maybe_assist
                break
            elif maybe_assist.player_id == shot.player_id and maybe_assist.type_name != "dribble":
                break
            elif maybe_assist.team_id != shot.team_id and maybe_assist.result_name == "success":
                break
        # (assist_type): The assist type
        assist_type = assist.type_name if assist else "direct"
        # TODO (assist_technique): The technique for crosses one of straight,
        # inswinging, or out swinging and whether the pass was a through ball
        df[idx] = {"type_assist": assist_type}
    return pd.DataFrame.from_dict(df, orient="index")


@feature_generator("actions", features=["fastbreak"])
def fastbreak(actions: SPADLActions, mask: Mask) -> Features:
    """Get whether the shot was part of a counter attack.

    A fastbreak is defined as a pattern of play in which the team wins the
    ball in its own third and shoots in the last quarter of the pitch within 25
    seconds.

    Parameters
    ----------
    actions : SPADLActions
        The actions of a game in SPADL format.
    mask : Mask
        A boolean mask to select the shots for which attributes should be
        computed.

    Returns
    -------
    Features
        A dataframe with a column containing whether shot was part of a fastbreak
        ('fastbreak').
    """
    df = {}
    for idx, shot in actions.loc[mask].iterrows():
        prev_actions = actions[
            (actions.time_seconds < shot.time_seconds)
            & (actions.time_seconds > shot.time_seconds - 25)
            & (actions.period_id == shot.period_id)
        ]
        has_recovery_own_third = not prev_actions[
            (prev_actions.team_id == shot.team_id)
            & (prev_actions.start_x < 105 / 3)
            & (
                prev_actions.type_name.isin(
                    [
                        "tackle",
                        "interception",
                        "keeper_save",
                        "keeper_claim",
                        "keeper_punch",
                        "keeper_pick_up",
                        "clearance",
                    ]
                )
            )
        ].empty
        shot_in_last_quarter = shot.start_x > spadlcfg.field_length / 4
        df[idx] = {"fastbreak": has_recovery_own_third and shot_in_last_quarter}
    return pd.DataFrame.from_dict(df, orient="index")


@feature_generator("actions", features=["rebound", "time_prev_shot"])
def rebound(actions: SPADLActions, mask: Mask) -> Features:
    """Get whether the shot was a rebound.

    A shot is a rebound if one of the two preceding actions was also a shot.

    Parameters
    ----------
    actions : SPADLActions
        The actions of a game in SPADL format.
    mask : Mask
        A boolean mask to select the shots for which attributes should be
        computed.

    Returns
    -------
    Features
        A dataframe with a column containing whether shot was a rebound ('rebound')
        and the time since the previous shot ('time_prev_shot').
    """
    actions = actions.copy()

    # Identify shot-related actions
    shot_mask = actions["type_name"].isin(["shot", "shot_freekick", "shot_penalty"])

    # Create new columns for shot indices, times, and action IDs
    actions["shot_idx"] = actions["action_id"].where(shot_mask)
    actions["shot_time_seconds"] = actions["time_seconds"].where(shot_mask)

    # Forward-fill and shift shot-related columns to get the last shot per group
    actions[["last_shot_idx", "last_shot_time_seconds"]] = (
        actions.groupby(["game_id", "period_id", "team_id"])[["shot_idx", "shot_time_seconds"]]
        .ffill()
        .shift(1)
    )

    # Calculate the number of actions and time since the previous shot
    actions["rebound"] = (actions["action_id"] - actions["last_shot_idx"]) <= 2
    actions["time_prev_shot"] = actions["time_seconds"] - actions["last_shot_time_seconds"]

    return actions.loc[mask, ["rebound", "time_prev_shot"]]


def _caley_shot_matrix(
    cfg: dict[str, float] = _spadl_cfg,
) -> list[list[tuple[float, float, float, float]]]:
    """Create the zones of Caley's shot location chart [1].

    .. [1] Caley, Micheal. "Shot Matrix I: Shot Location and Expected Goals",
            Cartilage Freecaptain SBNation, 13 November 2013,
            https://cartilagefreecaptain.sbnation.com/2013/11/13/5098186/shot-matrix-i-shot-location-and-expected-goals
    """
    m = (cfg["origin_y"] + cfg["width"]) / 2

    zones = []
    # Zone 1 is the central area of the six-yard box
    x1 = cfg["origin_x"] + cfg["length"] - cfg["six_yard_box_length"]
    x2 = cfg["origin_x"] + cfg["length"]
    y1 = m - cfg["goal_width"] / 2
    y2 = m + cfg["goal_width"] / 2
    zones.append([(x1, y1, x2, y2)])
    # Zone 2 includes the wide areas, left and right, of the six-yard box.
    # left
    x1 = cfg["origin_x"] + cfg["length"] - cfg["six_yard_box_length"]
    x2 = cfg["origin_x"] + cfg["length"]
    y1 = m - cfg["six_yard_box_width"] / 2
    y2 = m - cfg["goal_width"] / 2
    zone_left = (x1, y1, x2, y2)
    # right
    x1 = cfg["origin_x"] + cfg["length"] - cfg["six_yard_box_length"]
    x2 = cfg["origin_x"] + cfg["length"]
    y1 = m + cfg["goal_width"] / 2
    y2 = m + cfg["six_yard_box_width"] / 2
    zone_right = (x1, y1, x2, y2)
    zones.append([zone_left, zone_right])
    # Zone 3 is the central area between the edges of the six- and
    # eighteen-yard boxes.
    x1 = cfg["origin_x"] + cfg["length"] - cfg["penalty_box_length"]
    x2 = cfg["origin_x"] + cfg["length"] - cfg["six_yard_box_length"]
    y1 = m - cfg["six_yard_box_width"] / 2
    y2 = m + cfg["six_yard_box_width"] / 2
    zones.append([(x1, y1, x2, y2)])
    # Zone 4 comprises the wide areas in the eighteen-yard box, further from
    # the endline than the six-yard box extended.
    # left
    x1 = cfg["origin_x"] + cfg["length"] - cfg["penalty_box_length"]
    x2 = cfg["origin_x"] + cfg["length"] - cfg["six_yard_box_length"] - 2
    y1 = m - cfg["penalty_box_width"] / 2
    y2 = m - cfg["six_yard_box_width"] / 2
    zone_left = (x1, y1, x2, y2)
    # right
    x1 = cfg["origin_x"] + cfg["length"] - cfg["penalty_box_length"]
    x2 = cfg["origin_x"] + cfg["length"] - cfg["six_yard_box_length"] - 2
    y1 = m + cfg["six_yard_box_width"] / 2
    y2 = m + cfg["penalty_box_width"] / 2
    zone_right = (x1, y1, x2, y2)
    zones.append([zone_left, zone_right])
    # Zone 5 includes the wide areas left and right in the eighteen yard box
    # within the six-yard box extended.
    # left
    x1 = cfg["origin_x"] + cfg["length"] - cfg["six_yard_box_length"] - 2
    x2 = cfg["origin_x"] + cfg["length"]
    y1 = m - cfg["penalty_box_width"] / 2
    y2 = m - cfg["six_yard_box_width"] / 2
    zone_left = (x1, y1, x2, y2)
    # right
    x1 = cfg["origin_x"] + cfg["length"] - cfg["six_yard_box_length"] - 2
    x2 = cfg["origin_x"] + cfg["length"]
    y1 = m + cfg["six_yard_box_width"] / 2
    y2 = m + cfg["penalty_box_width"] / 2
    zone_right = (x1, y1, x2, y2)
    zones.append([zone_left, zone_right])
    # Zone 6 is the eighteen-yard box extended out to roughly 35 yards (=32m).
    x1 = cfg["origin_x"] + cfg["length"] - 32
    x2 = cfg["origin_x"] + cfg["length"] - cfg["penalty_box_length"]
    y1 = m - cfg["penalty_box_width"] / 2
    y2 = m + cfg["penalty_box_width"] / 2
    zones.append([(x1, y1, x2, y2)])
    # Zone 7 is the deep, deep area beyond that
    x1 = cfg["origin_x"]
    x2 = cfg["origin_x"] + cfg["length"] - 32
    y1 = cfg["origin_y"]
    y2 = cfg["origin_y"] + cfg["width"]
    zones.append([(x1, y1, x2, y2)])
    # Zone 8 comprises the regions right and left of the box.
    # left
    x1 = cfg["origin_x"] + cfg["length"] - 32
    x2 = cfg["origin_x"] + cfg["length"]
    y1 = cfg["origin_y"] + cfg["width"]
    y2 = m + cfg["penalty_box_width"] / 2
    zone_left = (x1, y1, x2, y2)
    # right
    x1 = cfg["origin_x"] + cfg["length"] - 32
    x2 = cfg["origin_x"] + cfg["length"]
    y1 = cfg["origin_y"]
    y2 = m - cfg["penalty_box_width"] / 2
    zone_right = (x1, y1, x2, y2)
    zones.append([zone_left, zone_right])
    return zones


def _point_in_rect(
    rect: tuple[float, float, float, float],
) -> Callable[[tuple[float, float]], bool]:
    x1, y1, x2, y2 = rect

    def fn(point: tuple[float, float]) -> bool:
        x, y = point
        if x1 <= x and x <= x2:
            if y1 <= y and y <= y2:
                return True
        return False

    return fn


caley_grid = feature_generator("actions", features=["caley_zone"])(
    custom_grid("caley_zone", _caley_shot_matrix(), _point_in_rect)
)
