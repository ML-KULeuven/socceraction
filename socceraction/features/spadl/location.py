"""Feature generators for the location of each action."""

import numpy as np
import pandas as pd
import socceraction.spadl.config as spadlcfg
from socceraction.types import Features, Mask, SPADLActions

from ..utils import ftype

_goal_x: float = spadlcfg.field_length
_goal_y: float = spadlcfg.field_width / 2


@ftype("actions")
def startlocation(actions: SPADLActions, mask: Mask) -> Features:
    """Get the location where each action started.

    Parameters
    ----------
    actions : SPADLActions
        The actions of a game.
    mask : Mask
        A boolean mask to filter actions.

    Returns
    -------
    Features
        The 'start_x' and 'start_y' location of each action.
    """
    return actions.loc[mask, ["start_x", "start_y"]]


@ftype("actions")
def endlocation(actions: SPADLActions, mask: Mask) -> Features:
    """Get the location where each action ended.

    Parameters
    ----------
    actions : SPADLActions
        The actions of a game.
    mask : Mask
        A boolean mask to filter actions.

    Returns
    -------
    Features
        The 'end_x' and 'end_y' location of each action.
    """
    return actions.loc[mask, ["end_x", "end_y"]]


@ftype("actions")
def startpolar(actions: SPADLActions, mask: Mask) -> Features:
    """Get the polar coordinates of each action's start location.

    The center of the opponent's goal is used as the origin.

    Parameters
    ----------
    actions : SPADLActions
        The actions of a game.
    mask : Mask
        A boolean mask to filter actions.

    Returns
    -------
    Features
        The 'start_dist_to_goal' and 'start_angle_to_goal' of each action.
    """
    polardf = pd.DataFrame(index=actions.index)
    dx = (_goal_x - actions["start_x"]).abs().values
    dy = (_goal_y - actions["start_y"]).abs().values
    polardf["start_dist_to_goal"] = np.sqrt(dx**2 + dy**2)
    with np.errstate(divide="ignore", invalid="ignore"):
        polardf["start_angle_to_goal"] = np.nan_to_num(np.arctan(dy / dx))
    return polardf.loc[mask]


@ftype("actions")
def endpolar(actions: SPADLActions, mask: Mask) -> Features:
    """Get the polar coordinates of each action's end location.

    The center of the opponent's goal is used as the origin.

    Parameters
    ----------
    actions : SPADLActions
        The actions of a game.
    mask : Mask
        A boolean mask to filter actions.

    Returns
    -------
    Features
        The 'end_dist_to_goal' and 'end_angle_to_goal' of each action.
    """
    polardf = pd.DataFrame(index=actions.index)
    dx = (_goal_x - actions["end_x"]).abs().values
    dy = (_goal_y - actions["end_y"]).abs().values
    polardf["end_dist_to_goal"] = np.sqrt(dx**2 + dy**2)
    with np.errstate(divide="ignore", invalid="ignore"):
        polardf["end_angle_to_goal"] = np.nan_to_num(np.arctan(dy / dx))
    return polardf.loc[mask]


@ftype("actions")
def movement(actions: SPADLActions, mask: Mask) -> Features:
    """Get the distance covered by each action.

    Parameters
    ----------
    actions : SPADLActions
        The actions of a game.
    mask : Mask
        A boolean mask to filter actions.

    Returns
    -------
    Features
        The horizontal ('dx'), vertical ('dy') and total ('movement') distance
        covered by each action.
    """
    mov = pd.DataFrame(index=actions.index)
    mov["dx"] = actions.end_x - actions.start_x
    mov["dy"] = actions.end_y - actions.start_y
    mov["movement"] = np.sqrt(mov.dx**2 + mov.dy**2)
    return mov.loc[mask]


def triangular_grid(name, angle_bins, dist_bins, symmetrical=False):
    def fn(actions, mask):
        zonedf = startpolar(actions, mask)
        if symmetrical:
            zonedf.loc[
                zonedf.start_angle_to_goal_a0 > np.pi / 2,
                "start_angle_to_goal_a0",
            ] -= np.pi / 2
        dist_bin = np.digitize(zonedf.start_dist_to_goal_a0, dist_bins)
        angle_bin = np.digitize(zonedf.start_angle_to_goal_a0, angle_bins)
        zonedf[name] = dist_bin * angle_bin + dist_bin
        zonedf[name] = pd.Categorical(
            zonedf[name],
            categories=list(range(len(dist_bins) * len(angle_bins))),
            ordered=False,
        )
        return zonedf[[name]]

    return fn


def rectangular_grid(name, x_bins, y_bins, symmetrical=False):
    def fn(actions, mask):
        zonedf = actions.loc[mask, ["start_x", "start_y"]].copy()
        if symmetrical:
            m = spadlcfg.field_width / 2
            zonedf.loc[zonedf.start_y > m, "start_y"] -= m
        x_bin = np.digitize(zonedf.start_x, x_bins)
        y_bin = np.digitize(zonedf.start_y, y_bins)
        zonedf[name] = x_bin * y_bin + y_bin
        zonedf[name] = pd.Categorical(
            zonedf[name],
            categories=list(range(len(x_bins) * len(y_bins))),
            ordered=False,
        )
        return zonedf[[name]]

    return fn


def custom_grid(name, zones, is_in_zone):
    def fn(actions, mask):
        zonedf = actions.loc[mask, ["start_x", "start_y"]].copy()
        zonedf[name] = [0] * len(actions)  # zone 0 if no match
        for i, zone in enumerate(zones):
            for subzone in zone:
                zonedf.loc[
                    np.apply_along_axis(
                        is_in_zone(subzone),
                        1,
                        zonedf[["start_x", "start_y"]].values,
                    ),
                    name,
                ] = i + 1
        zonedf[name] = pd.Categorical(
            zonedf[name], categories=list(range(len(zones) + 1)), ordered=False
        )
        return zonedf[[name]]

    return fn
