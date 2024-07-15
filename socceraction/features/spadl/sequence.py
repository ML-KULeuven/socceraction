"""Feature generators for action sequences."""

import numpy as np
import pandas as pd
from socceraction.types import Features, GameStates, Mask

from ..utils import ftype


@ftype("gamestates")
def time_delta(gamestates: GameStates, mask: Mask) -> Features:
    """Get the number of seconds between the last and previous actions.

    Parameters
    ----------
    gamestates : GameStates
        The game states of a game.

    Returns
    -------
    Features
        A dataframe with a column 'time_delta_i' for each <nb_prev_actions>
        containing the number of seconds between action ai and action a0.
    """
    a0 = gamestates[0].loc[mask]
    dt = pd.DataFrame(index=a0.index)
    for i, a in enumerate(gamestates[1:]):
        dt["time_delta_" + (str(i + 1))] = a0.time_seconds - a.loc[mask].time_seconds
    return dt


@ftype("gamestates")
def space_delta(gamestates: GameStates, mask: Mask) -> Features:
    """Get the distance covered between the last and previous actions.

    Parameters
    ----------
    gamestates : GameStates
        The gamestates of a game.

    Returns
    -------
    Features
        A dataframe with a column for the horizontal ('dx_a0i'), vertical
        ('dy_a0i') and total ('mov_a0i') distance covered between each
        <nb_prev_actions> action ai and action a0.
    """
    a0 = gamestates[0].loc[mask]
    spaced = pd.DataFrame(index=a0.index)
    for i, a in enumerate(gamestates[1:]):
        dx = a.loc[mask].end_x - a0.start_x
        spaced["dx_a0" + (str(i + 1))] = dx
        dy = a.loc[mask].end_y - a0.start_y
        spaced["dy_a0" + (str(i + 1))] = dy
        spaced["mov_a0" + (str(i + 1))] = np.sqrt(dx**2 + dy**2)
    return spaced


@ftype("gamestates")
def speed(gamestates: GameStates, mask: Mask) -> Features:
    """Get the speed at which the ball moved during the previous actions.

    Parameters
    ----------
    gamestates : GameStates
        The game states of a game.
    mask : Mask
        A boolean mask to filter game states.

    Returns
    -------
    Features
        A dataframe with columns 'speedx_a0i', 'speedy_a0i', 'speed_a0i'
        for each <nb_prev_actions> containing the ball speed in m/s  between
        action ai and action a0.
    """
    a0 = gamestates[0].loc[mask]
    speed = pd.DataFrame(index=a0.index)
    for i, a in enumerate(gamestates[1:]):
        dx = a.loc[mask].end_x - a0.start_x
        dy = a.loc[mask].end_y - a0.start_y
        dt = a0.time_seconds - a.loc[mask].time_seconds
        dt[dt <= 0] = 1e-6
        speed["speedx_a0" + (str(i + 1))] = dx.abs() / dt
        speed["speedy_a0" + (str(i + 1))] = dy.abs() / dt
        speed["speed_a0" + (str(i + 1))] = np.sqrt(dx**2 + dy**2) / dt
    return speed
