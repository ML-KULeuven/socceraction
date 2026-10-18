"""Opta event stream data to SPADL converter."""

from typing import Any, cast

import pandas as pd  # type: ignore
from pandera.typing import DataFrame

from . import config as spadlconfig
from .base import (
    _add_dribbles,
    _fix_clearances,
    _fix_direction_of_play,
    min_dribble_length,
)
from .schema import SPADLSchema


def convert_to_actions(events: pd.DataFrame, home_team_id: int) -> DataFrame[SPADLSchema]:
    """
    Convert Opta events to SPADL actions.

    Parameters
    ----------
    events : pd.DataFrame
        DataFrame containing Opta events from a single game.
    home_team_id : int
        ID of the home team in the corresponding game.

    Returns
    -------
    actions : pd.DataFrame
        DataFrame with corresponding SPADL actions.

    """
    actions = pd.DataFrame()

    actions["game_id"] = events.game_id
    actions["original_event_id"] = events.event_id.astype(object)
    actions["period_id"] = events.period_id

    actions["time_seconds"] = (
        60 * events.minute
        + events.second
        - ((events.period_id > 1) * 45 * 60)
        - ((events.period_id > 2) * 45 * 60)
        - ((events.period_id > 3) * 15 * 60)
        - ((events.period_id > 4) * 15 * 60)
    )
    actions["team_id"] = events.team_id
    actions["player_id"] = events.player_id

    for col in ["start_x", "end_x"]:
        actions[col] = events[col].clip(0, 100) / 100 * spadlconfig.field_length
    for col in ["start_y", "end_y"]:
        actions[col] = events[col].clip(0, 100) / 100 * spadlconfig.field_width

    actions["type_id"] = events[["type_name", "outcome", "qualifiers"]].apply(_get_type_id, axis=1)
    actions["result_id"] = events[["type_name", "outcome", "qualifiers"]].apply(
        _get_result_id, axis=1
    )
    actions["bodypart_id"] = events[["type_name", "outcome", "qualifiers"]].apply(
        _get_bodypart_id, axis=1
    )

    actions = _fix_recoveries(actions, events.type_name)
    actions = _fix_unintentional_ball_touches(actions, events.type_name, events.outcome)
    actions = (
        actions[actions.type_id != spadlconfig.actiontypes.index("non_action")]
        .sort_values(["game_id", "period_id", "time_seconds"], kind="mergesort")
        .reset_index(drop=True)
    )
    actions = _fix_owngoals(actions)
    actions = _fix_direction_of_play(actions, home_team_id)
    actions = _fix_clearances(actions)
    actions = _fix_interceptions(actions)
    actions["action_id"] = range(len(actions))
    actions = _add_dribbles(actions)

    return cast(DataFrame[SPADLSchema], actions)


def _get_bodypart_id(args: tuple[str, bool, dict[int, Any]]) -> int:
    e, outcome, q = args
    if 15 in q or 3 in q or 168 in q:
        b = "head"
    elif 21 in q:
        b = "other"
    elif 20 in q:
        b = "foot_right"
    elif 72 in q:
        b = "foot_left"
    elif 107 in q:  # throw-in
        b = "other"
    else:
        if e in ["save", "claim", "punch", "keeper pick-up"]:
            b = "other"
        else:
            b = "foot"
    return spadlconfig.bodyparts.index(b)


def _get_result_id(args: tuple[str, bool, dict[int, Any]]) -> int:
    e, outcome, q = args
    if e == "offside pass":
        r = "offside"  # offside
    elif e == "foul":
        r = "fail"
    elif e in ["attempt saved", "miss", "post"]:
        r = "fail"
    elif e == "goal":
        if 28 in q:
            r = "owngoal"  # own goal, x and y must be switched
        else:
            r = "success"
    elif e == "ball touch":
        r = "fail"
    elif outcome:
        r = "success"
    else:
        r = "fail"
    return spadlconfig.results.index(r)


def _get_type_id(args: tuple[str, bool, dict[int, Any]]) -> int:  # noqa: C901
    eventname, outcome, q = args
    fairplay = 238 in q
    if fairplay:
        a = "non_action"
    elif eventname in ("pass", "offside pass"):
        cross = 2 in q
        longball = 1 in q
        chipped = 155 in q
        freekick = 5 in q
        corner = 6 in q
        throw_in = 107 in q
        goalkick = 124 in q
        if throw_in:
            a = "throw_in"
        elif freekick and (cross or longball or chipped):
            a = "freekick_crossed"
        elif freekick:
            a = "freekick_short"
        elif corner and cross:
            a = "corner_crossed"
        elif corner:
            a = "corner_short"
        elif cross:
            a = "cross"
        elif goalkick:
            a = "goalkick"
        else:
            a = "pass"
    elif eventname == "take on":
        a = "take_on"
    elif eventname == "foul" and outcome is False:
        a = "foul"
    elif eventname == "tackle":
        a = "tackle"
    elif eventname in ("interception", "blocked pass"):
        a = "interception"
    elif eventname in ["miss", "post", "attempt saved", "goal"]:
        if 9 in q:
            a = "shot_penalty"
        elif 26 in q:
            a = "shot_freekick"
        else:
            a = "shot"
    elif eventname == "save":
        if 94 in q:
            a = "non_action"
        else:
            a = "keeper_save"
    elif eventname == "claim":
        a = "keeper_claim"
    elif eventname == "punch":
        a = "keeper_punch"
    elif eventname == "keeper pick-up":
        a = "keeper_pick_up"
    elif eventname == "clearance":
        a = "clearance"
    elif eventname == "ball touch" and outcome is False:
        a = "bad_touch"
    else:
        a = "non_action"
    return spadlconfig.actiontypes.index(a)


def _fix_owngoals(actions: pd.DataFrame) -> pd.DataFrame:
    owngoals_idx = (actions.result_id == spadlconfig.results.index("owngoal")) & (
        actions.type_id == spadlconfig.actiontypes.index("shot")
    )
    actions.loc[owngoals_idx, "end_x"] = (
        spadlconfig.field_length - actions[owngoals_idx].end_x.values
    )
    actions.loc[owngoals_idx, "end_y"] = (
        spadlconfig.field_width - actions[owngoals_idx].end_y.values
    )
    actions.loc[owngoals_idx, "type_id"] = spadlconfig.actiontypes.index("bad_touch")
    return actions


def _fix_recoveries(df_actions: pd.DataFrame, opta_types: pd.Series) -> pd.DataFrame:
    """Convert ball recovery events to dribbles.

    This function converts the Opta 'ball recovery' event (type_id 49) into
    a dribble.

    Parameters
    ----------
    df_actions : pd.DataFrame
        Opta actions dataframe
    opta_types : pd.Series
        Original Opta event types

    Returns
    -------
    pd.DataFrame
        Opta event dataframe without any ball recovery events
    """
    df_actions_next = df_actions.shift(-1)
    df_actions_next = df_actions_next.mask(
        df_actions_next.type_id == spadlconfig.actiontypes.index("non_action")
    ).bfill()

    selector_recovery = opta_types == "ball recovery"

    same_x = abs(df_actions["end_x"] - df_actions_next["start_x"]) < min_dribble_length
    same_y = abs(df_actions["end_y"] - df_actions_next["start_y"]) < min_dribble_length
    same_loc = same_x & same_y

    df_actions.loc[selector_recovery & ~same_loc, "type_id"] = spadlconfig.actiontypes.index(
        "dribble"
    )
    df_actions.loc[selector_recovery & same_loc, "type_id"] = spadlconfig.actiontypes.index(
        "non_action"
    )
    df_actions.loc[selector_recovery, ["end_x", "end_y"]] = df_actions_next.loc[
        selector_recovery, ["start_x", "start_y"]
    ].values

    return df_actions


def _fix_interceptions(df_actions: pd.DataFrame) -> pd.DataFrame:
    """Set the result of interceptions to 'fail' if they do not regain possession.

    Parameters
    ----------
    df_actions : pd.DataFrame
        Opta actions dataframe.

    Returns
    -------
    pd.DataFrame
        Opta event dataframe without any ball recovery events
    """
    mask_interception = df_actions.type_id == spadlconfig.actiontypes.index("interception")
    same_team = df_actions.team_id == df_actions.shift(-1).team_id
    df_actions.loc[mask_interception & ~same_team, "result_id"] = spadlconfig.results.index("fail")
    return df_actions


def _fix_unintentional_ball_touches(
    df_actions: pd.DataFrame, opta_type: pd.Series, opta_outcome: pd.Series
) -> pd.DataFrame:
    """Discard unintentional ball touches.

    Passes that are deflected but still reach their target are registered as
    successful passes. The (unintentional) deflection is not recored as an
    action, because players should not be credited for it.

    Parameters
    ----------
    df_actions : pd.DataFrame
        Opta actions dataframe
    opta_type : pd.Series
        Original Opta event types
    opta_outcome : pd.Series
        Original Opta event outcomes

    Returns
    -------
    pd.DataFrame
        Opta event dataframe without any unintentional ball touches.
    """
    df_actions_next = df_actions.shift(-2)
    selector_pass = df_actions["type_id"] == spadlconfig.actiontypes.index("pass")
    selector_deflected = (opta_type.shift(-1) == "ball touch") & (opta_outcome.shift(-1))
    selector_same_team = df_actions["team_id"] == df_actions_next["team_id"]
    df_actions.loc[selector_deflected, ["end_x", "end_y"]] = df_actions_next.loc[
        selector_deflected, ["start_x", "start_y"]
    ].values
    df_actions.loc[selector_pass & selector_deflected & selector_same_team, "result_id"] = (
        spadlconfig.results.index("success")
    )
    return df_actions
