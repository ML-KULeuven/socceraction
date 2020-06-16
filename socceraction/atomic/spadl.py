import os
import numpy as np
import pandas as pd

import socceraction.spadl as _spadl
import tqdm

bodyparts = _spadl.bodyparts
bodyparts_df = _spadl.bodyparts_df

min_dribble_length = 3
max_dribble_length = 60
max_dribble_duration = 10
# max_pass_duration = 15

actiontypes = _spadl.actiontypes + [
    "receival",
    "interception",
    "out",
    "offside",
    "goal",
    "owngoal",
    "yellow_card",
    "red_card",
    "corner",
    "freekick",
]


def actiontypes_df():
    return pd.DataFrame(
        list(enumerate(actiontypes)), columns=["type_id", "type_name"]
    )


def convert_to_atomic(actions):
    actions = actions.copy()
    actions = extra_from_passes(actions)
    actions = add_dribbles(actions)  # for some reason this adds more dribbles
    actions = extra_from_shots(actions)
    actions = extra_from_fouls(actions)
    actions = convert_columns(actions)
    actions = simplify(actions)
    return actions


def simplify(actions):
    a = actions
    ar = actiontypes

    cornerlike = ["corner_crossed","corner_short"]
    corner_ids = list(_spadl.actiontypes.index(ty) for ty in cornerlike)

    freekicklike = ["freekick_crossed","freekick_short","shot_freekick"]
    freekick_ids = list(_spadl.actiontypes.index(ty) for ty in freekicklike)

    a["type_id"] = a.type_id.mask(a.type_id.isin(corner_ids), ar.index("corner"))
    a["type_id"] = a.type_id.mask(a.type_id.isin(freekick_ids), ar.index("freekick"))
    return a


def extra_from_passes(actions):
    next_actions = actions.shift(-1)
    same_team = actions.team_id == next_actions.team_id

    passlike = [
        "pass",
        "cross",
        "throw_in",
        "freekick_short",
        "freekick_crossed",
        "corner_crossed",
        "corner_short",
        "clearance",
        "goalkick",
    ]
    pass_ids = list(_spadl.actiontypes.index(ty) for ty in passlike)

    interceptionlike = [
        "interception",
        "tackle",
        "keeper_punch",
        "keeper_save",
        "keeper_claim",
        "keeper_pick_up",
    ]
    interception_ids = list(_spadl.actiontypes.index(ty) for ty in interceptionlike)

    samegame = actions.game_id == next_actions.game_id
    sameperiod = actions.period_id == next_actions.period_id
    # samephase = next_actions.time_seconds - actions.time_seconds < max_pass_duration
    extra_idx = (
        actions.type_id.isin(pass_ids)
        & samegame
        & sameperiod  # & samephase
        & ~next_actions.type_id.isin(interception_ids)
    )

    prev = actions[extra_idx]
    nex = next_actions[extra_idx]

    extra = pd.DataFrame()
    extra["game_id"] = prev.game_id
    extra["period_id"] = prev.period_id
    extra["action_id"] = prev.action_id + 0.1
    extra["time_seconds"] = (prev.time_seconds + nex.time_seconds) / 2
    extra["timestamp"] = nex.timestamp
    extra["start_x"] = prev.end_x
    extra["start_y"] = prev.end_y
    extra["end_x"] = prev.end_x
    extra["end_y"] = prev.end_y
    extra["bodypart_id"] = bodyparts.index("foot")
    extra["result_id"] = -1

    offside = prev.result_id == _spadl.results.index("offside")
    out = ((nex.type_id == actiontypes.index("goalkick")) & (~same_team)) | (
        nex.type_id == actiontypes.index("throw_in")
    )
    ar = actiontypes
    extra["type_id"] = -1
    extra["type_id"] = (
        extra.type_id.mask(same_team, ar.index("receival"))
        .mask(~same_team, ar.index("interception"))
        .mask(out, ar.index("out"))
        .mask(offside, ar.index("offside"))
    )
    is_interception = extra["type_id"] == ar.index("interception")
    extra["team_id"] = prev.team_id.mask(is_interception, nex.team_id)
    extra["player_id"] = nex.player_id.mask(out | offside, prev.player_id)

    actions = pd.concat([actions, extra], ignore_index=True, sort=False)
    actions = actions.sort_values(["game_id", "period_id", "action_id"]).reset_index(
        drop=True
    )
    actions["action_id"] = range(len(actions))
    return actions


def extra_from_shots(actions):
    next_actions = actions.shift(-1)

    shotlike = ["shot", "shot_freekick", "shot_penalty"]
    shot_ids = list(_spadl.actiontypes.index(ty) for ty in shotlike)

    samegame = actions.game_id == next_actions.game_id
    sameperiod = actions.period_id == next_actions.period_id

    shot = actions.type_id.isin(shot_ids)
    goal = shot & (actions.result_id == _spadl.results.index("success"))
    owngoal = shot & (actions.result_id == _spadl.results.index("owngoal"))
    next_corner_goalkick = next_actions.type_id.isin(
        [
            actiontypes.index("corner_crossed"),
            actiontypes.index("corner_short"),
            actiontypes.index("goalkick"),
        ]
    )
    out = shot & next_corner_goalkick & samegame & sameperiod

    extra_idx = goal | owngoal | out
    prev = actions[extra_idx]
    nex = next_actions[extra_idx]

    extra = pd.DataFrame()
    extra["game_id"] = prev.game_id
    extra["period_id"] = prev.period_id
    extra["action_id"] = prev.action_id + 0.1
    extra["time_seconds"] = prev.time_seconds  # + nex.time_seconds) / 2
    extra["timestamp"] = prev.timestamp
    extra["start_x"] = prev.end_x
    extra["start_y"] = prev.end_y
    extra["end_x"] = prev.end_x
    extra["end_y"] = prev.end_y
    extra["bodypart_id"] = prev.bodypart_id
    extra["result_id"] = -1
    extra["team_id"] = prev.team_id
    extra["player_id"] = prev.player_id

    ar = actiontypes
    extra["type_id"] = -1
    extra["type_id"] = (
        extra.type_id.mask(goal, ar.index("goal"))
        .mask(owngoal, ar.index("owngoal"))
        .mask(out, ar.index("out"))
    )
    actions = pd.concat([actions, extra], ignore_index=True, sort=False)
    actions = actions.sort_values(["game_id", "period_id", "action_id"]).reset_index(
        drop=True
    )
    actions["action_id"] = range(len(actions))
    return actions


def extra_from_fouls(actions):
    yellow = actions.result_id == _spadl.results.index("yellow_card")
    red = actions.result_id == _spadl.results.index("red_card")

    prev = actions[yellow | red]
    extra = pd.DataFrame()
    extra["game_id"] = prev.game_id
    extra["period_id"] = prev.period_id
    extra["action_id"] = prev.action_id + 0.1
    extra["time_seconds"] = prev.time_seconds  # + nex.time_seconds) / 2
    extra["timestamp"] = prev.timestamp
    extra["start_x"] = prev.end_x
    extra["start_y"] = prev.end_y
    extra["end_x"] = prev.end_x
    extra["end_y"] = prev.end_y
    extra["bodypart_id"] = prev.bodypart_id
    extra["result_id"] = -1
    extra["team_id"] = prev.team_id
    extra["player_id"] = prev.player_id

    ar = actiontypes
    extra["type_id"] = -1
    extra["type_id"] = extra.type_id.mask(yellow, ar.index("yellow_card")).mask(
        red, ar.index("red_card")
    )
    actions = pd.concat([actions, extra], ignore_index=True, sort=False)
    actions = actions.sort_values(["game_id", "period_id", "action_id"]).reset_index(
        drop=True
    )
    actions["action_id"] = range(len(actions))
    return actions


def add_dribbles(actions):
    next_actions = actions.shift(-1)

    same_team = actions.team_id == next_actions.team_id
    # not_clearance = actions.type_id != actiontypes.index("clearance")

    dx = actions.end_x - next_actions.start_x
    dy = actions.end_y - next_actions.start_y
    far_enough = dx ** 2 + dy ** 2 >= min_dribble_length ** 2
    not_too_far = dx ** 2 + dy ** 2 <= max_dribble_length ** 2

    dt = next_actions.time_seconds - actions.time_seconds
    same_phase = dt < max_dribble_duration

    dribble_idx = same_team & far_enough & not_too_far & same_phase

    dribbles = pd.DataFrame()
    prev = actions[dribble_idx]
    nex = next_actions[dribble_idx]
    dribbles["game_id"] = nex.game_id
    dribbles["period_id"] = nex.period_id
    dribbles["action_id"] = prev.action_id + 0.1
    dribbles["time_seconds"] = (prev.time_seconds + nex.time_seconds) / 2
    dribbles["timestamp"] = nex.timestamp
    dribbles["team_id"] = nex.team_id
    dribbles["player_id"] = nex.player_id
    dribbles["start_x"] = prev.end_x
    dribbles["start_y"] = prev.end_y
    dribbles["end_x"] = nex.start_x
    dribbles["end_y"] = nex.start_y
    dribbles["bodypart_id"] = bodyparts.index("foot")
    dribbles["type_id"] = actiontypes.index("dribble")
    dribbles["result_id"] = _spadl.results.index("success")

    actions = pd.concat([actions, dribbles], ignore_index=True, sort=False)
    actions = actions.sort_values(["game_id", "period_id", "action_id"]).reset_index(
        drop=True
    )
    actions["action_id"] = range(len(actions))
    return actions


def convert_columns(actions):
    actions["x"] = actions.start_x
    actions["y"] = actions.start_y
    actions["dx"] = actions.end_x - actions.start_x
    actions["dy"] = actions.end_y - actions.start_y
    return actions[
        [
            "game_id",
            "period_id",
            "action_id",
            "time_seconds",
            "timestamp",
            "team_id",
            "player_id",
            "x",
            "y",
            "dx",
            "dy",
            "type_id",
            "bodypart_id",
        ]
    ]

