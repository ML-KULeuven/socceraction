import pandas as pd
import numpy as np
import tqdm

spadl_length = 105
spadl_width = 68

bodyparts = ["foot", "head", "other"]
results = ["fail", "success", "offside", "owngoal"]  # todo: add yellow and red card
actiontypes = [
    "pass",
    "cross",
    "throw_in",
    "freekick_crossed",
    "freekick_short",
    "corner_crossed",
    "corner_short",
    "take_on",
    "foul",
    "tackle",
    "interception",
    "shot",
    "shot_penalty",
    "shot_freekick",
    "keeper_save",
    "keeper_claim",
    "keeper_punch",
    "keeper_pick_up",
    "clearance",
    "bad_touch",
    "non_action",
    "dribble",
    "goalkick",
]


def convert_optah5(optah5, spadlh5):

    games = pd.read_hdf(optah5, key="games")
    games.to_hdf(spadlh5, key="games")

    players = pd.read_hdf(optah5, key="players")
    players = players.rename(
        index=str,
        columns={
            "firstname": "first_name",
            "lastname": "last_name",
            "knownname": "soccer_name",
        },
    )
    players["birthday"] = pd.NaT  # unavailabe
    players["nation_id"] = np.nan  # unavailable
    players.to_hdf(spadlh5, key="players")

    teams = pd.read_hdf(optah5, key="teams")
    teams.to_hdf(spadlh5, key="teams")

    teamgames = pd.read_hdf(optah5, key="teamgamestats")
    teamgames = teamgames.rename(index=str, columns={"formation_used": "formation"})
    teamgames.to_hdf(spadlh5, key="team_games")

    playergames = pd.read_hdf(optah5, key="playergamestats")
    playergames = teamgames.rename(
        index=str,
        columns={
            "mins_played": "minutes_played",
            "goal_assists": "assists",
            "total_att_assist": "keypasses",
            "second_goal_assist": "pre_assists",
        },
    )
    playergames.to_hdf(spadlh5, key="player_games")

    actiontypesdf = pd.DataFrame(
        list(enumerate(actiontypes)), columns=["type_id", "type_name"]
    )
    actiontypesdf.to_hdf(spadlh5, key="actiontypes")

    bodypartsdf = pd.DataFrame(
        list(enumerate(bodyparts)), columns=["bodypart_id", "bodypart_name"]
    )
    bodypartsdf.to_hdf(spadlh5, key="bodyparts")

    resultsdf = pd.DataFrame(
        list(enumerate(results)), columns=["result_id", "result_name"]
    )
    resultsdf.to_hdf(spadlh5, key="results")

    eventtypes = pd.read_hdf(optah5, "eventtypes")
    for game in tqdm.tqdm(list(games.itertuples())):

        events = pd.read_hdf(optah5, f"events/game_{game.game_id}")
        events = (
            events.merge(eventtypes, on="type_id")
            .sort_values(["game_id", "period_id", "minute", "second","timestamp"])
            .reset_index(drop=True)
        )
        actions = convert_to_actions(events, home_team_id=game.home_team_id)
        actions.to_hdf(spadlh5, f"actions/game_{game.game_id}")


def convert_to_actions(events, home_team_id):
    actions = events
    actions["time_seconds"] = 60 * actions.minute + actions.second
    for col in ["start_x", "end_x"]:
        actions[col] = actions[col] / 100 * spadl_length
    for col in ["start_y", "end_y"]:
        actions[col] = actions[col] / 100 * spadl_width
    actions["bodypart_id"] = actions.qualifiers.apply(get_bodypart_id)
    actions["type_id"] = actions[["type_name", "outcome", "qualifiers"]].apply(
        get_type_id, axis=1
    )
    actions["result_id"] = actions[["type_name", "outcome", "qualifiers"]].apply(
        get_result_id, axis=1
    )

    actions = (
        actions[actions.type_id != actiontypes.index("non_action")]
        .sort_values(["game_id", "period_id", "time_seconds", "timestamp"])
        .reset_index(drop=True)
    )
    actions = fix_owngoal_coordinates(actions)
    actions = fix_direction_of_play(actions, home_team_id)
    actions = fix_clearances(actions)
    actions = add_dribbles(actions)
    return actions[
        [
            "game_id",
            "period_id",
            "time_seconds",
            "timestamp",
            "team_id",
            "player_id",
            "start_x",
            "start_y",
            "end_x",
            "end_y",
            "result_id",
            "bodypart_id",
            "type_id",
        ]
    ]


def get_bodypart_id(qualifiers):
    if 15 in qualifiers:
        b = "head"
    elif 21 in qualifiers:
        b = "other"
    else:
        b = "foot"
    return bodyparts.index(b)


def get_result_id(args):
    e, outcome, q = args
    if e == "offside pass":
        r = "offside"  # offside
    elif e == "foul":
        r = "success"
    elif e in ["attempt saved", "miss", "post"]:
        r = "fail"
    elif e == "goal":
        if 28 in q:
            r = "owngoal"  # own goal, x and y must be switched
        else:
            r = "success"
    elif e == "bad touch":
        r = "fail"
    elif outcome:
        r = "success"
    else:
        r = "fail"
    return results.index(r)


def get_type_id(args):
    eventname, outcome, q = args
    if eventname == "pass" or eventname == "offside pass":
        cross = 2 in q
        freekick = 5 in q
        corner = 6 in q
        throw_in = 107 in q
        if throw_in:
            a = "throw_in"
        elif freekick and cross:
            a = "freekick_crossed"
        elif freekick:
            a = "freekick_short"
        elif corner and cross:
            a = "corner_crossed"
        elif corner:
            a = "corner_short"
        elif cross:
            a = "cross"
        else:
            a = "pass"
    elif eventname == "take on":
        a = "take_on"
    elif eventname == "foul" and outcome is False:
        a = "foul"
    elif eventname == "tackle":
        a = "tackle"
    elif eventname == "interception" or eventname == "blocked pass":
        a = "interception"
    elif eventname in ["miss", "post", "attempt saved", "goal"]:
        if 9 in q:
            a = "shot_penalty"
        elif 26 in q:
            a = "shot_freekick"
        else:
            a = "shot"
    elif eventname == "save":
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
    return actiontypes.index(a)


def fix_owngoal_coordinates(actions):
    owngoals_idx = (actions.result_id == results.index("owngoal")) & (
        actions.type_id == actiontypes.index("shot")
    )
    actions.loc[owngoals_idx, "end_x"] = (
        spadl_length - actions[owngoals_idx].end_x.values
    )
    actions.loc[owngoals_idx, "end_y"] = (
        spadl_width - actions[owngoals_idx].end_y.values
    )
    return actions


min_dribble_length = 3
max_dribble_length = 60
max_dribble_duration = 10


def add_dribbles(actions):
    next_actions = actions.shift(-1)

    same_team = actions.team_id == next_actions.team_id
    #not_clearance = actions.type_id != actiontypes.index("clearance")

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
    dribbles["result_id"] = results.index("success")

    actions = pd.concat([actions, dribbles], ignore_index=True, sort=False)
    actions = actions.sort_values(["game_id", "period_id", "time_seconds", "timestamp"])
    actions.reset_index(drop=True, inplace=True)
    return actions

def fix_clearances(actions):
    next_actions = actions.shift(-1)
    clearance_idx = actions.type_id == actiontypes.index("clearance")
    actions.loc[clearance_idx,"end_x"] = next_actions[clearance_idx].start_x.values
    actions.loc[clearance_idx,"end_y"] = next_actions[clearance_idx].start_y.values
    return actions

def fix_direction_of_play(actions, home_team_id):
    away_idx = (actions.team_id != home_team_id).values
    for col in ["start_x", "end_x"]:
        actions.loc[away_idx, col] = spadl_length - actions[away_idx][col].values
    for col in ["start_y", "end_y"]:
        actions.loc[away_idx, col] = spadl_width - actions[away_idx][col].values

    return actions
