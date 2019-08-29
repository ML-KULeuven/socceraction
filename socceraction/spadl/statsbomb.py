import pandas as pd
import numpy as np
import tqdm
import ujson as json
import os

###############################################
# Convert statbomb json files to statsbomb.h5
###############################################


def jsonfiles_to_h5(datafolder, h5file):
    print(f"...Adding competitions to {h5file}")
    add_competitions(os.path.join(datafolder, "competitions.json"), h5file)
    print(f"...Adding matches to {h5file}")
    add_matches(os.path.join(datafolder, "matches/"), h5file)
    print(f"...Adding players and teams to {h5file}")
    add_players_and_teams(os.path.join(datafolder, "lineups/"), h5file)
    add_events(os.path.join(datafolder, "events/"), h5file)


def add_competitions(competitions_url, h5file):
    with open(competitions_url, "rt", encoding='utf-8') as fh:
        competitions = json.load(fh)
    pd.DataFrame(competitions).to_hdf(h5file, "competitions")


def add_matches(matches_url, h5file):
    matches = []
    for competition_file in get_jsonfiles(matches_url):
        with open(competition_file, "rt", encoding='utf-8') as fh:
            matches += json.load(fh)
    pd.DataFrame([flatten(m) for m in matches]).to_hdf(h5file, "matches")


def add_players_and_teams(lineups_url, h5file):
    lineups = []
    players = []
    for competition_file in get_jsonfiles(lineups_url):
        with open(competition_file, "rt", encoding='utf-8') as fh:
            lineups += json.load(fh)
            for lineup in lineups:
                players += [flatten_id(p) for p in lineup["lineup"]]
    players = pd.DataFrame(players)
    players.drop_duplicates("player_id").reset_index(drop=True).to_hdf(
        h5file, "players"
    )
    teams = pd.DataFrame(lineups)[["team_id", "team_name"]]
    teams.drop_duplicates("team_id").reset_index(drop=True).to_hdf(h5file, "teams")


def get_match_id(url):
    return int(url.split("/")[-1].replace(".json", ""))


def add_events(events_url, h5file):
    for events_file in tqdm.tqdm(
        get_jsonfiles(events_url), desc=f"converting events files to {h5file}"
    ):
        with open(events_file, "rt", encoding='utf-8') as fh:
            events = json.load(fh)
        eventsdf = pd.DataFrame([flatten_id(e) for e in events])
        match_id = get_match_id(events_file)
        eventsdf["match_id"] = match_id
        eventsdf.to_hdf(h5file, f"events/match_{match_id}")


def get_jsonfiles(path):
    files = []
    # r=root, d=directories, f = files
    for r, d, f in os.walk(path):
        for file in f:
            if ".json" in file:
                files.append(os.path.join(r, file))
    return files


def flatten(d):
    newd = {}
    for k, v in d.items():
        if isinstance(v, dict):
            newd = {**newd, **flatten(v)}
        else:
            newd[k] = v
    return newd


def flatten_id(d):
    newd = {}
    extra = {}
    for k, v in d.items():
        if isinstance(v, dict):
            if len(v) == 2 and "id" in v and "name" in v:
                newd[k + "_id"] = v["id"]
                newd[k + "_name"] = v["name"]
            else:
                extra[k] = v
        else:
            newd[k] = v
    newd["extra"] = extra
    return newd


###################################
# Convert statbomb.h5 to spadl.h5
###################################

import socceraction.spadl.config as spadlcfg

spadl_length = spadlcfg.spadl_length
spadl_width = spadlcfg.spadl_width

bodyparts = spadlcfg.bodyparts
results = spadlcfg.results
actiontypes = spadlcfg.actiontypes

min_dribble_length = 3
max_dribble_length = 60
max_dribble_duration = 10


def convert_to_spadl(sbh5, spadlh5):
    print("...Converting matches to games")
    matches = pd.read_hdf(sbh5, "matches")
    matches["game_id"] = matches.match_id
    games = matches
    games.to_hdf(spadlh5, "games")
    for key in ["players","teams","competitions"]:
        print(f"...Converting {key}")
        pd.read_hdf(sbh5, key).to_hdf(spadlh5, key)

    print("...Inserting actiontypes")
    actiontypesdf = pd.DataFrame(
        list(enumerate(actiontypes)), columns=["type_id", "type_name"]
    )
    actiontypesdf.to_hdf(spadlh5, key="actiontypes")

    print("...Inserting bodyparts")
    bodypartsdf = pd.DataFrame(
        list(enumerate(bodyparts)), columns=["bodypart_id", "bodypart_name"]
    )
    bodypartsdf.to_hdf(spadlh5, key="bodyparts")

    print("...Inserting results")
    resultsdf = pd.DataFrame(
        list(enumerate(results)), columns=["result_id", "result_name"]
    )
    resultsdf.to_hdf(spadlh5, key="results")

    print("... computing playergames (minutes played in each game")
    player_games = []
    for game_id in tqdm.tqdm(list(games.game_id),unit="game"):
        events = pd.read_hdf(sbh5, f"events/match_{game_id}")
        pg = get_playergames(events,game_id)
        player_games.append(pg)
    player_gamesdf = pd.concat(player_games)
    player_gamesdf.to_hdf(spadlh5,key="player_games")

    print("...Converting events to actions")
    for game in tqdm.tqdm(
        list(games.itertuples()), 
        unit="game"
    ):
        events = pd.read_hdf(sbh5, f"events/match_{game.game_id}")
        actions = convert_to_actions(events, game.home_team_id)
        actions.to_hdf(spadlh5, f"actions/game_{game.game_id}")

def get_playergames(events,game_id):
    game_minutes = max(events[events.type_name == "Half End"].minute)

    players = {}
    for startxi in events[events.type_name == 'Starting XI'].itertuples():
        team_id,team_name = startxi.team_id,startxi.team_name
        for player in startxi.extra["tactics"]["lineup"]:
            player = flatten_id(player)
            player = {**player,**{"game_id" : game_id, 
                                  "team_id": team_id, 
                                  "team_name": team_name,
                                  "minutes_played" : game_minutes}
                     }
            players[player["player_id"]] = player
    for substitution in events[events.type_name == 'Substitution'].itertuples():
        replacement = substitution.extra["substitution"]["replacement"]
        replacement = {"player_id": replacement["id"],"player_name" : replacement["name"],
                       "minutes_played": game_minutes - substitution.minute,
                      "team_id": substitution.team_id,
                      "game_id": game_id,
                      "team_name": substitution.team_name}
        players[replacement["player_id"]] = replacement
        #minutes_played = substitution.minute
        players[substitution.player_id]["minutes_played"] = substitution.minute
    pg = pd.DataFrame(players.values()).fillna(0)
    for col in pg.columns:
        if '_id' in col:
            pg[col] = pg[col].astype(int)
    return pg


def convert_to_actions(events, home_team_id):
    actions = pd.DataFrame()

    events["extra"] = events["extra"].fillna({})
    events = events.fillna(0)

    actions["game_id"] = events.match_id
    actions["period_id"] = events.period

    actions["time_seconds"] = (
        60 * events.minute - ((events.period == 2) * 45 * 60) + events.second
    )
    actions["timestamp"] = events.timestamp
    actions["team_id"] = events.team_id
    actions["player_id"] = events.player_id

    actions["start_x"] = events.location.apply(lambda x: x[0] if x else 1)
    actions["start_y"] = events.location.apply(lambda x: x[1] if x else 1)
    actions["start_x"] = ((actions["start_x"] - 1) / 119) * spadl_length
    actions["start_y"] = 68 - ((actions["start_y"] - 1) / 79) * spadl_width

    end_location = events[["location", "extra"]].apply(get_end_location, axis=1)
    actions["end_x"] = end_location.apply(lambda x: x[0] if x else 1)
    actions["end_y"] = end_location.apply(lambda x: x[1] if x else 1)
    actions["end_x"] = ((actions["end_x"] - 1) / 119) * spadl_length
    actions["end_y"] = 68 - ((actions["end_y"] - 1) / 79) * spadl_width

    actions["type_id"] = events[["type_name", "extra"]].apply(get_type_id, axis=1)
    actions["result_id"] = events[["type_name", "extra"]].apply(get_result_id, axis=1)
    actions["bodypart_id"] = events[["type_name", "extra"]].apply(
        get_bodypart_id, axis=1
    )

    actions = (
        actions[actions.type_id != actiontypes.index("non_action")]
        .sort_values(["game_id", "period_id", "time_seconds", "timestamp"])
        .reset_index(drop=True)
    )

    actions = fix_direction_of_play(actions, home_team_id)
    actions = fix_clearances(actions)
    actions = add_dribbles(actions)

    for col in actions.columns:
        if "_id" in col:
            actions[col] = actions[col].astype(int)
    return actions


def get_end_location(q):
    start_location, extra = q
    for event in ["pass", "shot", "carry"]:
        if event in extra and "end_location" in extra[event]:
            return extra[event]["end_location"]
    return start_location


def get_type_id(q):
    t, extra = q
    a = "non_action"
    if t == "Pass":
        a = "pass"  # default
        p = extra.get("pass", {})
        ptype = p.get("type", {}).get("name")
        height = p.get("height", {}).get("name")
        cross = p.get("cross")
        if ptype == "Free Kick":
            if height == "High Pass" or cross:
                a = "freekick_crossed"
            else:
                a = "freekick_short"
        elif ptype == "Corner":
            if height == "High Pass" or cross:
                a = "corner_crossed"
            else:
                a = "corner_short"
        elif ptype == "Goal Kick":
            a = "goalkick"
        elif ptype == "Throw-in":
            a = "throw_in"
        elif cross:
            a = "cross"
        else:
            a = "pass"
    elif t == "Dribble":
        a = "take_on"
    elif t == "Carry":
        a = "dribble"
    elif t == "Foul Committed":
        a = "foul"
    elif t == "Duel" and extra.get("duel", {}).get("type", {}).get("name") == "Tackle":
        a = "tackle"
    elif t == "Interception":
        a = "interception"
    elif t == "Shot":
        extra_type = extra.get("shot", {}).get("type", {}).get("name")
        if extra_type == "Free Kick":
            a = "shot_freekick"
        elif extra_type == "Penalty":
            a = "shot_penalty"
        else:
            a = "shot"
    elif t == "Own Goal Against":
        a = "shot"
    elif t == "Goal Keeper":
        extra_type = extra.get("goalkeeper", {}).get("type", {}).get("name")
        if extra_type == "Shot Saved":
            a = "keeper_save"
        elif extra_type == "Collected" or extra_type == "Keeper Sweeper":
            a = "keeper_claim"
        elif extra_type == "Punch":
            a = "keeper_punch"
        else:
            a = "non_action"
    elif t == "Clearance":
        a = "clearance"
    elif t == "Miscontrol":
        a = "bad_touch"
    else:
        a = "non_action"
    return actiontypes.index(a)


def get_result_id(q):
    t, x = q

    if t == "Pass":
        pass_outcome = x.get("pass", {}).get("outcome", {}).get("name")
        if pass_outcome in ["Incomplete", "Out"]:
            r = "fail"
        elif pass_outcome == "Pass Offside":
            r = "offside"
        else:
            r = "success"
    elif t == "Shot":
        shot_outcome = x.get("shot", {}).get("outcome", {}).get("name")
        if shot_outcome == "Goal":
            r = "success"
        elif shot_outcome in ["Blocked", "Off T", "Post", "Saved", "Wayward"]:
            r = "fail"
        else:
            r = "fail"
    elif t == "Dribble":
        dribble_outcome = x.get("dribble", {}).get("outcome", {}).get("name")
        if dribble_outcome == "Incomplete":
            r = "fail"
        elif dribble_outcome == "Complete":
            r = "success"
        else:
            r = "success"
    elif t == "Foul Committed":
        foul_card = x.get("foul_committed", {}).get("card", {}).get("name", "")
        if "Yellow" in foul_card:
            r = "yellow_card"
        elif "Red" in foul_card:
            r = "red_card"
        else:
            r = "success"
    elif t == "Duel":
        duel_outcome = x.get("duel", {}).get("outcome", {}).get("name")
        if duel_outcome in ["Lost in Play", "Lost Out"]:
            r = "fail"
        elif duel_outcome in ["Success in Play", "Won"]:
            r = "success"
        else:
            r = "success"
    elif t == "Interception":
        interception_outcome = x.get("interception", {}).get("outcome", {}).get("name")
        if interception_outcome in ["Lost In Play", "Lost Out"]:
            r = "fail"
        elif interception_outcome == "Won":
            r = "success"
        else:
            r = "success"
    elif t == "Own Goal Against":
        r = "owngoal"
    elif t == "Goal Keeper":
        goalkeeper_outcome = x.get("goalkeeper", {}).get("outcome", {}).get("name", "x")
        if goalkeeper_outcome in [
            "Claim",
            "Clear",
            "Collected Twice",
            "In Play Safe",
            "Success",
            "Touched Out",
        ]:
            r = "success"
        elif goalkeeper_outcome in ["In Play Danger", "No Touch"]:
            r = "fail"
        else:
            r = "success"
    elif t == "Clearance":
        r = "success"
    elif t == "Miscontrol":
        r = "fail"
    else:
        r = "success"

    return results.index(r)


def get_bodypart_id(q):
    t, x = q
    if t == "Shot":
        bp = x.get("shot", {}).get("body_part", {}).get("name")
    elif t == "Pass":
        bp = x.get("pass", {}).get("body_part", {}).get("name")
    elif t == "Goal Keeper":
        bp = x.get("goalkeeper", {}).get("body_part", {}).get("name")
    else:
        bp = None

    if bp is None:
        b = "foot"
    elif "Head" in bp:
        b = "head"
    elif "Foot" in bp or bp == "Drop Kick":
        b = "foot"
    else:
        b = "other"

    return bodyparts.index(b)


def fix_clearances(actions):
    next_actions = actions.shift(-1)
    clearance_idx = actions.type_id == actiontypes.index("clearance")
    actions.loc[clearance_idx, "end_x"] = next_actions[clearance_idx].start_x.values
    actions.loc[clearance_idx, "end_y"] = next_actions[clearance_idx].start_y.values
    return actions


def fix_direction_of_play(actions, home_team_id):
    away_idx = (actions.team_id != home_team_id).values
    for col in ["start_x", "end_x"]:
        actions.loc[away_idx, col] = spadl_length - actions[away_idx][col].values
    for col in ["start_y", "end_y"]:
        actions.loc[away_idx, col] = spadl_width - actions[away_idx][col].values

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
