import pandas as pd
import numpy as np
import tqdm
import json
import os

###############################################
# Convert wyscout json files to wyscout.h5
###############################################


def jsonfiles_to_h5(jsonfiles, h5file):

    matches = []
    players = []
    teams = []

    with pd.HDFStore(h5file) as store:
        for jsonfile in jsonfiles:
            with open(jsonfile, "r", encoding="utf-8") as fh:
                root = json.load(fh)
            matches.append(get_match(root))
            teams += get_teams(root)
            players += get_players(root)

            events = get_events(root)
            store[f"events/match_{get_match_id(root)}"] = pd.DataFrame(events)

        store["matches"] = pd.DataFrame(matches).drop_duplicates("wyId")
        store["teams"] = pd.DataFrame(teams).drop_duplicates("wyId")
        store["players"] = pd.DataFrame(players).drop_duplicates("wyId")


def get_match(root):
    return root["match"]


def get_match_id(root):
    return root["match"]["wyId"]


def get_teams(root):
    return [t["team"] for t in root["teams"].values() if t.get("team")]


def get_players(root):
    return [
        player["player"]
        for team in root["players"].values()
        for player in team
        if player.get("player")
    ]


def get_events(root):
    return root["events"]


###################################
# Convert wyscout.h5 to spadl.h5
# WARNING: HERE BE DRAGONS
# This code for converting wyscout data was organically grown over a long period of time.
# It works for now, but needs to be cleaned up in the future.
# Enter at your own risk.
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


def convert_to_spadl(wyscouth5, spadlh5):

    with pd.HDFStore(wyscouth5) as wyscoutstore, pd.HDFStore(spadlh5) as spadlstore:

        print("...Inserting actiontypes")
        spadlstore["actiontypes"] = pd.DataFrame(
            list(enumerate(actiontypes)), columns=["type_id", "type_name"]
        )

        print("...Inserting bodyparts")
        spadlstore["bodyparts"] = pd.DataFrame(
            list(enumerate(bodyparts)), columns=["bodypart_id", "bodypart_name"]
        )

        print("...Inserting results")
        spadlstore["results"] = pd.DataFrame(
            list(enumerate(results)), columns=["result_id", "result_name"]
        )

        print("...Converting games")
        matches = wyscoutstore["matches"]
        games = convert_games(matches)
        spadlstore["games"] = games

        print("...Converting players")
        spadlstore["players"] = convert_players(wyscoutstore["players"])

        print("...Converting teams")
        spadlstore["teams"] = convert_teams(wyscoutstore["teams"])

        print("...Generating player_games")
        player_games = []
        for match in tqdm.tqdm(list(matches.itertuples()), unit="game"):
            events = wyscoutstore[f"events/match_{match.wyId}"]
            pg = get_player_games(match, events)
            player_games.append(pg)
        player_games = pd.concat(player_games)
        spadlstore["player_games"] = player_games

        print("...Converting events to actions")
        for game in tqdm.tqdm(list(games.itertuples()), unit="game"):
            events = wyscoutstore[f"events/match_{game.game_id}"]
            actions = convert_actions(events, game.home_team_id)
            spadlstore[f"actions/game_{game.game_id}"] = actions


gamesmapping = {
    "wyId": "game_id",
    "dateutc": "game_date",
    "competitionId": "competition_id",
    "seasonId": "season_id",
}


def convert_games(matches):
    cols = ["game_id", "competition_id", "season_id", "game_date"]
    games = matches.rename(columns=gamesmapping)[cols]
    games["home_team_id"] = matches.teamsData.apply(lambda x: get_team_id(x, "home"))
    games["away_team_id"] = matches.teamsData.apply(lambda x: get_team_id(x, "away"))
    return games


def get_team_id(teamsData, side):
    for team_id, data in teamsData.items():
        if data["side"] == side:
            return int(team_id)


playermapping = {
    "wyId": "player_id",
    "shortName": "short_name",
    "firstName": "first_name",
    "lastName": "last_name",
    "birthDate": "birth_date",
}


def convert_players(players):
    cols = ["player_id", "short_name", "first_name", "last_name", "birth_date"]
    return players.rename(columns=playermapping)[cols]


teammapping = {
    "wyId": "team_id",
    "name": "short_team_name",
    "officialName": "team_name",
}


def convert_teams(teams):
    cols = ["team_id", "short_team_name", "team_name"]
    return teams.rename(columns=teammapping)[cols]


def get_player_games(match, events):
    game_id = match.wyId
    teamsData = match.teamsData
    duration = 45 + events[events.matchPeriod == "2H"].eventSec.max() / 60
    playergames = {}
    for team_id, teamData in teamsData.items():
        formation = teamData.get("formation", {})
        pg = {
            player["playerId"]: {
                "game_id": game_id,
                "team_id": team_id,
                "player_id": player["playerId"],
                "minutes_played": duration,
            }
            for player in formation.get("lineup", [])
        }

        substitutions = formation.get("substitutions", [])

        if substitutions != "null":
            for substitution in substitutions:
                substitute = {
                    "game_id": game_id,
                    "team_id": team_id,
                    "player_id": substitution["playerIn"],
                    "minutes_played": duration - substitution["minute"],
                }
                pg[substitution["playerIn"]] = substitute
                pg[substitution["playerOut"]]["minutes_played"] = substitution["minute"]
        playergames = {**playergames, **pg}
    return pd.DataFrame(playergames.values())


def convert_actions(events, home_team_id):
    events = augment_events(events)
    events = fix_wyscout_events(events)
    actions = create_df_actions(events)
    actions = fix_actions(actions)
    actions = fix_direction_of_play(actions, home_team_id)
    actions = fix_clearances(actions)
    actions = add_dribbles(actions)
    return actions


def augment_events(events_df):
    events_df = pd.concat([events_df, get_tagsdf(events_df)], axis=1)
    events_df = make_new_positions(events_df)
    events_df["type_id"] = (
        events_df["eventId"] if "eventId" in events_df.columns else events_df.eventName
    )
    events_df["subtype_id"] = (
        events_df["subEventId"]
        if "subEventId" in events_df.columns
        else events_df.subEventName
    )
    events_df["period_id"] = events_df.matchPeriod.apply(lambda x: wyscout_periods[x])
    events_df["player_id"] = events_df["playerId"]
    events_df["team_id"] = events_df["teamId"]
    events_df["game_id"] = events_df["matchId"]
    events_df["milliseconds"] = events_df.eventSec * 1000
    return events_df


def get_tag_set(tags):
    return {tag["id"] for tag in tags}


def get_tagsdf(events):
    tags = events.tags.apply(get_tag_set)
    tagsdf = pd.DataFrame()
    for (tag_id, column) in wyscout_tags:
        tagsdf[column] = tags.apply(lambda x: tag_id in x)
    return tagsdf


wyscout_periods = {"1H": 1, "2H": 2, "E1": 3, "E2": 4, "P": 5}


wyscout_tags = [
    (101, "goal"),
    (102, "own_goal"),
    (301, "assist"),
    (302, "key_pass"),
    (1901, "counter_attack"),
    (401, "left_foot"),
    (402, "right_foot"),
    (403, "head/body"),
    (1101, "direct"),
    (1102, "indirect"),
    (2001, "dangerous_ball_lost"),
    (2101, "blocked"),
    (801, "high"),
    (802, "low"),
    (1401, "interception"),
    (1501, "clearance"),
    (201, "opportunity"),
    (1301, "feint"),
    (1302, "missed_ball"),
    (501, "free_space_right"),
    (502, "free_space_left"),
    (503, "take_on_left"),
    (504, "take_on_right"),
    (1601, "sliding_tackle"),
    (601, "anticipated"),
    (602, "anticipation"),
    (1701, "red_card"),
    (1702, "yellow_card"),
    (1703, "second_yellow_card"),
    (1201, "position_goal_low_center"),
    (1202, "position_goal_low_right"),
    (1203, "position_goal_mid_center"),
    (1204, "position_goal_mid_left"),
    (1205, "position_goal_low_left"),
    (1206, "position_goal_mid_right"),
    (1207, "position_goal_high_center"),
    (1208, "position_goal_high_left"),
    (1209, "position_goal_high_right"),
    (1210, "position_out_low_right"),
    (1211, "position_out_mid_left"),
    (1212, "position_out_low_left"),
    (1213, "position_out_mid_right"),
    (1214, "position_out_high_center"),
    (1215, "position_out_high_left"),
    (1216, "position_out_high_right"),
    (1217, "position_post_low_right"),
    (1218, "position_post_mid_left"),
    (1219, "position_post_low_left"),
    (1220, "position_post_mid_right"),
    (1221, "position_post_high_center"),
    (1222, "position_post_high_left"),
    (1223, "position_post_high_right"),
    (901, "through"),
    (1001, "fairplay"),
    (701, "lost"),
    (702, "neutral"),
    (703, "won"),
    (1801, "accurate"),
    (1802, "not_accurate"),
]


def make_position_vars(event_id, positions):
    if len(positions) == 2:  # if less than 2 then action is removed
        start_x = positions[0]["x"]
        start_y = positions[0]["y"]
        end_x = positions[1]["x"]
        end_y = positions[1]["y"]
    elif len(positions) == 1:
        start_x = positions[0]["x"]
        start_y = positions[0]["y"]
        end_x = start_x
        end_y = start_y
    else:
        start_x = None
        start_y = None
        end_x = None
        end_y = None
    return pd.Series([event_id, start_x, start_y, end_x, end_y])


def make_new_positions(events_df):
    new_positions = events_df[["id", "positions"]].apply(
        lambda x: make_position_vars(x[0], x[1]), axis=1
    )
    new_positions.columns = ["id", "start_x", "start_y", "end_x", "end_y"]
    events_df = pd.merge(events_df, new_positions, left_on="id", right_on="id")
    events_df = events_df.drop("positions", axis=1)
    return events_df


def fix_wyscout_events(df_events):
    """
    This function does some fixes on the Wyscout events such that the 
    spadl action dataframe can be built

    Args:
    df_events (pd.DataFrame): Wyscout event dataframe

    Returns:
    pd.DataFrame: Wyscout event dataframe with an extra column 'offside'
    """
    df_events = create_shot_coordinates(df_events)
    df_events = convert_duels(df_events)
    df_events = insert_interception_passes(df_events)
    df_events = add_offside_variable(df_events)
    df_events = convert_touches(df_events)
    return df_events


def create_shot_coordinates(df_events):
    """
    This function creates shot coordinates (estimates) from the Wyscout tags

    Args:
    df_events (pd.DataFrame): Wyscout event dataframe

    Returns:
    pd.DataFrame: Wyscout event dataframe with end coordinates for shots
    """
    goal_center_idx = (
        df_events["position_goal_low_center"]
        | df_events["position_goal_mid_center"]
        | df_events["position_goal_high_center"]
    )
    df_events.loc[goal_center_idx, "end_x"] = 100.0
    df_events.loc[goal_center_idx, "end_y"] = 50.0

    goal_right_idx = (
        df_events["position_goal_low_right"]
        | df_events["position_goal_mid_right"]
        | df_events["position_goal_high_right"]
    )
    df_events.loc[goal_right_idx, "end_x"] = 100.0
    df_events.loc[goal_right_idx, "end_y"] = 55.0

    goal_left_idx = (
        df_events["position_goal_mid_left"]
        | df_events["position_goal_low_left"]
        | df_events["position_goal_high_left"]
    )
    df_events.loc[goal_left_idx, "end_x"] = 100.0
    df_events.loc[goal_left_idx, "end_y"] = 45.0

    out_center_idx = (
        df_events["position_out_high_center"] | df_events["position_post_high_center"]
    )
    df_events.loc[out_center_idx, "end_x"] = 100.0
    df_events.loc[out_center_idx, "end_y"] = 50.0

    out_right_idx = (
        df_events["position_out_low_right"]
        | df_events["position_out_mid_right"]
        | df_events["position_out_high_right"]
    )
    df_events.loc[out_right_idx, "end_x"] = 100.0
    df_events.loc[out_right_idx, "end_y"] = 60.0

    out_left_idx = (
        df_events["position_out_mid_left"]
        | df_events["position_out_low_left"]
        | df_events["position_out_high_left"]
    )
    df_events.loc[out_left_idx, "end_x"] = 100.0
    df_events.loc[out_left_idx, "end_y"] = 40.0

    post_left_idx = (
        df_events["position_post_mid_left"]
        | df_events["position_post_low_left"]
        | df_events["position_post_high_left"]
    )
    df_events.loc[post_left_idx, "end_x"] = 100.0
    df_events.loc[post_left_idx, "end_y"] = 55.38

    post_right_idx = (
        df_events["position_post_low_right"]
        | df_events["position_post_mid_right"]
        | df_events["position_post_high_right"]
    )
    df_events.loc[post_right_idx, "end_x"] = 100.0
    df_events.loc[post_right_idx, "end_y"] = 44.62

    blocked_idx = df_events["blocked"]
    df_events.loc[blocked_idx, "end_x"] = df_events.loc[blocked_idx, "start_x"]
    df_events.loc[blocked_idx, "end_y"] = df_events.loc[blocked_idx, "start_y"]

    return df_events


def convert_duels(df_events):
    """
    This function converts Wyscout duels that end with the ball out of field
    (subtype_id 50) into a pass for the player winning the duel to the location
    of where the ball went out of field. The remaining duels are removed as
    they are not on-the-ball actions.

    Args:
    df_events (pd.DataFrame): Wyscout event dataframe

    Returns:
    pd.DataFrame: Wyscout event dataframe in which the duels are either removed or transformed into a pass
    """

    # Shift events dataframe by one and two time steps
    df_events1 = df_events.shift(-1)
    df_events2 = df_events.shift(-2)

    # Define selector for same period id
    selector_same_period = df_events["period_id"] == df_events2["period_id"]

    # Define selector for duels that are followed by an 'out of field' event
    selector_duel_out_of_field = (
        (df_events["type_id"] == 1)
        & (df_events1["type_id"] == 1)
        & (df_events2["subtype_id"] == 50)
        & selector_same_period
    )

    # Define selectors for current time step
    selector0_duel_won = selector_duel_out_of_field & (
        df_events["team_id"] != df_events2["team_id"]
    )
    selector0_duel_won_air = selector0_duel_won & (df_events["subtype_id"] == 10)
    selector0_duel_won_not_air = selector0_duel_won & (df_events["subtype_id"] != 10)

    # Define selectors for next time step
    selector1_duel_won = selector_duel_out_of_field & (
        df_events1["team_id"] != df_events2["team_id"]
    )
    selector1_duel_won_air = selector1_duel_won & (df_events1["subtype_id"] == 10)
    selector1_duel_won_not_air = selector1_duel_won & (df_events1["subtype_id"] != 10)

    # Aggregate selectors
    selector_duel_won = selector0_duel_won | selector1_duel_won
    selector_duel_won_air = selector0_duel_won_air | selector1_duel_won_air
    selector_duel_won_not_air = selector0_duel_won_not_air | selector1_duel_won_not_air

    # Set types and subtypes
    df_events.loc[selector_duel_won, "type_id"] = 8
    df_events.loc[selector_duel_won_air, "subtype_id"] = 82
    df_events.loc[selector_duel_won_not_air, "subtype_id"] = 85

    # set end location equal to ball out of field location
    df_events.loc[selector_duel_won, "accurate"] = False
    df_events.loc[selector_duel_won, "not_accurate"] = True
    df_events.loc[selector_duel_won, "end_x"] = (
        100 - df_events2.loc[selector_duel_won, "start_x"]
    )
    df_events.loc[selector_duel_won, "end_y"] = (
        100 - df_events2.loc[selector_duel_won, "start_y"]
    )

    # df_events.loc[selector_duel_won, 'end_x'] = df_events2.loc[selector_duel_won, 'start_x']
    # df_events.loc[selector_duel_won, 'end_y'] = df_events2.loc[selector_duel_won, 'start_y']

    # Define selector for ground attacking duels with take on
    selector_attacking_duel = df_events["subtype_id"] == 11
    selector_take_on = (df_events["take_on_left"]) | (df_events["take_on_right"])
    selector_att_duel_take_on = selector_attacking_duel & selector_take_on

    # Set take ons type to 0
    df_events.loc[selector_att_duel_take_on, "type_id"] = 0

    # Set sliding tackles type to 0
    df_events.loc[df_events["sliding_tackle"], "type_id"] = 0

    # Remove the remaining duels
    df_events = df_events[df_events["type_id"] != 1]

    # Reset the index
    df_events = df_events.reset_index(drop=True)

    return df_events


def insert_interception_passes(df_events):
    """
    This function converts passes (type_id 8) that are also interceptions
    (tag interception) in the Wyscout event data into two separate events,
    first an interception and then a pass.

    Args:
    df_events (pd.DataFrame): Wyscout event dataframe

    Returns:
    pd.DataFrame: Wyscout event dataframe in which passes that were also denoted as interceptions in the Wyscout
    notation are transformed into two events
    """

    df_events_interceptions = df_events[
        df_events["interception"] & (df_events["type_id"] == 8)
    ].copy()

    if not df_events_interceptions.empty:
        df_events_interceptions["interception"] = True
        df_events_interceptions["type_id"] = 0
        df_events_interceptions["subtype_id"] = 0
        df_events_interceptions[["end_x", "end_y"]] = df_events_interceptions[
            ["start_x", "start_y"]
        ]

        df_events = pd.concat([df_events_interceptions, df_events], ignore_index=True)
        df_events = df_events.sort_values(["period_id", "milliseconds"])
        df_events = df_events.reset_index(drop=True)

    return df_events


def add_offside_variable(df_events):
    """
    This function removes the offside events in the Wyscout event data and adds
    sets offside to 1 for the previous event (if this was a passing event)

    Args:
    df_events (pd.DataFrame): Wyscout event dataframe

    Returns:
    pd.DataFrame: Wyscout event dataframe with an extra column 'offside'
    """

    # Create a new column for the offside variable
    df_events["offside"] = 0

    # Shift events dataframe by one timestep
    df_events1 = df_events.shift(-1)

    # Select offside passes
    selector_offside = (df_events1["type_id"] == 6) & (df_events["type_id"] == 8)

    # Set variable 'offside' to 1 for all offside passes
    df_events.loc[selector_offside, "offside"] = 1

    # Remove offside events
    df_events = df_events[df_events["type_id"] != 6]

    # Reset index
    df_events = df_events.reset_index(drop=True)

    return df_events


def convert_touches(df_events):
    """
    This function converts the Wyscout 'touch' event (sub_type_id 72) into either
    a dribble or a pass (accurate or not depending on receiver)

    Args:
    df_events (pd.DataFrame): Wyscout event dataframe

    Returns:
    pd.DataFrame: Wyscout event dataframe without any touch events
    """

    df_events1 = df_events.shift(-1)

    selector_touch = (df_events["subtype_id"] == 72) & ~df_events["interception"]

    selector_same_player = df_events["player_id"] == df_events1["player_id"]
    selector_same_team = df_events["team_id"] == df_events1["team_id"]

    selector_touch_same_player = selector_touch & selector_same_player
    selector_touch_same_team = (
        selector_touch & ~selector_same_player & selector_same_team
    )
    selector_touch_other = selector_touch & ~selector_same_player & ~selector_same_team

    same_x = abs(df_events["end_x"] - df_events1["start_x"]) < min_dribble_length
    same_y = abs(df_events["end_y"] - df_events1["start_y"]) < min_dribble_length
    same_loc = same_x & same_y

    # df_events.loc[selector_touch_same_player & same_loc, 'subtype_id'] = 70
    # df_events.loc[selector_touch_same_player & same_loc, 'accurate'] = True
    # df_events.loc[selector_touch_same_player & same_loc, 'not_accurate'] = False

    df_events.loc[selector_touch_same_team & same_loc, "type_id"] = 8
    df_events.loc[selector_touch_same_team & same_loc, "subtype_id"] = 85
    df_events.loc[selector_touch_same_team & same_loc, "accurate"] = True
    df_events.loc[selector_touch_same_team & same_loc, "not_accurate"] = False

    df_events.loc[selector_touch_other & same_loc, "type_id"] = 8
    df_events.loc[selector_touch_other & same_loc, "subtype_id"] = 85
    df_events.loc[selector_touch_other & same_loc, "accurate"] = False
    df_events.loc[selector_touch_other & same_loc, "not_accurate"] = True

    return df_events


def create_df_actions(df_events):
    """
    This function creates the SciSports action dataframe

    Args:
    df_events (pd.DataFrame): Wyscout event dataframe

    Returns:
    pd.DataFrame: SciSports action dataframe
    """
    df_events["time_seconds"] = df_events["milliseconds"] / 1000
    df_actions = df_events[
        [
            "game_id",
            "period_id",
            "time_seconds",
            "team_id",
            "player_id",
            "start_x",
            "start_y",
            "end_x",
            "end_y",
        ]
    ].copy()
    df_actions["bodypart_id"] = df_events.apply(determine_bodypart_id, axis=1)
    df_actions["type_id"] = df_events.apply(determine_type_id, axis=1)
    df_actions["result_id"] = df_events.apply(determine_result_id, axis=1)

    df_actions = remove_non_actions(df_actions)  # remove all non-actions left

    return df_actions


def determine_bodypart_id(event):
    """
    This function determines the body part used for an event

    Args:
    event (pd.Series): Wyscout event Series

    Returns:
    int: id of the body part used for the action
    """
    if event["subtype_id"] in [81, 36, 21, 90, 91]:
        body_part = "other"
    elif event["subtype_id"] == 82:  # or event['head_or_body']:
        body_part = "head"
    else:  # all other cases
        body_part = "foot"
    return bodyparts.index(body_part)


def determine_type_id(event):
    """
    This function transforms the Wyscout events, sub_events and tags
    into the corresponding SciSports action type

    Args:
    event (pd.Series): A series from the Wyscout event dataframe

    Returns:
    str: A string representing the SciSports action type
    """
    if event["type_id"] == 8:
        if event["subtype_id"] == 80:
            action_type = "cross"
        else:
            action_type = "pass"
    elif event["subtype_id"] == 36:
        action_type = "throw_in"
    elif event["subtype_id"] == 30:
        if event["high"]:
            action_type = "corner_crossed"
        else:
            action_type = "corner_short"
    elif event["subtype_id"] == 32:
        action_type = "freekick_crossed"
    elif event["subtype_id"] == 31:
        action_type = "freekick_short"
    elif event["subtype_id"] == 34:
        action_type = "goalkick"
    elif event["type_id"] == 2:
        action_type = "foul"
    elif event["type_id"] == 10:
        action_type = "shot"
    elif event["subtype_id"] == 35:
        action_type = "shot_penalty"
    elif event["subtype_id"] == 33:
        action_type = "shot_freekick"
    elif event["type_id"] == 9:
        action_type = "keeper_save"
    elif event["subtype_id"] == 71:
        action_type = "clearance"
    elif event["subtype_id"] == 72 and event["not_accurate"]:
        action_type = "bad_touch"
    elif event["subtype_id"] == 70:
        action_type = "dribble"
    elif event["take_on_left"] or event["take_on_right"]:
        action_type = "take_on"
    elif event["sliding_tackle"]:
        action_type = "tackle"
    elif event["interception"] and (event["subtype_id"] in [0, 10, 11, 12, 13, 72]):
        action_type = "interception"
    else:
        action_type = "non_action"
    return actiontypes.index(action_type)


def determine_result_id(event):
    """
    This function determines the result of an event

    Args:
    event (pd.Series): Wyscout event Series

    Returns:
    int: result of the action
    """
    if event["offside"] == 1:
        return 2
    elif event["type_id"] == 2:  # foul
        return 1
    elif event["goal"]:  # goal
        return 1
    elif event["own_goal"]:  # own goal
        return 3
    elif event["subtype_id"] in [100, 33, 35]:  # no goal, so 0
        return 0
    elif event["accurate"]:
        return 1
    elif event["not_accurate"]:
        return 0
    elif (
        event["interception"] or event["clearance"] or event["subtype_id"] == 71
    ):  # interception or clearance always success
        return 1
    elif event["type_id"] == 9:  # keeper save always success
        return 1
    else:
        # no idea, assume it was successful
        return 1


def remove_non_actions(df_actions):
    """
    This function removes the remaining non_actions from the action dataframe

    Args:
    df_actions (pd.DataFrame): SciSports action dataframe

    Returns:
    pd.DataFrame: SciSports action dataframe without non-actions
    """
    df_actions = df_actions[df_actions["type_id"] != actiontypes.index("non_action")]
    # remove remaining ball out of field, whistle and goalkeeper from line
    df_actions = df_actions.reset_index(drop=True)
    return df_actions


def fix_actions(df_actions):
    """
    This function fixes the generated actions

    Args:
    df_events (pd.DataFrame): Wyscout event dataframe

    Returns:
    pd.DataFrame: Wyscout event dataframe with end coordinates for shots
    """
    df_actions["start_x"] = df_actions["start_x"] * spadl_length / 100
    df_actions["start_y"] = (
        (100 - df_actions["start_y"]) * spadl_width / 100
    )  # y is from top to bottom in Wyscout
    df_actions["end_x"] = df_actions["end_x"] * spadl_length / 100
    df_actions["end_y"] = (
        (100 - df_actions["end_y"]) * spadl_width / 100
    )  # y is from top to bottom in Wyscout
    df_actions = fix_goalkick_coordinates(df_actions)
    df_actions = adjust_goalkick_result(df_actions)
    df_actions = fix_foul_coordinates(df_actions)
    df_actions = fix_keeper_save_coordinates(df_actions)
    df_actions = remove_keeper_goal_actions(df_actions)
    df_actions.reset_index(drop=True, inplace=True)

    return df_actions


def fix_goalkick_coordinates(df_actions):
    """
    This function sets the goalkick start coordinates to (5,34)

    Args:
    df_actions (pd.DataFrame): SciSports action dataframe with
    start coordinates for goalkicks in the corner of the pitch

    Returns:
    pd.DataFrame: SciSports action dataframe including start coordinates for goalkicks
    """
    goalkicks_idx = df_actions["type_id"] == actiontypes.index("goalkick")
    df_actions.loc[goalkicks_idx, "start_x"] = 5.0
    df_actions.loc[goalkicks_idx, "start_y"] = 34.0

    return df_actions


def fix_foul_coordinates(df_actions):
    """
    This function sets foul end coordinates equal to the foul start coordinates

    Args:
    df_actions (pd.DataFrame): SciSports action dataframe with no end coordinates for fouls

    Returns:
    pd.DataFrame: SciSports action dataframe including start coordinates for goalkicks
    """
    fouls_idx = df_actions["type_id"] == actiontypes.index("foul")
    df_actions.loc[fouls_idx, "end_x"] = df_actions.loc[fouls_idx, "start_x"]
    df_actions.loc[fouls_idx, "end_y"] = df_actions.loc[fouls_idx, "start_y"]

    return df_actions


def fix_keeper_save_coordinates(df_actions):
    """
    This function sets keeper_save start coordinates equal to
    keeper_save end coordinates. It also inverts the shot coordinates to the own goal.

    Args:
    df_actions (pd.DataFrame): SciSports action dataframe with start coordinates in the corner of the pitch

    Returns:
    pd.DataFrame: SciSports action dataframe with correct keeper_save coordinates
    """
    saves_idx = df_actions["type_id"] == actiontypes.index("keeper_save")
    # invert the coordinates
    df_actions.loc[saves_idx, "end_x"] = 105.0 - df_actions.loc[saves_idx, "end_x"]
    df_actions.loc[saves_idx, "end_y"] = 68.0 - df_actions.loc[saves_idx, "end_y"]
    # set start coordinates equal to start coordinates
    df_actions.loc[saves_idx, "start_x"] = df_actions.loc[saves_idx, "end_x"]
    df_actions.loc[saves_idx, "start_y"] = df_actions.loc[saves_idx, "end_y"]

    return df_actions


def remove_keeper_goal_actions(df_actions):
    """
    This function removes keeper_save actions that appear directly after a goal

    Args:
    df_actions (pd.DataFrame): SciSports action dataframe with keeper actions directly after a goal

    Returns:
    pd.DataFrame: SciSports action dataframe without keeper actions directly after a goal
    """
    prev_actions = df_actions.shift(1)
    same_phase = prev_actions.time_seconds + 10 > df_actions.time_seconds
    shot_goals = (prev_actions.type_id == actiontypes.index("shot")) & (
        prev_actions.result_id == 1
    )
    penalty_goals = (prev_actions.type_id == actiontypes.index("shot_penalty")) & (
        prev_actions.result_id == 1
    )
    freekick_goals = (prev_actions.type_id == actiontypes.index("shot_freekick")) & (
        prev_actions.result_id == 1
    )
    goals = shot_goals | penalty_goals | freekick_goals
    keeper_save = df_actions["type_id"] == actiontypes.index("keeper_save")
    goals_keepers_idx = same_phase & goals & keeper_save
    df_actions = df_actions.drop(df_actions.index[goals_keepers_idx])
    df_actions = df_actions.reset_index(drop=True)

    return df_actions


def adjust_goalkick_result(df_actions):
    """
    This function adjusts goalkick results depending on whether
    the next action is performed by the same team or not

    Args:
    df_actions (pd.DataFrame): SciSports action dataframe with incorrect goalkick results

    Returns:
    pd.DataFrame: SciSports action dataframe with correct goalkick results
    """
    nex_actions = df_actions.shift(-1)
    goalkicks = df_actions["type_id"] == actiontypes.index("goalkick")
    same_team = df_actions["team_id"] == nex_actions["team_id"]
    accurate = same_team & goalkicks
    not_accurate = ~same_team & goalkicks
    df_actions.loc[accurate, "result_id"] = 1
    df_actions.loc[not_accurate, "result_id"] = 0

    return df_actions


def fix_clearances(actions):
    next_actions = actions.shift(-1)
    next_actions[-1:] = actions[-1:]
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
    actions = actions.sort_values(["game_id", "period_id", "time_seconds"])
    actions.reset_index(drop=True, inplace=True)
    return actions
