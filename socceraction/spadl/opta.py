import tqdm # type: ignore
import json # type: ignore
import pandas as pd # type: ignore
import numpy as np # type: ignore
import unidecode # type: ignore


#####################################
# Convert opta json files to opta.h5
#####################################


def jsonfiles_to_h5(jsonfiles, h5file, append=True):

    eventtypesdf = pd.DataFrame(eventtypes, columns=["type_id", "type_name"])
    eventtypesdf.to_hdf(h5file, key="eventtypes")

    seen_files = set()
    if append:
        try:
            df = pd.read_hdf(h5file, key="files")
            seen_files = set(df.file_url)
        except KeyError:
            pass
    jsonfiles = set(jsonfiles) - seen_files

    d : dict = {
        key: []
        for key in [
            "games",
            "teams",
            "players",
            "referees",
            "teamgamestats",
            "playergamestats",
            "files",
        ]
    }
    with pd.HDFStore(h5file) as optastore:
        for jsonfile_url in tqdm.tqdm(jsonfiles):
            try:
                data = extract_data(jsonfile_url)

                d["games"] += [data["game"]]
                d["players"] += data["players"]
                d["teams"] += data["teams"]
                d["referees"] += [data["referee"]]
                d["teamgamestats"] += data["teamgamestats"]
                d["playergamestats"] += data["playergamestats"]

                game_id = data["game"]["game_id"]
                key = f"events/game_{game_id}"
                eventsdf = pd.DataFrame(data["events"])
                eventsdf["timestamp"] = pd.to_datetime(eventsdf["timestamp"])
                optastore[key] = eventsdf
                d["files"] += [{"file_url": jsonfile_url, "corrupt": False}]
            except (ValueError, MissingDataError):
                d["files"] += [{"file_url": jsonfile_url, "corrupt": True}]

        deduplic = dict(
            games=("game_id", "game_id"),
            teams=(["team_id", "team_name", "team_short", "team_abbr"], "team_id"),
            players=(
                ["player_id", "firstname", "lastname", "fullname", "knownname"],
                "player_id",
            ),
            referees=(["referee_firstname", "referee_lastname"], "referee_id"),
            teamgamestats=(["game_id", "team_id"], ["game_id", "team_id"]),
            playergamestats=(
                ["game_id", "team_id", "player_id"],
                ["game_id", "team_id", "player_id"],
            ),
            files=("file_url", "file_url"),
        )

        for k, v in d.items():
            d[k] = pd.DataFrame(v)
        d["games"]["game_date"] = pd.to_datetime(d["games"]["game_date"])

        for k, df in d.items():
            if append:
                try:
                    ori_df = optastore[k]
                    df = pd.concat([ori_df, df])
                except (FileNotFoundError, KeyError):
                    pass
            sortcols, idcols = deduplic[k]
            df.sort_values(by=sortcols, ascending=False, inplace=True)
            df.drop_duplicates(subset=idcols, inplace=True)
            optastore[k] = df


def extract_data(jsonfile):
    with open(jsonfile, encoding="utf-8") as fh:
        root = json.load(fh)

    return {
        "game": extract_game(root),
        "players": extract_players(root),
        "teams": extract_teams(root),
        "referee": extract_referee(root),
        "teamgamestats": extract_teamgamestats(root),
        "playergamestats": extract_playergamestats(root),
        "events": extract_events(root),
    }


class MissingDataError(Exception):
    pass


def get_f24feed(root):
    for node in root:
        if "Games" in node["data"].keys():
            return node
    raise MissingDataError


def get_f9feed(root):
    for node in root:
        if "OptaFeed" in node["data"].keys():
            return node
    raise MissingDataError


def get_feeds(root):
    f24 = get_f24feed(root)
    f9 = get_f9feed(root)
    assert f9 != f24, str(root)

    return f24, f9


def assertget(dictionary, key):
    value = dictionary.get(key)
    assert value is not None, "KeyError: " + key + " not found in " + str(dictionary)
    return value


def extract_game(root):
    f24 = get_f24feed(root)

    data = assertget(f24, "data")
    games = assertget(data, "Games")
    game = assertget(games, "Game")
    attr = assertget(game, "@attributes")

    f9 = get_f9feed(root)

    data = assertget(f9, "data")
    optafeed = assertget(data, "OptaFeed")
    optadocument = assertget(optafeed, "OptaDocument")[0]
    venue = assertget(optadocument, "Venue")
    matchdata = assertget(optadocument, "MatchData")
    matchofficial = assertget(matchdata, "MatchOfficial")
    matchinfo = assertget(matchdata, "MatchInfo")
    stat = assertget(matchdata, "Stat")
    assert stat["@attributes"]["Type"] == "match_time"

    game_dict = dict(
        competition_id=int(assertget(attr, "competition_id")),
        game_id=int(assertget(attr, "id")),
        season_id=int(assertget(attr, "season_id")),
        matchday=int(assertget(attr, "matchday")),
        home_team_id=int(assertget(attr, "home_team_id")),
        away_team_id=int(assertget(attr, "away_team_id")),
        venue_id=int(venue["@attributes"]["uID"].replace("v", "")),
        referee_id=int(matchofficial["@attributes"]["uID"].replace("o", "")),
        game_date=pd.to_datetime(assertget(matchinfo, "Date")),
        attendance=int(matchinfo.get("Attendance", 0)),
        duration=int(stat["@value"]),
    )
    return game_dict


def extract_players(root):
    f9 = get_f9feed(root)
    teams = f9["data"]["OptaFeed"]["OptaDocument"][0]["Team"]

    players = []
    for team in teams:
        for player in team["Player"]:
            player_id = int(player["@attributes"]["uID"].replace("p", ""))

            assert "nameObj" in player["PersonName"]
            nameobj = player["PersonName"]["nameObj"]
            if not nameobj.get("is_unknown"):
                player = dict(
                    player_id=player_id,
                    firstname=nameobj.get("first").strip() or None,
                    lastname=nameobj.get("last").strip() or None,
                    fullname=nameobj.get("full").strip() or None,
                    knownname=nameobj.get("known")
                    or nameobj.get("full").strip()
                    or None,
                )
                for f in ["firstname", "lastname", "fullname", "knownname"]:
                    if player[f]:
                        player[f] = unidecode.unidecode(player[f])
                players.append(player)
    return players


def extract_teams(root):
    f9 = get_f9feed(root)
    rootf9 = f9["data"]["OptaFeed"]["OptaDocument"][0]["Team"]

    teams = []
    for team in rootf9:
        if "id" in team.keys():
            nameobj = team.get("nameObj")
            team = dict(
                team_id=int(team["id"]),
                team_name=nameobj.get("name"),
                team_short=nameobj.get("short"),
                team_abbr=nameobj.get("abbr"),
            )
            for f in ["team_name", "team_short", "team_abbr"]:
                if team[f]:
                    team[f] = unidecode.unidecode(team[f])

            teams.append(team)
    return teams


def extract_referee(root):
    f9 = get_f9feed(root)

    rootf9 = f9["data"]["OptaFeed"]["OptaDocument"][0]["MatchData"]["MatchOfficial"]
    name = rootf9["OfficialName"]
    nameobj = name["nameObj"]
    referee = dict(
        referee_id=int(rootf9["@attributes"]["uID"].replace("o", "")),
        referee_firstname=name.get("First") or nameobj.get("first"),
        referee_lastname=name.get("Last") or nameobj.get("last"),
    )
    for f in ["referee_firstname", "referee_lastname"]:
        if referee[f]:
            referee[f] = unidecode.unidecode(referee[f])
    return referee


def extract_teamgamestats(root):
    game_id = extract_game(root)["game_id"]

    f9 = get_f9feed(root)

    rootf9 = f9["data"]["OptaFeed"]["OptaDocument"][0]["MatchData"]["TeamData"]
    teams_gamestats = []
    for team in rootf9:
        attr = team["@attributes"]
        statsdict = {
            stat["@attributes"]["Type"]: stat["@value"] for stat in team["Stat"]
        }

        team_gamestats = dict(
            game_id=game_id,
            team_id=int(attr["TeamRef"].replace("t", "")),
            side=attr["Side"],
            score=attr["Score"],
            shootout_score=attr["ShootOutScore"],
            **statsdict,
        )

        teams_gamestats.append(team_gamestats)
    return teams_gamestats


def extract_playergamestats(root):
    f9 = get_f9feed(root)
    rootf9 = f9["data"]["OptaFeed"]["OptaDocument"][0]["MatchData"]["TeamData"]
    game_id = extract_game(root)["game_id"]

    players_gamestats = []
    for team in rootf9:
        team_id = int(team["@attributes"]["TeamRef"].replace("t", ""))
        for player in team["PlayerLineUp"]["MatchPlayer"]:
            attr = player["@attributes"]
            statsdict = {
                stat["@attributes"]["Type"]: stat["@value"] for stat in player["Stat"]
            }

            p = dict(
                game_id=game_id,
                team_id=team_id,
                player_id=int(attr["PlayerRef"].replace("p", "")),
                shirtnumber=attr["ShirtNumber"],
                player_type=attr["Position"],
                status=attr["Status"],
                **statsdict,
            )

            players_gamestats.append(p)
    return players_gamestats


def extract_events(root):
    f24 = get_f24feed(root)

    game_id = extract_game(root)["game_id"]

    data = assertget(f24, "data")
    games = assertget(data, "Games")
    game = assertget(games, "Game")

    events = []
    for element in assertget(game, "Event"):
        attr = element["@attributes"]
        timestamp = attr["TimeStamp"].get("locale") if attr.get("TimeStamp") else None
        qualifiers = {}
        qualifiers = {
            int(q["@attributes"]["qualifier_id"]): q["@attributes"]["value"]
            for q in element.get("Q", [])
        }
        start_x = float(assertget(attr, "x"))
        start_y = float(assertget(attr, "y"))
        end_x = get_end_x(qualifiers)
        end_y = get_end_y(qualifiers)
        if end_x is None:
            end_x = start_x
        if end_y is None:
            end_y = start_y

        event = dict(
            game_id=game_id,
            event_id=int(assertget(attr, "event_id")),
            type_id=int(assertget(attr, "type_id")),
            period_id=int(assertget(attr, "period_id")),
            minute=int(assertget(attr, "min")),
            second=int(assertget(attr, "sec")),
            timestamp=timestamp,
            player_id=int(assertget(attr, "player_id")),
            team_id=int(assertget(attr, "team_id")),
            outcome=bool(int(attr.get("outcome", 1))),
            start_x=start_x,
            start_y=start_y,
            end_x=end_x,
            end_y=end_y,
            assist=bool(int(attr.get("assist", 0))),
            keypass=bool(int(attr.get("keypass", 0))),
            qualifiers=qualifiers,
        )
        events.append(event)
    return events


def get_end_x(qualifiers):
    try:
        # pass
        if 140 in qualifiers:
            return float(qualifiers[140])
        # blocked shot
        elif 146 in qualifiers:
            return float(qualifiers[146])
        # passed the goal line
        elif 102 in qualifiers:
            return float(100)
        else:
            return None
    except ValueError:
        return None


def get_end_y(qualifiers):
    try:
        # pass
        if 141 in qualifiers:
            return float(qualifiers[141])
        # blocked shot
        elif 147 in qualifiers:
            return float(qualifiers[147])
        # passed the goal line
        elif 102 in qualifiers:
            return float(qualifiers[102])
        else:
            return None
    except ValueError:
        return None


eventtypes = [
    (1, "pass"),
    (2, "offside pass"),
    (3, "take on"),
    (4, "foul"),
    (5, "out"),
    (6, "corner awarded"),
    (7, "tackle"),
    (8, "interception"),
    (9, "turnover"),
    (10, "save"),
    (11, "claim"),
    (12, "clearance"),
    (13, "miss"),
    (14, "post"),
    (15, "attempt saved"),
    (16, "goal"),
    (17, "card"),
    (18, "player off"),
    (19, "player on"),
    (20, "player retired"),
    (21, "player returns"),
    (22, "player becomes goalkeeper"),
    (23, "goalkeeper becomes player"),
    (24, "condition change"),
    (25, "official change"),
    (26, "unknown26"),
    (27, "start delay"),
    (28, "end delay"),
    (29, "unknown29"),
    (30, "end"),
    (31, "unknown31"),
    (32, "start"),
    (33, "unknown33"),
    (34, "team set up"),
    (35, "player changed position"),
    (36, "player changed jersey number"),
    (37, "collection end"),
    (38, "temp_goal"),
    (39, "temp_attempt"),
    (40, "formation change"),
    (41, "punch"),
    (42, "good skill"),
    (43, "deleted event"),
    (44, "aerial"),
    (45, "challenge"),
    (46, "unknown46"),
    (47, "rescinded card"),
    (48, "unknown46"),
    (49, "ball recovery"),
    (50, "dispossessed"),
    (51, "error"),
    (52, "keeper pick-up"),
    (53, "cross not claimed"),
    (54, "smother"),
    (55, "offside provoked"),
    (56, "shield ball opp"),
    (57, "foul throw in"),
    (58, "penalty faced"),
    (59, "keeper sweeper"),
    (60, "chance missed"),
    (61, "ball touch"),
    (62, "unknown62"),
    (63, "temp_save"),
    (64, "resume"),
    (65, "contentious referee decision"),
    (66, "possession data"),
    (67, "50/50"),
    (68, "referee drop ball"),
    (69, "failed to block"),
    (70, "injury time announcement"),
    (71, "coach setup"),
    (72, "caught offside"),
    (73, "other ball contact"),
    (74, "blocked pass"),
    (75, "delayed start"),
    (76, "early end"),
    (77, "player off pitch"),
]


###############################
# Convert opta.h5 to spadl.h5
##############################

import socceraction.spadl.config as spadlconfig

spadl_length = spadlconfig.field_length
spadl_width = spadlconfig.field_width

bodyparts = spadlconfig.bodyparts
results = spadlconfig.results
actiontypes = spadlconfig.actiontypes


def convert_to_spadl(optah5, spadlh5):

    with pd.HDFStore(optah5) as optastore, pd.HDFStore(spadlh5) as spadlstore:
        games = optastore["games"]
        spadlstore["games"] = games

        players = optastore["players"]
        players = players.rename(
            index=str,
            columns={
                "firstname": "first_name",
                "lastname": "last_name",
                "knownname": "soccer_name",
            },
        )
        # players["birthday"] = pd.NaT  # unavailabe
        # players["nation_id"] = np.nan  # unavailable
        spadlstore["players"] = players

        teams = optastore["teams"]
        spadlstore["teams"] = teams

        teamgames = optastore["teamgamestats"]
        teamgames = teamgames.rename(index=str, columns={"formation_used": "formation"})
        spadlstore["teamgames"] = teamgames

        playergames = optastore["playergamestats"]
        playergames = playergames.rename(
            index=str,
            columns={
                "mins_played": "minutes_played",
                "goal_assists": "assists",
                "total_att_assist": "keypasses",
                "second_goal_assist": "pre_assists",
            },
        )
        spadlstore["playergames"] = playergames

        spadlstore["actiontypes"] = pd.DataFrame(
            list(enumerate(actiontypes)), columns=["type_id", "type_name"]
        )

        spadlstore["bodyparts"] = pd.DataFrame(
            list(enumerate(bodyparts)), columns=["bodypart_id", "bodypart_name"]
        )

        spadlstore["results"] = pd.DataFrame(
            list(enumerate(results)), columns=["result_id", "result_name"]
        )

        eventtypes = optastore["eventtypes"]
        for game in tqdm.tqdm(list(games.itertuples())):

            events = optastore[f"events/game_{game.game_id}"]
            events = (
                events.merge(eventtypes, on="type_id", how="left")
                .sort_values(["game_id", "period_id", "minute", "second", "timestamp"])
                .reset_index(drop=True)
            )
            actions = convert_to_actions(events, home_team_id=game.home_team_id)
            spadlstore[f"actions/game_{game.game_id}"] = actions


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
    actions["action_id"] = range(len(actions))
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
    # not_clearance = actions.type_id != actiontypes.index("clearance")

    dx = actions.end_x - next_actions.start_x
    dy = actions.end_y - next_actions.start_y
    far_enough = dx ** 2 + dy ** 2 >= min_dribble_length ** 2
    not_too_far = dx ** 2 + dy ** 2 <= max_dribble_length ** 2

    dt = next_actions.time_seconds - actions.time_seconds
    same_phase = dt < max_dribble_duration
    same_period = actions.period_id == next_actions.period_id

    dribble_idx = same_team & far_enough & not_too_far & same_phase & same_period

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
    dribbles["result_id"] = results.index("success")

    actions = pd.concat([actions, dribbles], ignore_index=True, sort=False)
    actions = actions.sort_values(["game_id", "period_id", "action_id"]).reset_index(
        drop=True
    )
    actions["action_id"] = range(len(actions))
    return actions


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
