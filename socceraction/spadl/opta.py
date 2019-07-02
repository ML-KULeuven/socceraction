import tqdm
import ujson as json
import pandas as pd
import numpy as np
import unidecode


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

    d = {
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
            eventsdf.to_hdf(h5file, key=key)
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

    for k,v in d.items():
        d[k] = pd.DataFrame(v)
    d["games"]["game_date"] = pd.to_datetime(d["games"]["game_date"])

    for k, df in d.items():
        if append:
            try:
                ori_df = pd.read_hdf(h5file, key=k)
                df = pd.concat([ori_df, df])
            except (FileNotFoundError, KeyError):
                pass
        sortcols, idcols = deduplic[k]
        df.sort_values(by=sortcols, ascending=False, inplace=True)
        df.drop_duplicates(subset=idcols, inplace=True)
        df.to_hdf(h5file, key=k)


def extract_data(jsonfile):
    with open(jsonfile) as fh:
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
        attendance=int(matchinfo.get("Attendance",0)),
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


