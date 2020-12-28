import glob
import copy
import json  # type: ignore
import os
import warnings
from abc import ABC
from collections import defaultdict
from datetime import datetime
from typing import Dict, List

import pandas as pd  # type: ignore
import pandera as pa
import tqdm  # type: ignore
import unidecode  # type: ignore
from lxml import objectify
from pandera.typing import Series

from . import config as spadlconfig
from .base import (CompetitionSchema, EventDataLoader, EventSchema, GameSchema,
                   MissingDataError, PlayerSchema, TeamSchema, _add_dribbles,
                   _fix_clearances, _fix_direction_of_play)


__all__ = ['OptaLoader', 'convert_to_actions']


class OptaCompetitionSchema(CompetitionSchema):
    pass


class OptaGameSchema(GameSchema):
    venue_id: Series[int] = pa.Field(nullable=True)
    referee_id: Series[int] = pa.Field(nullable=True)
    attendance: Series[int] = pa.Field(nullable=True)
    duration: Series[int]
    home_score: Series[int]
    away_score: Series[int]


class OptaPlayerSchema(PlayerSchema):
    firstname: Series[str]
    lastname: Series[str]
    nickname: Series[str] = pa.Field(nullable=True)
    starting_position_id: Series[int]
    starting_position_name: Series[str]


class OptaTeamSchema(TeamSchema):
    pass


class OptaEventSchema(EventSchema):
    timestamp: Series[str]
    minute: Series[int]
    second: Series[int]
    outcome: Series[bool]
    start_x: Series[float] = pa.Field(nullable=True)
    start_y: Series[float] = pa.Field(nullable=True)
    end_x: Series[float] = pa.Field(nullable=True)
    end_y: Series[float] = pa.Field(nullable=True)
    assist: Series[bool] = pa.Field(nullable=True)
    keypass: Series[bool] = pa.Field(nullable=True)
    qualifiers: Series[object]


def _deepupdate(target, src):
    """Deep update target dict with src
    For each k,v in src: if k doesn't exist in target, it is deep copied from
    src to target. Otherwise, if v is a list, target[k] is extended with
    src[k]. If v is a set, target[k] is updated with v, If v is a dict,
    recursively deep-update it.

    Examples:
    >>> t = {'name': 'Ferry', 'hobbies': ['programming', 'sci-fi']}
    >>> deepupdate(t, {'hobbies': ['gaming']})
    >>> print(t)
    {'name': 'Ferry', 'hobbies': ['programming', 'sci-fi', 'gaming']}
    """
    for k, v in src.items():
        if type(v) == list:
            if not k in target:
                target[k] = copy.deepcopy(v)
            else:
                target[k].extend(v)
        elif type(v) == dict:
            if not k in target:
                target[k] = copy.deepcopy(v)
            else:
                _deepupdate(target[k], v)
        elif type(v) == set:
            if not k in target:
                target[k] = v.copy()
            else:
                target[k].update(v.copy())
        else:
            target[k] = copy.copy(v)


class OptaLoader(EventDataLoader):
    """
    Load Opta data from a local folder.

    Parameters
    ----------
    root : str
        Root-path of the data.
    feeds : dict
        Glob pattern for each feed that should be parsed. For example:

            {
                'f7': "f7-{competition_id}-{season_id}-{game_id}.xml",
                'f24': "f24-{competition_id}-{season_id}-{game_id}.xml"
            }
    parser : str or dict
        Either 'xml', 'json' or your custom parser for each feed.
        The default xml parser supports F7 and F24 feeds; the default json
        parser supports F1, F9 and F24 feeds. Custom parsers can be specified
        as
            {
                'f7': F7Parser
                'f24': F24Parser
            }
        where F7Parser and F24Parser are classes implementing :class:`OptaParser`.
 
    """

    def __init__(self, root: str, feeds: dict, parser):
        self.root = root
        if parser == "json":
            self.parsers = self._get_parsers_for_feeds(_jsonparsers, feeds)
        elif parser == "xml":
            self.parsers = self._get_parsers_for_feeds(_xmlparsers, feeds)
        else:
            self.parsers = self._get_parsers_for_feeds(parser, feeds)
        self.feeds = feeds

    def _get_parsers_for_feeds(self, available_parsers, feeds):
        """
        Selects the appropriate parser for each feed and raises a warning if
        there is no parser available for any of the provided feeds.
        """
        parsers = {}
        for feed, _ in feeds.items():
            if feed in available_parsers:
                parsers[feed] = available_parsers[feed]
            else:
                warnings.warn("No parser available for {} feeds. This feed is ignored.".format(feed))
        return parsers

    def competitions(self) -> pd.DataFrame:
        data = {}
        for feed in self.feeds.keys():
            parser = self.parsers[feed]
            glob_pattern = self.feeds[feed].format(competition_id='*', season_id='*', game_id='*')
            feed_files = glob.glob(os.path.join(self.root, glob_pattern))
            for ffp in feed_files:
                _deepupdate(data, parser.extract_competitions(ffp))
        return pd.DataFrame(list(data.values()))

    def games(self, competition_id: int, season_id: int) -> pd.DataFrame:
        data = {}
        for feed in self.feeds.keys():
            parser = self.parsers[feed]
            glob_pattern = self.feeds[feed].format(competition_id=competition_id, season_id=season_id, game_id='*')
            feed_files = glob.glob(os.path.join(self.root, glob_pattern))
            for ffp in feed_files:
                _deepupdate(data, parser.extract_games(ffp))
        return pd.DataFrame(list(data.values()))

    def teams(self, game_id: int) -> pd.DataFrame:
        data = {}
        for feed in self.feeds.keys():
            parser = self.parsers[feed]
            glob_pattern = self.feeds[feed].format(competition_id='*', season_id='*', game_id=game_id)
            feed_files = glob.glob(os.path.join(self.root, glob_pattern))
            for ffp in feed_files:
                _deepupdate(data, parser.extract_teams(ffp))
        return pd.DataFrame(list(data.values()))

    def players(self, game_id: int) -> pd.DataFrame:
        data = {}
        for feed in self.feeds.keys():
            parser = self.parsers[feed]
            glob_pattern = self.feeds[feed].format(competition_id='*', season_id='*', game_id=game_id)
            feed_files = glob.glob(os.path.join(self.root, glob_pattern))
            for ffp in feed_files:
                _deepupdate(data, parser.extract_players(ffp))
        df_players = pd.DataFrame(list(data.values()))
        df_players["game_id"] = game_id
        return df_players

    def events(self, game_id: int) -> pd.DataFrame:
        data = {}
        for feed in self.feeds.keys():
            parser = self.parsers[feed]
            glob_pattern = self.feeds[feed].format(competition_id='*', season_id='*', game_id=game_id)
            feed_files = glob.glob(os.path.join(self.root, glob_pattern))
            for ffp in feed_files:
                _deepupdate(data, parser.extract_events(ffp))
        events = (
                pd.DataFrame(list(data.values()))
                .merge(_eventtypesdf, on="type_id", how="left")
                .sort_values(["game_id", "period_id", "minute", "second", "timestamp"])
                .reset_index(drop=True)
                )
        return events


class OptaParser(ABC):

    @staticmethod
    def extract_competitions(root):
        return {}

    @staticmethod
    def extract_games(root):
        return {}

    @staticmethod
    def extract_teams(root):
        return {}

    @staticmethod
    def extract_players(root):
        return {}

    @staticmethod
    def extract_events(root):
        return {}


class OptaJSONParser(OptaParser):

    @staticmethod
    def get(path: str) -> List[Dict]:
        with open(path, "rt", encoding="utf-8") as fh:
            return json.load(fh)


class OptaXMLParser(OptaParser):

    @staticmethod
    def get(path: str) -> List[Dict]:
        with open(path, "rb") as fh:
            return objectify.fromstring(fh.read())


class _F1JSONParser(OptaJSONParser):

    @staticmethod
    def get_feed(path):
        root = _F1JSONParser.get(path)
        for node in root:
            if "OptaFeed" in node["data"].keys():
                return node
        raise MissingDataError

    @staticmethod
    def get_doc(root):
        f1 = _F1JSONParser.get_feed(root)
        data = assertget(f1, "data")
        optafeed = assertget(data, "OptaFeed")
        optadocument = assertget(optafeed, "OptaDocument")
        return optadocument

    @staticmethod
    def extract_competitions(root):
        optadocument = _F1JSONParser.get_doc(root)
        attr = assertget(optadocument, "@attributes")
        competition_id = int(assertget(attr, "competition_id"))
        competition = dict(
                season_id=int(assertget(attr, "season_id")),
                season_name=str(assertget(attr, "season_id")),
                competition_id=competition_id,
                competition_name=assertget(attr, "competition_name")
                )
        return {competition_id: competition}

    @staticmethod
    def extract_games(root):
        optadocument = _F1JSONParser.get_doc(root)
        attr = assertget(optadocument, "@attributes")
        matchdata = assertget(optadocument, "MatchData")
        matches = {}
        for match in matchdata:
            match_dict = dict(
                competition_id=int(assertget(attr, "competition_id")),
                season_id=int(assertget(attr, "season_id")))
            matchattr = assertget(match, "@attributes")
            match_dict["game_id"] = int(assertget(matchattr, "uID")[1:])
            matchinfo = assertget(match, "MatchInfo")
            matchinfoattr = assertget(matchinfo, "@attributes")
            match_dict["game_day"] = int(assertget(matchinfoattr, "MatchDay"))
            match_dict["venue_id"] = int(assertget(matchinfoattr, "Venue_id"))
            match_dict["game_date"] = datetime.strptime(assertget(matchinfo, "Date"), '%Y-%m-%d %H:%M:%S')
            teamdata = assertget(match, "TeamData")
            for team in teamdata:
                teamattr = assertget(team, "@attributes")
                side = assertget(teamattr, "Side")
                teamid = assertget(teamattr, "TeamRef")
                if side == "Home":
                    match_dict["home_team_id"] = int(teamid[1:])
                else:
                    match_dict["away_team_id"] = int(teamid[1:])
            matches[match_dict["game_id"]] = match_dict
        return matches



class _F9JSONParser(OptaJSONParser):

    @staticmethod
    def get_feed(path):
        root = _F9JSONParser.get(path)
        for node in root:
            if "OptaFeed" in node["data"].keys():
                return node
        raise MissingDataError

    @staticmethod
    def get_doc(root):
        f9 = _F9JSONParser.get_feed(root)
        data = assertget(f9, "data")
        optafeed = assertget(data, "OptaFeed")
        optadocument = assertget(optafeed, "OptaDocument")[0]
        return optadocument

    @staticmethod
    def extract_games(root):
        optadocument = _F9JSONParser.get_doc(root)
        attr = assertget(optadocument, "@attributes")
        venue = assertget(optadocument, "Venue")
        matchdata = assertget(optadocument, "MatchData")
        matchofficial = assertget(matchdata, "MatchOfficial")
        matchinfo = assertget(matchdata, "MatchInfo")
        stat = assertget(matchdata, "Stat")
        assert stat["@attributes"]["Type"] == "match_time"
        teamdata = assertget(matchdata, "TeamData")
        scores = {}
        for t in teamdata:
            scores[t["@attributes"]["Side"]] = t["@attributes"]["Score"]

        game_id = int(assertget(attr,"uID")[1:])
        game_dict = {game_id: dict(
            game_id=game_id,
            venue_id=int(venue["@attributes"]["uID"].replace("v", "")),
            referee_id=int(matchofficial["@attributes"]["uID"].replace("o", "")),
            game_date=datetime.strptime(assertget(matchinfo, "Date"), '%Y%m%dT%H%M%S%z').replace(tzinfo=None),
            attendance=int(matchinfo.get("Attendance", 0)),
            duration=int(stat["@value"]),
            home_score=int(scores["Home"]),
            away_score=int(scores["Away"]),
        )}
        return game_dict


    @staticmethod
    def extract_teams(root):
        optadocument = _F9JSONParser.get_doc(root)
        root_teams = assertget(optadocument, "Team")

        teams = {}
        for team in root_teams:
            if "id" in team.keys():
                nameobj = team.get("nameObj")
                team_id=int(team["id"])
                team = dict(
                        team_id=team_id,
                        team_name=nameobj.get("name"),
                        )
                for f in ["team_name"]:
                    team[f] = unidecode.unidecode(team[f]) if f in team else team[f]
                teams[team_id] = team
        return teams

    @staticmethod
    def extract_players(root):
        optadocument = _F9JSONParser.get_doc(root)
        root_teams = assertget(optadocument, "Team")
        lineups =_F9JSONParser.extract_lineups(root)

        players = {}
        for team in root_teams:
            team_id = int(team["@attributes"]["uID"].replace("t", ""))
            for player in team["Player"]:
                player_id = int(player["@attributes"]["uID"].replace("p", ""))

                assert "nameObj" in player["PersonName"]
                nameobj = player["PersonName"]["nameObj"]
                if not nameobj.get("is_unknown"):
                    player = dict(
                            team_id=team_id,
                            player_id=player_id,
                            firstname=nameobj.get("first").strip() or None,
                            lastname=nameobj.get("last").strip() or None,
                            player_name=nameobj.get("full").strip() or None,
                            nickname=nameobj.get("known")
                            or nameobj.get("full").strip()
                            or None,
                            )
                    if player_id in lineups[team_id]['players']:
                        player = dict(
                            **player,
                            jersey_number=lineups[team_id]['players'][player_id]['jersey_number'],
                            starting_position_name=lineups[team_id]['players'][player_id]['starting_position_name'],
                            starting_position_id=lineups[team_id]['players'][player_id]['starting_position_id'],
                            is_starter=lineups[team_id]['players'][player_id]['is_starter'],
                            minutes_played=lineups[team_id]['players'][player_id]['minutes_played'],
                            )
                    for f in ["firstname", "lastname", "player_name", "nickname"]:
                        if player[f]:
                            player[f] = unidecode.unidecode(player[f])
                    players[player_id] = player
        return players

    @staticmethod
    def extract_referee(root):
        optadocument = _F9JSONParser.get_doc(root)

        try:
            rootf9 = optadocument["MatchData"]["MatchOfficial"]
        except KeyError:
            raise MissingDataError

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

    @staticmethod
    def extract_teamgamestats(root):
        optadocument = _F9JSONParser.get_doc(root)
        attr = assertget(optadocument, "@attributes")
        game_id = int(assertget(attr,"uID")[1:])

        try:
            rootf9 = optadocument["MatchData"]["TeamData"]
        except KeyError:
            raise MissingDataError
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

    @staticmethod
    def extract_lineups(root):
        optadocument = _F9JSONParser.get_doc(root)
        attr = assertget(optadocument, "@attributes")
        game_id = int(assertget(attr,"uID")[1:])

        try:
            rootf9 = optadocument["MatchData"]["TeamData"]
        except KeyError:
            raise MissingDataError
        matchstats = optadocument["MatchData"]["Stat"]
        matchstats = [matchstats] if isinstance(matchstats, dict) else matchstats
        matchstatsdict = {
                stat["@attributes"]["Type"]: stat["@value"] for stat in matchstats
                }

        lineups = {}
        for team in rootf9:
            # lineup attributes
            team_id = int(team["@attributes"]["TeamRef"].replace("t", ""))
            lineups[team_id] = dict(
                    players=dict())
            #substitutes
            subst = [s["@attributes"] for s in team["Substitution"]]
            for player in team["PlayerLineUp"]["MatchPlayer"]:
                attr = player["@attributes"]
                player_id=int(attr["PlayerRef"].replace("p", ""))
                playerstatsdict = {
                    stat["@attributes"]["Type"]: stat["@value"] for stat in player["Stat"]
                }
                sub_on = next((item["Time"] for item in subst if item["SubOn"] == f"p{player_id}"), 0)
                sub_off = next((item["Time"] for item in subst if item["SubOff"] == f"p{player_id}"), matchstatsdict["match_time"])
                minutes_played = sub_off - sub_on
                lineups[team_id]["players"][player_id] = dict(
                    jersey_number=attr["ShirtNumber"],
                    starting_position_name=attr["Position"],
                    starting_position_id=attr["position_id"],
                    is_starter=attr["Status"] == 'Start',
                    minutes_played=minutes_played,
                    **playerstatsdict,
                )
        return lineups


class _F24JSONParser(OptaJSONParser):

    @staticmethod
    def get_feed(path):
        root = _F24JSONParser.get(path)
        for node in root:
            if "Games" in node["data"].keys():
                return node
        raise MissingDataError

    @staticmethod
    def extract_games(root):
        f24 = _F24JSONParser.get_feed(root)

        data = assertget(f24, "data")
        games = assertget(data, "Games")
        game = assertget(games, "Game")
        attr = assertget(game, "@attributes")

        game_id = int(assertget(attr, "id"))
        game_dict = {game_id: dict(
            competition_id=int(assertget(attr, "competition_id")),
            game_id=game_id,
            season_id=int(assertget(attr, "season_id")),
            game_day=int(assertget(attr, "matchday")),
            home_team_id=int(assertget(attr, "home_team_id")),
            away_team_id=int(assertget(attr, "away_team_id")),
        )}
        return game_dict

    @staticmethod
    def extract_events(root):
        f24 = _F24JSONParser.get_feed(root)

        data = assertget(f24, "data")
        games = assertget(data, "Games")
        game = assertget(games, "Game")
        game_attr = assertget(game, "@attributes")
        game_id = int(assertget(game_attr, "id"))

        events = {}
        for element in assertget(game, "Event"):
            attr = element["@attributes"]
            timestamp = attr["TimeStamp"].get("locale") if attr.get("TimeStamp") else None
            qualifiers = {
                int(q["@attributes"]["qualifier_id"]): q["@attributes"]["value"]
                for q in element.get("Q", [])
            }
            start_x = float(assertget(attr, "x"))
            start_y = float(assertget(attr, "y"))
            end_x = _get_end_x(qualifiers)
            end_y = _get_end_y(qualifiers)
            if end_x is None:
                end_x = start_x
            if end_y is None:
                end_y = start_y

            event_id = int(assertget(attr, "event_id"))
            event = dict(
                game_id=game_id,
                event_id=event_id,
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
            events[event_id] = event
        return events


class _F7XMLParser(OptaXMLParser):

    @staticmethod
    def get_doc(path):
        root = _F7XMLParser.get(path)
        optadocument = root.find("SoccerDocument")
        return optadocument

    @staticmethod
    def extract_competitions(root):
        optadocument = _F7XMLParser.get_doc(root)
        competition = optadocument.Competition

        stats = {}
        for stat in competition.find('Stat'):
            stats[stat.attrib["Type"]] = stat.text
        competition_id = int(competition.attrib["uID"][1:])
        competition_dict = dict(
                competition_id=competition_id,
                season_id=int(assertget(stats, "season_id")),
                season_name=assertget(stats, "season_name"),
                competition_name=competition.Name.text)
        return {competition_id: competition_dict}

    @staticmethod
    def extract_games(root):
        optadocument = _F7XMLParser.get_doc(root)
        match_info = optadocument.MatchData.MatchInfo
        game_id = int(optadocument.attrib['uID'][1:])
        stats = {}
        for stat in optadocument.MatchData.find('Stat'):
            stats[stat.attrib["Type"]] = stat.text

        game_dict = dict(
            game_id=game_id,
            venue_id=int(optadocument.Venue.attrib['uID'][1:]),
            referee_id=int(optadocument.MatchData.MatchOfficial.attrib['uID'][1:]),
            game_date=datetime.strptime(match_info.Date.text, '%Y%m%dT%H%M%S%z').replace(tzinfo=None),
            attendance=int(match_info.Attendance),
            duration=int(stats["match_time"]),
        )
        return {game_id: game_dict}

    @staticmethod
    def extract_teams(root):
        optadocument = _F7XMLParser.get_doc(root)
        lineups = _F7XMLParser.extract_lineups(root)
        team_elms = list(optadocument.iterchildren("Team"))
        teams = {}
        for team_elm in team_elms:
            team_id = int(assertget(team_elm.attrib, "uID")[1:])
            team = dict(
                team_id=team_id,
                team_name=team_elm.Name.text,
            )
            teams[team_id] = team
        return teams

    @staticmethod
    def extract_lineups(root):
        optadocument = _F7XMLParser.get_doc(root)

        stats = {}
        for stat in optadocument.MatchData.find('Stat'):
            stats[stat.attrib["Type"]] = stat.text

        lineup_elms = optadocument.MatchData.iterchildren("TeamData")
        lineups = {}
        for team_elm in lineup_elms:
            # lineup attributes
            team_id = int(team_elm.attrib["TeamRef"][1:])
            lineups[team_id] = dict(
                formation=team_elm.attrib["Formation"],
                score=int(team_elm.attrib["Score"]),
                side=team_elm.attrib["Side"],
                players=dict())
            #substitutes
            subst_elms = team_elm.iterchildren("Substitution")
            subst = [subst_elm.attrib for subst_elm in subst_elms]
            # players
            player_elms = team_elm.PlayerLineUp.iterchildren("MatchPlayer")
            for player_elm in player_elms:
                player_id = int(player_elm.attrib["PlayerRef"][1:])
                status = player_elm.attrib["Status"]
                sub_on = int(next((item["Time"] for item in subst if item["SubOn"] == f"p{player_id}"), 0))
                sub_off = int(next((item["Time"] for item in subst if item["SubOff"] == f"p{player_id}"), stats["match_time"]))
                minutes_played = sub_off - sub_on
                lineups[team_id]["players"][player_id] = dict(
                    starting_position_id=int(player_elm.attrib["Formation_Place"]),
                    starting_position_name=player_elm.attrib["Position"],
                    jersey_number=int(player_elm.attrib["ShirtNumber"]),
                    is_starter=player_elm.attrib["Formation_Place"] != 0,
                    minutes_played=minutes_played)
        return lineups

    @staticmethod
    def extract_players(root):
        optadocument = _F7XMLParser.get_doc(root)
        lineups = _F7XMLParser.extract_lineups(root)
        team_elms = list(optadocument.iterchildren("Team"))
        players = {}
        for team_elm in team_elms:
            team_id = int(team_elm.attrib['uID'][1:])
            for player_elm in team_elm.iterchildren("Player"):
                player_id = int(player_elm.attrib["uID"][1:])
                firstname = str(player_elm.find("PersonName").find("First"))
                lastname = str(player_elm.find("PersonName").find("Last"))
                nickname = str(player_elm.find("PersonName").find("Known"))
                player = dict(
                        team_id=team_id,
                        player_id=player_id,
                        player_name=" ".join([firstname, lastname]),
                        firstname=firstname,
                        lastname=lastname,
                        nickname=nickname,
                        **lineups[team_id]["players"][player_id]
                    )
                players[player_id] = player

        return players


class _F24XMLParser(OptaXMLParser):

    @staticmethod
    def get_doc(path):
        root = _F24XMLParser.get(path)
        return root

    @staticmethod
    def extract_games(root):
        optadocument = _F24XMLParser.get_doc(root)
        game_elem = optadocument.find("Game")
        attr = game_elem.attrib
        game_id = int(assertget(attr, "id"))
        game_dict = dict(
            game_id=game_id,
            competition_id=int(assertget(attr, "competition_id")),
            season_id=int(assertget(attr, "season_id")),
            game_day=int(assertget(attr, "matchday")),
            game_date=datetime.strptime(assertget(attr, "game_date"), '%Y-%m-%dT%H:%M:%S'),
            home_team_id=int(assertget(attr, "home_team_id")),
            home_score=int(assertget(attr, "home_score")),
            away_team_id=int(assertget(attr, "away_team_id")),
            away_score=int(assertget(attr, "away_score")),
        )
        return {game_id: game_dict}

    @staticmethod
    def extract_events(root):
        optadocument = _F24XMLParser.get_doc(root)
        game_elm = optadocument.find("Game")
        attr = game_elm.attrib
        game_id = int(assertget(attr, "id"))
        events = {}
        for event_elm in game_elm.iterchildren("Event"):
            attr = dict(event_elm.attrib)
            event_id = int(attr["id"])

            qualifiers = {
                    int(qualifier_elm.attrib["qualifier_id"]): qualifier_elm.attrib.get("value")
                    for qualifier_elm in event_elm.iterchildren("Q")
                    }
            start_x = float(assertget(attr, "x"))
            start_y = float(assertget(attr, "y"))
            end_x = _get_end_x(qualifiers)
            end_y = _get_end_y(qualifiers)
            if end_x is None:
                end_x = start_x
            if end_y is None:
                end_y = start_y

            event = dict(
                game_id=game_id,
                event_id=event_id,
                type_id=int(assertget(attr, "type_id")),
                period_id=int(assertget(attr, "period_id")),
                minute=int(assertget(attr, "min")),
                second=int(assertget(attr, "sec")),
                timestamp=assertget(attr, "timestamp"),
                player_id=int(attr.get("player_id", 0)),
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
            events[event_id] = event
        return events


_jsonparsers = {
        "f1": _F1JSONParser,
        "f9": _F9JSONParser,
        "f24": _F24JSONParser
        }

_xmlparsers = {
        "f7": _F7XMLParser,
        "f24": _F24XMLParser
        }



def assertget(dictionary, key):
    value = dictionary.get(key)
    assert value is not None, "KeyError: " + key + " not found in " + str(dictionary)
    return value


def _get_end_x(qualifiers):
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


def _get_end_y(qualifiers):
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


_eventtypesdf = pd.DataFrame([
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
], columns=["type_id", "type_name"])


def convert_to_actions(events: pd.DataFrame, home_team_id: int) -> pd.DataFrame:
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
    actions["period_id"] = events.period_id

    actions["time_seconds"] = 60 * events.minute + events.second
    actions["timestamp"] = events.timestamp
    actions["team_id"] = events.team_id
    actions["player_id"] = events.player_id

    for col in ["start_x", "end_x"]:
        actions[col] = events[col] / 100 * spadlconfig.field_length
    for col in ["start_y", "end_y"]:
        actions[col] = events[col] / 100 * spadlconfig.field_width

    actions["type_id"] = events[["type_name", "outcome", "qualifiers"]].apply(_get_type_id, axis=1)
    actions["result_id"] = events[["type_name", "outcome", "qualifiers"]].apply(_get_result_id, axis=1)
    actions["bodypart_id"] = events.qualifiers.apply(_get_bodypart_id)

    actions = (
        actions[actions.type_id != spadlconfig.actiontypes.index("non_action")]
        .sort_values(["game_id", "period_id", "time_seconds", "timestamp"])
        .reset_index(drop=True)
    )
    actions = _fix_owngoal_coordinates(actions)
    actions = _fix_direction_of_play(actions, home_team_id)
    actions = _fix_clearances(actions)
    actions["action_id"] = range(len(actions))
    actions = _add_dribbles(actions)

    for col in actions.columns:
        if "_id" in col:
            actions[col] = actions[col].astype(int)
    return actions


def _get_bodypart_id(qualifiers):
    if 15 in qualifiers:
        b = "head"
    elif 21 in qualifiers:
        b = "other"
    else:
        b = "foot"
    return spadlconfig.bodyparts.index(b)


def _get_result_id(args):
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


def _get_type_id(args):
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
    return spadlconfig.actiontypes.index(a)


def _fix_owngoal_coordinates(actions):
    owngoals_idx = (actions.result_id == spadlconfig.results.index("owngoal")) & (
        actions.type_id == spadlconfig.actiontypes.index("shot")
    )
    actions.loc[owngoals_idx, "end_x"] = (
        spadlconfig.field_length - actions[owngoals_idx].end_x.values
    )
    actions.loc[owngoals_idx, "end_y"] = (
        spadlconfig.field_width - actions[owngoals_idx].end_y.values
    )
    return actions
