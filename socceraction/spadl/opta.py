# -*- coding: utf-8 -*-
"""Opta event stream data to SPADL converter."""
import copy
import glob
import json  # type: ignore
import os
import re
import warnings
from abc import ABC
from datetime import datetime, timedelta
from typing import Any, Dict, List, Mapping, Optional, Tuple, Type

import pandas as pd  # type: ignore
import pandera as pa
import unidecode  # type: ignore
from lxml import objectify
from pandera.typing import DataFrame, DateTime, Series

from . import config as spadlconfig
from .base import (
    CompetitionSchema,
    EventDataLoader,
    EventSchema,
    GameSchema,
    MissingDataError,
    PlayerSchema,
    TeamSchema,
    _add_dribbles,
    _fix_clearances,
    _fix_direction_of_play,
)

__all__ = [
    'OptaLoader',
    'convert_to_actions',
    'OptaCompetitionSchema',
    'OptaGameSchema',
    'OptaPlayerSchema',
    'OptaTeamSchema',
    'OptaEventSchema',
]


class OptaCompetitionSchema(CompetitionSchema):
    """Definition of a dataframe containing a list of competitions and seasons."""


class OptaGameSchema(GameSchema):
    """Definition of a dataframe containing a list of games."""

    venue: Series[str] = pa.Field(nullable=True)
    referee_id: Series[int] = pa.Field(nullable=True)
    attendance: Series[int] = pa.Field(nullable=True)
    duration: Series[int]
    home_score: Series[int]
    away_score: Series[int]


class OptaPlayerSchema(PlayerSchema):
    """Definition of a dataframe containing the list of teams of a game."""

    firstname: Optional[Series[str]]
    lastname: Optional[Series[str]]
    nickname: Optional[Series[str]] = pa.Field(nullable=True)
    starting_position_id: Series[int]
    starting_position_name: Series[str]
    height: Optional[Series[float]]
    weight: Optional[Series[float]]
    age: Optional[Series[int]]


class OptaTeamSchema(TeamSchema):
    """Definition of a dataframe containing the list of players of a game."""


class OptaEventSchema(EventSchema):
    """Definition of a dataframe containing event stream data of a game."""

    timestamp: Series[DateTime]
    minute: Series[int]
    second: Series[int] = pa.Field(ge=0, le=59)
    outcome: Series[bool]
    start_x: Series[float] = pa.Field(nullable=True)
    start_y: Series[float] = pa.Field(nullable=True)
    end_x: Series[float] = pa.Field(nullable=True)
    end_y: Series[float] = pa.Field(nullable=True)
    assist: Series[bool] = pa.Field(nullable=True)
    keypass: Series[bool] = pa.Field(nullable=True)
    qualifiers: Series[object]


def _deepupdate(target: Dict[Any, Any], src: Dict[Any, Any]) -> None:
    """Deep update target dict with src.

    For each k,v in src: if k doesn't exist in target, it is deep copied from
    src to target. Otherwise, if v is a list, target[k] is extended with
    src[k]. If v is a set, target[k] is updated with v, If v is a dict,
    recursively deep-update it.

    Examples
    --------
    >>> t = {'name': 'Ferry', 'hobbies': ['programming', 'sci-fi']}
    >>> deepupdate(t, {'hobbies': ['gaming']})
    >>> print(t)
    {'name': 'Ferry', 'hobbies': ['programming', 'sci-fi', 'gaming']}
    """
    for k, v in src.items():
        if isinstance(v, list):
            if k not in target:
                target[k] = copy.deepcopy(v)
            else:
                target[k].extend(v)
        elif isinstance(v, dict):
            if k not in target:
                target[k] = copy.deepcopy(v)
            else:
                _deepupdate(target[k], v)
        elif isinstance(v, set):
            if k not in target:
                target[k] = v.copy()
            else:
                target[k].update(v.copy())
        else:
            target[k] = copy.copy(v)


def _extract_ids_from_path(path: str, pattern: str) -> Dict[str, int]:
    regex = re.compile(
        '.+?'
        + re.escape(pattern)
        .replace(r'\{competition_id\}', r'(?P<competition_id>\d+)')
        .replace(r'\{season_id\}', r'(?P<season_id>\d+)')
        .replace(r'\{game_id\}', r'(?P<game_id>\d+)')
    )
    m = re.match(regex, path)
    if m is None:
        raise ValueError('The filepath {} does not match the format {}.'.format(path, pattern))
    ids = m.groupdict()
    return {k: int(v) for k, v in ids.items()}


class OptaParser(ABC):
    """Extract data from an Opta data stream.

    Parameters
    ----------
    path : str
        Path of the data file.
    """

    def __init__(self, path: str, *args: Any, **kwargs: Any):
        pass

    def extract_competitions(self) -> Dict[int, Dict[str, Any]]:
        return {}

    def extract_games(self) -> Dict[int, Dict[str, Any]]:
        return {}

    def extract_teams(self) -> Dict[int, Dict[str, Any]]:
        return {}

    def extract_players(self) -> Dict[int, Dict[str, Any]]:
        return {}

    def extract_events(self) -> Dict[int, Dict[str, Any]]:
        return {}


class OptaLoader(EventDataLoader):
    """
    Load Opta data from a local folder.

    Parameters
    ----------
    root : str
        Root-path of the data.
    feeds : dict
        Glob pattern for each feed that should be parsed. For example::

            {
                'f7': "f7-{competition_id}-{season_id}-{game_id}.xml",
                'f24': "f24-{competition_id}-{season_id}-{game_id}.xml"
            }

        If you use JSON files obtained from `WhoScored <whoscored.com>`__ use::

            {
                'whoscored': "{competition_id}-{season_id}/{game_id}.json",
            }

    parser : str or dict
        Either 'xml', 'json', 'whoscored' or your custom parser for each feed.
        The default xml parser supports F7 and F24 feeds; the default json
        parser supports F1, F9 and F24 feeds. Custom parsers can be specified
        as::

            {
                'feed1_name': Feed1Parser
                'feed2_name': Feed2Parser
            }

        where Feed1Parser and Feed2Parser are classes implementing
        :class:`~socceraction.spadl.opta.OptaParser` and 'feed1_name' and
        'feed2_name' correspond to the keys in 'feeds'.

    """

    def __init__(self, root: str, feeds: Dict[str, str], parser: Mapping[str, Type[OptaParser]]):
        self.root = root
        if parser == 'json':
            self.parsers = self._get_parsers_for_feeds(_jsonparsers, feeds)
        elif parser == 'xml':
            self.parsers = self._get_parsers_for_feeds(_xmlparsers, feeds)
        elif parser == 'whoscored':
            self.parsers = self._get_parsers_for_feeds(_whoscoredparsers, feeds)
        else:
            self.parsers = self._get_parsers_for_feeds(parser, feeds)
        self.feeds = feeds

    def _get_parsers_for_feeds(
        self, available_parsers: Mapping[str, Type[OptaParser]], feeds: Dict[str, str]
    ) -> Mapping[str, Type[OptaParser]]:
        """Select the appropriate parser for each feed.

        Parameters
        ----------
        available_parsers : dict(str, OptaParser)
            Dictionary with all available parsers.
        feeds : dict(str, str)
            All feeds that should be parsed.

        Returns
        -------
        dict(str, OptaParser)
            A mapping between all feeds that should be parsed and the
            corresponding parser class.

        Warns
        -----
        Raises a warning if there is no parser available for any of the
        provided feeds.
        """
        parsers = {}
        for feed in feeds:
            if feed in available_parsers:
                parsers[feed] = available_parsers[feed]
            else:
                warnings.warn(
                    'No parser available for {} feeds. This feed is ignored.'.format(feed)
                )
        return parsers

    def competitions(self) -> DataFrame[OptaCompetitionSchema]:
        """Return a dataframe with all available competitions and seasons.

        Returns
        -------
        pd.DataFrame
            A dataframe containing all available competitions and seasons. See
            :class:`~socceraction.spadl.opta.OptaCompetitionSchema` for the schema.
        """
        data: Dict[int, Dict[str, Any]] = {}
        for feed, feed_pattern in self.feeds.items():
            glob_pattern = feed_pattern.format(competition_id='*', season_id='*', game_id='*')
            feed_files = glob.glob(os.path.join(self.root, glob_pattern))
            for ffp in feed_files:
                ids = _extract_ids_from_path(ffp, feed_pattern)
                parser = self.parsers[feed](ffp, **ids)
                _deepupdate(data, parser.extract_competitions())
        return pd.DataFrame(list(data.values()))

    def games(self, competition_id: int, season_id: int) -> DataFrame[OptaGameSchema]:
        """Return a dataframe with all available games in a season.

        Parameters
        ----------
        competition_id : int
            The ID of the competition.
        season_id : int
            The ID of the season.

        Returns
        -------
        pd.DataFrame
            A dataframe containing all available games. See
            :class:`~socceraction.spadl.opta.OptaGameSchema` for the schema.
        """
        data: Dict[int, Dict[str, Any]] = {}
        for feed, feed_pattern in self.feeds.items():
            glob_pattern = feed_pattern.format(
                competition_id=competition_id, season_id=season_id, game_id='*'
            )
            feed_files = glob.glob(os.path.join(self.root, glob_pattern))
            for ffp in feed_files:
                try:
                    ids = _extract_ids_from_path(ffp, feed_pattern)
                    parser = self.parsers[feed](ffp, **ids)
                    _deepupdate(data, parser.extract_games())
                except Exception:
                    warnings.warn('Could not parse {}'.format(ffp))
        return pd.DataFrame(list(data.values()))

    def teams(self, game_id: int) -> DataFrame[OptaTeamSchema]:
        """Return a dataframe with both teams that participated in a game.

        Parameters
        ----------
        game_id : int
            The ID of the game.

        Returns
        -------
        pd.DataFrame
            A dataframe containing both teams. See
            :class:`~socceraction.spadl.opta.OptaTeamSchema` for the schema.
        """
        data: Dict[int, Dict[str, Any]] = {}
        for feed, feed_pattern in self.feeds.items():
            glob_pattern = feed_pattern.format(competition_id='*', season_id='*', game_id=game_id)
            feed_files = glob.glob(os.path.join(self.root, glob_pattern))
            for ffp in feed_files:
                ids = _extract_ids_from_path(ffp, feed_pattern)
                parser = self.parsers[feed](ffp, **ids)
                _deepupdate(data, parser.extract_teams())
        return pd.DataFrame(list(data.values()))

    def players(self, game_id: int) -> DataFrame[OptaPlayerSchema]:
        """Return a dataframe with all players that participated in a game.

        Parameters
        ----------
        game_id : int
            The ID of the game.

        Returns
        -------
        pd.DataFrame
            A dataframe containing all players. See
            :class:`~socceraction.spadl.opta.OptaPlayerSchema` for the schema.
        """
        data: Dict[int, Dict[str, Any]] = {}
        for feed, feed_pattern in self.feeds.items():
            glob_pattern = feed_pattern.format(competition_id='*', season_id='*', game_id=game_id)
            feed_files = glob.glob(os.path.join(self.root, glob_pattern))
            for ffp in feed_files:
                ids = _extract_ids_from_path(ffp, feed_pattern)
                parser = self.parsers[feed](ffp, **ids)
                _deepupdate(data, parser.extract_players())
        df_players = pd.DataFrame(list(data.values()))
        df_players['game_id'] = game_id
        return df_players

    def events(self, game_id: int) -> DataFrame[OptaEventSchema]:
        """Return a dataframe with the event stream of a game.

        Parameters
        ----------
        game_id : int
            The ID of the game.

        Returns
        -------
        pd.DataFrame
            A dataframe containing the event stream. See
            :class:`~socceraction.spadl.opta.OptaEventSchema` for the schema.
        """
        data: Dict[int, Dict[str, Any]] = {}
        for feed, feed_pattern in self.feeds.items():
            glob_pattern = feed_pattern.format(competition_id='*', season_id='*', game_id=game_id)
            feed_files = glob.glob(os.path.join(self.root, glob_pattern))
            for ffp in feed_files:
                ids = _extract_ids_from_path(ffp, feed_pattern)
                parser = self.parsers[feed](ffp, **ids)
                _deepupdate(data, parser.extract_events())
        events = (
            pd.DataFrame(list(data.values()))
            .merge(_eventtypesdf, on='type_id', how='left')
            .sort_values(['game_id', 'period_id', 'minute', 'second', 'timestamp'])
            .reset_index(drop=True)
        )
        return events


class OptaJSONParser(OptaParser):
    """Extract data from an Opta JSON data stream.

    Parameters
    ----------
    path : str
        Path of the data file.
    """

    def __init__(self, path: str, *args: Any, **kwargs: Any):
        with open(path, 'rt', encoding='utf-8') as fh:
            self.root = json.load(fh)


class OptaXMLParser(OptaParser):
    """Extract data from an Opta XML data stream.

    Parameters
    ----------
    path : str
        Path of the data file.
    """

    def __init__(self, path: str, *args: Any, **kwargs: Any):
        with open(path, 'rb') as fh:
            self.root = objectify.fromstring(fh.read())


class _F1JSONParser(OptaJSONParser):
    def get_feed(self) -> Dict[str, Any]:
        for node in self.root:
            if 'OptaFeed' in node['data'].keys():
                return node
        raise MissingDataError

    def get_doc(self) -> Dict[str, Any]:
        f1 = self.get_feed()
        data = assertget(f1, 'data')
        optafeed = assertget(data, 'OptaFeed')
        optadocument = assertget(optafeed, 'OptaDocument')
        return optadocument

    def extract_competitions(self) -> Dict[int, Dict[str, Any]]:
        optadocument = self.get_doc()
        attr = assertget(optadocument, '@attributes')
        competition_id = int(assertget(attr, 'competition_id'))
        competition = dict(
            season_id=int(assertget(attr, 'season_id')),
            season_name=str(assertget(attr, 'season_id')),
            competition_id=competition_id,
            competition_name=assertget(attr, 'competition_name'),
        )
        return {competition_id: competition}

    def extract_games(self) -> Dict[int, Dict[str, Any]]:
        optadocument = self.get_doc()
        attr = assertget(optadocument, '@attributes')
        matchdata = assertget(optadocument, 'MatchData')
        matches = {}
        for match in matchdata:
            match_dict: Dict[str, Any] = {}
            match_dict['competition_id'] = int(assertget(attr, 'competition_id'))
            match_dict['season_id'] = int(assertget(attr, 'season_id'))
            matchattr = assertget(match, '@attributes')
            match_dict['game_id'] = int(assertget(matchattr, 'uID')[1:])
            matchinfo = assertget(match, 'MatchInfo')
            matchinfoattr = assertget(matchinfo, '@attributes')
            match_dict['game_day'] = int(assertget(matchinfoattr, 'MatchDay'))
            match_dict['venue'] = str(assertget(matchinfoattr, 'Venue_id'))
            match_dict['game_date'] = datetime.strptime(
                assertget(matchinfo, 'Date'), '%Y-%m-%d %H:%M:%S'
            )
            teamdata = assertget(match, 'TeamData')
            for team in teamdata:
                teamattr = assertget(team, '@attributes')
                side = assertget(teamattr, 'Side')
                teamid = assertget(teamattr, 'TeamRef')
                if side == 'Home':
                    match_dict['home_team_id'] = int(teamid[1:])
                else:
                    match_dict['away_team_id'] = int(teamid[1:])
            matches[match_dict['game_id']] = match_dict
        return matches


class _F9JSONParser(OptaJSONParser):
    def get_feed(self) -> Dict[str, Any]:
        for node in self.root:
            if 'OptaFeed' in node['data'].keys():
                return node
        raise MissingDataError

    def get_doc(self) -> Dict[str, Any]:
        f9 = self.get_feed()
        data = assertget(f9, 'data')
        optafeed = assertget(data, 'OptaFeed')
        optadocument = assertget(optafeed, 'OptaDocument')[0]
        return optadocument

    def extract_games(self) -> Dict[int, Dict[str, Any]]:
        optadocument = self.get_doc()
        attr = assertget(optadocument, '@attributes')
        venue = assertget(optadocument, 'Venue')
        matchdata = assertget(optadocument, 'MatchData')
        matchofficial = assertget(matchdata, 'MatchOfficial')
        matchinfo = assertget(matchdata, 'MatchInfo')
        stat = assertget(matchdata, 'Stat')
        assert stat['@attributes']['Type'] == 'match_time'
        teamdata = assertget(matchdata, 'TeamData')
        scores = {}
        for t in teamdata:
            scores[t['@attributes']['Side']] = t['@attributes']['Score']

        game_id = int(assertget(attr, 'uID')[1:])
        game_dict = {
            game_id: dict(
                game_id=game_id,
                venue=str(
                    venue['@attributes']['uID']
                ),  # The venue's name is not included in this stream
                referee_id=int(matchofficial['@attributes']['uID'].replace('o', '')),
                game_date=datetime.strptime(
                    assertget(matchinfo, 'Date'), '%Y%m%dT%H%M%S%z'
                ).replace(tzinfo=None),
                attendance=int(matchinfo.get('Attendance', 0)),
                duration=int(stat['@value']),
                home_score=int(scores['Home']),
                away_score=int(scores['Away']),
            )
        }
        return game_dict

    def extract_teams(self) -> Dict[int, Dict[str, Any]]:
        optadocument = self.get_doc()
        root_teams = assertget(optadocument, 'Team')

        teams = {}
        for team in root_teams:
            if 'id' in team.keys():
                nameobj = team.get('nameObj')
                team_id = int(team['id'])
                team = dict(
                    team_id=team_id,
                    team_name=nameobj.get('name'),
                )
                for f in ['team_name']:
                    team[f] = unidecode.unidecode(team[f]) if f in team else team[f]
                teams[team_id] = team
        return teams

    def extract_players(self) -> Dict[int, Dict[str, Any]]:
        optadocument = self.get_doc()
        root_teams = assertget(optadocument, 'Team')
        lineups = self.extract_lineups()

        players = {}
        for team in root_teams:
            team_id = int(team['@attributes']['uID'].replace('t', ''))
            for player in team['Player']:
                player_id = int(player['@attributes']['uID'].replace('p', ''))

                assert 'nameObj' in player['PersonName']
                nameobj = player['PersonName']['nameObj']
                if not nameobj.get('is_unknown'):
                    player = dict(
                        team_id=team_id,
                        player_id=player_id,
                        firstname=nameobj.get('first').strip() or None,
                        lastname=nameobj.get('last').strip() or None,
                        player_name=nameobj.get('full').strip() or None,
                        nickname=nameobj.get('known') or nameobj.get('full').strip() or None,
                    )
                    if player_id in lineups[team_id]['players']:
                        player = dict(
                            **player,
                            jersey_number=lineups[team_id]['players'][player_id]['jersey_number'],
                            starting_position_name=lineups[team_id]['players'][player_id][
                                'starting_position_name'
                            ],
                            starting_position_id=lineups[team_id]['players'][player_id][
                                'starting_position_id'
                            ],
                            is_starter=lineups[team_id]['players'][player_id]['is_starter'],
                            minutes_played=lineups[team_id]['players'][player_id][
                                'minutes_played'
                            ],
                        )
                    for f in ['firstname', 'lastname', 'player_name', 'nickname']:
                        if player[f]:
                            player[f] = unidecode.unidecode(player[f])
                    players[player_id] = player
        return players

    def extract_referee(self) -> Dict[int, Dict[str, Any]]:
        optadocument = self.get_doc()

        try:
            rootf9 = optadocument['MatchData']['MatchOfficial']
        except KeyError:
            raise MissingDataError

        name = rootf9['OfficialName']
        nameobj = name['nameObj']
        referee_id = int(rootf9['@attributes']['uID'].replace('o', ''))
        referee = dict(
            referee_id=referee_id,
            referee_firstname=name.get('First') or nameobj.get('first'),
            referee_lastname=name.get('Last') or nameobj.get('last'),
        )
        for f in ['referee_firstname', 'referee_lastname']:
            if referee[f]:
                referee[f] = unidecode.unidecode(referee[f])
        return {referee_id: referee}

    def extract_teamgamestats(self) -> List[Dict[str, Any]]:
        optadocument = self.get_doc()
        attr = assertget(optadocument, '@attributes')
        game_id = int(assertget(attr, 'uID')[1:])

        try:
            rootf9 = optadocument['MatchData']['TeamData']
        except KeyError:
            raise MissingDataError
        teams_gamestats = []
        for team in rootf9:
            attr = team['@attributes']
            statsdict = {stat['@attributes']['Type']: stat['@value'] for stat in team['Stat']}

            team_gamestats = dict(
                game_id=game_id,
                team_id=int(attr['TeamRef'].replace('t', '')),
                side=attr['Side'],
                score=attr['Score'],
                shootout_score=attr['ShootOutScore'],
                **statsdict,
            )

            teams_gamestats.append(team_gamestats)
        return teams_gamestats

    def extract_lineups(self) -> Dict[int, Dict[str, Any]]:
        optadocument = self.get_doc()
        attr = assertget(optadocument, '@attributes')

        try:
            rootf9 = optadocument['MatchData']['TeamData']
        except KeyError:
            raise MissingDataError
        matchstats = optadocument['MatchData']['Stat']
        matchstats = [matchstats] if isinstance(matchstats, dict) else matchstats
        matchstatsdict = {stat['@attributes']['Type']: stat['@value'] for stat in matchstats}

        lineups: Dict[int, Dict[str, Any]] = {}
        for team in rootf9:
            # lineup attributes
            team_id = int(team['@attributes']['TeamRef'].replace('t', ''))
            lineups[team_id] = dict(players=dict())
            # substitutes
            subst = [s['@attributes'] for s in team['Substitution']]
            for player in team['PlayerLineUp']['MatchPlayer']:
                attr = player['@attributes']
                player_id = int(attr['PlayerRef'].replace('p', ''))
                playerstatsdict = {
                    stat['@attributes']['Type']: stat['@value'] for stat in player['Stat']
                }
                sub_on = next(
                    (item['Time'] for item in subst if item['SubOn'] == f'p{player_id}'), 0
                )
                sub_off = next(
                    (item['Time'] for item in subst if item['SubOff'] == f'p{player_id}'),
                    matchstatsdict['match_time'],
                )
                minutes_played = sub_off - sub_on
                lineups[team_id]['players'][player_id] = dict(
                    jersey_number=attr['ShirtNumber'],
                    starting_position_name=attr['Position'],
                    starting_position_id=attr['position_id'],
                    is_starter=attr['Status'] == 'Start',
                    minutes_played=minutes_played,
                    **playerstatsdict,
                )
        return lineups


class _F24JSONParser(OptaJSONParser):
    def get_feed(self) -> Dict[str, Any]:
        for node in self.root:
            if 'Games' in node['data'].keys():
                return node
        raise MissingDataError

    def extract_games(self) -> Dict[int, Dict[str, Any]]:
        f24 = self.get_feed()

        data = assertget(f24, 'data')
        games = assertget(data, 'Games')
        game = assertget(games, 'Game')
        attr = assertget(game, '@attributes')

        game_id = int(assertget(attr, 'id'))
        game_dict = {
            game_id: dict(
                competition_id=int(assertget(attr, 'competition_id')),
                game_id=game_id,
                season_id=int(assertget(attr, 'season_id')),
                game_day=int(assertget(attr, 'matchday')),
                home_team_id=int(assertget(attr, 'home_team_id')),
                away_team_id=int(assertget(attr, 'away_team_id')),
            )
        }
        return game_dict

    def extract_events(self) -> Dict[int, Dict[str, Any]]:
        f24 = self.get_feed()

        data = assertget(f24, 'data')
        games = assertget(data, 'Games')
        game = assertget(games, 'Game')
        game_attr = assertget(game, '@attributes')
        game_id = int(assertget(game_attr, 'id'))

        events = {}
        for element in assertget(game, 'Event'):
            attr = element['@attributes']
            timestamp = attr['TimeStamp'].get('locale') if attr.get('TimeStamp') else None
            timestamp = datetime.strptime(timestamp, '%Y-%m-%dT%H:%M:%S.%fZ')
            qualifiers = {
                int(q['@attributes']['qualifier_id']): q['@attributes']['value']
                for q in element.get('Q', [])
            }
            start_x = float(assertget(attr, 'x'))
            start_y = float(assertget(attr, 'y'))
            end_x = _get_end_x(qualifiers)
            end_y = _get_end_y(qualifiers)
            if end_x is None:
                end_x = start_x
            if end_y is None:
                end_y = start_y

            event_id = int(assertget(attr, 'event_id'))
            event = dict(
                game_id=game_id,
                event_id=event_id,
                type_id=int(assertget(attr, 'type_id')),
                period_id=int(assertget(attr, 'period_id')),
                minute=int(assertget(attr, 'min')),
                second=int(assertget(attr, 'sec')),
                timestamp=timestamp,
                player_id=int(assertget(attr, 'player_id')),
                team_id=int(assertget(attr, 'team_id')),
                outcome=bool(int(attr.get('outcome', 1))),
                start_x=start_x,
                start_y=start_y,
                end_x=end_x,
                end_y=end_y,
                assist=bool(int(attr.get('assist', 0))),
                keypass=bool(int(attr.get('keypass', 0))),
                qualifiers=qualifiers,
            )
            events[event_id] = event
        return events


class _F7XMLParser(OptaXMLParser):
    def get_doc(self) -> Type[objectify.ObjectifiedElement]:
        optadocument = self.root.find('SoccerDocument')
        return optadocument

    def extract_competitions(self) -> Dict[int, Dict[str, Any]]:
        optadocument = self.get_doc()
        competition = optadocument.Competition

        stats = {}
        for stat in competition.find('Stat'):
            stats[stat.attrib['Type']] = stat.text
        competition_id = int(competition.attrib['uID'][1:])
        competition_dict = dict(
            competition_id=competition_id,
            season_id=int(assertget(stats, 'season_id')),
            season_name=assertget(stats, 'season_name'),
            competition_name=competition.Name.text,
        )
        return {competition_id: competition_dict}

    def extract_games(self) -> Dict[int, Dict[str, Any]]:
        optadocument = self.get_doc()
        match_info = optadocument.MatchData.MatchInfo
        game_id = int(optadocument.attrib['uID'][1:])
        stats = {}
        for stat in optadocument.MatchData.find('Stat'):
            stats[stat.attrib['Type']] = stat.text

        game_dict = dict(
            game_id=game_id,
            venue=optadocument.Venue.Name.text,
            referee_id=int(optadocument.MatchData.MatchOfficial.attrib['uID'][1:]),
            game_date=datetime.strptime(match_info.Date.text, '%Y%m%dT%H%M%S%z').replace(
                tzinfo=None
            ),
            attendance=int(match_info.Attendance),
            duration=int(stats['match_time']),
        )
        return {game_id: game_dict}

    def extract_teams(self) -> Dict[int, Dict[str, Any]]:
        optadocument = self.get_doc()
        team_elms = list(optadocument.iterchildren('Team'))
        teams = {}
        for team_elm in team_elms:
            team_id = int(assertget(team_elm.attrib, 'uID')[1:])
            team = dict(
                team_id=team_id,
                team_name=team_elm.Name.text,
            )
            teams[team_id] = team
        return teams

    def extract_lineups(self) -> Dict[int, Dict[str, Any]]:
        optadocument = self.get_doc()

        stats = {}
        for stat in optadocument.MatchData.find('Stat'):
            stats[stat.attrib['Type']] = stat.text

        lineup_elms = optadocument.MatchData.iterchildren('TeamData')
        lineups = {}
        for team_elm in lineup_elms:
            # lineup attributes
            team_id = int(team_elm.attrib['TeamRef'][1:])
            lineups[team_id] = dict(
                formation=team_elm.attrib['Formation'],
                score=int(team_elm.attrib['Score']),
                side=team_elm.attrib['Side'],
                players=dict(),
            )
            # substitutes
            subst_elms = team_elm.iterchildren('Substitution')
            subst = [subst_elm.attrib for subst_elm in subst_elms]
            # players
            player_elms = team_elm.PlayerLineUp.iterchildren('MatchPlayer')
            for player_elm in player_elms:
                player_id = int(player_elm.attrib['PlayerRef'][1:])
                sub_on = int(
                    next((item['Time'] for item in subst if item['SubOn'] == f'p{player_id}'), 0)
                )
                sub_off = int(
                    next(
                        (item['Time'] for item in subst if item['SubOff'] == f'p{player_id}'),
                        stats['match_time'],
                    )
                )
                minutes_played = sub_off - sub_on
                lineups[team_id]['players'][player_id] = dict(
                    starting_position_id=int(player_elm.attrib['Formation_Place']),
                    starting_position_name=player_elm.attrib['Position'],
                    jersey_number=int(player_elm.attrib['ShirtNumber']),
                    is_starter=player_elm.attrib['Formation_Place'] != 0,
                    minutes_played=minutes_played,
                )
        return lineups

    def extract_players(self) -> Dict[int, Dict[str, Any]]:
        optadocument = self.get_doc()
        lineups = self.extract_lineups()
        team_elms = list(optadocument.iterchildren('Team'))
        players = {}
        for team_elm in team_elms:
            team_id = int(team_elm.attrib['uID'][1:])
            for player_elm in team_elm.iterchildren('Player'):
                player_id = int(player_elm.attrib['uID'][1:])
                firstname = str(player_elm.find('PersonName').find('First'))
                lastname = str(player_elm.find('PersonName').find('Last'))
                nickname = str(player_elm.find('PersonName').find('Known'))
                player = dict(
                    team_id=team_id,
                    player_id=player_id,
                    player_name=' '.join([firstname, lastname]),
                    firstname=firstname,
                    lastname=lastname,
                    nickname=nickname,
                    **lineups[team_id]['players'][player_id],
                )
                players[player_id] = player

        return players


class _F24XMLParser(OptaXMLParser):
    def get_doc(self) -> Type[objectify.ObjectifiedElement]:
        return self.root

    def extract_games(self) -> Dict[int, Dict[str, Any]]:
        optadocument = self.get_doc()
        game_elem = optadocument.find('Game')
        attr = game_elem.attrib
        game_id = int(assertget(attr, 'id'))
        game_dict = dict(
            game_id=game_id,
            competition_id=int(assertget(attr, 'competition_id')),
            season_id=int(assertget(attr, 'season_id')),
            game_day=int(assertget(attr, 'matchday')),
            game_date=datetime.strptime(assertget(attr, 'game_date'), '%Y-%m-%dT%H:%M:%S'),
            home_team_id=int(assertget(attr, 'home_team_id')),
            home_score=int(assertget(attr, 'home_score')),
            away_team_id=int(assertget(attr, 'away_team_id')),
            away_score=int(assertget(attr, 'away_score')),
        )
        return {game_id: game_dict}

    def extract_events(self) -> Dict[int, Dict[str, Any]]:
        optadocument = self.get_doc()
        game_elm = optadocument.find('Game')
        attr = game_elm.attrib
        game_id = int(assertget(attr, 'id'))
        events = {}
        for event_elm in game_elm.iterchildren('Event'):
            attr = dict(event_elm.attrib)
            event_id = int(attr['id'])

            qualifiers = {
                int(qualifier_elm.attrib['qualifier_id']): qualifier_elm.attrib.get('value')
                for qualifier_elm in event_elm.iterchildren('Q')
            }
            start_x = float(assertget(attr, 'x'))
            start_y = float(assertget(attr, 'y'))
            end_x = _get_end_x(qualifiers)
            end_y = _get_end_y(qualifiers)
            if end_x is None:
                end_x = start_x
            if end_y is None:
                end_y = start_y

            event = dict(
                game_id=game_id,
                event_id=event_id,
                type_id=int(assertget(attr, 'type_id')),
                period_id=int(assertget(attr, 'period_id')),
                minute=int(assertget(attr, 'min')),
                second=int(assertget(attr, 'sec')),
                timestamp=datetime.strptime(assertget(attr, 'timestamp'), '%Y-%m-%dT%H:%M:%S.%f'),
                player_id=int(attr.get('player_id', 0)),
                team_id=int(assertget(attr, 'team_id')),
                outcome=bool(int(attr.get('outcome', 1))),
                start_x=start_x,
                start_y=start_y,
                end_x=end_x,
                end_y=end_y,
                assist=bool(int(attr.get('assist', 0))),
                keypass=bool(int(attr.get('keypass', 0))),
                qualifiers=qualifiers,
            )
            events[event_id] = event
        return events


class _WhoScoredParser(OptaParser):
    """Extract data from a JSON data stream scraped from WhoScored.

    Parameters
    ----------
    path : str
        Path of the data file.
    competition_id : int
        ID of the competition to which the provided data file belongs. If
        None, this information is extracted from a field 'competition_id' in
        the JSON.
    season_id : int
        ID of the season to which the provided data file belongs. If None,
        this information is extracted from a field 'season_id' in the JSON.
    game_id : int
        ID of the game to which the provided data file belongs. If None, this
        information is extracted from a field 'game_id' in the JSON.
    """

    def __init__(  # noqa: C901
        self,
        path: str,
        competition_id: Optional[int] = None,
        season_id: Optional[int] = None,
        game_id: Optional[int] = None,
        *args: Any,
        **kwargs: Any,
    ):
        with open(path, 'rt', encoding='utf-8') as fh:
            self.root = json.load(fh)
            self.position_mapping = lambda formation, x, y: 'Unknown'

        if competition_id is None:
            try:
                competition_id = int(assertget(self.root, 'competition_id'))
            except AssertionError:
                raise MissingDataError(
                    """Could not determine the competition id. Add it to the
                    file path or include a field 'competition_id' in the
                    JSON."""
                )
        self.competition_id = competition_id

        if season_id is None:
            try:
                season_id = int(assertget(self.root, 'season_id'))
            except AssertionError:
                raise MissingDataError(
                    """Could not determine the season id. Add it to the file
                    path or include a field 'season_id' in the JSON."""
                )
        self.season_id = season_id

        if game_id is None:
            try:
                game_id = int(assertget(self.root, 'game_id'))
            except AssertionError:
                raise MissingDataError(
                    """Could not determine the game id. Add it to the file
                    path or include a field 'game_id' in the JSON."""
                )
        self.game_id = game_id

    def get_period_id(self, event: Dict[str, Any]) -> int:
        period = assertget(event, 'period')
        period_id = int(assertget(period, 'value'))
        return period_id

    def get_period_milliseconds(self, event: Dict[str, Any]) -> int:
        period_minute_limits = assertget(self.root, 'periodMinuteLimits')
        period_id = self.get_period_id(event)
        if period_id == 16:  # Pre-match
            return 0
        if period_id == 14:  # Post-game
            return 0
        minute = int(assertget(event, 'minute'))
        period_minute = minute
        if period_id > 1:
            period_minute = minute - period_minute_limits[str(period_id - 1)]
        period_second = period_minute * 60 + int(event.get('second', 0))
        return period_second * 1000

    def extract_games(self) -> Dict[int, Dict[str, Any]]:
        team_home = assertget(self.root, 'home')
        team_away = assertget(self.root, 'away')
        game_id = self.game_id
        game_dict = dict(
            game_id=game_id,
            season_id=self.season_id,
            competition_id=self.competition_id,
            game_day=0,  # TODO: not defined in the JSON object
            game_date=datetime.strptime(
                assertget(self.root, 'startTime'), '%Y-%m-%dT%H:%M:%S'
            ),  # Dates are UTC
            home_team_id=int(assertget(team_home, 'teamId')),
            away_team_id=int(assertget(team_away, 'teamId')),
            # is_regular=None, # TODO
            # is_extra_time=None, # TODO
            # is_penalties=None, # TODO
            # is_golden_goal=None, # TODO
            # is_silver_goal=None, # TODO
            # Optional fields
            home_score=int(assertget(assertget(self.root['home'], 'scores'), 'fulltime')),
            away_score=int(assertget(assertget(self.root['away'], 'scores'), 'fulltime')),
            attendance=int(self.root.get('attendance', 0)),
            venue=str(self.root.get('venueName')),
            referee_id=int(self.root.get('referee', {}).get('officialId', 0)),
            duration=int(self.root.get('expandedMaxMinute')),
        )
        return {game_id: game_dict}

    def extract_players(self) -> Dict[int, Dict[str, Any]]:
        player_gamestats = self.extract_playergamestats()
        game_id = self.game_id
        players = {}
        for team in [self.root['home'], self.root['away']]:
            team_id = int(assertget(team, 'teamId'))
            for p in team['players']:
                player_id = int(assertget(p, 'playerId'))
                player = dict(
                    game_id=game_id,
                    team_id=team_id,
                    player_id=int(assertget(p, 'playerId')),
                    is_starter=bool(p.get('isFirstEleven', False)),
                    player_name=str(assertget(p, 'name')),
                    age=int(p['age']),
                    # nation_code=None,
                    # line_code=str(assertget(p, "position")),
                    # preferred_foot=None,
                    # gender=None,
                    height=float(p.get('height', float('NaN'))),
                    weight=float(p.get('weight', float('NaN'))),
                    minutes_played=player_gamestats[player_id]['minutes_played'],
                    jersey_number=player_gamestats[player_id]['jersey_number'],
                    starting_position_id=0,  # TODO
                    starting_position_name=player_gamestats[player_id]['position_code'],
                )
                for f in ['player_name']:
                    if player[f]:
                        player[f] = unidecode.unidecode(player[f])
                players[player_id] = player
        return players

    def extract_substitutions(self) -> Dict[int, Dict[str, Any]]:
        game_id = self.game_id

        subs = {}
        subonevents = [e for e in self.root['events'] if e['type'].get('value') == 19]
        for e in subonevents:
            sub_id = int(assertget(e, 'playerId'))
            sub = dict(
                game_id=game_id,
                team_id=int(assertget(e, 'teamId')),
                period_id=int(assertget(assertget(e, 'period'), 'value')),
                period_milliseconds=self.get_period_milliseconds(e),
                player_in_id=int(assertget(e, 'playerId')),
                player_out_id=int(assertget(e, 'relatedPlayerId')),
            )
            subs[sub_id] = sub
        return subs

    def extract_positions(self) -> Dict[int, Dict[str, Any]]:  # noqa: C901
        game_id = self.game_id

        positions = {}
        for t in [self.root['home'], self.root['away']]:
            team_id = int(assertget(t, 'teamId'))
            for f in assertget(t, 'formations'):
                fpositions = assertget(f, 'formationPositions')
                playersIds = assertget(f, 'playerIds')
                formation = assertget(f, 'formationName')

                period_end_minutes = assertget(self.root, 'periodEndMinutes')
                period_minute_limits = assertget(self.root, 'periodMinuteLimits')
                start_minute = int(assertget(f, 'startMinuteExpanded'))
                end_minute = int(assertget(f, 'endMinuteExpanded'))
                for period_id in sorted(period_end_minutes.keys()):
                    if period_end_minutes[period_id] > start_minute:
                        break
                period_id = int(period_id)
                period_minute = start_minute
                if period_id > 1:
                    period_minute = start_minute - period_minute_limits[str(period_id - 1)]

                for i, p in enumerate(fpositions):
                    x = float(assertget(p, 'vertical'))
                    y = float(assertget(p, 'horizontal'))
                    try:
                        position_code = self.position_mapping(formation, x, y)
                    except KeyError:
                        position_code = 'Unknown'
                    pos = dict(
                        game_id=game_id,
                        team_id=team_id,
                        period_id=period_id,
                        period_milliseconds=(period_minute * 60 * 1000),
                        start_milliseconds=(start_minute * 60 * 1000),
                        end_milliseconds=(end_minute * 60 * 1000),
                        formation_scheme=formation,
                        player_id=int(playersIds[i]),
                        player_position=position_code,
                        player_position_x=x,
                        player_position_y=y,
                    )
                    positions[team_id] = pos
        return positions

    def extract_teams(self) -> Dict[int, Dict[str, Any]]:
        teams = {}
        for t in [self.root['home'], self.root['away']]:
            team_id = int(assertget(t, 'teamId'))
            team = dict(
                team_id=team_id,
                team_name=assertget(t, 'name'),
            )
            for f in ['team_name']:
                if team[f]:
                    team[f] = unidecode.unidecode(team[f])

            teams[team_id] = team
        return teams

    def extract_referee(self) -> Dict[int, Dict[str, Any]]:
        if 'referee' not in self.root:
            return {
                0: dict(referee_id=0, first_name='Unkown', last_name='Unkown', short_name='Unkown')
            }

        r = self.root['referee']
        referee_id = int(assertget(r, 'officialId'))
        referee = dict(
            referee_id=referee_id,
            first_name=r.get('firstName'),
            last_name=r.get('lastName'),
            short_name=r.get('name'),
        )
        for f in ['first_name', 'last_name', 'short_name']:
            if referee[f]:
                referee[f] = unidecode.unidecode(referee[f])
        return {referee_id: referee}

    def extract_teamgamestats(self) -> List[Dict[str, Any]]:
        game_id = self.game_id

        teams_gamestats = []
        teams = [self.root['home'], self.root['away']]
        for team in teams:
            statsdict = {}
            for name in team['stats']:
                if isinstance(team['stats'][name], dict):
                    statsdict[camel_to_snake(name)] = sum(team['stats'][name].values())

            scores = assertget(team, 'scores')
            team_gamestats = dict(
                game_id=game_id,
                team_id=int(assertget(team, 'teamId')),
                side=assertget(team, 'field'),
                score=assertget(scores, 'fulltime'),
                shootout_score=scores.get('penalty', 0),
                **{k: statsdict[k] for k in statsdict if not k.endswith('Success')},
            )

            teams_gamestats.append(team_gamestats)
        return teams_gamestats

    def extract_playergamestats(self) -> Dict[int, Dict[str, Any]]:  # noqa: C901
        game_id = self.game_id

        players_gamestats = {}
        for team in [self.root['home'], self.root['away']]:
            team_id = int(assertget(team, 'teamId'))
            for player in team['players']:
                statsdict = {
                    camel_to_snake(name): sum(stat.values())
                    for name, stat in player['stats'].items()
                }
                stats = [k for k in statsdict if not k.endswith('Success')]

                player_id = int(assertget(player, 'playerId'))
                p = dict(
                    game_id=game_id,
                    team_id=team_id,
                    player_id=player_id,
                    is_starter=bool(player.get('isFirstEleven', False)),
                    position_code=player.get('position', None),
                    # optional fields
                    jersey_number=int(player.get('shirtNo', 0)),
                    mvp=bool(player.get('isManOfTheMatch', False)),
                    **{k: statsdict[k] for k in stats},
                )
                if 'subbedInExpandedMinute' in player:
                    p['minute_start'] = player['subbedInExpandedMinute']
                if 'subbedOutExpandedMinute' in player:
                    p['minute_end'] = player['subbedOutExpandedMinute']

                # Did not play
                p['minutes_played'] = 0
                # Played the full game
                if p['is_starter'] and 'minute_end' not in p:
                    p['minute_start'] = 0
                    p['minute_end'] = self.root['expandedMaxMinute']
                    p['minutes_played'] = self.root['expandedMaxMinute']
                # Started but substituted out
                elif p['is_starter'] and 'minute_end' in p:
                    p['minute_start'] = 0
                    p['minutes_played'] = p['minute_end']
                # Substitud in and played the remainder of the game
                elif 'minute_start' in p and 'minute_end' not in p:
                    p['minute_end'] = self.root['expandedMaxMinute']
                    p['minutes_played'] = self.root['expandedMaxMinute'] - p['minute_start']
                # Substitud in and out
                elif 'minute_start' in p and 'minute_end' in p:
                    p['minutes_played'] = p['minute_end'] - p['minute_start']

                players_gamestats[player_id] = p
        return players_gamestats

    def extract_events(self) -> Dict[int, Dict[str, Any]]:
        events = {}

        game_id = self.game_id
        time_start_str = str(assertget(self.root, 'startTime'))
        time_start = datetime.strptime(time_start_str, '%Y-%m-%dT%H:%M:%S')
        for attr in self.root['events']:
            qualifiers = {}
            qualifiers = {
                int(q['type']['value']): q.get('value', True) for q in attr.get('qualifiers', [])
            }
            start_x = float(assertget(attr, 'x'))
            start_y = float(assertget(attr, 'y'))
            end_x = _get_end_x(qualifiers)
            end_y = _get_end_y(qualifiers)
            if end_x is None:
                end_x = start_x
            if end_y is None:
                end_y = start_y
            eventtype = attr.get('type', {})
            period = attr.get('period', {})
            outcome = attr.get('outcomeType', {'value': 1})
            eventIdKey = 'eventId'
            if 'id' in attr:
                eventIdKey = 'id'
            minute = int(assertget(attr, 'expandedMinute'))
            second = int(attr.get('second', 0))
            event_id = int(assertget(attr, eventIdKey))
            event = dict(
                game_id=game_id,
                event_id=event_id,
                type_id=int(assertget(eventtype, 'value')),
                period_id=int(assertget(period, 'value')),
                minute=minute,
                second=second,
                timestamp=(time_start + timedelta(seconds=(minute * 60 + second))),
                player_id=int(attr.get('playerId', 0)),
                team_id=int(assertget(attr, 'teamId')),
                outcome=bool(int(outcome.get('value', 1))),
                start_x=start_x,
                start_y=start_y,
                end_x=end_x,
                end_y=end_y,
                assist=bool(int(attr.get('assist', 0))),
                keypass=bool(int(attr.get('keypass', 0))),
                qualifiers=qualifiers,
            )
            events[event_id] = event

        return events


_jsonparsers = {'f1': _F1JSONParser, 'f9': _F9JSONParser, 'f24': _F24JSONParser}

_xmlparsers = {'f7': _F7XMLParser, 'f24': _F24XMLParser}

_whoscoredparsers = {'whoscored': _WhoScoredParser}


def assertget(dictionary: Dict[str, Any], key: str) -> Any:
    value = dictionary.get(key)
    assert value is not None, 'KeyError: ' + key + ' not found in ' + str(dictionary)
    return value


def camel_to_snake(name: str) -> str:
    s1 = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', name)
    return re.sub('([a-z0-9])([A-Z])', r'\1_\2', s1).lower()


def _get_end_x(qualifiers: Dict[int, Any]) -> Optional[float]:
    try:
        # pass
        if 140 in qualifiers:
            return float(qualifiers[140])
        # blocked shot
        if 146 in qualifiers:
            return float(qualifiers[146])
        # passed the goal line
        if 102 in qualifiers:
            return float(100)
        return None
    except ValueError:
        return None


def _get_end_y(qualifiers: Dict[int, Any]) -> Optional[float]:
    try:
        # pass
        if 141 in qualifiers:
            return float(qualifiers[141])
        # blocked shot
        if 147 in qualifiers:
            return float(qualifiers[147])
        # passed the goal line
        if 102 in qualifiers:
            return float(qualifiers[102])
        return None
    except ValueError:
        return None


_eventtypesdf = pd.DataFrame(
    [
        (1, 'pass'),
        (2, 'offside pass'),
        (3, 'take on'),
        (4, 'foul'),
        (5, 'out'),
        (6, 'corner awarded'),
        (7, 'tackle'),
        (8, 'interception'),
        (9, 'turnover'),
        (10, 'save'),
        (11, 'claim'),
        (12, 'clearance'),
        (13, 'miss'),
        (14, 'post'),
        (15, 'attempt saved'),
        (16, 'goal'),
        (17, 'card'),
        (18, 'player off'),
        (19, 'player on'),
        (20, 'player retired'),
        (21, 'player returns'),
        (22, 'player becomes goalkeeper'),
        (23, 'goalkeeper becomes player'),
        (24, 'condition change'),
        (25, 'official change'),
        (26, 'unknown26'),
        (27, 'start delay'),
        (28, 'end delay'),
        (29, 'unknown29'),
        (30, 'end'),
        (31, 'unknown31'),
        (32, 'start'),
        (33, 'unknown33'),
        (34, 'team set up'),
        (35, 'player changed position'),
        (36, 'player changed jersey number'),
        (37, 'collection end'),
        (38, 'temp_goal'),
        (39, 'temp_attempt'),
        (40, 'formation change'),
        (41, 'punch'),
        (42, 'good skill'),
        (43, 'deleted event'),
        (44, 'aerial'),
        (45, 'challenge'),
        (46, 'unknown46'),
        (47, 'rescinded card'),
        (48, 'unknown46'),
        (49, 'ball recovery'),
        (50, 'dispossessed'),
        (51, 'error'),
        (52, 'keeper pick-up'),
        (53, 'cross not claimed'),
        (54, 'smother'),
        (55, 'offside provoked'),
        (56, 'shield ball opp'),
        (57, 'foul throw in'),
        (58, 'penalty faced'),
        (59, 'keeper sweeper'),
        (60, 'chance missed'),
        (61, 'ball touch'),
        (62, 'unknown62'),
        (63, 'temp_save'),
        (64, 'resume'),
        (65, 'contentious referee decision'),
        (66, 'possession data'),
        (67, '50/50'),
        (68, 'referee drop ball'),
        (69, 'failed to block'),
        (70, 'injury time announcement'),
        (71, 'coach setup'),
        (72, 'caught offside'),
        (73, 'other ball contact'),
        (74, 'blocked pass'),
        (75, 'delayed start'),
        (76, 'early end'),
        (77, 'player off pitch'),
    ],
    columns=['type_id', 'type_name'],
)


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

    actions['game_id'] = events.game_id
    actions['original_event_id'] = events.event_id.astype(object)
    actions['period_id'] = events.period_id

    actions['time_seconds'] = (
        60 * events.minute
        + events.second
        - ((events.period_id > 1) * 45 * 60)
        - ((events.period_id > 2) * 45 * 60)
        - ((events.period_id > 3) * 15 * 60)
        - ((events.period_id > 4) * 15 * 60)
    )
    actions['team_id'] = events.team_id
    actions['player_id'] = events.player_id

    for col in ['start_x', 'end_x']:
        actions[col] = events[col] / 100 * spadlconfig.field_length
    for col in ['start_y', 'end_y']:
        actions[col] = events[col] / 100 * spadlconfig.field_width

    actions['type_id'] = events[['type_name', 'outcome', 'qualifiers']].apply(_get_type_id, axis=1)
    actions['result_id'] = events[['type_name', 'outcome', 'qualifiers']].apply(
        _get_result_id, axis=1
    )
    actions['bodypart_id'] = events.qualifiers.apply(_get_bodypart_id)

    actions = (
        actions[actions.type_id != spadlconfig.actiontypes.index('non_action')]
        .sort_values(['game_id', 'period_id', 'time_seconds'])
        .reset_index(drop=True)
    )
    actions = _fix_owngoal_coordinates(actions)
    actions = _fix_direction_of_play(actions, home_team_id)
    actions = _fix_clearances(actions)
    actions['action_id'] = range(len(actions))
    actions = _add_dribbles(actions)

    for col in [c for c in actions.columns.values if c != 'original_event_id']:
        if '_id' in col:
            actions[col] = actions[col].astype(int)
    return actions


def _get_bodypart_id(qualifiers: Dict[int, Any]) -> int:
    if 15 in qualifiers:
        b = 'head'
    elif 21 in qualifiers:
        b = 'other'
    else:
        b = 'foot'
    return spadlconfig.bodyparts.index(b)


def _get_result_id(args: Tuple[str, bool, Dict[int, Any]]) -> int:
    e, outcome, q = args
    if e == 'offside pass':
        r = 'offside'  # offside
    elif e == 'foul':
        r = 'fail'
    elif e in ['attempt saved', 'miss', 'post']:
        r = 'fail'
    elif e == 'goal':
        if 28 in q:
            r = 'owngoal'  # own goal, x and y must be switched
        else:
            r = 'success'
    elif e == 'ball touch':
        r = 'fail'
    elif outcome:
        r = 'success'
    else:
        r = 'fail'
    return spadlconfig.results.index(r)


def _get_type_id(args: Tuple[str, bool, Dict[int, Any]]) -> int:  # noqa: C901
    eventname, outcome, q = args
    if eventname in ('pass', 'offside pass'):
        cross = 2 in q
        freekick = 5 in q
        corner = 6 in q
        throw_in = 107 in q
        goalkick = 124 in q
        if throw_in:
            a = 'throw_in'
        elif freekick and cross:
            a = 'freekick_crossed'
        elif freekick:
            a = 'freekick_short'
        elif corner and cross:
            a = 'corner_crossed'
        elif corner:
            a = 'corner_short'
        elif cross:
            a = 'cross'
        elif goalkick:
            a = 'goalkick'
        else:
            a = 'pass'
    elif eventname == 'take on':
        a = 'take_on'
    elif eventname == 'foul' and outcome is False:
        a = 'foul'
    elif eventname == 'tackle':
        a = 'tackle'
    elif eventname in ('interception', 'blocked pass'):
        a = 'interception'
    elif eventname in ['miss', 'post', 'attempt saved', 'goal']:
        if 9 in q:
            a = 'shot_penalty'
        elif 26 in q:
            a = 'shot_freekick'
        else:
            a = 'shot'
    elif eventname == 'save':
        a = 'keeper_save'
    elif eventname == 'claim':
        a = 'keeper_claim'
    elif eventname == 'punch':
        a = 'keeper_punch'
    elif eventname == 'keeper pick-up':
        a = 'keeper_pick_up'
    elif eventname == 'clearance':
        a = 'clearance'
    elif eventname == 'ball touch' and outcome is False:
        a = 'bad_touch'
    else:
        a = 'non_action'
    return spadlconfig.actiontypes.index(a)


def _fix_owngoal_coordinates(actions: pd.DataFrame) -> pd.DataFrame:
    owngoals_idx = (actions.result_id == spadlconfig.results.index('owngoal')) & (
        actions.type_id == spadlconfig.actiontypes.index('shot')
    )
    actions.loc[owngoals_idx, 'end_x'] = (
        spadlconfig.field_length - actions[owngoals_idx].end_x.values
    )
    actions.loc[owngoals_idx, 'end_y'] = (
        spadlconfig.field_width - actions[owngoals_idx].end_y.values
    )
    return actions
