# -*- coding: utf-8 -*-
"""Wyscout event stream data to SPADL converter."""
import glob
import os
import re
import warnings
from pathlib import Path
from typing import Any, Dict, List, Optional, Set
from urllib.parse import urlparse
from urllib.request import urlopen, urlretrieve
from zipfile import ZipFile, is_zipfile

import pandas as pd  # type: ignore
import pandera as pa
import tqdm  # type: ignore
from pandera.typing import DataFrame, DateTime, Series

from . import config as spadlconfig
from .base import (
    CompetitionSchema,
    EventDataLoader,
    EventSchema,
    GameSchema,
    MissingDataError,
    ParseError,
    PlayerSchema,
    TeamSchema,
    _add_dribbles,
    _fix_clearances,
    _fix_direction_of_play,
    min_dribble_length,
)

__all__ = [
    'PublicWyscoutLoader',
    'WyscoutLoader',
    'convert_to_actions',
    'WyscoutCompetitionSchema',
    'WyscoutGameSchema',
    'WyscoutPlayerSchema',
    'WyscoutTeamSchema',
    'WyscoutEventSchema',
]


class WyscoutCompetitionSchema(CompetitionSchema):
    """Definition of a dataframe containing a list of competitions and seasons."""

    country_name: Series[str]
    competition_gender: Series[str]


class WyscoutGameSchema(GameSchema):
    """Definition of a dataframe containing a list of games."""


class WyscoutPlayerSchema(PlayerSchema):
    """Definition of a dataframe containing the list of teams of a game."""

    firstname: Series[str]
    lastname: Series[str]
    nickname: Series[str] = pa.Field(nullable=True)
    birth_date: Series[DateTime] = pa.Field(nullable=True)


class WyscoutTeamSchema(TeamSchema):
    """Definition of a dataframe containing the list of players of a game."""

    team_name_short: Series[str]


class WyscoutEventSchema(EventSchema):
    """Definition of a dataframe containing event stream data of a game."""

    milliseconds: Series[float]
    subtype_id: Series[int]
    subtype_name: Series[str]
    positions: Series[object]
    tags: Series[object]


class PublicWyscoutLoader(EventDataLoader):
    """
    Load the public Wyscout dataset [1]_.

    Parameters
    ----------
    root : str
        Path where a local copy of the dataset is stored or where the
        downloaded dataset should be stored.
    download : bool
        Whether to force a redownload of the data.


    .. [1] Pappalardo, L., Cintia, P., Rossi, A. et al. A public data set of
        spatio-temporal match events in soccer competitions. Sci Data 6, 236
        (2019). https://doi.org/10.1038/s41597-019-0247-7
    """

    def __init__(self, root: Optional[str] = None, download: bool = False):
        if root is None:
            root = os.path.join(os.getcwd(), 'wyscout_data')
            os.makedirs(root, exist_ok=True)
        super().__init__(root, 'local')

        if download or len(os.listdir(self.root)) == 0:
            self._download_repo()

        self._index = pd.DataFrame(
            [
                {
                    'competition_id': 524,
                    'season_id': 181248,
                    'season_name': '2017/2018',
                    'db_matches': 'matches_Italy.json',
                    'db_events': 'events_Italy.json',
                },
                {
                    'competition_id': 364,
                    'season_id': 181150,
                    'season_name': '2017/2018',
                    'db_matches': 'matches_England.json',
                    'db_events': 'events_England.json',
                },
                {
                    'competition_id': 795,
                    'season_id': 181144,
                    'season_name': '2017/2018',
                    'db_matches': 'matches_Spain.json',
                    'db_events': 'events_Spain.json',
                },
                {
                    'competition_id': 412,
                    'season_id': 181189,
                    'season_name': '2017/2018',
                    'db_matches': 'matches_France.json',
                    'db_events': 'events_France.json',
                },
                {
                    'competition_id': 426,
                    'season_id': 181137,
                    'season_name': '2017/2018',
                    'db_matches': 'matches_Germany.json',
                    'db_events': 'events_Germany.json',
                },
                {
                    'competition_id': 102,
                    'season_id': 9291,
                    'season_name': '2016',
                    'db_matches': 'matches_European_Championship.json',
                    'db_events': 'events_European_Championship.json',
                },
                {
                    'competition_id': 28,
                    'season_id': 10078,
                    'season_name': '2018',
                    'db_matches': 'matches_World_Cup.json',
                    'db_events': 'events_World_Cup.json',
                },
            ]
        ).set_index(['competition_id', 'season_id'])
        self._match_index = self._create_match_index().set_index('match_id')

    def _download_repo(self) -> None:
        dataset_urls = dict(
            competitions='https://ndownloader.figshare.com/files/15073685',
            teams='https://ndownloader.figshare.com/files/15073697',
            players='https://ndownloader.figshare.com/files/15073721',
            matches='https://ndownloader.figshare.com/files/14464622',
            events='https://ndownloader.figshare.com/files/14464685',
        )
        # download and unzip Wyscout open data
        for url in tqdm.tqdm(dataset_urls.values(), desc='Downloading data'):
            url_obj = urlopen(url).geturl()
            path = Path(urlparse(url_obj).path)
            file_name = os.path.join(self.root, path.name)
            file_local, _ = urlretrieve(url_obj, file_name)
            if is_zipfile(file_local):
                with ZipFile(file_local) as zip_file:
                    zip_file.extractall(self.root)

    def _create_match_index(self) -> DataFrame:
        df_matches = pd.concat(
            [pd.DataFrame(self.get(path)) for path in glob.iglob(f'{self.root}/matches_*.json')]
        )
        df_matches.rename(
            columns={
                'wyId': 'match_id',
                'competitionId': 'competition_id',
                'seasonId': 'season_id',
            },
            inplace=True,
        )
        return pd.merge(
            df_matches[['match_id', 'competition_id', 'season_id']],
            self._index,
            on=['competition_id', 'season_id'],
            how='left',
        )

    def competitions(self) -> DataFrame[WyscoutCompetitionSchema]:
        """Return a dataframe with all available competitions and seasons.

        Returns
        -------
        pd.DataFrame
            A dataframe containing all available competitions and seasons. See
            :class:`~socceraction.spadl.wyscout.WyscoutCompetitionSchema` for the schema.
        """
        df_competitions = pd.DataFrame(self.get(os.path.join(self.root, 'competitions.json')))
        df_competitions.rename(
            columns={'wyId': 'competition_id', 'name': 'competition_name'}, inplace=True
        )
        df_competitions['country_name'] = df_competitions.apply(
            lambda x: x.area['name'] if x.area['name'] != '' else 'International', axis=1
        )
        df_competitions['competition_gender'] = 'male'
        df_competitions = pd.merge(
            df_competitions,
            self._index.reset_index()[['competition_id', 'season_id', 'season_name']],
            on='competition_id',
            how='left',
        )
        return df_competitions.reset_index()[
            [
                'competition_id',
                'season_id',
                'country_name',
                'competition_name',
                'competition_gender',
                'season_name',
            ]
        ]

    def games(self, competition_id: int, season_id: int) -> DataFrame[WyscoutGameSchema]:
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
            :class:`~socceraction.spadl.wyscout.WyscoutGameSchema` for the schema.
        """
        path = os.path.join(self.root, self._index.at[(competition_id, season_id), 'db_matches'])
        df_matches = pd.DataFrame(self.get(path))
        return convert_games(df_matches)

    def _lineups(self, game_id: int) -> List[Dict[str, Any]]:
        competition_id, season_id = self._match_index.loc[game_id, ['competition_id', 'season_id']]
        path = os.path.join(self.root, self._index.at[(competition_id, season_id), 'db_matches'])
        df_matches = pd.DataFrame(self.get(path)).set_index('wyId')
        return list(df_matches.at[game_id, 'teamsData'].values())

    def teams(self, game_id: int) -> DataFrame[WyscoutTeamSchema]:
        """Return a dataframe with both teams that participated in a game.

        Parameters
        ----------
        game_id : int
            The ID of the game.

        Returns
        -------
        pd.DataFrame
            A dataframe containing both teams. See
            :class:`~socceraction.spadl.wyscout.WyscoutTeamSchema` for the schema.
        """
        df_teams = pd.DataFrame(self.get(os.path.join(self.root, 'teams.json'))).set_index('wyId')
        df_teams_match_id = pd.DataFrame(self._lineups(game_id))['teamId']
        df_teams_match = df_teams.loc[df_teams_match_id].reset_index()
        return convert_teams(df_teams_match)

    def players(self, game_id: int) -> DataFrame[WyscoutPlayerSchema]:
        """Return a dataframe with all players that participated in a game.

        Parameters
        ----------
        game_id : int
            The ID of the game.

        Returns
        -------
        pd.DataFrame
            A dataframe containing all players. See
            :class:`~socceraction.spadl.wyscout.WyscoutPlayerSchema` for the schema.
        """
        df_players = pd.DataFrame(self.get(os.path.join(self.root, 'players.json'))).set_index(
            'wyId'
        )
        lineups = self._lineups(game_id)
        players_match = []
        for team in lineups:
            playerlist = team['formation']['lineup']
            for p in team['formation']['substitutions']:
                playerlist.append(
                    next(
                        item
                        for item in team['formation']['bench']
                        if item['playerId'] == p['playerIn']
                    )
                )
            df = pd.DataFrame(playerlist)
            df['side'] = team['side']
            df['team_id'] = team['teamId']
            players_match.append(df)
        df_players_match = (
            pd.concat(players_match)
            .rename(columns={'playerId': 'wyId'})
            .set_index('wyId')
            .join(df_players, how='left')
        )
        df_players_match.reset_index(inplace=True)
        for c in ['shortName', 'lastName', 'firstName']:
            df_players_match[c] = df_players_match[c].apply(
                lambda x: x.encode().decode('unicode-escape')
            )
        df_players_match = convert_players(df_players_match)

        # get minutes played
        competition_id, season_id = self._match_index.loc[game_id, ['competition_id', 'season_id']]
        path_events = os.path.join(
            self.root, self._index.at[(competition_id, season_id), 'db_events']
        )
        mp = get_minutes_played(lineups, self.get(path_events))
        df_players_match = pd.merge(df_players_match, mp, on='player_id', how='right')
        df_players_match['minutes_played'] = df_players_match.minutes_played.fillna(0)
        df_players_match['game_id'] = game_id
        return df_players_match

    def events(self, game_id: int) -> DataFrame[WyscoutEventSchema]:
        """Return a dataframe with the event stream of a game.

        Parameters
        ----------
        game_id : int
            The ID of the game.

        Returns
        -------
        pd.DataFrame
            A dataframe containing the event stream. See
            :class:`~socceraction.spadl.wyscout.WyscoutEventSchema` for the schema.
        """
        competition_id, season_id = self._match_index.loc[game_id, ['competition_id', 'season_id']]
        path = os.path.join(self.root, self._index.at[(competition_id, season_id), 'db_events'])
        df_events = pd.DataFrame(self.get(path)).set_index('matchId')
        return convert_events(df_events.loc[game_id].reset_index())


class WyscoutLoader(EventDataLoader):
    """Load event data either from a remote location or from a local folder.

    Parameters
    ----------
    root : str
        Root-path of the data.
    getter : str
        "remote", "local" or None. If None, custom feeds should be provided.
    feeds : dict(str, str)
        Glob pattern for each feed that should be parsed. The fefault feeds for
        a "remote" getter are::

            {
                'competitions': 'competitions',
                'seasons': 'competitions/{season_id}/seasons',
                'games': 'seasons/{season_id}/matches',
                'events': 'matches/{game_id}/events'
            }

        The fefault feeds for a "local" getter are::

            {
                'competitions': 'competitions.json',
                'seasons': 'seasons_{competition_id}.json',
                'games': 'matches_{season_id}.json',
                'events': 'matches/events_{game_id}.json',
            }

    """

    _wyscout_api: str = 'https://apirest.wyscout.com/v2/'

    def __init__(
        self,
        root: str = _wyscout_api,
        getter: str = 'remote',
        feeds: Optional[Dict[str, str]] = None,
    ):
        super().__init__(root, getter)
        if feeds is not None:
            self.feeds = feeds
        elif getter == 'remote':
            self.feeds = {
                'competitions': 'competitions',
                'seasons': 'competitions/{season_id}/seasons',
                'games': 'seasons/{season_id}/matches',
                'events': 'matches/{game_id}/events',
            }
        elif getter == 'local':
            self.feeds = {
                'competitions': 'competitions.json',
                'seasons': 'seasons_{competition_id}.json',
                'games': 'matches_{season_id}.json',
                'events': 'matches/events_{game_id}.json',
            }

    def _get_file_or_url(
        self,
        feed: str,
        competition_id: Optional[int] = None,
        season_id: Optional[int] = None,
        game_id: Optional[int] = None,
    ) -> List[str]:
        competition_id_glob = '*' if competition_id is None else competition_id
        season_id_glob = '*' if season_id is None else season_id
        game_id_glob = '*' if game_id is None else game_id
        glob_pattern = self.feeds[feed].format(
            competition_id=competition_id_glob, season_id=season_id_glob, game_id=game_id_glob
        )
        if '*' in glob_pattern:
            files = glob.glob(os.path.join(self.root, glob_pattern))
            if len(files) == 0:
                raise MissingDataError
            return files
        return [glob_pattern]

    def competitions(self) -> DataFrame[WyscoutCompetitionSchema]:
        """Return a dataframe with all available competitions and seasons.

        Returns
        -------
        pd.DataFrame
            A dataframe containing all available competitions and seasons. See
            :class:`~socceraction.spadl.wyscout.WyscoutCompetitionSchema` for the schema.
        """
        # Get all competitions
        if 'competitions' in self.feeds:
            competitions_url = self._get_file_or_url('competitions')[0]
            path = os.path.join(self.root, competitions_url)
            obj = self.get(path)
            if not isinstance(obj, dict) or 'competitions' not in obj:
                raise ParseError('{} should contain a list of competitions'.format(path))
            seasons_urls = [
                self._get_file_or_url('seasons', competition_id=c['wyId'])[0]
                for c in obj['competitions']
            ]
        else:
            seasons_urls = self._get_file_or_url('seasons')
        # Get seasons in each competition
        competitions = []
        seasons = []
        for seasons_url in seasons_urls:
            try:
                path = os.path.join(self.root, seasons_url)
                obj = self.get(path)
                if not isinstance(obj, dict) or 'competition' not in obj or 'seasons' not in obj:
                    raise ParseError(
                        '{} should contain a list of competition and list of seasons'.format(path)
                    )
                competitions.append(obj['competition'])
                seasons.extend([s['season'] for s in obj['seasons']])
            except FileNotFoundError:
                warnings.warn('File not found: {}'.format(seasons_url))
        df_competitions = convert_competitions(pd.DataFrame(competitions))
        df_seasons = convert_seasons(pd.DataFrame(seasons))
        # Merge into a single dataframe
        return pd.merge(df_competitions, df_seasons, on='competition_id')

    def games(self, competition_id: int, season_id: int) -> DataFrame[WyscoutGameSchema]:
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
            :class:`~socceraction.spadl.wyscout.WyscoutGameSchema` for the schema.
        """
        # Get all games
        if 'games' in self.feeds:
            games_url = self._get_file_or_url(
                'games', competition_id=competition_id, season_id=season_id
            )[0]
            path = os.path.join(self.root, games_url)
            obj = self.get(path)
            if not isinstance(obj, dict) or 'matches' not in obj:
                raise ParseError('{} should contain a list of teams'.format(path))
            gamedetails_urls = [
                self._get_file_or_url(
                    'events',
                    competition_id=competition_id,
                    season_id=season_id,
                    game_id=g['matchId'],
                )[0]
                for g in obj['matches']
            ]
        else:
            gamedetails_urls = self._get_file_or_url(
                'events', competition_id=competition_id, season_id=season_id
            )
        games = []
        for gamedetails_url in gamedetails_urls:
            try:
                path = os.path.join(self.root, gamedetails_url)
                obj = self.get(path)
                if not isinstance(obj, dict) or 'match' not in obj:
                    raise ParseError('{} should contain a match'.format(path))
                games.append(obj['match'])
            except FileNotFoundError:
                warnings.warn('File not found: {}'.format(gamedetails_url))
        df_games = convert_games(pd.DataFrame(games))
        return df_games

    def teams(self, game_id: int) -> DataFrame[WyscoutTeamSchema]:
        """Return a dataframe with both teams that participated in a game.

        Parameters
        ----------
        game_id : int
            The ID of the game.

        Returns
        -------
        pd.DataFrame
            A dataframe containing both teams. See
            :class:`~socceraction.spadl.wyscout.WyscoutTeamSchema` for the schema.
        """
        events_url = self._get_file_or_url('events', game_id=game_id)[0]
        path = os.path.join(self.root, events_url)
        obj = self.get(path)
        if not isinstance(obj, dict) or 'teams' not in obj:
            raise ParseError('{} should contain a list of matches'.format(path))
        teams = [t['team'] for t in obj['teams'].values() if t.get('team')]
        df_teams = convert_teams(pd.DataFrame(teams))
        return df_teams

    def players(self, game_id: int) -> DataFrame[WyscoutPlayerSchema]:
        """Return a dataframe with all players that participated in a game.

        Parameters
        ----------
        game_id : int
            The ID of the game.

        Returns
        -------
        pd.DataFrame
            A dataframe containing all players. See
            :class:`~socceraction.spadl.wyscout.WyscoutPlayerSchema` for the schema.
        """
        events_url = self._get_file_or_url('events', game_id=game_id)[0]
        path = os.path.join(self.root, events_url)
        obj = self.get(path)
        if not isinstance(obj, dict) or 'players' not in obj:
            raise ParseError('{} should contain a list of players'.format(path))
        players = [
            player['player']
            for team in obj['players'].values()
            for player in team
            if player.get('player')
        ]
        df_players = convert_players(pd.DataFrame(players).drop_duplicates('wyId'))
        df_players = pd.merge(
            df_players,
            get_minutes_played(obj['match']['teamsData'], obj['events']),
            on='player_id',
            how='right',
        )
        df_players['minutes_played'] = df_players.minutes_played.fillna(0)
        df_players['game_id'] = game_id
        return df_players

    def events(self, game_id: int) -> DataFrame[WyscoutEventSchema]:
        """Return a dataframe with the event stream of a game.

        Parameters
        ----------
        game_id : int
            The ID of the game.

        Returns
        -------
        pd.DataFrame
            A dataframe containing the event stream. See
            :class:`~socceraction.spadl.wyscout.WyscoutEventSchema` for the schema.
        """
        events_url = self._get_file_or_url('events', game_id=game_id)[0]
        path = os.path.join(self.root, events_url)
        obj = self.get(path)
        if not isinstance(obj, dict) or 'events' not in obj:
            raise ParseError('{} should contain a list of events'.format(path))
        df_events = convert_events(pd.DataFrame(obj['events']))
        return df_events


###################################
# WARNING: HERE BE DRAGONS
# This code for converting wyscout data was organically grown over a long period of time.
# It works for now, but needs to be cleaned up in the future.
# Enter at your own risk.
###################################


def convert_competitions(competitions: pd.DataFrame) -> pd.DataFrame:
    competitionsmapping = {
        'wyId': 'competition_id',
        'name': 'competition_name',
        'gender': 'competition_gender',
    }
    cols = ['competition_id', 'competition_name', 'country_name', 'competition_gender']
    competitions['country_name'] = competitions.apply(
        lambda x: x.area['name'] if x.area['name'] != '' else 'International', axis=1
    )
    competitions = competitions.rename(columns=competitionsmapping)[cols]
    return competitions


def convert_seasons(seasons: pd.DataFrame) -> pd.DataFrame:
    seasonsmapping = {
        'wyId': 'season_id',
        'name': 'season_name',
        'competitionId': 'competition_id',
    }
    cols = ['season_id', 'season_name', 'competition_id']
    seasons = seasons.rename(columns=seasonsmapping)[cols]
    return seasons


def convert_games(matches: pd.DataFrame) -> pd.DataFrame:
    gamesmapping = {
        'wyId': 'game_id',
        'dateutc': 'game_date',
        'competitionId': 'competition_id',
        'seasonId': 'season_id',
        'gameweek': 'game_day',
    }
    cols = ['game_id', 'competition_id', 'season_id', 'game_date', 'game_day']
    games = matches.rename(columns=gamesmapping)[cols]
    games['game_date'] = pd.to_datetime(games['game_date'])
    games['home_team_id'] = matches.teamsData.apply(lambda x: get_team_id(x, 'home'))
    games['away_team_id'] = matches.teamsData.apply(lambda x: get_team_id(x, 'away'))
    return games


def get_team_id(teamsData: Dict[int, Any], side: str) -> int:
    for team_id, data in teamsData.items():
        if data['side'] == side:
            return int(team_id)
    raise ValueError()


def convert_players(players: pd.DataFrame) -> pd.DataFrame:
    playermapping = {
        'wyId': 'player_id',
        'shortName': 'nickname',
        'firstName': 'firstname',
        'lastName': 'lastname',
        'birthDate': 'birth_date',
    }
    cols = ['player_id', 'nickname', 'firstname', 'lastname', 'birth_date']
    df_players = players.rename(columns=playermapping)[cols]
    df_players['player_name'] = df_players[['firstname', 'lastname']].agg(' '.join, axis=1)
    df_players['birth_date'] = pd.to_datetime(df_players['birth_date'])
    return df_players


def convert_teams(teams: pd.DataFrame) -> pd.DataFrame:
    teammapping = {
        'wyId': 'team_id',
        'name': 'team_name_short',
        'officialName': 'team_name',
    }
    cols = ['team_id', 'team_name_short', 'team_name']
    return teams.rename(columns=teammapping)[cols]


def convert_events(raw_events: pd.DataFrame) -> pd.DataFrame:
    eventmapping = {
        'id': 'event_id',
        'match_id': 'game_id',
        'event_name': 'type_name',
        'sub_event_name': 'subtype_name',
    }
    cols = [
        'event_id',
        'game_id',
        'period_id',
        'milliseconds',
        'team_id',
        'player_id',
        'type_id',
        'type_name',
        'subtype_id',
        'subtype_name',
        'positions',
        'tags',
    ]
    events = raw_events.copy()
    # Camel case to snake case column names
    pattern = re.compile(r'(?<!^)(?=[A-Z])')
    events.columns = [pattern.sub('_', c).lower() for c in events.columns]
    #
    events['type_id'] = (
        pd.to_numeric(
            events['event_id'] if 'event_id' in events.columns else None, errors='coerce'
        )
        .fillna(0)
        .astype(int)
    )
    del events['event_id']
    events['subtype_id'] = (
        pd.to_numeric(
            events['sub_event_id'] if 'sub_event_id' in events.columns else None, errors='coerce'
        )
        .fillna(0)
        .astype(int)
    )
    del events['sub_event_id']
    events['period_id'] = events.match_period.apply(lambda x: wyscout_periods[x])
    events['milliseconds'] = events.event_sec * 1000
    return events.rename(columns=eventmapping)[cols]


def get_minutes_played(teamsData: pd.DataFrame, events: pd.DataFrame) -> pd.DataFrame:
    periods_ts = {i: [0] for i in range(6)}
    for e in events:
        period_id = wyscout_periods[e['matchPeriod']]
        periods_ts[period_id].append(e['eventSec'])
    duration = int(sum([max(periods_ts[i]) / 60 for i in range(5)]))
    playergames: Dict[int, Dict[str, Any]] = {}
    if isinstance(teamsData, dict):
        teamsData = list(teamsData.values())
    for teamData in teamsData:
        formation = teamData.get('formation', {})
        pg = {
            player['playerId']: {
                'team_id': teamData['teamId'],
                'player_id': player['playerId'],
                'jersey_number': player.get('shirtNumber', 0),
                'minutes_played': duration,
                'is_starter': True,
            }
            for player in formation.get('lineup', [])
        }

        substitutions = formation.get('substitutions', [])

        if substitutions != 'null':
            for substitution in substitutions:
                substitute = {
                    'team_id': teamData['teamId'],
                    'player_id': substitution['playerIn'],
                    'jersey_number': next(
                        p.get('shirtNumber', 0)
                        for p in formation.get('bench', [])
                        if p['playerId'] == substitution['playerIn']
                    ),
                    'minutes_played': duration - substitution['minute'],
                    'is_starter': False,
                }
                pg[substitution['playerIn']] = substitute
                pg[substitution['playerOut']]['minutes_played'] = substitution['minute']
        playergames = {**playergames, **pg}
    return pd.DataFrame(playergames.values())


def convert_to_actions(events: pd.DataFrame, home_team_id: int) -> pd.DataFrame:
    """
    Convert Wyscout events to SPADL actions.

    Parameters
    ----------
    events : pd.DataFrame
        DataFrame containing Wyscout events from a single game.
    home_team_id : int
        ID of the home team in the corresponding game.

    Returns
    -------
    actions : pd.DataFrame
        DataFrame with corresponding SPADL actions.

    """
    events = pd.concat([events, get_tagsdf(events)], axis=1)
    events = make_new_positions(events)
    events = fix_wyscout_events(events)
    actions = create_df_actions(events)
    actions = fix_actions(actions)
    actions = _fix_direction_of_play(actions, home_team_id)
    actions = _fix_clearances(actions)
    actions['action_id'] = range(len(actions))
    actions = _add_dribbles(actions)
    for col in [c for c in actions.columns.values if c != 'original_event_id']:
        if '_id' in col:
            actions[col] = actions[col].astype(int)
    return actions


def get_tag_set(tags: List[Dict[str, Any]]) -> Set[int]:
    return {tag['id'] for tag in tags}


def get_tagsdf(events: pd.DataFrame) -> pd.DataFrame:
    tags = events.tags.apply(get_tag_set)
    tagsdf = pd.DataFrame()
    for (tag_id, column) in wyscout_tags:
        tagsdf[column] = tags.apply(lambda x, tag=tag_id: tag in x)
    return tagsdf


wyscout_periods = {'1H': 1, '2H': 2, 'E1': 3, 'E2': 4, 'P': 5}


wyscout_tags = [
    (101, 'goal'),
    (102, 'own_goal'),
    (301, 'assist'),
    (302, 'key_pass'),
    (1901, 'counter_attack'),
    (401, 'left_foot'),
    (402, 'right_foot'),
    (403, 'head/body'),
    (1101, 'direct'),
    (1102, 'indirect'),
    (2001, 'dangerous_ball_lost'),
    (2101, 'blocked'),
    (801, 'high'),
    (802, 'low'),
    (1401, 'interception'),
    (1501, 'clearance'),
    (201, 'opportunity'),
    (1301, 'feint'),
    (1302, 'missed_ball'),
    (501, 'free_space_right'),
    (502, 'free_space_left'),
    (503, 'take_on_left'),
    (504, 'take_on_right'),
    (1601, 'sliding_tackle'),
    (601, 'anticipated'),
    (602, 'anticipation'),
    (1701, 'red_card'),
    (1702, 'yellow_card'),
    (1703, 'second_yellow_card'),
    (1201, 'position_goal_low_center'),
    (1202, 'position_goal_low_right'),
    (1203, 'position_goal_mid_center'),
    (1204, 'position_goal_mid_left'),
    (1205, 'position_goal_low_left'),
    (1206, 'position_goal_mid_right'),
    (1207, 'position_goal_high_center'),
    (1208, 'position_goal_high_left'),
    (1209, 'position_goal_high_right'),
    (1210, 'position_out_low_right'),
    (1211, 'position_out_mid_left'),
    (1212, 'position_out_low_left'),
    (1213, 'position_out_mid_right'),
    (1214, 'position_out_high_center'),
    (1215, 'position_out_high_left'),
    (1216, 'position_out_high_right'),
    (1217, 'position_post_low_right'),
    (1218, 'position_post_mid_left'),
    (1219, 'position_post_low_left'),
    (1220, 'position_post_mid_right'),
    (1221, 'position_post_high_center'),
    (1222, 'position_post_high_left'),
    (1223, 'position_post_high_right'),
    (901, 'through'),
    (1001, 'fairplay'),
    (701, 'lost'),
    (702, 'neutral'),
    (703, 'won'),
    (1801, 'accurate'),
    (1802, 'not_accurate'),
]


def make_position_vars(event_id: int, positions: List[Dict[str, Optional[float]]]) -> pd.Series:
    if len(positions) == 2:  # if less than 2 then action is removed
        start_x = positions[0]['x']
        start_y = positions[0]['y']
        end_x = positions[1]['x']
        end_y = positions[1]['y']
    elif len(positions) == 1:
        start_x = positions[0]['x']
        start_y = positions[0]['y']
        end_x = start_x
        end_y = start_y
    else:
        start_x = None
        start_y = None
        end_x = None
        end_y = None
    return pd.Series([event_id, start_x, start_y, end_x, end_y])


def make_new_positions(events_df: pd.DataFrame) -> pd.DataFrame:
    new_positions = events_df[['event_id', 'positions']].apply(
        lambda x: make_position_vars(x[0], x[1]), axis=1
    )
    new_positions.columns = ['event_id', 'start_x', 'start_y', 'end_x', 'end_y']
    events_df = pd.merge(events_df, new_positions, left_on='event_id', right_on='event_id')
    events_df = events_df.drop('positions', axis=1)
    return events_df


def fix_wyscout_events(df_events: pd.DataFrame) -> pd.DataFrame:
    """Perform some fixes on the Wyscout events such that the spadl action dataframe can be built.

    Parameters
    ----------
    df_events : pd.DataFrame
        Wyscout event dataframe

    Returns
    -------
    pd.DataFrame
        Wyscout event dataframe with an extra column 'offside'
    """
    df_events = create_shot_coordinates(df_events)
    df_events = convert_duels(df_events)
    df_events = insert_interception_passes(df_events)
    df_events = add_offside_variable(df_events)
    df_events = convert_touches(df_events)
    return df_events


def create_shot_coordinates(df_events: pd.DataFrame) -> pd.DataFrame:
    """Create shot coordinates (estimates) from the Wyscout tags.

    Parameters
    ----------
    df_events : pd.DataFrame
        Wyscout event dataframe

    Returns
    -------
    pd.DataFrame
        Wyscout event dataframe with end coordinates for shots
    """
    goal_center_idx = (
        df_events['position_goal_low_center']
        | df_events['position_goal_mid_center']
        | df_events['position_goal_high_center']
    )
    df_events.loc[goal_center_idx, 'end_x'] = 100.0
    df_events.loc[goal_center_idx, 'end_y'] = 50.0

    goal_right_idx = (
        df_events['position_goal_low_right']
        | df_events['position_goal_mid_right']
        | df_events['position_goal_high_right']
    )
    df_events.loc[goal_right_idx, 'end_x'] = 100.0
    df_events.loc[goal_right_idx, 'end_y'] = 55.0

    goal_left_idx = (
        df_events['position_goal_mid_left']
        | df_events['position_goal_low_left']
        | df_events['position_goal_high_left']
    )
    df_events.loc[goal_left_idx, 'end_x'] = 100.0
    df_events.loc[goal_left_idx, 'end_y'] = 45.0

    out_center_idx = df_events['position_out_high_center'] | df_events['position_post_high_center']
    df_events.loc[out_center_idx, 'end_x'] = 100.0
    df_events.loc[out_center_idx, 'end_y'] = 50.0

    out_right_idx = (
        df_events['position_out_low_right']
        | df_events['position_out_mid_right']
        | df_events['position_out_high_right']
    )
    df_events.loc[out_right_idx, 'end_x'] = 100.0
    df_events.loc[out_right_idx, 'end_y'] = 60.0

    out_left_idx = (
        df_events['position_out_mid_left']
        | df_events['position_out_low_left']
        | df_events['position_out_high_left']
    )
    df_events.loc[out_left_idx, 'end_x'] = 100.0
    df_events.loc[out_left_idx, 'end_y'] = 40.0

    post_left_idx = (
        df_events['position_post_mid_left']
        | df_events['position_post_low_left']
        | df_events['position_post_high_left']
    )
    df_events.loc[post_left_idx, 'end_x'] = 100.0
    df_events.loc[post_left_idx, 'end_y'] = 55.38

    post_right_idx = (
        df_events['position_post_low_right']
        | df_events['position_post_mid_right']
        | df_events['position_post_high_right']
    )
    df_events.loc[post_right_idx, 'end_x'] = 100.0
    df_events.loc[post_right_idx, 'end_y'] = 44.62

    blocked_idx = df_events['blocked']
    df_events.loc[blocked_idx, 'end_x'] = df_events.loc[blocked_idx, 'start_x']
    df_events.loc[blocked_idx, 'end_y'] = df_events.loc[blocked_idx, 'start_y']

    return df_events


def convert_duels(df_events: pd.DataFrame) -> pd.DataFrame:
    """Convert duel events.

    This function converts Wyscout duels that end with the ball out of field
    (subtype_id 50) into a pass for the player winning the duel to the location
    of where the ball went out of field. The remaining duels are removed as
    they are not on-the-ball actions.

    Parameters
    ----------
    df_events : pd.DataFrame
        Wyscout event dataframe

    Returns
    -------
    pd.DataFrame
        Wyscout event dataframe in which the duels are either removed or
        transformed into a pass
    """
    # Shift events dataframe by one and two time steps
    df_events1 = df_events.shift(-1)
    df_events2 = df_events.shift(-2)

    # Define selector for same period id
    selector_same_period = df_events['period_id'] == df_events2['period_id']

    # Define selector for duels that are followed by an 'out of field' event
    selector_duel_out_of_field = (
        (df_events['type_id'] == 1)
        & (df_events1['type_id'] == 1)
        & (df_events2['subtype_id'] == 50)
        & selector_same_period
    )

    # Define selectors for current time step
    selector0_duel_won = selector_duel_out_of_field & (
        df_events['team_id'] != df_events2['team_id']
    )
    selector0_duel_won_air = selector0_duel_won & (df_events['subtype_id'] == 10)
    selector0_duel_won_not_air = selector0_duel_won & (df_events['subtype_id'] != 10)

    # Define selectors for next time step
    selector1_duel_won = selector_duel_out_of_field & (
        df_events1['team_id'] != df_events2['team_id']
    )
    selector1_duel_won_air = selector1_duel_won & (df_events1['subtype_id'] == 10)
    selector1_duel_won_not_air = selector1_duel_won & (df_events1['subtype_id'] != 10)

    # Aggregate selectors
    selector_duel_won = selector0_duel_won | selector1_duel_won
    selector_duel_won_air = selector0_duel_won_air | selector1_duel_won_air
    selector_duel_won_not_air = selector0_duel_won_not_air | selector1_duel_won_not_air

    # Set types and subtypes
    df_events.loc[selector_duel_won, 'type_id'] = 8
    df_events.loc[selector_duel_won_air, 'subtype_id'] = 82
    df_events.loc[selector_duel_won_not_air, 'subtype_id'] = 85

    # set end location equal to ball out of field location
    df_events.loc[selector_duel_won, 'accurate'] = False
    df_events.loc[selector_duel_won, 'not_accurate'] = True
    df_events.loc[selector_duel_won, 'end_x'] = 100 - df_events2.loc[selector_duel_won, 'start_x']
    df_events.loc[selector_duel_won, 'end_y'] = 100 - df_events2.loc[selector_duel_won, 'start_y']

    # df_events.loc[selector_duel_won, 'end_x'] = df_events2.loc[selector_duel_won, 'start_x']
    # df_events.loc[selector_duel_won, 'end_y'] = df_events2.loc[selector_duel_won, 'start_y']

    # Define selector for ground attacking duels with take on
    selector_attacking_duel = df_events['subtype_id'] == 11
    selector_take_on = (df_events['take_on_left']) | (df_events['take_on_right'])
    selector_att_duel_take_on = selector_attacking_duel & selector_take_on

    # Set take ons type to 0
    df_events.loc[selector_att_duel_take_on, 'type_id'] = 0

    # Set sliding tackles type to 0
    df_events.loc[df_events['sliding_tackle'], 'type_id'] = 0

    # Remove the remaining duels
    df_events = df_events[df_events['type_id'] != 1]

    # Reset the index
    df_events = df_events.reset_index(drop=True)

    return df_events


def insert_interception_passes(df_events: pd.DataFrame) -> pd.DataFrame:
    """Insert interception actions before passes.

    This function converts passes (type_id 8) that are also interceptions
    (tag interception) in the Wyscout event data into two separate events,
    first an interception and then a pass.

    Parameters
    ----------
    df_events : pd.DataFrame
        Wyscout event dataframe

    Returns
    -------
    pd.DataFrame
        Wyscout event dataframe in which passes that were also denoted as
        interceptions in the Wyscout notation are transformed into two events
    """
    df_events_interceptions = df_events[
        df_events['interception'] & (df_events['type_id'] == 8)
    ].copy()

    if not df_events_interceptions.empty:
        df_events_interceptions.loc[:, [t[1] for t in wyscout_tags]] = False
        df_events_interceptions['interception'] = True
        df_events_interceptions['type_id'] = 0
        df_events_interceptions['subtype_id'] = 0
        df_events_interceptions[['end_x', 'end_y']] = df_events_interceptions[
            ['start_x', 'start_y']
        ]

        df_events = pd.concat([df_events_interceptions, df_events], ignore_index=True)
        df_events = df_events.sort_values(['period_id', 'milliseconds'])
        df_events = df_events.reset_index(drop=True)

    return df_events


def add_offside_variable(df_events: pd.DataFrame) -> pd.DataFrame:
    """Attach offside events to the previous action.

    This function removes the offside events in the Wyscout event data and adds
    sets offside to 1 for the previous event (if this was a passing event)

    Parameters
    ----------
    df_events : pd.DataFrame
        Wyscout event dataframe

    Returns
    -------
    pd.DataFrame
        Wyscout event dataframe with an extra column 'offside'
    """
    # Create a new column for the offside variable
    df_events['offside'] = 0

    # Shift events dataframe by one timestep
    df_events1 = df_events.shift(-1)

    # Select offside passes
    selector_offside = (df_events1['type_id'] == 6) & (df_events['type_id'] == 8)

    # Set variable 'offside' to 1 for all offside passes
    df_events.loc[selector_offside, 'offside'] = 1

    # Remove offside events
    df_events = df_events[df_events['type_id'] != 6]

    # Reset index
    df_events = df_events.reset_index(drop=True)

    return df_events


def convert_touches(df_events: pd.DataFrame) -> pd.DataFrame:
    """Convert touch events to dribbles or passes.

    This function converts the Wyscout 'touch' event (sub_type_id 72) into either
    a dribble or a pass (accurate or not depending on receiver)

    Parameters
    ----------
    df_events : pd.DataFrame
        Wyscout event dataframe

    Returns
    -------
    pd.DataFrame
        Wyscout event dataframe without any touch events
    """
    df_events1 = df_events.shift(-1)

    selector_touch = (df_events['subtype_id'] == 72) & ~df_events['interception']

    selector_same_player = df_events['player_id'] == df_events1['player_id']
    selector_same_team = df_events['team_id'] == df_events1['team_id']

    # selector_touch_same_player = selector_touch & selector_same_player
    selector_touch_same_team = selector_touch & ~selector_same_player & selector_same_team
    selector_touch_other = selector_touch & ~selector_same_player & ~selector_same_team

    same_x = abs(df_events['end_x'] - df_events1['start_x']) < min_dribble_length
    same_y = abs(df_events['end_y'] - df_events1['start_y']) < min_dribble_length
    same_loc = same_x & same_y

    # df_events.loc[selector_touch_same_player & same_loc, 'subtype_id'] = 70
    # df_events.loc[selector_touch_same_player & same_loc, 'accurate'] = True
    # df_events.loc[selector_touch_same_player & same_loc, 'not_accurate'] = False

    df_events.loc[selector_touch_same_team & same_loc, 'type_id'] = 8
    df_events.loc[selector_touch_same_team & same_loc, 'subtype_id'] = 85
    df_events.loc[selector_touch_same_team & same_loc, 'accurate'] = True
    df_events.loc[selector_touch_same_team & same_loc, 'not_accurate'] = False

    df_events.loc[selector_touch_other & same_loc, 'type_id'] = 8
    df_events.loc[selector_touch_other & same_loc, 'subtype_id'] = 85
    df_events.loc[selector_touch_other & same_loc, 'accurate'] = False
    df_events.loc[selector_touch_other & same_loc, 'not_accurate'] = True

    return df_events


def create_df_actions(df_events: pd.DataFrame) -> pd.DataFrame:
    """Create the SciSports action dataframe.

    Parameters
    ----------
    df_events : pd.DataFrame
        Wyscout event dataframe

    Returns
    -------
    pd.DataFrame
        SciSports action dataframe
    """
    df_events['time_seconds'] = df_events['milliseconds'] / 1000
    df_actions = df_events[
        [
            'game_id',
            'period_id',
            'time_seconds',
            'team_id',
            'player_id',
            'start_x',
            'start_y',
            'end_x',
            'end_y',
        ]
    ].copy()
    df_actions['original_event_id'] = df_events['event_id'].astype(object)
    df_actions['bodypart_id'] = df_events.apply(determine_bodypart_id, axis=1)
    df_actions['type_id'] = df_events.apply(determine_type_id, axis=1)
    df_actions['result_id'] = df_events.apply(determine_result_id, axis=1)

    df_actions = remove_non_actions(df_actions)  # remove all non-actions left

    return df_actions


def determine_bodypart_id(event: pd.DataFrame) -> pd.DataFrame:
    """Determint eht body part for each action.

    Parameters
    ----------
    event : pd.Series
        Wyscout event Series

    Returns
    -------
    int
        id of the body part used for the action
    """
    if event['subtype_id'] in [81, 36, 21, 90, 91]:
        body_part = 'other'
    elif event['subtype_id'] == 82:
        body_part = 'head'
    elif event['type_id'] == 10 and event['head/body']:
        body_part = 'head/other'
    else:  # all other cases
        body_part = 'foot'
    return spadlconfig.bodyparts.index(body_part)


def determine_type_id(event: pd.DataFrame) -> pd.DataFrame:  # noqa: C901
    """Determine the type of each action.

    This function transforms the Wyscout events, sub_events and tags
    into the corresponding SciSports action type

    Parameters
    ----------
    event : pd.Series
        A series from the Wyscout event dataframe

    Returns
    -------
    str
        A string representing the SciSports action type
    """
    if event['type_id'] == 8:
        if event['subtype_id'] == 80:
            action_type = 'cross'
        else:
            action_type = 'pass'
    elif event['subtype_id'] == 36:
        action_type = 'throw_in'
    elif event['subtype_id'] == 30:
        if event['high']:
            action_type = 'corner_crossed'
        else:
            action_type = 'corner_short'
    elif event['subtype_id'] == 32:
        action_type = 'freekick_crossed'
    elif event['subtype_id'] == 31:
        action_type = 'freekick_short'
    elif event['subtype_id'] == 34:
        action_type = 'goalkick'
    elif event['type_id'] == 2:
        action_type = 'foul'
    elif event['type_id'] == 10:
        action_type = 'shot'
    elif event['subtype_id'] == 35:
        action_type = 'shot_penalty'
    elif event['subtype_id'] == 33:
        action_type = 'shot_freekick'
    elif event['type_id'] == 9:
        action_type = 'keeper_save'
    elif event['subtype_id'] == 71:
        action_type = 'clearance'
    elif event['subtype_id'] == 72 and (event['not_accurate'] or event['own_goal']):
        action_type = 'bad_touch'
    elif event['subtype_id'] == 70:
        action_type = 'dribble'
    elif event['take_on_left'] or event['take_on_right']:
        action_type = 'take_on'
    elif event['sliding_tackle']:
        action_type = 'tackle'
    elif event['interception'] and (event['subtype_id'] in [0, 10, 11, 12, 13, 72]):
        action_type = 'interception'
    else:
        action_type = 'non_action'
    return spadlconfig.actiontypes.index(action_type)


def determine_result_id(event: pd.DataFrame) -> pd.DataFrame:  # noqa: C901
    """Determine the result of each event.

    Parameters
    ----------
    event : pd.Series
        Wyscout event Series

    Returns
    -------
    int
        result of the action
    """
    if event['offside'] == 1:
        return 2
    if event['type_id'] == 2:  # foul
        return 1
    if event['goal']:  # goal
        return 1
    if event['own_goal']:  # own goal
        return 3
    if event['subtype_id'] in [100, 33, 35]:  # no goal, so 0
        return 0
    if event['accurate']:
        return 1
    if event['not_accurate']:
        return 0
    if (
        event['interception'] or event['clearance'] or event['subtype_id'] == 71
    ):  # interception or clearance always success
        return 1
    if event['type_id'] == 9:  # keeper save always success
        return 1
    # no idea, assume it was successful
    return 1


def remove_non_actions(df_actions: pd.DataFrame) -> pd.DataFrame:
    """Remove the remaining non_actions from the action dataframe.

    Parameters
    ----------
    df_actions : pd.DataFrame
        SciSports action dataframe

    Returns
    -------
    pd.DataFrame
        SciSports action dataframe without non-actions
    """
    df_actions = df_actions[df_actions['type_id'] != spadlconfig.actiontypes.index('non_action')]
    # remove remaining ball out of field, whistle and goalkeeper from line
    df_actions = df_actions.reset_index(drop=True)
    return df_actions


def fix_actions(df_actions: pd.DataFrame) -> pd.DataFrame:
    """Fix the generated actions.

    Parameters
    ----------
    df_events : pd.DataFrame
        Wyscout event dataframe

    Returns
    -------
    pd.DataFrame
        Wyscout event dataframe with end coordinates for shots
    """
    df_actions['start_x'] = df_actions['start_x'] * spadlconfig.field_length / 100
    df_actions['start_y'] = (
        (100 - df_actions['start_y']) * spadlconfig.field_width / 100
    )  # y is from top to bottom in Wyscout
    df_actions['end_x'] = df_actions['end_x'] * spadlconfig.field_length / 100
    df_actions['end_y'] = (
        (100 - df_actions['end_y']) * spadlconfig.field_width / 100
    )  # y is from top to bottom in Wyscout
    df_actions = fix_goalkick_coordinates(df_actions)
    df_actions = adjust_goalkick_result(df_actions)
    df_actions = fix_foul_coordinates(df_actions)
    df_actions = fix_keeper_save_coordinates(df_actions)
    df_actions = remove_keeper_goal_actions(df_actions)
    df_actions.reset_index(drop=True, inplace=True)

    return df_actions


def fix_goalkick_coordinates(df_actions: pd.DataFrame) -> pd.DataFrame:
    """Fix goalkick coordinates.

    This function sets the goalkick start coordinates to (5,34)

    Parameters
    ----------
    df_actions : pd.DataFrame
        SciSports action dataframe with start coordinates for goalkicks in the
        corner of the pitch

    Returns
    -------
    pd.DataFrame
        SciSports action dataframe including start coordinates for goalkicks
    """
    goalkicks_idx = df_actions['type_id'] == spadlconfig.actiontypes.index('goalkick')
    df_actions.loc[goalkicks_idx, 'start_x'] = 5.0
    df_actions.loc[goalkicks_idx, 'start_y'] = 34.0

    return df_actions


def fix_foul_coordinates(df_actions: pd.DataFrame) -> pd.DataFrame:
    """Fix fould coordinates.

    This function sets foul end coordinates equal to the foul start coordinates

    Parameters
    ----------
    df_actions : pd.DataFrame
        SciSports action dataframe with no end coordinates for fouls

    Returns
    -------
    pd.DataFrame
        SciSports action dataframe including start coordinates for goalkicks
    """
    fouls_idx = df_actions['type_id'] == spadlconfig.actiontypes.index('foul')
    df_actions.loc[fouls_idx, 'end_x'] = df_actions.loc[fouls_idx, 'start_x']
    df_actions.loc[fouls_idx, 'end_y'] = df_actions.loc[fouls_idx, 'start_y']

    return df_actions


def fix_keeper_save_coordinates(df_actions: pd.DataFrame) -> pd.DataFrame:
    """Fix keeper save coordinates.

    This function sets keeper_save start coordinates equal to
    keeper_save end coordinates. It also inverts the shot coordinates to the own goal.

    Parameters
    ----------
    df_actions : pd.DataFrame
        SciSports action dataframe with start coordinates in the corner of the pitch

    Returns
    -------
    pd.DataFrame
        SciSports action dataframe with correct keeper_save coordinates
    """
    saves_idx = df_actions['type_id'] == spadlconfig.actiontypes.index('keeper_save')
    # invert the coordinates
    df_actions.loc[saves_idx, 'end_x'] = 105.0 - df_actions.loc[saves_idx, 'end_x']
    df_actions.loc[saves_idx, 'end_y'] = 68.0 - df_actions.loc[saves_idx, 'end_y']
    # set start coordinates equal to start coordinates
    df_actions.loc[saves_idx, 'start_x'] = df_actions.loc[saves_idx, 'end_x']
    df_actions.loc[saves_idx, 'start_y'] = df_actions.loc[saves_idx, 'end_y']

    return df_actions


def remove_keeper_goal_actions(df_actions: pd.DataFrame) -> pd.DataFrame:
    """Remove keeper goal-saving actions.

    This function removes keeper_save actions that appear directly after a goal

    Parameters
    ----------
    df_actions : pd.DataFrame
        SciSports action dataframe with keeper actions directly after a goal

    Returns
    -------
    pd.DataFrame
        SciSports action dataframe without keeper actions directly after a goal
    """
    prev_actions = df_actions.shift(1)
    same_phase = prev_actions.time_seconds + 10 > df_actions.time_seconds
    shot_goals = (prev_actions.type_id == spadlconfig.actiontypes.index('shot')) & (
        prev_actions.result_id == 1
    )
    penalty_goals = (prev_actions.type_id == spadlconfig.actiontypes.index('shot_penalty')) & (
        prev_actions.result_id == 1
    )
    freekick_goals = (prev_actions.type_id == spadlconfig.actiontypes.index('shot_freekick')) & (
        prev_actions.result_id == 1
    )
    goals = shot_goals | penalty_goals | freekick_goals
    keeper_save = df_actions['type_id'] == spadlconfig.actiontypes.index('keeper_save')
    goals_keepers_idx = same_phase & goals & keeper_save
    df_actions = df_actions.drop(df_actions.index[goals_keepers_idx])
    df_actions = df_actions.reset_index(drop=True)

    return df_actions


def adjust_goalkick_result(df_actions: pd.DataFrame) -> pd.DataFrame:
    """Adjust goalkick results.

    This function adjusts goalkick results depending on whether
    the next action is performed by the same team or not

    Parameters
    ----------
    df_actions : pd.DataFrame
        SciSports action dataframe with incorrect goalkick results

    Returns
    -------
    pd.DataFrame
        SciSports action dataframe with correct goalkick results
    """
    nex_actions = df_actions.shift(-1)
    goalkicks = df_actions['type_id'] == spadlconfig.actiontypes.index('goalkick')
    same_team = df_actions['team_id'] == nex_actions['team_id']
    accurate = same_team & goalkicks
    not_accurate = ~same_team & goalkicks
    df_actions.loc[accurate, 'result_id'] = 1
    df_actions.loc[not_accurate, 'result_id'] = 0

    return df_actions
