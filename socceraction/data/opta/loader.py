"""Implements serializers for Opta data."""
import copy
import glob
import os
import re
import warnings
from typing import Any, Dict, Mapping, Type, Union

import pandas as pd  # type: ignore
from pandera.typing import DataFrame

from socceraction.data.base import EventDataLoader

from .parsers import (
    F1JSONParser,
    F7XMLParser,
    F9JSONParser,
    F24JSONParser,
    F24XMLParser,
    MA1JSONParser,
    MA3JSONParser,
    OptaParser,
    WhoScoredParser,
)
from .schema import (
    OptaCompetitionSchema,
    OptaEventSchema,
    OptaGameSchema,
    OptaPlayerSchema,
    OptaTeamSchema,
)

_jsonparsers = {
    'f1': F1JSONParser,
    'f9': F9JSONParser,
    'f24': F24JSONParser,
    'ma1': MA1JSONParser,
    'ma3': MA3JSONParser,
}

_xmlparsers = {
    'f7': F7XMLParser,
    'f24': F24XMLParser,
}

_whoscoredparsers = {
    'whoscored': WhoScoredParser,
}

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


def _deepupdate(target: Dict[Any, Any], src: Dict[Any, Any]) -> None:
    """Deep update target dict with src.

    For each k,v in src: if k doesn't exist in target, it is deep copied from
    src to target. Otherwise, if v is a list, target[k] is extended with
    src[k]. If v is a set, target[k] is updated with v, If v is a dict,
    recursively deep-update it.

    Parameters
    ----------
    target: dict
        The original dictionary which is updated.
    src: dict
        The dictionary with which `target` is updated.

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


def _extract_ids_from_path(path: str, pattern: str) -> Dict[str, Union[str, int]]:
    regex = re.compile(
        '.+?'
        + re.escape(pattern)
        .replace(r'\{competition_id\}', r'(?P<competition_id>[a-zA-Z0-9]+)')
        .replace(r'\{season_id\}', r'(?P<season_id>[a-zA-Z0-9]+)')
        .replace(r'\{game_id\}', r'(?P<game_id>[a-zA-Z0-9]+)')
    )
    m = re.match(regex, path)
    if m is None:
        raise ValueError('The filepath {} does not match the format {}.'.format(path, pattern))
    ids = m.groupdict()
    return {k: int(v) if v.isdigit() else v for k, v in ids.items()}


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

    Raises
    ------
    ValueError
        If an invalid parser is provided.
    """

    def __init__(
        self, root: str, feeds: Dict[str, str], parser: Union[str, Mapping[str, Type[OptaParser]]]
    ) -> None:
        self.root = root
        if parser == 'json':
            self.parsers = self._get_parsers_for_feeds(_jsonparsers, feeds)
        elif parser == 'xml':
            self.parsers = self._get_parsers_for_feeds(_xmlparsers, feeds)
        elif parser == 'whoscored':
            self.parsers = self._get_parsers_for_feeds(_whoscoredparsers, feeds)
        elif isinstance(parser, dict):
            self.parsers = self._get_parsers_for_feeds(parser, feeds)
        else:
            raise ValueError('Invalid parser provided.')
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
        return pd.DataFrame(list(data.values())).pipe(DataFrame[OptaCompetitionSchema])

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
        return pd.DataFrame(list(data.values())).pipe(DataFrame[OptaGameSchema])

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
        return pd.DataFrame(list(data.values())).pipe(DataFrame[OptaTeamSchema])

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
        return df_players.pipe(DataFrame[OptaPlayerSchema])

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
        return events.pipe(DataFrame[OptaEventSchema])
