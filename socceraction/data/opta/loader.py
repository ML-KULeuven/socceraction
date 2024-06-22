"""Implements serializers for Opta data."""

import copy
import datetime
import glob
import os
import re
import warnings
from collections.abc import Mapping
from pathlib import Path
from typing import Any, Optional, Union, cast

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
    "f1": F1JSONParser,
    "f9": F9JSONParser,
    "f24": F24JSONParser,
    "ma1": MA1JSONParser,
    "ma3": MA3JSONParser,
}

_xmlparsers = {
    "f7": F7XMLParser,
    "f24": F24XMLParser,
}

_statsperformparsers = {
    "ma1": MA1JSONParser,
    "ma3": MA3JSONParser,
}

_whoscoredparsers = {
    "whoscored": WhoScoredParser,
}

_eventtypesdf = pd.DataFrame(
    [
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
        (78, "temp card"),
        (79, "coverage interruption"),
        (80, "drop of ball"),
        (81, "obstacle"),
        (83, "attempted tackle"),
        (84, "deleted after review"),
        (10000, "offside given"),  # Seems specific to WhoScored
    ],
    columns=["type_id", "type_name"],
)


def _deepupdate(target: dict[Any, Any], src: dict[Any, Any]) -> None:
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
    >>> t = {'name': 'ferry', 'hobbies': ['programming', 'sci-fi']}
    >>> deepupdate(t, {'hobbies': ['gaming']})
    >>> print(t)
    {'name': 'ferry', 'hobbies': ['programming', 'sci-fi', 'gaming']}
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


def _extract_ids_from_path(path: str, pattern: str) -> dict[str, Union[str, int]]:
    regex = re.compile(
        ".+?"
        + re.escape(pattern)
        .replace(r"\{competition_id\}", r"(?P<competition_id>[a-zA-Zà-üÀ-Ü0-9-_ ]+)")
        .replace(r"\{season_id\}", r"(?P<season_id>[a-zA-Zà-üÀ-Ü0-9-_ ]+)")
        .replace(r"\{game_id\}", r"(?P<game_id>[a-zA-Zà-üÀ-Ü0-9-_ ]+)")
    )
    m = re.match(regex, path)
    if m is None:
        raise ValueError(f"The filepath {path} does not match the format {pattern}.")
    ids = m.groupdict()
    return {k: int(v) if v.isdigit() else v for k, v in ids.items()}


class OptaLoader(EventDataLoader):
    """Load Opta data feeds from a local folder.

    Parameters
    ----------
    root : str
        Root-path of the data.
    parser : str or dict
        Either 'xml', 'json', 'statsperform', 'whoscored' or a dict with
        a custom parser for each feed. The default xml parser supports F7 and
        F24 feeds; the default json parser supports F1, F9 and F24 feeds, the
        StatsPerform parser supports MA1 and MA3 feeds. Custom parsers can be
        specified as::

            {
                'feed1_name': Feed1Parser
                'feed2_name': Feed2Parser
            }

        where Feed1Parser and Feed2Parser are classes implementing
        :class:`~socceraction.spadl.opta.OptaParser` and 'feed1_name' and
        'feed2_name' are a unique ID for each feed that matches to the keys in
        `feeds`.
    feeds : dict
        Glob pattern describing from which files the data from a specific game
        can be retrieved. For example, if files are named::

            f7-1-2021-17362.xml
            f24-1-2021-17362.xml

        use::

            feeds = {
                'f7': "f7-{competition_id}-{season_id}-{game_id}.xml",
                'f24': "f24-{competition_id}-{season_id}-{game_id}.xml"
            }

    Raises
    ------
    ValueError
        If an invalid parser is provided.
    """

    def __init__(  # noqa: C901
        self,
        root: str,
        parser: Union[str, Mapping[str, type[OptaParser]]] = "xml",
        feeds: Optional[dict[str, str]] = None,
    ) -> None:
        self.root = root
        if parser == "json":
            if feeds is None:
                feeds = {
                    "f1": "f1-{competition_id}-{season_id}.json",
                    "f9": "f9-{competition_id}-{season_id}-{game_id}.json",
                    "f24": "f24-{competition_id}-{season_id}-{game_id}.json",
                }
            self.parsers = self._get_parsers_for_feeds(_jsonparsers, feeds)
        elif parser == "xml":
            if feeds is None:
                feeds = {
                    "f7": "f7-{competition_id}-{season_id}-{game_id}.xml",
                    "f24": "f24-{competition_id}-{season_id}-{game_id}.xml",
                }
            self.parsers = self._get_parsers_for_feeds(_xmlparsers, feeds)
        elif parser == "statsperform":
            if feeds is None:
                feeds = {
                    "ma1": "ma1-{competition_id}-{season_id}.json",
                    "ma3": "ma3-{competition_id}-{season_id}-{game_id}.json",
                }
            self.parsers = self._get_parsers_for_feeds(_statsperformparsers, feeds)
        elif parser == "whoscored":
            if feeds is None:
                feeds = {
                    "whoscored": "{competition_id}-{season_id}-{game_id}.json",
                }
            self.parsers = self._get_parsers_for_feeds(_whoscoredparsers, feeds)
        elif isinstance(parser, dict):
            if feeds is None:
                raise ValueError("You must specify a feed for each parser.")
            self.parsers = self._get_parsers_for_feeds(parser, feeds)
        else:
            raise ValueError("Invalid parser provided.")
        self.feeds = {k: str(Path(v)) for k, v in feeds.items()}

    def _get_parsers_for_feeds(
        self, available_parsers: Mapping[str, type[OptaParser]], feeds: dict[str, str]
    ) -> Mapping[str, type[OptaParser]]:
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
                warnings.warn(f"No parser available for {feed} feeds. This feed is ignored.")
        return parsers

    def competitions(self) -> DataFrame[OptaCompetitionSchema]:
        """Return a dataframe with all available competitions and seasons.

        Returns
        -------
        pd.DataFrame
            A dataframe containing all available competitions and seasons. See
            :class:`~socceraction.spadl.opta.OptaCompetitionSchema` for the schema.
        """
        data: dict[int, dict[str, Any]] = {}
        loaded_seasons = set()
        for feed, feed_pattern in self.feeds.items():
            glob_pattern = feed_pattern.format(competition_id="*", season_id="*", game_id="*")
            feed_files = glob.glob(os.path.join(self.root, glob_pattern))
            for ffp in feed_files:
                ids = _extract_ids_from_path(ffp, feed_pattern)
                # For efficiency, we only parse one game for each season. This
                # only works if both the competition and season are part of
                # the file name.
                competition_id = ids.get("competition_id")
                season_id = ids.get("season_id")
                if competition_id is not None and season_id is not None:
                    if (competition_id, season_id) in loaded_seasons:
                        continue
                    else:
                        loaded_seasons.add((competition_id, season_id))
                parser = self.parsers[feed](ffp, **ids)
                _deepupdate(data, parser.extract_competitions())
        return cast(DataFrame[OptaCompetitionSchema], pd.DataFrame(list(data.values())))

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
        data: dict[int, dict[str, Any]] = {}
        for feed, feed_pattern in self.feeds.items():
            glob_pattern = feed_pattern.format(
                competition_id=competition_id, season_id=season_id, game_id="*"
            )
            feed_files = glob.glob(os.path.join(self.root, glob_pattern))
            for ffp in feed_files:
                ids = _extract_ids_from_path(ffp, feed_pattern)
                parser = self.parsers[feed](ffp, **ids)
                _deepupdate(data, parser.extract_games())
        return cast(DataFrame[OptaGameSchema], pd.DataFrame(list(data.values())))

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
        data: dict[int, dict[str, Any]] = {}
        for feed, feed_pattern in self.feeds.items():
            glob_pattern = feed_pattern.format(competition_id="*", season_id="*", game_id=game_id)
            feed_files = glob.glob(os.path.join(self.root, glob_pattern))
            for ffp in feed_files:
                ids = _extract_ids_from_path(ffp, feed_pattern)
                parser = self.parsers[feed](ffp, **ids)
                _deepupdate(data, parser.extract_teams())
        return cast(DataFrame[OptaTeamSchema], pd.DataFrame(list(data.values())))

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
        data: dict[int, dict[str, Any]] = {}
        for feed, feed_pattern in self.feeds.items():
            glob_pattern = feed_pattern.format(competition_id="*", season_id="*", game_id=game_id)
            feed_files = glob.glob(os.path.join(self.root, glob_pattern))
            for ffp in feed_files:
                ids = _extract_ids_from_path(ffp, feed_pattern)
                parser = self.parsers[feed](ffp, **ids)
                _deepupdate(data, parser.extract_players())
        df_players = pd.DataFrame(list(data.values()))
        df_players["game_id"] = game_id
        return cast(DataFrame[OptaPlayerSchema], df_players)

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
        data: dict[int, dict[str, Any]] = {}
        for feed, feed_pattern in self.feeds.items():
            glob_pattern = feed_pattern.format(competition_id="*", season_id="*", game_id=game_id)
            feed_files = glob.glob(os.path.join(self.root, glob_pattern))
            for ffp in feed_files:
                ids = _extract_ids_from_path(ffp, feed_pattern)
                parser = self.parsers[feed](ffp, **ids)
                _deepupdate(data, parser.extract_events())
        events = (
            pd.DataFrame(list(data.values()))
            .merge(_eventtypesdf, on="type_id", how="left")
            .sort_values(
                ["game_id", "period_id", "minute", "second", "timestamp"], kind="mergesort"
            )
            .reset_index(drop=True)
        )

        # sometimes pre-match events has -3, -2 and -1 seconds
        events.loc[events.second < 0, "second"] = 0
        events = events.sort_values(
            ["game_id", "period_id", "minute", "second", "timestamp"], kind="mergesort"
        )

        # deleted events has wrong datetime which occurs OutOfBoundsDatetime error
        events = events[events.type_id != 43]
        events = events[
            ~(
                (events.timestamp < datetime.datetime(1900, 1, 1))
                | (events.timestamp > datetime.datetime(2100, 1, 1))
            )
        ]

        return cast(DataFrame[OptaEventSchema], events)
