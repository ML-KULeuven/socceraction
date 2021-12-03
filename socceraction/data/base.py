# -*- coding: utf-8 -*-
"""Base class and utility functions for all event stream data serializers.

A serializer should extend the 'EventDataLoader' class to (down)load event
stream data.
"""
import json
from abc import ABC, abstractmethod
from typing import Any, Dict, List, Union

import requests
from pandera.typing import DataFrame

JSONType = Union[str, int, float, bool, None, Dict[str, Any], List[Any]]


class ParseError(Exception):
    """Exception raised when a file is not correctly formatted."""


class MissingDataError(Exception):
    """Exception raised when a field is missing in the input data."""


def _remoteloadjson(path: str) -> JSONType:
    return requests.get(path).json()


def _localloadjson(path: str) -> JSONType:
    with open(path, 'rt', encoding='utf-8') as fh:
        return json.load(fh)


class EventDataLoader(ABC):
    """
    Load event data either from a remote location or from a local folder.

    Parameters
    ----------
    root : str
        Root-path of the data.
    getter : str
        "remote" or "local"
    """

    def __init__(self, root: str, getter: str) -> None:
        self.root = root

        if getter == 'remote':
            self.get = _remoteloadjson
        elif getter == 'local':
            self.get = _localloadjson
        else:
            raise Exception('Invalid getter specified')

    @abstractmethod
    def competitions(self) -> DataFrame[Any]:
        """Return a dataframe with all available competitions and seasons.

        Returns
        -------
        pd.DataFrame
            A dataframe containing all available competitions and seasons. See
            :class:`~socceraction.spadl.base.CompetitionSchema` for the schema.
        """
        raise NotImplementedError

    @abstractmethod
    def games(self, competition_id: int, season_id: int) -> DataFrame[Any]:
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
            :class:`~socceraction.spadl.base.GameSchema` for the schema.
        """
        raise NotImplementedError

    @abstractmethod
    def teams(self, game_id: int) -> DataFrame[Any]:
        """Return a dataframe with both teams that participated in a game.

        Parameters
        ----------
        game_id : int
            The ID of the game.

        Returns
        -------
        pd.DataFrame
            A dataframe containing both teams. See
            :class:`~socceraction.spadl.base.TeamSchema` for the schema.
        """
        raise NotImplementedError

    @abstractmethod
    def players(self, game_id: int) -> DataFrame[Any]:
        """Return a dataframe with all players that participated in a game.

        Parameters
        ----------
        game_id : int
            The ID of the game.

        Returns
        -------
        pd.DataFrame
            A dataframe containing all players. See
            :class:`~socceraction.spadl.base.PlayerSchema` for the schema.
        """
        raise NotImplementedError

    @abstractmethod
    def events(self, game_id: int) -> DataFrame[Any]:
        """Return a dataframe with the event stream of a game.

        Parameters
        ----------
        game_id : int
            The ID of the game.

        Returns
        -------
        pd.DataFrame
            A dataframe containing the event stream. See
            :class:`~socceraction.spadl.base.EventSchema` for the schema.
        """
        raise NotImplementedError
