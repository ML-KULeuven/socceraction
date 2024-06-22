"""Base class and utility functions for all event stream data serializers.

A serializer should extend the 'EventDataLoader' class to (down)load event
stream data.
"""

import base64
import json
import warnings
from abc import ABC, abstractmethod
from typing import Any, Union
from urllib import request

from pandera.typing import DataFrame

JSONType = Union[str, int, float, bool, None, dict[str, Any], list[Any]]


class ParseError(Exception):
    """Exception raised when a file is not correctly formatted."""


class MissingDataError(Exception):
    """Exception raised when a field is missing in the input data."""


class NoAuthWarning(UserWarning):
    """Warning raised when no user credentials are provided."""


def _remoteloadjson(path: str) -> JSONType:
    """Load JSON data from a URL.

    Parameters
    ----------
    path : str
        URL of the data source.

    Returns
    -------
    JSONType
        A dictionary with the loaded JSON data.
    """
    return json.loads(request.urlopen(path).read())


def _auth_remoteloadjson(user: str, passwd: str) -> None:
    """Add a Authorization header to all requests.

    Parameters
    ----------
    user : str
        Username.
    passwd : str
        Password.
    """
    auth = base64.b64encode(f"{user}:{passwd}".encode())
    opener = request.build_opener()
    opener.addheaders = [("Authorization", f"Basic {auth.decode()}")]
    request.install_opener(opener)


def _localloadjson(path: str) -> JSONType:
    """Load a dictionary from a JSON's filepath.

    Parameters
    ----------
    path : str
        JSON's filepath.

    Returns
    -------
    JSONType
        A dictionary with the data loaded.
    """
    with open(path, encoding="utf-8") as fh:
        return json.load(fh)


def _has_auth(creds: dict[str, str]) -> bool:
    """Check if user credentials are provided.

    Parameters
    ----------
    creds : dict
        A dictionary with user credentials. It should contain "user" and
        "passwd" keys.

    Returns
    -------
    bool
        True if user credentials are provided, False otherwise.
    """
    if creds.get("user") in [None, ""] or creds.get("passwd") in [None, ""]:
        warnings.warn("Credentials were not supplied. Public data access only.", NoAuthWarning)
        return False
    return True


def _expand_minute(minute: int, periods_duration: list[int]) -> int:
    """Expand a timestamp with injury time of previous periods.

    Parameters
    ----------
    minute : int
        Timestamp in minutes.
    periods_duration : List[int]
        Total duration of each period in minutes.

    Returns
    -------
    int
        Timestamp expanded with injury time.
    """
    expanded_minute = minute
    periods_regular = [45, 45, 15, 15, 0]
    for period in range(len(periods_duration) - 1):
        if minute > sum(periods_regular[: period + 1]):
            expanded_minute += periods_duration[period] - periods_regular[period]
        else:
            break
    return expanded_minute


class EventDataLoader(ABC):
    """Load event data either from a remote location or from a local folder.

    Parameters
    ----------
    root : str
        Root-path of the data.
    getter : str
        "remote" or "local"
    """

    @abstractmethod
    def competitions(self) -> DataFrame[Any]:
        """Return a dataframe with all available competitions and seasons.

        Returns
        -------
        pd.DataFrame
            A dataframe containing all available competitions and seasons. See
            :class:`~socceraction.spadl.base.CompetitionSchema` for the schema.
        """

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
