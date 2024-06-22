"""Base class for all Opta(-derived) event stream parsers.

A parser reads a single data file and should extend the 'OptaParser' class to
extract data about competitions, games, players, teams and events that is
encoded in the file.

"""

import json  # type: ignore
from typing import Any, Optional

from lxml import objectify


class OptaParser:
    """Extract data from an Opta data stream.

    Parameters
    ----------
    path : str
        Path of the data file.
    """

    def __init__(self, path: str, **kwargs: Any) -> None:  # noqa: ANN401
        raise NotImplementedError

    def extract_competitions(self) -> dict[tuple[Any, Any], dict[str, Any]]:
        """Return a dictionary with all available competitions.

        Returns
        -------
        dict
            A mapping between (competion ID, season ID) tuples and the
            information available about each competition in the data stream.
        """
        return {}

    def extract_games(self) -> dict[Any, dict[str, Any]]:
        """Return a dictionary with all available games.

        Returns
        -------
        dict
            A mapping between game IDs and the information available about
            each game in the data stream.
        """
        return {}

    def extract_teams(self) -> dict[Any, dict[str, Any]]:
        """Return a dictionary with all available teams.

        Returns
        -------
        dict
            A mapping between team IDs and the information available about
            each team in the data stream.
        """
        return {}

    def extract_players(self) -> dict[tuple[Any, Any], dict[str, Any]]:
        """Return a dictionary with all available players.

        Returns
        -------
        dict
            A mapping between (game ID, player ID) tuples and the information
            available about each player in the data stream.
        """
        return {}

    def extract_lineups(self) -> dict[Any, dict[str, Any]]:
        """Return a dictionary with the lineup of each team.

        Returns
        -------
        dict
            A mapping between team IDs and the information available about
            each team's lineup in the data stream.
        """
        return {}

    def extract_events(self) -> dict[tuple[Any, Any], dict[str, Any]]:
        """Return a dictionary with all available events.

        Returns
        -------
        dict
            A mapping between (game ID, event ID) tuples and the information
            available about each event in the data stream.
        """
        return {}


class OptaJSONParser(OptaParser):
    """Extract data from an Opta JSON data stream.

    Parameters
    ----------
    path : str
        Path of the data file.
    """

    def __init__(self, path: str, **kwargs: Any) -> None:  # noqa: ANN401
        with open(path, encoding="utf-8") as fh:
            self.root = json.load(fh)


class OptaXMLParser(OptaParser):
    """Extract data from an Opta XML data stream.

    Parameters
    ----------
    path : str
        Path of the data file.
    """

    def __init__(self, path: str, **kwargs: Any) -> None:  # noqa: ANN401
        with open(path, "rb") as fh:
            self.root = objectify.fromstring(fh.read())


def assertget(dictionary: dict[str, Any], key: str) -> Any:  # noqa: ANN401
    """Return the value of the item with the specified key.

    In contrast to the default `get` method, this version will raise an
    assertion error if the given key is not present in the dict.

    Parameters
    ----------
    dictionary : dict
        A Python dictionary.
    key : str
        A key in the dictionary.

    Returns
    -------
    Any
        Returns the value for the specified key if the key is in the dictionary.

    Raises
    ------
    AssertionError
        If the given key could not be found in the dictionary.
    """
    value = dictionary.get(key)
    assert value is not None, "KeyError: " + key + " not found in " + str(dictionary)
    return value


def _get_end_x(qualifiers: dict[int, Any]) -> Optional[float]:
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


def _get_end_y(qualifiers: dict[int, Any]) -> Optional[float]:
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
