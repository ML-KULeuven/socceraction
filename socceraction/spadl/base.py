# -*- coding: utf-8 -*-
"""Base class for all event stream to SPADL converters.

A converter should extend the 'EventDataLoader' class to (down)load event
stream data and implement 'convert_to_actions' to convert the events to the
SPADL format.

"""
import json
from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional, Union

import pandas as pd  # type: ignore
import pandera as pa
import requests
from pandera.typing import DataFrame, DateTime, Object, Series

from . import config as spadlconfig


class ParseError(Exception):
    """Exception raised when a file is not correctly formatted."""


class MissingDataError(Exception):
    """Exception raised when a field is missing in the input data."""


class CompetitionSchema(pa.SchemaModel):
    """Definition of a dataframe containing a list of competitions and seasons."""

    season_id: Series[int]
    season_name: Series[str]
    competition_id: Series[int]
    competition_name: Series[str]

    class Config:  # noqa: D106
        strict = True


class GameSchema(pa.SchemaModel):
    """Definition of a dataframe containing a list of games."""

    game_id: Series[int]
    season_id: Series[int]
    competition_id: Series[int]
    game_day: Series[int]
    game_date: Series[DateTime]
    home_team_id: Series[int]
    away_team_id: Series[int]

    class Config:  # noqa: D106
        strict = True


class TeamSchema(pa.SchemaModel):
    """Definition of a dataframe containing the list of teams of a game."""

    team_id: Series[int]
    team_name: Series[str]

    class Config:  # noqa: D106
        strict = True


class PlayerSchema(pa.SchemaModel):
    """Definition of a dataframe containing the list of players of a game."""

    game_id: Series[int]
    team_id: Series[int]
    player_id: Series[int]
    player_name: Series[str]
    is_starter: Series[bool]
    minutes_played: Series[int]
    jersey_number: Series[int]

    class Config:  # noqa: D106
        strict = True


class EventSchema(pa.SchemaModel):
    """Definition of a dataframe containing event stream data of a game."""

    game_id: Series[int]
    event_id: Series[int]
    period_id: Series[int]
    team_id: Series[int] = pa.Field(nullable=True)
    player_id: Series[int] = pa.Field(nullable=True)
    type_id: Series[int]
    type_name: Series[str]

    class Config:  # noqa: D106
        strict = True


class SPADLSchema(pa.SchemaModel):
    """Definition of a SPADL dataframe."""

    game_id: Series[int]
    original_event_id: Series[Object] = pa.Field(nullable=True)
    action_id: Series[int] = pa.Field(allow_duplicates=False)
    period_id: Series[int] = pa.Field(ge=1, le=5)
    time_seconds: Series[float] = pa.Field(ge=0, le=60 * 60)  # assuming overtime < 15 min
    team_id: Series[int]
    player_id: Series[int]
    start_x: Series[float] = pa.Field(ge=0, le=spadlconfig.field_length)
    start_y: Series[float] = pa.Field(ge=0, le=spadlconfig.field_width)
    end_x: Series[float] = pa.Field(ge=0, le=spadlconfig.field_length)
    end_y: Series[float] = pa.Field(ge=0, le=spadlconfig.field_width)
    bodypart_id: Series[int] = pa.Field(isin=spadlconfig.bodyparts_df().bodypart_id)
    bodypart_name: Optional[Series[str]] = pa.Field(isin=spadlconfig.bodyparts_df().bodypart_name)
    type_id: Series[int] = pa.Field(isin=spadlconfig.actiontypes_df().type_id)
    type_name: Optional[Series[str]] = pa.Field(isin=spadlconfig.actiontypes_df().type_name)
    result_id: Series[int] = pa.Field(isin=spadlconfig.results_df().result_id)
    result_name: Optional[Series[str]] = pa.Field(isin=spadlconfig.results_df().result_name)

    class Config:  # noqa: D106
        strict = True


JSONType = Union[str, int, float, bool, None, Dict[str, Any], List[Any]]


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

    def __init__(self, root: str, getter: str):
        self.root = root

        if getter == 'remote':
            self.get = _remoteloadjson
        elif getter == 'local':
            self.get = _localloadjson
        else:
            raise Exception('Invalid getter specified')

    @abstractmethod
    def competitions(self) -> DataFrame[CompetitionSchema]:
        """Return a dataframe with all available competitions and seasons.

        Returns
        -------
        pd.DataFrame
            A dataframe containing all available competitions and seasons. See
            :class:`~socceraction.spadl.base.CompetitionSchema` for the schema.
        """
        raise NotImplementedError

    @abstractmethod
    def games(self, competition_id: int, season_id: int) -> DataFrame[GameSchema]:
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
    def teams(self, game_id: int) -> DataFrame[TeamSchema]:
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
    def players(self, game_id: int) -> DataFrame[PlayerSchema]:
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
    def events(self, game_id: int) -> DataFrame[EventSchema]:
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


def _fix_clearances(actions: DataFrame) -> DataFrame:
    next_actions = actions.shift(-1)
    next_actions[-1:] = actions[-1:]
    clearance_idx = actions.type_id == spadlconfig.actiontypes.index('clearance')
    actions.loc[clearance_idx, 'end_x'] = next_actions[clearance_idx].start_x.values
    actions.loc[clearance_idx, 'end_y'] = next_actions[clearance_idx].start_y.values

    return actions


def _fix_direction_of_play(actions: DataFrame, home_team_id: int) -> DataFrame:
    away_idx = (actions.team_id != home_team_id).values
    for col in ['start_x', 'end_x']:
        actions.loc[away_idx, col] = spadlconfig.field_length - actions[away_idx][col].values
    for col in ['start_y', 'end_y']:
        actions.loc[away_idx, col] = spadlconfig.field_width - actions[away_idx][col].values

    return actions


min_dribble_length: float = 3.0
max_dribble_length: float = 60.0
max_dribble_duration: float = 10.0


def _add_dribbles(actions: DataFrame) -> DataFrame:
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
    dribbles['game_id'] = nex.game_id
    dribbles['period_id'] = nex.period_id
    dribbles['action_id'] = prev.action_id + 0.1
    dribbles['time_seconds'] = (prev.time_seconds + nex.time_seconds) / 2
    if 'timestamp' in actions.columns:
        dribbles['timestamp'] = nex.timestamp
    dribbles['team_id'] = nex.team_id
    dribbles['player_id'] = nex.player_id
    dribbles['start_x'] = prev.end_x
    dribbles['start_y'] = prev.end_y
    dribbles['end_x'] = nex.start_x
    dribbles['end_y'] = nex.start_y
    dribbles['bodypart_id'] = spadlconfig.bodyparts.index('foot')
    dribbles['type_id'] = spadlconfig.actiontypes.index('dribble')
    dribbles['result_id'] = spadlconfig.results.index('success')

    actions = pd.concat([actions, dribbles], ignore_index=True, sort=False)
    actions = actions.sort_values(['game_id', 'period_id', 'action_id']).reset_index(drop=True)
    actions['action_id'] = range(len(actions))
    return actions
