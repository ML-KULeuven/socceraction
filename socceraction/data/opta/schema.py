"""SPADL schema for Opta data."""
from typing import Any, Optional

import pandera as pa
from pandera.typing import DateTime, Object, Series

from socceraction.data.schema import (
    CompetitionSchema,
    EventSchema,
    GameSchema,
    PlayerSchema,
    TeamSchema,
)


class OptaCompetitionSchema(CompetitionSchema):
    """Definition of a dataframe containing a list of competitions and seasons."""


class OptaGameSchema(GameSchema):
    """Definition of a dataframe containing a list of games."""

    venue: Series[str] = pa.Field(nullable=True)
    referee_id: Series[Any] = pa.Field(nullable=True)
    attendance: Series[int] = pa.Field(nullable=True)
    duration: Series[int]
    home_score: Series[int]
    away_score: Series[int]


class OptaPlayerSchema(PlayerSchema):
    """Definition of a dataframe containing the list of players of a game."""

    firstname: Optional[Series[str]]
    lastname: Optional[Series[str]]
    nickname: Optional[Series[str]] = pa.Field(nullable=True)
    starting_position_id: Series[int]
    starting_position_name: Series[str]
    height: Optional[Series[float]]
    weight: Optional[Series[float]]
    age: Optional[Series[int]]


class OptaTeamSchema(TeamSchema):
    """Definition of a dataframe containing the list of teams of a game."""


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
    qualifiers: Series[Object]


class StatsPerformCompetitionSchema(OptaCompetitionSchema):
    """Definition of a dataframe containing a list of competitions and seasons."""

    season_id: Series[Object]
    competition_id: Series[Object]


class StatsPerformGameSchema(OptaGameSchema):
    """Definition of a dataframe containing a list of games."""

    game_id: Series[Object]
    season_id: Series[Object]
    competition_id: Series[Object]
    home_team_id: Series[Object]
    away_team_id: Series[Object]
    referee_id: Series[Object] = pa.Field(nullable=True)


class StatsPerformPlayerSchema(OptaPlayerSchema):
    """Definition of a dataframe containing the list of players of a game."""

    team_id: Series[Object]


class StatsPerformTeamSchema(OptaTeamSchema):
    """Definition of a dataframe containing the list of teams of a game."""

    game_id: Series[Object]
    team_id: Series[Object]
    player_id: Series[Object]


class StatsPerformEventSchema(OptaEventSchema):
    """Definition of a dataframe containing event stream data of a game."""

    game_id: Series[Object]
    team_id: Series[Object] = pa.Field(nullable=True)
    player_id: Series[Object] = pa.Field(nullable=True)
