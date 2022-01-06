"""SPADL schema for Opta data."""
from typing import Optional

import pandas as pd
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

    home_score: Optional[Series[int]]
    away_score: Optional[Series[int]]
    duration: Optional[Series[int]]
    referee: Optional[Series[str]] = pa.Field(nullable=True)
    venue: Optional[Series[str]] = pa.Field(nullable=True)
    attendance: Optional[Series[int]] = pa.Field(nullable=True)
    home_manager: Optional[Series[str]] = pa.Field(nullable=True)
    away_manager: Optional[Series[str]] = pa.Field(nullable=True)


class OptaPlayerSchema(PlayerSchema):
    """Definition of a dataframe containing the list of players of a game."""

    starting_position: Series[str]


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
    qualifiers: Series[Object]
    assist: Optional[Series[bool]]
    keypass: Optional[Series[bool]]
    goal: Optional[Series[bool]]
    shot: Optional[Series[bool]]
    touch: Optional[Series[bool]]
    related_player_id: Optional[Series[pd.Int64Dtype]] = pa.Field(nullable=True)


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
