"""SPADL schema for StatsBomb data."""
import pandera as pa
from pandera.typing import DateTime, Object, Series

from socceraction.data.schema import (
    CompetitionSchema,
    EventSchema,
    GameSchema,
    PlayerSchema,
    TeamSchema,
)


class StatsBombCompetitionSchema(CompetitionSchema):
    """Definition of a dataframe containing a list of competitions and seasons."""

    country_name: Series[str]
    competition_gender: Series[str]


class StatsBombGameSchema(GameSchema):
    """Definition of a dataframe containing a list of games."""

    competition_stage: Series[str]
    home_score: Series[int]
    away_score: Series[int]
    venue: Series[str] = pa.Field(nullable=True)
    referee_id: Series[int]


class StatsBombPlayerSchema(PlayerSchema):
    """Definition of a dataframe containing the list of players of a game."""

    nickname: Series[str] = pa.Field(nullable=True)
    starting_position_id: Series[int]
    starting_position_name: Series[str]


class StatsBombTeamSchema(TeamSchema):
    """Definition of a dataframe containing the list of teams of a game."""


class StatsBombEventSchema(EventSchema):
    """Definition of a dataframe containing event stream data of a game."""

    event_id: Series[Object]
    index: Series[int]
    timestamp: Series[DateTime]
    minute: Series[int]
    second: Series[int] = pa.Field(ge=0, le=59)
    possession: Series[int]
    possession_team_id: Series[int]
    possession_team_name: Series[str]
    play_pattern_id: Series[int]
    play_pattern_name: Series[str]
    team_name: Series[str]
    duration: Series[float] = pa.Field(nullable=True)
    extra: Series[Object]
    related_events: Series[Object]
    player_name: Series[str] = pa.Field(nullable=True)
    position_id: Series[float] = pa.Field(nullable=True)
    position_name: Series[str] = pa.Field(nullable=True)
    location: Series[Object] = pa.Field(nullable=True)
    under_pressure: Series[bool] = pa.Field(nullable=True)
    counterpress: Series[bool] = pa.Field(nullable=True)
