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

    home_score: Optional[Series[int]] = pa.Field(nullable=True)
    """The final score of the home team."""
    away_score: Optional[Series[int]] = pa.Field(nullable=True)
    """The final score of the away team."""
    duration: Optional[Series[int]] = pa.Field(nullable=True)
    """The total duration of the game in minutes."""
    referee: Optional[Series[str]] = pa.Field(nullable=True)
    """The name of the referee."""
    venue: Optional[Series[str]] = pa.Field(nullable=True)
    """The name of the stadium where the game was played."""
    attendance: Optional[Series[int]] = pa.Field(nullable=True)
    """The number of people who attended the game."""
    home_manager: Optional[Series[str]] = pa.Field(nullable=True)
    """The name of the manager of the home team."""
    away_manager: Optional[Series[str]] = pa.Field(nullable=True)
    """The name of the manager of the away team."""


class OptaPlayerSchema(PlayerSchema):
    """Definition of a dataframe containing the list of players of a game."""

    starting_position: Series[str]
    """The starting position of the player."""


class OptaTeamSchema(TeamSchema):
    """Definition of a dataframe containing the list of teams of a game."""


class OptaEventSchema(EventSchema):
    """Definition of a dataframe containing event stream data of a game."""

    timestamp: Series[DateTime]
    """Time in the match the event takes place, recorded to the millisecond."""
    minute: Series[int]
    """The minutes on the clock at the time of this event."""
    second: Series[int] = pa.Field(ge=0, le=59)
    """The second part of the timestamp."""
    outcome: Series[bool]
    """Whether the event had a successful outcome or not."""
    start_x: Series[float] = pa.Field(nullable=True)
    """The x coordinate of the location where the event started."""
    start_y: Series[float] = pa.Field(nullable=True)
    """The y coordinate of the location where the event started."""
    end_x: Series[float] = pa.Field(nullable=True)
    """The x coordinate of the location where the event ended."""
    end_y: Series[float] = pa.Field(nullable=True)
    """The y coordinate of the location where the event ended."""
    qualifiers: Series[Object]
    """A JSON object containing the Opta qualifiers of the event."""
    assist: Optional[Series[bool]]
    """Whether the event was an assist or not."""
    keypass: Optional[Series[bool]]
    """Whether the event was a keypass or not."""
    goal: Optional[Series[bool]]
    """Whether the event was a goal or not."""
    shot: Optional[Series[bool]]
    """Whether the event was a shot or not."""
    touch: Optional[Series[bool]]
    """Whether the event was a on-the-ball action or not."""
    related_player_id: Optional[Series[pd.Int64Dtype]] = pa.Field(nullable=True)
    """The ID of a second player that was involved in this event."""
