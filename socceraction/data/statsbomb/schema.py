"""SPADL schema for StatsBomb data."""

from typing import Optional

import pandera as pa
from pandera.typing import Object, Series, Timedelta

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
    """The name of the country the competition relates to."""
    competition_gender: Series[str]
    """The gender of the players competing in the competition."""


class StatsBombGameSchema(GameSchema):
    """Definition of a dataframe containing a list of games."""

    competition_stage: Series[str]
    """The name of the phase of the competition this game is in."""
    home_score: Series[int]
    """The final score of the home team."""
    away_score: Series[int]
    """The final score of the away team."""
    venue: Series[str] = pa.Field(nullable=True)
    """The name of the stadium where the game was played."""
    referee: Series[str] = pa.Field(nullable=True)
    """The name of the referee."""


class StatsBombPlayerSchema(PlayerSchema):
    """Definition of a dataframe containing the list of players of a game."""

    nickname: Series[str] = pa.Field(nullable=True)
    """The nickname of the player on the team."""
    starting_position_id: Series[int]
    """The unique identifier for the starting position of the player on the team."""
    starting_position_name: Series[str]
    """The name of the starting position of the player on the team."""


class StatsBombTeamSchema(TeamSchema):
    """Definition of a dataframe containing the list of teams of a game."""


class StatsBombEventSchema(EventSchema):
    """Definition of a dataframe containing event stream data of a game."""

    index: Series[int]
    """Sequence notation for the ordering of events within each match."""
    timestamp: Series[Timedelta]
    """Time in the match the event takes place, recorded to the millisecond."""
    minute: Series[int]
    """The minutes on the clock at the time of this event."""
    second: Series[int] = pa.Field(ge=0, le=59)
    """The second part of the timestamp."""
    possession: Series[int]
    """Indicates the current unique possession in the game."""
    possession_team_id: Series[int]
    """The ID of the team that started this possession in control of the ball."""
    possession_team_name: Series[str]
    """The name of the team that started this possession in control of the ball."""
    play_pattern_id: Series[int]
    """The ID of the play pattern relevant to this event."""
    play_pattern_name: Series[str]
    """The name of the play pattern relevant to this event."""
    team_name: Series[str]
    """The name of the team this event relates to."""
    duration: Series[float] = pa.Field(nullable=True)
    """If relevant, the length in seconds the event lasted."""
    extra: Series[Object]
    """A JSON string containing type-specific information."""
    related_events: Series[Object]
    """A comma separated list of the IDs of related events."""
    player_name: Series[str] = pa.Field(nullable=True)
    """The name of the player this event relates to."""
    position_id: Series[float] = pa.Field(nullable=True)
    """The ID of the position the player was in at the time of this event."""
    position_name: Series[str] = pa.Field(nullable=True)
    """The name of the position the player was in at the time of this event."""
    location: Series[Object] = pa.Field(nullable=True)
    """Array containing the x and y coordinates of the event."""
    under_pressure: Series[bool] = pa.Field(nullable=True)
    """Whether the action was performed while being pressured by an opponent."""
    counterpress: Series[bool] = pa.Field(nullable=True)
    """Pressing actions within 5 seconds of an open play turnover."""
    visible_area_360: Optional[Series[Object]] = pa.Field(nullable=True)
    """An array of coordinates describing the polygon visible to the camera / in the 360 frame."""
    freeze_frame_360: Optional[Series[Object]] = pa.Field(nullable=True)
    """An array of freeze frame objects."""
