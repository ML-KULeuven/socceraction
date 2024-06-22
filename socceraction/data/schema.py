"""Base schemas used by all event stream serializers.

Each serializer should create dataframes that contain at least the fields
included in these base schemas. Each serializer can add different additional
fields on top.

"""

import pandas as pd
import pandera as pa
from pandera.typing import DateTime, Object, Series


class CompetitionSchema(pa.SchemaModel):
    """Definition of a dataframe containing a list of competitions and seasons."""

    season_id: Series[Object] = pa.Field()
    """The unique identifier for the season."""
    season_name: Series[str] = pa.Field()
    """The name of the season."""
    competition_id: Series[Object] = pa.Field()
    """The unique identifier for the competition."""
    competition_name: Series[str] = pa.Field()
    """The name of the competition."""

    class Config:  # noqa: D106
        strict = True
        coerce = True


class GameSchema(pa.SchemaModel):
    """Definition of a dataframe containing a list of games."""

    game_id: Series[Object] = pa.Field()
    """The unique identifier for the game."""
    season_id: Series[Object] = pa.Field()
    """The unique identifier for the season."""
    competition_id: Series[Object] = pa.Field()
    """The unique identifier for the competition."""
    game_day: Series[pd.Int64Dtype] = pa.Field(nullable=True)
    """Number corresponding to the weeks or rounds into the competition this game is."""
    game_date: Series[DateTime] = pa.Field()
    """The date when the game was played."""
    home_team_id: Series[Object] = pa.Field()
    """The unique identifier for the home team in this game."""
    away_team_id: Series[Object] = pa.Field()
    """The unique identifier for the away team in this game."""

    class Config:  # noqa: D106
        strict = True
        coerce = True


class TeamSchema(pa.SchemaModel):
    """Definition of a dataframe containing the list of teams of a game."""

    team_id: Series[Object] = pa.Field()
    """The unique identifier for the team."""
    team_name: Series[str] = pa.Field()
    """The name of the team."""

    class Config:  # noqa: D106
        strict = True
        coerce = True


class PlayerSchema(pa.SchemaModel):
    """Definition of a dataframe containing the list of players on the teamsheet of a game."""

    game_id: Series[Object] = pa.Field()
    """The unique identifier for the game."""
    team_id: Series[Object] = pa.Field()
    """The unique identifier for the player's team."""
    player_id: Series[Object] = pa.Field()
    """The unique identifier for the player."""
    player_name: Series[str] = pa.Field()
    """The name of the player."""
    is_starter: Series[bool] = pa.Field()
    """Whether the player is in the starting lineup."""
    minutes_played: Series[int] = pa.Field()
    """The number of minutes the player played in the game."""
    jersey_number: Series[int] = pa.Field()
    """The player's jersey number."""

    class Config:  # noqa: D106
        strict = True
        coerce = True


class EventSchema(pa.SchemaModel):
    """Definition of a dataframe containing event stream data of a game."""

    game_id: Series[Object] = pa.Field()
    """The unique identifier for the game."""
    event_id: Series[Object] = pa.Field()
    """The unique identifier for the event."""
    period_id: Series[int] = pa.Field()
    """The unique identifier for the part of the game in which the event took place."""
    team_id: Series[Object] = pa.Field(nullable=True)
    """The unique identifier for the team this event relates to."""
    player_id: Series[Object] = pa.Field(nullable=True)
    """The unique identifier for the player this event relates to."""
    type_id: Series[int] = pa.Field()
    """The unique identifier for the type of this event."""
    type_name: Series[str] = pa.Field()
    """The name of the type of this event."""

    class Config:  # noqa: D106
        strict = True
        coerce = True
