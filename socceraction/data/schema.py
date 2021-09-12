"""Base schemas used by all event stream serializers.

Each serializer should create dataframes that contain at least the fields
included in these base schemas. Each serializer can add different additional
fields on top.

"""
import pandera as pa
from pandera.typing import DateTime, Object, Series


class CompetitionSchema(pa.SchemaModel):
    """Definition of a dataframe containing a list of competitions and seasons."""

    season_id: Series[Object] = pa.Field(coerce=True)
    season_name: Series[str] = pa.Field()
    competition_id: Series[Object] = pa.Field(coerce=True)
    competition_name: Series[str] = pa.Field()

    class Config:  # noqa: D106
        strict = True


class GameSchema(pa.SchemaModel):
    """Definition of a dataframe containing a list of games."""

    game_id: Series[Object] = pa.Field(coerce=True)
    season_id: Series[Object] = pa.Field(coerce=True)
    competition_id: Series[Object] = pa.Field(coerce=True)
    game_day: Series[int] = pa.Field()
    game_date: Series[DateTime] = pa.Field()
    home_team_id: Series[Object] = pa.Field(coerce=True)
    away_team_id: Series[Object] = pa.Field(coerce=True)

    class Config:  # noqa: D106
        strict = True


class TeamSchema(pa.SchemaModel):
    """Definition of a dataframe containing the list of teams of a game."""

    team_id: Series[Object] = pa.Field(coerce=True)
    team_name: Series[str] = pa.Field()

    class Config:  # noqa: D106
        strict = True


class PlayerSchema(pa.SchemaModel):
    """Definition of a dataframe containing the list of players of a game."""

    game_id: Series[Object] = pa.Field(coerce=True)
    team_id: Series[Object] = pa.Field(coerce=True)
    player_id: Series[Object] = pa.Field(coerce=True)
    player_name: Series[str] = pa.Field()
    is_starter: Series[bool] = pa.Field()
    minutes_played: Series[int] = pa.Field()
    jersey_number: Series[int] = pa.Field()

    class Config:  # noqa: D106
        strict = True


class EventSchema(pa.SchemaModel):
    """Definition of a dataframe containing event stream data of a game."""

    game_id: Series[Object] = pa.Field(coerce=True)
    event_id: Series[Object] = pa.Field(coerce=True)
    period_id: Series[int] = pa.Field()
    team_id: Series[Object] = pa.Field(coerce=True, nullable=True)
    player_id: Series[Object] = pa.Field(coerce=True, nullable=True)
    type_id: Series[int] = pa.Field()
    type_name: Series[str] = pa.Field()

    class Config:  # noqa: D106
        strict = True
