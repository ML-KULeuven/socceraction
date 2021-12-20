"""Base schemas used by all event stream serializers.

Each serializer should create dataframes that contain at least the fields
included in these base schemas. Each serializer can add different additional
fields on top.

"""
import pandera as pa
from pandera.typing import DateTime, Object, Series


class CompetitionSchema(pa.SchemaModel):
    """Definition of a dataframe containing a list of competitions and seasons."""

    season_id: Series[Object] = pa.Field()
    season_name: Series[str] = pa.Field()
    competition_id: Series[Object] = pa.Field()
    competition_name: Series[str] = pa.Field()

    class Config:  # noqa: D106
        strict = True
        coerce = True


class GameSchema(pa.SchemaModel):
    """Definition of a dataframe containing a list of games."""

    game_id: Series[Object] = pa.Field()
    season_id: Series[Object] = pa.Field()
    competition_id: Series[Object] = pa.Field()
    game_day: Series[int] = pa.Field()
    game_date: Series[DateTime] = pa.Field()
    home_team_id: Series[Object] = pa.Field()
    away_team_id: Series[Object] = pa.Field()

    class Config:  # noqa: D106
        strict = True
        coerce = True


class TeamSchema(pa.SchemaModel):
    """Definition of a dataframe containing the list of teams of a game."""

    team_id: Series[Object] = pa.Field()
    team_name: Series[str] = pa.Field()

    class Config:  # noqa: D106
        strict = True
        coerce = True


class PlayerSchema(pa.SchemaModel):
    """Definition of a dataframe containing the list of players of a game."""

    game_id: Series[Object] = pa.Field()
    team_id: Series[Object] = pa.Field()
    player_id: Series[Object] = pa.Field()
    player_name: Series[str] = pa.Field()
    is_starter: Series[bool] = pa.Field()
    minutes_played: Series[int] = pa.Field()
    jersey_number: Series[int] = pa.Field()

    class Config:  # noqa: D106
        strict = True
        coerce = True


class EventSchema(pa.SchemaModel):
    """Definition of a dataframe containing event stream data of a game."""

    game_id: Series[Object] = pa.Field()
    event_id: Series[Object] = pa.Field()
    period_id: Series[int] = pa.Field()
    team_id: Series[Object] = pa.Field(nullable=True)
    player_id: Series[Object] = pa.Field(nullable=True)
    type_id: Series[int] = pa.Field()
    type_name: Series[str] = pa.Field()

    class Config:  # noqa: D106
        strict = False
        coerce = True
