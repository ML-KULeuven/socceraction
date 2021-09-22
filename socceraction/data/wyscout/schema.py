"""SPADL schema for Wyscout data."""
import pandera as pa
from pandera.typing import DateTime, Object, Series

from socceraction.data.schema import (
    CompetitionSchema,
    EventSchema,
    GameSchema,
    PlayerSchema,
    TeamSchema,
)


class WyscoutCompetitionSchema(CompetitionSchema):
    """Definition of a dataframe containing a list of competitions and seasons."""

    country_name: Series[str]
    competition_gender: Series[str]


class WyscoutGameSchema(GameSchema):
    """Definition of a dataframe containing a list of games."""


class WyscoutPlayerSchema(PlayerSchema):
    """Definition of a dataframe containing the list of teams of a game."""

    firstname: Series[str]
    lastname: Series[str]
    nickname: Series[str] = pa.Field(nullable=True)
    birth_date: Series[DateTime] = pa.Field(nullable=True)


class WyscoutTeamSchema(TeamSchema):
    """Definition of a dataframe containing the list of players of a game."""

    team_name_short: Series[str]


class WyscoutEventSchema(EventSchema):
    """Definition of a dataframe containing event stream data of a game."""

    milliseconds: Series[float]
    subtype_id: Series[int]
    subtype_name: Series[str]
    positions: Series[Object]
    tags: Series[Object]
