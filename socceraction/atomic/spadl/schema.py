"""Schema for Atomic-SPADL actions."""
from typing import Optional

import pandera as pa
from pandera.typing import Object, Series

from . import config as spadlconfig


class AtomicSPADLSchema(pa.SchemaModel):
    """Definition of an Atomic-SPADL dataframe."""

    game_id: Series[int]
    original_event_id: Series[Object] = pa.Field(nullable=True)
    action_id: Series[int] = pa.Field(allow_duplicates=False)
    period_id: Series[int] = pa.Field(ge=1, le=5)
    time_seconds: Series[float] = pa.Field(ge=0, le=60 * 60)  # assuming overtime < 15 min
    team_id: Series[int]
    player_id: Series[int]
    x: Series[float] = pa.Field(ge=0, le=spadlconfig.field_length)
    y: Series[float] = pa.Field(ge=0, le=spadlconfig.field_width)
    dx: Series[float] = pa.Field(ge=-spadlconfig.field_length, le=spadlconfig.field_length)
    dy: Series[float] = pa.Field(ge=-spadlconfig.field_width, le=spadlconfig.field_width)
    bodypart_id: Series[int] = pa.Field(isin=spadlconfig.bodyparts_df().bodypart_id)
    bodypart_name: Optional[Series[str]] = pa.Field(isin=spadlconfig.bodyparts_df().bodypart_name)
    type_id: Series[int] = pa.Field(isin=spadlconfig.actiontypes_df().type_id)
    type_name: Optional[Series[str]] = pa.Field(isin=spadlconfig.actiontypes_df().type_name)

    class Config:  # noqa: D106
        strict = True
