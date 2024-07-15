"""Module for loading Opta event data."""

__all__ = [
    "OptaLoader",
    "OptaCompetitionSchema",
    "OptaGameSchema",
    "OptaPlayerSchema",
    "OptaTeamSchema",
    "OptaEventSchema",
]

from .loader import OptaLoader
from .schema import (
    OptaCompetitionSchema,
    OptaEventSchema,
    OptaGameSchema,
    OptaPlayerSchema,
    OptaTeamSchema,
)
