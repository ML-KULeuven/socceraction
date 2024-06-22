"""Implementation of the SPADL language."""

__all__ = [
    "opta",
    "statsbomb",
    "wyscout",
    "kloppy",
    "config",
    "SPADLSchema",
    "bodyparts_df",
    "actiontypes_df",
    "results_df",
    "add_names",
    "play_left_to_right",
]

from . import config, opta, statsbomb, wyscout
from .config import actiontypes_df, bodyparts_df, results_df
from .schema import SPADLSchema
from .utils import add_names, play_left_to_right

try:
    from . import kloppy
except ImportError:
    pass
