"""Implements interfaces for loading the event data of various providers."""

__all__ = ["OptaLoader", "StatsBombLoader", "PublicWyscoutLoader", "WyscoutLoader"]

from .opta import OptaLoader
from .statsbomb import StatsBombLoader
from .wyscout import PublicWyscoutLoader, WyscoutLoader
