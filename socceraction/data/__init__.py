"""Load, transform and store event stream data."""

__all__ = [
    "OptaLoader",
    "StatsBombLoader",
    "WyscoutLoader",
    "PublicWyscoutLoader",
    "Dataset",
    "FSSpecAdapter",
    "HDFDataset",
    "ParquetDataset",
    "SQLDataset",
    "PartitionIdentifier",
    "providers",
    "schema",
    "transforms",
]

from . import providers, schema, transforms
from .dataset import (
    Dataset,
    FSSpecAdapter,
    HDFDataset,
    ParquetDataset,
    PartitionIdentifier,
    SQLDataset,
)
from .providers.opta import OptaLoader
from .providers.statsbomb import StatsBombLoader
from .providers.wyscout import PublicWyscoutLoader, WyscoutLoader
