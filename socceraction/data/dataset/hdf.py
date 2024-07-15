"""HDF store interface."""

import warnings
from typing import Any, List, Optional, cast

import pandas as pd
from pandera.typing import DataFrame

from socceraction.data.schema import EventSchema

from .base import Dataset, PartitionIdentifier


class HDFDataset(pd.io.pytables.HDFStore, Dataset):
    """Wrapper for a HDF database storing SPADL data.

    Parameters
    ----------
    path : str
        File path to HDF5 file.
    mode : {'a', 'w', 'r', 'r+'}, default 'a'
        ``'r'``
            Read-only; no data can be modified.
        ``'w'``
            Write; a new file is created (an existing file with the same
            name would be deleted).
        ``'a'``
            Append; an existing file is opened for reading and writing,
            and if the file does not exist it is created.
        ``'r+'``
            It is similar to ``'a'``, but the file must already exist.
    complevel : int, 0-9, default None
        Specifies a compression level for data.
        A value of 0 or None disables compression.
    complib : {'zlib', 'lzo', 'bzip2', 'blosc'}, default 'zlib'
        Specifies the compression library to be used.
        As of v0.20.2 these additional compressors for Blosc are supported
        (default if no compressor specified: 'blosc:blosclz'):
        {'blosc:blosclz', 'blosc:lz4', 'blosc:lz4hc', 'blosc:snappy',
         'blosc:zlib', 'blosc:zstd'}.
        Specifying a compression library which is not available issues
        a ValueError.
    fletcher32 : bool, default False
        If applying compression use the fletcher32 checksum.
    **kwargs
        These parameters will be passed to the PyTables open_file method.
    """

    def upsert_table(
        self,
        table: str,
        data: DataFrame[Any],
        primary_keys: list[str],
    ) -> None:
        data = data.copy()
        try:
            data = cast(DataFrame[Any], pd.concat([self.read_table(table), data]))
        except KeyError:
            pass
        data.drop_duplicates(subset=primary_keys, keep="last", inplace=True)
        self.put(f"{table}", data, format="table", data_columns=True)

    def insert_partition_table(
        self,
        table: str,
        data: DataFrame[Any],
        partition_identifiers: PartitionIdentifier,
    ) -> None:
        data = data.copy()
        self.put(f"{table}", data, format="table", data_columns=True)

        self.put(
            f"{table}/game_{partition_identifiers.game_id}",
            data,
            format="table",
            data_columns=True,
        )

    def read_table(self, table: str, *identifiers: PartitionIdentifier) -> DataFrame[Any]:
        def _get(node: str, warn_on_fail: bool = False) -> Optional[DataFrame[Any]]:
            try:
                return self[node]
            except KeyError:
                _, groups, leaves = next(self.walk("/"))
                path = node.split("/", 1)
                if len(path) < 2 or path[0] not in leaves + groups:
                    msg = f"No table found with name={node}. The following tables exist: {leaves + groups}"
                    raise KeyError(msg) from None
                msg = f"No partition '{path[1]}' found in table '{table}'"
                if warn_on_fail:
                    warnings.warn(msg)
                    return None
                raise IndexError(msg)

        if len(identifiers) == 0:
            return _get(table, warn_on_fail=False)

        result = []
        for partition in identifiers:
            if partition.game_id is None:
                games = self.games()
                if partition.competition_id is not None:
                    games = games[games.competition_id == partition.competition_id]
                if partition.season_id is not None:
                    games = games[games.season_id == partition.season_id]
                for game_id in games.game_id:
                    result.append(_get(f"{table}/game_{game_id}", warn_on_fail=True))
            else:
                result.append(_get(f"{table}/game_{partition.game_id}", warn_on_fail=True))
        result = [r for r in result if r is not None]
        if len(result) == 0:
            return pd.DataFrame()
        return cast(DataFrame[Any], pd.concat(result))

    def _insert_events(
        self, events: DataFrame[EventSchema], partition: PartitionIdentifier
    ) -> None:
        events = events.copy()
        events["event_id"] = events["event_id"].astype("str")
        with warnings.catch_warnings():
            warnings.filterwarnings("ignore", category=pd.io.pytables.PerformanceWarning)
            self.put(
                f"events/game_{partition.game_id}",
                events,
                format="fixed",
                data_columns=True,
            )
