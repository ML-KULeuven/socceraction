"""Parquet dataset interface supporting local and remote fsspec filesystems."""

import os
from types import TracebackType
from typing import Any, Literal, Optional, Union, cast

import pandas as pd
from pandera.typing import DataFrame
from tqdm import tqdm
from typing_extensions import override

try:
    import pyarrow  # noqa: F401
except ImportError:
    pyarrow = None  # type: ignore

from socceraction.data.schema import EventSchema

from .base import Dataset, PartitionIdentifier
from .fsspec import FSSpecAdapter


class ParquetDataset(Dataset):
    """Wrapper for a Parquet database storing SPADL data on any fsspec filesystem.

    Parameters
    ----------
    path : str or os.PathLike
        Root directory path or URL where the dataset is stored (e.g., local directory,
        'memory://dataset', 's3://bucket/dataset').
    storage_options : dict, optional
        Extra options passed to the underlying fsspec filesystem backend.
    adapter : FSSpecAdapter, optional
        Custom FSSpecAdapter instance to use for filesystem operations. If not
        provided, an adapter is created using `storage_options`.
    engine : {'pyarrow', 'fastparquet'}, default 'pyarrow'
        Parquet engine to use for reading and writing dataframes.
    """

    def __init__(
        self,
        path: Union[str, os.PathLike[str]],
        storage_options: Optional[dict[str, object]] = None,
        adapter: Optional[FSSpecAdapter] = None,
        engine: Literal["pyarrow", "fastparquet"] = "pyarrow",
    ) -> None:
        self._path = str(path).rstrip("/")
        self.engine = engine
        self._adapter = adapter or FSSpecAdapter(storage_options=storage_options)
        self._adapter.makedirs(self._path, exist_ok=True)

    def __enter__(self) -> "ParquetDataset":
        """Enter the runtime context for the ParquetDataset.

        Returns
        -------
        ParquetDataset
            The dataset instance.
        """
        return self

    def __exit__(
        self,
        exc_type: Optional[type[BaseException]],
        exc_val: Optional[BaseException],
        exc_tb: Optional[TracebackType],
    ) -> None:
        """Exit the runtime context for the ParquetDataset.

        Parameters
        ----------
        exc_type : type of BaseException, optional
            The exception type if an error occurred.
        exc_val : BaseException, optional
            The exception value.
        exc_tb : TracebackType, optional
            The traceback.
        """
        pass

    def _get_table_path(self, table: str) -> str:
        return f"{self._path}/{table}.parquet"

    def _get_partition_path(self, table: str, partition: PartitionIdentifier) -> str:
        return f"{self._path}/{table}/game_{partition.game_id}.parquet"

    def _get_partition_dir(self, table: str) -> str:
        return f"{self._path}/{table}"

    def _get_tables(self) -> tuple[set[str], set[str]]:
        """List unpartitioned and partitioned tables in the dataset.

        Returns
        -------
        leaves : set of str
            Names of unpartitioned tables (single files).
        groups : set of str
            Names of partitioned tables (directories).
        """
        items = self._adapter.list_directory(self._path, recursive=False)
        leaves: set[str] = set()
        groups: set[str] = set()
        for item in items:
            name = item.rstrip("/").split("/")[-1]
            if name.endswith(".parquet"):
                leaves.add(name[:-8])
            elif self._adapter.is_directory(item):
                groups.add(name)
        return leaves, groups

    def _get_partition_keys(self, table: str, partitions: list[PartitionIdentifier]) -> list[str]:
        """Get file paths for specified partition identifiers.

        Parameters
        ----------
        table : str
            The name of the table.
        partitions : list of PartitionIdentifier
            The partitions to read.

        Returns
        -------
        list of str
            Existing partition file paths.
        """
        keys: set[str] = set()
        games_df: Optional[DataFrame[Any]] = None
        for partition in partitions:
            if partition.game_id is not None:
                path = self._get_partition_path(table, partition)
                if self._adapter.exists(path):
                    keys.add(path)
            else:
                if games_df is None:
                    games_df = self.games()
                selected_games = games_df
                if partition.competition_id is not None:
                    selected_games = selected_games[
                        selected_games.competition_id == partition.competition_id
                    ]
                if partition.season_id is not None:
                    selected_games = selected_games[
                        selected_games.season_id == partition.season_id
                    ]
                for game_id in selected_games.game_id:
                    path = self._get_partition_path(table, PartitionIdentifier(game_id=game_id))
                    if self._adapter.exists(path):
                        keys.add(path)
        return sorted(keys)

    def _get_keys(
        self, table: str, partitions: Optional[list[PartitionIdentifier]] = None
    ) -> list[str]:
        """Get the file paths corresponding to a table and optional partitions.

        Parameters
        ----------
        table : str
            The name of the table.
        partitions : list of PartitionIdentifier, optional
            The partitions to read.

        Returns
        -------
        list of str
            File paths to read.

        Raises
        ------
        KeyError
            If no table with the given name exists.
        """
        leaves, groups = self._get_tables()
        if table in leaves:
            return [self._get_table_path(table)]

        if table not in groups:
            available = sorted(leaves | groups)
            raise KeyError(
                f"No table found with name={table}. The following tables exist: {available}"
            )

        if partitions is None or len(partitions) == 0:
            part_files = self._adapter.list_directory(
                self._get_partition_dir(table), recursive=False
            )
            return sorted([f for f in part_files if f.endswith(".parquet") and "game_" in f])

        return self._get_partition_keys(table, partitions)

    def _read_parquet(self, path: str) -> DataFrame[Any]:
        with self._adapter.open(path, "rb") as f:
            return cast(DataFrame[Any], pd.read_parquet(f, engine=self.engine))

    def _write_parquet(self, path: str, data: DataFrame[Any]) -> None:
        parent_dir = path.rsplit("/", 1)[0]
        if parent_dir and parent_dir != self._path:
            self._adapter.makedirs(parent_dir, exist_ok=True)
        with self._adapter.open(path, "wb") as f:
            data.to_parquet(f, engine=self.engine, index=False)

    @override
    def upsert_table(
        self,
        table: str,
        data: DataFrame[Any],
        primary_keys: list[str],
    ) -> None:
        data = data.copy()
        try:
            existing = self.read_table(table)
            data = cast(DataFrame[Any], pd.concat([existing, data]))
        except KeyError:
            pass
        data.drop_duplicates(subset=primary_keys, keep="last", inplace=True)
        self._write_parquet(self._get_table_path(table), data)

    @override
    def insert_partition_table(
        self,
        table: str,
        data: DataFrame[Any],
        partition: PartitionIdentifier,
    ) -> None:
        data = data.copy()
        self._write_parquet(self._get_partition_path(table, partition), data)

    @override
    def read_table(
        self,
        table: str,
        *,
        partitions: Optional[list[PartitionIdentifier]] = None,
        engine: Literal["pandas", "dask"] = "pandas",
        show_progress: bool = False,
    ) -> DataFrame[Any]:
        files = self._get_keys(table, partitions)
        if len(files) == 0:
            if engine == "dask":
                import dask.dataframe as dd

                return cast(DataFrame[Any], dd.from_pandas(pd.DataFrame(), npartitions=1))
            return cast(DataFrame[Any], pd.DataFrame())

        if engine == "dask":
            import dask.dataframe as dd

            fs = self._adapter._get_filesystem_for_reading(files[0])
            return cast(
                DataFrame[Any],
                dd.read_parquet(files, filesystem=fs, engine=self.engine),
            )
        else:
            files_verbose = tqdm(files, desc=f"Reading {table}") if show_progress else files
            result_dfs = [self._read_parquet(f) for f in files_verbose]
            result_dfs = [r for r in result_dfs if r is not None and len(r) > 0]
            if len(result_dfs) == 0:
                return cast(DataFrame[Any], pd.DataFrame())
            return cast(DataFrame[Any], pd.concat(result_dfs, ignore_index=True))

    @override
    def _insert_events(
        self, events: DataFrame[EventSchema], partition: PartitionIdentifier
    ) -> None:
        events = events.copy()
        events["event_id"] = events["event_id"].astype("str")
        self.insert_partition_table("events", events, partition)

    def __getitem__(self, key: str) -> DataFrame[Any]:
        """Retrieve table or partition by key.

        Parameters
        ----------
        key : str
            Table name or partition path (e.g. 'competitions' or 'events/game_3795107').

        Returns
        -------
        pandas.DataFrame
            The requested table or partition data.

        Raises
        ------
        KeyError
            If the table or partition does not exist.
        """
        key = key.lstrip("/")
        if "/" in key:
            table, partition_file = key.split("/", 1)
            partition_name = partition_file.replace(".parquet", "")
            path = f"{self._path}/{table}/{partition_name}.parquet"
            if not self._adapter.exists(path):
                raise KeyError(f"No partition '{partition_file}' found in table '{table}'")
            return self._read_parquet(path)
        return self.read_table(key)

    def __contains__(self, key: str) -> bool:
        """Check if table or partition exists.

        Parameters
        ----------
        key : str
            Table name or partition key.

        Returns
        -------
        bool
            True if exists, False otherwise.
        """
        try:
            key = key.lstrip("/")
            if "/" in key:
                table, partition_file = key.split("/", 1)
                partition_name = partition_file.replace(".parquet", "")
                path = f"{self._path}/{table}/{partition_name}.parquet"
                return self._adapter.exists(path)
            table_path = self._get_table_path(key)
            dir_path = self._get_partition_dir(key)
            return self._adapter.is_file(table_path) or self._adapter.is_directory(dir_path)
        except Exception:
            return False

    @override
    def close(self) -> None:
        """Close dataset resources."""
        pass
