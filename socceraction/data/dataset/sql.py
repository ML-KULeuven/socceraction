"""SQLite database interface."""

import json
from collections.abc import Iterator
from contextlib import AbstractContextManager, contextmanager
from types import TracebackType
from typing import Any, Literal, Optional, Union, cast

import pandas as pd
from pandas.io.sql import SQLDatabase
from pandera.typing import DataFrame
from typing_extensions import override

try:
    from sqlalchemy.types import JSON
except ImportError:
    # use strings for the sqlite3 legacy mode
    JSON = "json"  # type: ignore

from socceraction.data.schema import EventSchema

from .base import Dataset, PartitionIdentifier


class SQLDataset(SQLDatabase, Dataset):
    """Wrapper for a SQL database holding the raw data.

    Parameters
    ----------
    con : SQLAlchemy Connectable or URI string.
        Connectable to connect with the database. Using SQLAlchemy makes it
        possible to use any DB supported by that library.
    schema : string, default None
        Name of SQL schema in database to write to (if database flavor
        supports this). If None, use default schema (default).
    need_transaction : bool, default False
        If True, SQLDatabase will create a transaction.
    """

    def __init__(
        self,
        con: Union[str, object],
        schema: Optional[str] = None,
        need_transaction: bool = False,
    ) -> None:
        super().__init__(con, schema=schema, need_transaction=need_transaction)
        self._transaction_depth: int = 0

    def _begin_transaction(self) -> Optional[AbstractContextManager[object]]:
        if hasattr(self.con, "in_transaction") and callable(self.con.in_transaction):
            if self.con.in_transaction():
                return self.con.begin_nested()
            return self.con.begin()
        return None

    def _end_transaction(self, *, commit: bool) -> None:
        method_name = "commit" if commit else "rollback"
        action = getattr(self.con, method_name, None)
        if callable(action):
            in_txn = getattr(self.con, "in_transaction", None)
            if in_txn is None or (callable(in_txn) and in_txn()):
                action()

    @contextmanager
    @override
    def transaction(self) -> Iterator[Any]:
        """Context manager for transactional operations.

        Ensures atomic execution of database writes. If a transaction is
        already active on the connection, a savepoint (nested transaction)
        is used. If an exception occurs, changes are rolled back.
        Otherwise, changes are committed upon exiting the outermost block.

        Yields
        ------
        Any
            The underlying database connection.
        """
        if not hasattr(self, "_transaction_depth"):
            self._transaction_depth = 0

        is_outer = self._transaction_depth == 0
        self._transaction_depth += 1
        txn = self._begin_transaction()

        try:
            if txn is not None:
                with txn:
                    yield self.con
            else:
                yield self.con

            if is_outer:
                self._end_transaction(commit=True)
        except Exception:
            if is_outer:
                self._end_transaction(commit=False)
            raise
        finally:
            self._transaction_depth -= 1

    @override
    def upsert_table(
        self,
        table: str,
        data: DataFrame[Any],
        primary_keys: list[str],
    ) -> None:
        with self.transaction():
            self.to_sql(data, table, if_exists="append", index=False)
            self._drop_duplicates(table, primary_keys)

    @override
    def insert_partition_table(
        self,
        table: str,
        data: DataFrame[Any],
        partition: PartitionIdentifier,
    ) -> None:
        data = data.copy()
        with self.transaction():
            if self.has_table(table):
                clause = partition.to_clause("sql")
                if clause:
                    query = f"DELETE FROM {table} WHERE {clause}"
                    self.execute(query)
            data["competition_id"] = partition.competition_id
            data["season_id"] = partition.season_id
            data["game_id"] = partition.game_id
            self.to_sql(data, table, if_exists="append", index=False)

    @override
    def read_table(
        self,
        table: str,
        *,
        partitions: Optional[list[PartitionIdentifier]] = None,
        engine: Literal["pandas", "dask"] = "pandas",
        show_progress: bool = False,
    ) -> DataFrame[Any]:
        if not self.has_table(table):
            raise KeyError(f"No table found with name={table}")
        query = f"SELECT * FROM {table}"
        filters = []
        if partitions is not None:
            for identifier in partitions:
                clause = identifier.to_clause("sql")
                if clause:
                    filters.append(clause)
        if len(filters) > 0:
            query += f" WHERE {' OR '.join(f'({f})' for f in filters)}"
        if engine == "dask":
            raise NotImplementedError("Dask engine is not supported for SQLDataset.")
        df = cast(DataFrame[Any], self.read_query(query))
        if table == "events":
            for col in ("extra", "related_events", "location"):
                if col in df.columns:
                    df[col] = df[col].apply(self._deserialize_json)
        return df

    @staticmethod
    def _deserialize_json(val: object) -> object:
        if isinstance(val, str):
            try:
                return json.loads(val)
            except (ValueError, TypeError):
                return val
        return val

    @override
    def _insert_events(
        self, events: DataFrame[EventSchema], partition: PartitionIdentifier
    ) -> None:
        with self.transaction():
            self.to_sql(
                events,
                "events",
                if_exists="append",
                index=False,
                dtype={
                    "extra": JSON,
                    "related_events": JSON,
                    "location": JSON,
                },
            )
            self._drop_duplicates("events", ["game_id", "event_id"])

    @override
    def close(self) -> None:
        """Close database connection and release resources."""
        self._end_transaction(commit=True)
        if hasattr(self, "exit_stack"):
            self.exit_stack.close()

    @override
    def __enter__(self) -> "SQLDataset":
        """Enter runtime context."""
        return self

    @override
    def __exit__(
        self,
        exc_type: Optional[type[BaseException]],
        exc_val: Optional[BaseException],
        exc_tb: Optional[TracebackType],
    ) -> None:
        """Exit runtime context and close resources."""
        if exc_type is None:
            self._end_transaction(commit=True)
        else:
            self._end_transaction(commit=False)
        super().__exit__(exc_type, exc_val, exc_tb)

    def _drop_duplicates(self, table: str, keys: list[str]) -> None:
        """Drop duplicate rows from a table.

        Parameters
        ----------
        table : str
            The name of the table.
        keys : list
            The columns to use to determine duplicates.
        """
        try:
            dialect = self.con.dialect.name
        except AttributeError:
            dialect = "sqlite"

        row_id = "ctid" if dialect == "postgresql" else "rowid"
        query = f"""
            DELETE FROM {table}
            WHERE {row_id} NOT IN (
                SELECT MIN({row_id})
                FROM {table}
                GROUP BY {", ".join(keys)}
            )
        """
        self.execute(query)

    @override
    def get_home_away_team_id(self, game_id: int) -> tuple[int, int]:
        game_val = f"'{game_id}'" if isinstance(game_id, str) else game_id
        query = f"""
            SELECT home_team_id, away_team_id
            FROM games
            WHERE game_id = {game_val}
        """
        result = self.read_query(query)
        assert isinstance(result, pd.DataFrame)
        if result.empty:
            raise LookupError(f"No game found with ID={game_id}") from None
        home_team_id, away_team_id = result.iloc[0]
        return int(home_team_id), int(away_team_id)

    def __contains__(self, key: str) -> bool:
        """Check if table exists in dataset.

        Parameters
        ----------
        key : str
            Table name.

        Returns
        -------
        bool
            True if exists, False otherwise.
        """
        return self.has_table(key)
