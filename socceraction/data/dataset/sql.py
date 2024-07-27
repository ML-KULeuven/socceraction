"""SQLite database interface."""

from typing import Any, Optional, cast

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

    @override
    def upsert_table(
        self,
        table: str,
        data: DataFrame[Any],
        primary_keys: list[str],
    ) -> None:
        self.to_sql(data, table, if_exists="append", index=False)
        self._drop_duplicates(table, primary_keys)

    @override
    def insert_partition_table(
        self,
        table: str,
        data: DataFrame[Any],
        partition: PartitionIdentifier,
    ) -> None:
        # Drop partition
        if self.has_table(table):
            query = f"""
                DELETE IGNORE FROM {table}
                WHERE competition_id = {partition.competition_id}
                    AND season_id = {partition.season_id}
                    AND game_id = {partition.game_id}
            """
            self.execute(query)
        data["competition_id"] = partition.competition_id
        data["season_id"] = partition.season_id
        data["game_id"] = partition.game_id
        self.to_sql(data, table, if_exists="append", index=False)

    @override
    def read_table(
        self, table: str, *, partitions: Optional[list[PartitionIdentifier]] = None
    ) -> DataFrame[Any]:
        query = f"SELECT * FROM {table}"
        filters = []
        if partitions is not None:
            for identifier in partitions:
                filters.append(identifier.to_clause())
        if len(filters) > 0:
            query += f" WHERE {' OR '.join(filters)}"
        return cast(DataFrame[Any], self.read_query(query))

    @override
    def _insert_events(
        self, events: DataFrame[EventSchema], partition: PartitionIdentifier
    ) -> None:
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

    def _drop_duplicates(self, table: str, keys: list[str]) -> None:
        """Drop duplicate rows from a table.

        Parameters
        ----------
        table : str
            The name of the table.
        keys : list
            The columns to use to determine duplicates.
        """
        query = f"""
            DELETE FROM {table}
            WHERE rowid NOT IN (
                SELECT MIN(rowid)
                FROM {table}
                GROUP BY {", ".join(keys)}
            )
        """
        self.execute(query)

    @override
    def get_home_away_team_id(self, game_id: int) -> tuple[int, int]:
        query = f"""
            SELECT home_team_id, away_team_id
            FROM games
            WHERE game_id = {game_id}
        """
        result = self.read_query(query)
        assert isinstance(result, pd.DataFrame)
        if result.empty:
            raise LookupError(f"No game found with ID={game_id}") from None
        home_team_id, away_team_id = result.iloc[0]
        return home_team_id, away_team_id
