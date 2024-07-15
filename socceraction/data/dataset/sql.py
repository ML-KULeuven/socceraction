"""SQLite database interface."""

from typing import Any, List, Tuple

import pandas as pd
from pandera.typing import DataFrame

try:
    import sqlalchemy
except ImportError:
    sqlalchemy = None

from socceraction.data.schema import EventSchema

from .base import Dataset, PartitionIdentifier


class SQLDataset(pd.io.sql.SQLDatabase, Dataset):
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

    def upsert_table(
        self,
        table: str,
        data: DataFrame[Any],
        primary_keys: list[str],
    ) -> None:
        self.to_sql(data, table, if_exists="append", index=False)
        self._drop_duplicates(table, primary_keys)

    def insert_partition_table(
        self,
        table: str,
        data: DataFrame[Any],
        partition_identifiers: PartitionIdentifier,
    ) -> None:
        # Drop partition
        if self.has_table(table):
            query = f"""
                DELETE IGNORE FROM {table}
                WHERE competition_id = {partition_identifiers.competition_id}
                    AND season_id = {partition_identifiers.season_id}
                    AND game_id = {partition_identifiers.game_id}
            """
            self.execute(query)
        data["competition_id"] = partition_identifiers.competition_id
        data["season_id"] = partition_identifiers.season_id
        data["game_id"] = partition_identifiers.game_id
        self.to_sql(data, table, if_exists="append", index=False)

    def read_table(self, table: str, *partitions: PartitionIdentifier) -> DataFrame[Any]:
        query = f"SELECT * FROM {table}"
        filters = []
        for identifier in partitions:
            filters.append(identifier.to_clause())
        if len(filters) > 0:
            query += f" WHERE {' OR '.join(filters)}"
        return self.read_query(query)

    def _insert_events(
        self, events: DataFrame[EventSchema], partition: PartitionIdentifier
    ) -> None:
        self.to_sql(
            events,
            "events",
            if_exists="append",
            index=False,
            dtype={
                "extra": sqlalchemy.types.JSON,
                "related_events": sqlalchemy.types.JSON,
                "location": sqlalchemy.types.JSON,
            },
        )
        self._drop_duplicates("events", ["game_id", "event_id"])

    def _import_table(self, table: pd.DataFrame, name: str) -> None:
        self.to_sql(table, name, if_exists="append", index=False)
        self._drop_duplicates(name, table.columns.tolist())

    def _drop_duplicates(self, table: str, keys: list) -> None:
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

    def get_home_away_team_id(self, game_id: int) -> tuple[int, int]:
        query = f"""
            SELECT home_team_id, away_team_id
            FROM games
            WHERE game_id = {game_id}
        """
        try:
            home_team_id, away_team_id = self.read_query(query).loc[0]
            return home_team_id, away_team_id
        except KeyError:
            raise IndexError(f"No game found with ID={game_id}") from None
