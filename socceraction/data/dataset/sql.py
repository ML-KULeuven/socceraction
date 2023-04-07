"""SQLite database interface."""
from typing import Optional, Tuple

import pandas as pd

from .base import Dataset


class SQLDataset(Dataset, pd.io.sql.SQLDatabase):
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

    def _import_competitions(self, competitions: pd.DataFrame) -> None:
        self.to_sql(competitions, "competitions", if_exists="append", index=False)

    def _import_games(self, games: pd.DataFrame) -> None:
        self.to_sql(games, "games", if_exists="append", index=False)

    def _import_teams(self, teams: pd.DataFrame) -> None:
        self.to_sql(teams, "teams", if_exists="append", index=False)

    def _import_players(self, players: pd.DataFrame) -> None:
        self.to_sql(players, "players", if_exists="append", index=False)

    def _import_actions(self, actions: pd.DataFrame) -> None:
        self.to_sql(actions, "actions", if_exists="append", index=False)

    def games(
        self, competition_id: Optional[int] = None, season_id: Optional[int] = None
    ) -> pd.DataFrame:
        query = "SELECT * FROM games"
        filters = []
        if competition_id is not None:
            filters.append(f"competition_id = {competition_id}")
        if season_id is not None:
            filters.append(f"season_id = {season_id}")
        if len(filters):
            query += " WHERE " + " AND ".join(filters)
        return self.read_query(query, index_col="game_id")

    def teams(self) -> pd.DataFrame:
        return self.read_query("SELECT * FROM teams", index_col="team_id")

    def players(self) -> pd.DataFrame:
        return self.read_query("SELECT * FROM players", index_col="player_id")

    def actions(self, game_id: int) -> pd.DataFrame:
        query = f"SELECT * FROM actions WHERE game_id = {game_id}"
        df_actions = self.read_query(query, index_col=["game_id", "action_id"])
        if df_actions.empty:
            raise IndexError(f"No game found with ID={game_id}")
        return df_actions

    def get_home_away_team_id(self, game_id: int) -> Tuple[int, int]:
        query = f"""
            SELECT home_team_id, away_team_id
            FROM games
            WHERE game_id = {game_id}
        """
        try:
            home_team_id, away_team_id = self.read_query(query).loc[0]
            return home_team_id, away_team_id
        except KeyError:
            raise IndexError(f"No game found with ID={game_id}")
