"""An API wrapper to access a SPADL dataset."""
import logging
from abc import ABC, abstractmethod
from typing import Callable, Optional

import pandas as pd
from tqdm import tqdm

from socceraction.data.base import EventDataLoader
from socceraction.spadl.schema import SPADLSchema

logger = logging.getLogger(__name__)


class Dataset(ABC):
    """An object that provides easy access to a SPADL event stream dataset.

    A dataset is an interface for loading games, teams, players, and actions
    contained in a SPADL dataset and defines a couple of methods to easily
    execute common queries on the SPADL data.
    """

    def __enter__(self):  # type: ignore
        """Make a database connection and return it."""
        return self

    def __exit__(  # type: ignore
        self,
        exc_type,
        exc_val,
        exc_tb,
    ):
        """Close the database connection."""
        self.close()

    def close(self) -> None:
        """Close the database connection."""

    def import_data(
        self,
        data_loader: EventDataLoader,
        fn_convert_to_actions: Callable[[pd.DataFrame, int], pd.DataFrame],
        competition_id: Optional[int] = None,
        season_id: Optional[int] = None,
        game_id: Optional[int] = None,
    ) -> None:
        """Import data."""
        # Retrieve all available competitions
        competitions = data_loader.competitions()
        if competition_id is not None:
            competitions = competitions[competitions.competition_id == competition_id]
        if season_id is not None:
            competitions = competitions[competitions.season_id == season_id]

        # Store competitions
        self._import_competitions(competitions)

        # Retrieve games from all selected competitions
        games = pd.concat(
            [
                data_loader.games(row.competition_id, row.season_id)
                for row in competitions.itertuples()
            ]
        )
        if game_id is not None:
            games = games[games.game_id == game_id]
        if games.empty:
            logger.exception("No games found with given criteria.")
            return

        # Load and convert match data
        games_verbose = tqdm(list(games.itertuples()), desc="Loading game data...")
        teams, players = [], []
        for game in games_verbose:
            try:
                teams.append(data_loader.teams(game.game_id))
                players.append(data_loader.players(game.game_id))
                events = data_loader.events(game.game_id)
                # Store actions
                actions = fn_convert_to_actions(events, game.home_team_id)
                self._import_actions(actions)
                self._import_games(games[games.game_id == game.game_id])
            except FileNotFoundError:
                logger.exception("Error adding game %s.", game.game_id)

        # Store teams
        self._import_teams(pd.concat(teams))

        # Store players
        self._import_players(pd.concat(players))

    @abstractmethod
    def _import_competitions(self, competitions: pd.DataFrame) -> None:
        pass

    @abstractmethod
    def _import_games(self, games: pd.DataFrame) -> None:
        pass

    @abstractmethod
    def _import_teams(self, teams: pd.DataFrame) -> None:
        pass

    @abstractmethod
    def _import_players(self, players: pd.DataFrame) -> None:
        pass

    @abstractmethod
    def _import_actions(self, actions: pd.DataFrame) -> None:
        pass

    @abstractmethod
    def games(
        self, competition_id: Optional[int] = None, season_id: Optional[int] = None
    ) -> pd.DataFrame:
        """Get a DataFrame of games.

        Parameters
        ----------
        competition_id : int, optional
            The ID of the competition.
        season_id : int, optional
            The ID of the season.

        Returns
        -------
        pandas.DataFrame
            A DataFrame of games.
        """

    @abstractmethod
    def teams(self) -> pd.DataFrame:
        """Get a DataFrame of teams.

        Returns
        -------
        pandas.DataFrame
            A DataFrame of teams.
        """

    @abstractmethod
    def players(self, game_id: Optional[int] = None) -> pd.DataFrame:
        """Get a DataFrame of players.

        Parameters
        ----------
        game_id : int, optional
            Only players that played in this game are returned.

        Returns
        -------
        pandas.DataFrame
            A DataFrame of players.
        """

    @abstractmethod
    def actions(self, game_id: int) -> pd.DataFrame:
        """Get a DataFrame of actions.

        Parameters
        ----------
        game_id : int
            The ID of the game.

        Returns
        -------
        pandas.DataFrame
            A DataFrame of actions.
        """

    # Games ##################################################################

    def get_home_away_team_id(self, game_id: int):
        """Return the id of the home and away team in a given game.

        Parameters
        ----------
        game_id : int
            The ID of a game.

        Returns
        -------
        (int, int)
            The ID of the home and away team.

        Raises
        ------
        IndexError
            If no game exists with the provided ID.
        """
        try:
            return self.games().loc[game_id, ["home_team_id", "away_team_id"]].values
        except KeyError:
            raise IndexError(f"No game found with ID={game_id}")

    # Players ################################################################

    def get_player_name(self, player_id: int):
        """Return the name of a player with a given ID.

        Parameters
        ----------
        player_id : int
            The ID of a player.

        Returns
        -------
            The name of the player.

        Raises
        ------
        IndexError
            If no player exists with the provided ID.
        """
        try:
            return self.players().at[player_id, "player_name"]
        except KeyError:
            raise IndexError("No player found with the provided ID.")

    def search_player(self, query: str, limit: int = 10):
        """Search for a player by name.

        Parameters
        ----------
        query : str
            The name of a player.
        limit : int
            Max number of results that are returned.

        Returns
        -------
        pd.DataFrame
            The first `limit` players that game the given query.

        """
        df = self.players()
        return df[df.player_name.str.contains(query, case=False)].head(limit)

    # Teams ##################################################################

    def get_team_name(self, team_id: int):
        """Return the name of a team with a given ID.

        Parameters
        ----------
        team_id : int
            The ID of a team.

        Returns
        -------
            The name of the team.

        Raises
        ------
        IndexError
            If no team exists with the provided ID.
        """
        try:
            return self.teams().at[team_id, "team_name"]
        except KeyError:
            raise IndexError("No team found with the provided ID.")

    def search_team(self, query, limit=10):
        """Search for a team by name.

        Parameters
        ----------
        query : str
            The name of a team.
        limit : int
            Max number of results that are returned.

        Returns
        -------
        pd.DataFrame
            The first `limit` teams that game the given query.

        """
        df = self.teams()
        return df[df.team_name.str.contains(query, case=False)].head(limit)
