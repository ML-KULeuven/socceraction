"""An API wrapper to access a SPADL dataset."""

import warnings
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any, Literal, Optional, Union, cast

import pandas as pd
from pandera.typing import DataFrame
from tqdm import tqdm

from socceraction.data.providers.base import EventDataLoader
from socceraction.data.schema import (
    CompetitionSchema,
    EventSchema,
    GameSchema,
    PlayerSchema,
    TeamSchema,
)
from socceraction.data.transforms import Transform, TransformException


@dataclass
class PartitionIdentifier:
    """A partition of a dataset.

    Attributes
    ----------
    competition_id : str or int, optional
        The ID of the competition.
    season_id : str or int, optional
        The ID of the season.
    game_id : str or int, optional
        The ID of the game.
    """

    competition_id: Optional[Union[str, int]] = None
    season_id: Optional[Union[str, int]] = None
    game_id: Optional[Union[str, int]] = None

    def to_clause(self, syntax: Literal["sql", "pandas"] = "sql") -> str:
        """Convert the partition to a SQL WHERE clause."""
        if syntax.lower() == "sql":
            return " AND ".join(
                f"{key}={value}" for key, value in self.__dict__.items() if value is not None
            )
        elif syntax.lower() == "pandas":
            return " and ".join(
                f"{key} == {value}" for key, value in self.__dict__.items() if value is not None
            )
        else:
            raise ValueError(f"Unsupported syntax: {syntax}")


class Dataset(ABC):
    """An object that provides easy access to an event stream dataset.

    A dataset is an interface for loading games, teams, players, and events
    contained in a structured format. The interface defines a couple of methods
    to easily execute common queries and transformations on the data.
    """

    def import_data(
        self,
        data_loader: EventDataLoader[Union[str, int]],
        partition: Optional[PartitionIdentifier] = None,
    ) -> None:
        """Import data, upserting it if it already exists.

        The data loader is used to retrieve the data from a source.
        The data that should be imported can be filtered by providing
        a competition, season, and/or game ID.

        Parameters
        ----------
        data_loader : EventDataLoader
            The data loader to use.
        partition : PartitionIdentifier, optional
            The IDs of the competition, season and/or game to add.
            If these are not given, all available data is imported.

        Raises
        ------
        LookupError
            If no data is found for the given partition.
        """
        competition_id = partition.competition_id if partition else None
        season_id = partition.season_id if partition else None
        game_id = partition.game_id if partition else None

        # Retrieve all available competitions
        competitions = data_loader.competitions()

        # Store competitions
        if competitions.empty:
            if competition_id is not None and season_id is not None:
                warnings.warn("No competition data was found.", stacklevel=2)
                games = data_loader.games(competition_id, season_id)
            else:
                raise LookupError(
                    "No competitions were found for the given criteria. "
                    "If no data about available competions is available, "
                    "you can load invididual seasons by providing "
                    "both a competition and season id."
                )
        else:
            if competition_id is not None:
                competitions = competitions.loc[competitions.competition_id == competition_id]
            if season_id is not None:
                competitions = competitions.loc[competitions.season_id == season_id]

            self._insert_competitions(competitions)

            # Retrieve games from all selected competitions
            games = pd.concat(
                [
                    data_loader.games(row.competition_id, row.season_id)
                    for row in competitions.itertuples()
                ]
            )
        if game_id is not None:
            games = games.loc[games.game_id == game_id]
        if games.empty:
            raise LookupError("No games found for the given criteria.")

        # Load and convert match data
        games_verbose = tqdm(games.itertuples(), total=len(games), desc="Loading game data...")
        teams, players = [], []
        for game in games_verbose:
            try:
                teams.append(data_loader.teams(game.game_id))
                players.append(data_loader.players(game.game_id))
                events = data_loader.events(game.game_id)
                self._insert_events(
                    events,
                    PartitionIdentifier(
                        competition_id=game.competition_id,
                        season_id=game.season_id,
                        game_id=game.game_id,
                    ),
                )
            except FileNotFoundError:
                warnings.warn(f"Game {game_id} not found. Skipping.", stacklevel=2)
            except Exception as e:
                warnings.warn(f"Error parsing game {game_id}: {e}. Skipping.", stacklevel=2)

        # Store teams
        self._insert_teams(cast(DataFrame[TeamSchema], pd.concat(teams)))

        # Store players
        self._insert_players(cast(DataFrame[PlayerSchema], pd.concat(players)))

        # Store games
        self._insert_games(cast(DataFrame[GameSchema], games))

    def competitions(self) -> DataFrame[CompetitionSchema]:
        """Get a DataFrame with all competitions in the dataset.

        Returns
        -------
        pandas.DataFrame
            A DataFrame of competitions.
        """
        return self.read_table("competitions")

    def _insert_competitions(self, competitions: DataFrame[CompetitionSchema]) -> None:
        self.upsert_table("competitions", competitions, ["competition_id", "season_id"])

    def games(self) -> DataFrame[GameSchema]:
        """Get a DataFrame with all games in the dataset.

        Returns
        -------
        pandas.DataFrame
            A DataFrame of games.
        """
        return self.read_table("games")

    def _insert_games(self, games: DataFrame[GameSchema]) -> None:
        self.upsert_table("games", games, ["game_id"])

    def teams(self) -> DataFrame[TeamSchema]:
        """Get a DataFrame with all teams in the dataset.

        Returns
        -------
        pandas.DataFrame
            A DataFrame of teams.
        """
        return self.read_table("teams")

    def _insert_teams(self, teams: DataFrame[TeamSchema]) -> None:
        self.upsert_table("teams", teams, ["team_id"])

    def players(self, game_id: Optional[int] = None) -> DataFrame[PlayerSchema]:
        """Get a DataFrame all players in the dataset.

        Parameters
        ----------
        game_id : int, optional
            Only players that played in this game are returned.

        Returns
        -------
        pandas.DataFrame
            A DataFrame of players.

        Raises
        ------
        LookupError
            If no game is found with the provided ID.
        """
        player_games = self.read_table("player_games")

        if game_id is not None:
            players = player_games[player_games.game_id == game_id]
            if len(players) == 0:
                raise LookupError(f"No game found with ID={game_id}") from None
            return players

        cols = ["team_id", "player_id", "player_name"]
        return player_games[cols].drop_duplicates()

    def _insert_players(self, players: DataFrame[PlayerSchema]) -> None:
        self.upsert_table("player_games", players, ["player_id", "game_id"])

    def events(self, game_id: int) -> DataFrame[EventSchema]:
        """Get a DataFrame of events.

        Parameters
        ----------
        game_id : int
            The ID of the game.

        Returns
        -------
        pandas.DataFrame
            A DataFrame of events.

        Raises
        ------
        LookupError
            If no game is found with the provided ID.
        """
        with warnings.catch_warnings():
            warnings.filterwarnings("ignore")
            events = self.read_table("events", partitions=[PartitionIdentifier(game_id=game_id)])
        if len(events) == 0:
            raise LookupError(f"No events found for game with ID={game_id}")
        return events

    def _insert_events(
        self, events: DataFrame[EventSchema], partition: PartitionIdentifier
    ) -> None:
        self.insert_partition_table("events", events, partition)

    @abstractmethod
    def upsert_table(
        self,
        table: str,
        data: DataFrame[Any],
        primary_keys: list[str],
    ) -> None:
        """Add a dataframe to the dataset.

        This method should be used to store data in a single table. Typically,
        this is useful for storing data that is not related to a specific game.

        The data is upserted, meaning that if a row with the same primary key
        already exists, the row is updated.

        For inserting data that is stored in multiple tables, use the
        `insert_partition_table` method.

        Parameters
        ----------
        table : str
            The name of the table to insert the data into.
        data : pandas.DataFrame
            The data to insert.
        primary_keys : list[str]
            The columns that should be used as primary keys. These columns
            should uniquely identify a row in the data.
        """

    @abstractmethod
    def insert_partition_table(
        self,
        table: str,
        data: DataFrame[Any],
        partition: PartitionIdentifier,
    ) -> None:
        """Add a dataframe with data of a single game to the dataset.

        This method stores data that is related to a specific competition,
        season and/or game in a partitioned table.

        The data is overwritten if a table for the given partition already
        exists.

        For storing data in a single table, use the `upsert_table` method.

        Parameters
        ----------
        table : str
            The name of the table to insert the data into.
        data : pandas.DataFrame
            The data of a single game to insert.
        partition: PartitionIdentifier
            The IDs of the competition, season and game that the data belongs to.
        """

    @abstractmethod
    def read_table(
        self, table: str, *, partitions: Optional[list[PartitionIdentifier]] = None
    ) -> DataFrame[Any]:
        """Read a table from the dataset.

        Parameters
        ----------
        table : str
            The name of the table to read.
        partitions : list of PartitionIdentifier, optional
            The IDs of the competitions, seasons and/or games to read. If not
            provided, all data is read.

        Returns
        -------
        pandas.DataFrame
            The data from the table.
        """

    # Games ##################################################################

    def get_home_away_team_id(self, game_id: int) -> tuple[int, int]:
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
        LookupError
            If no game exists with the provided ID.
        """
        try:
            return (
                self.games()
                .set_index("game_id")
                .loc[game_id, ["home_team_id", "away_team_id"]]
                .values
            )
        except KeyError:
            raise LookupError(f"No game found with ID={game_id}") from None

    # Players ################################################################

    def get_player_name(self, player_id: int) -> str:
        """Return the name of a player with a given ID.

        Parameters
        ----------
        player_id : int
            The ID of a player.

        Returns
        -------
        str
            The name of the player.

        Raises
        ------
        IndexError
            If no player exists with the provided ID.
        """
        try:
            return self.players().set_index("player_id").at[player_id, "player_name"]
        except KeyError:
            raise IndexError(f"No player found with ID={player_id}") from None

    def search_player(self, query: str, limit: int = 10) -> pd.DataFrame:
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
        return df.loc[df.player_name.str.contains(query, case=False)].head(limit)

    # Teams ##################################################################

    def get_team_name(self, team_id: int) -> str:
        """Return the name of a team with a given ID.

        Parameters
        ----------
        team_id : int
            The ID of a team.

        Returns
        -------
        str
            The name of the team.

        Raises
        ------
        IndexError
            If no team exists with the provided ID.
        """
        try:
            return self.teams().set_index("team_id").at[team_id, "team_name"]
        except KeyError:
            raise IndexError(f"No team found with ID={team_id}") from None

    def search_team(self, query: str, limit: int = 10) -> pd.DataFrame:
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
        return df.loc[df.team_name.str.contains(query, case=False)].head(limit)

    ## Transformations #######################################################

    def transform(
        self,
        transformation: Transform[Any],
        from_table: Union[str, list[str]],
        to_table: Optional[str],
        *,
        partitions: Optional[list[PartitionIdentifier]] = None,
        on_fail: Literal["raise", "warn"] = "warn",
    ) -> Optional[pd.DataFrame]:
        """Apply a transformer to the dataset.

        Parameters
        ----------
        transformation : Transform
            The transformation to apply.
        from_table : str or list[str]
            The table(s) to read data from. If multiple tables are given,
            the data is passed as a list to the transformation function.
        to_table : str, optional
            The table to insert the transformed data into. If not given, the
            transformed data is returned as a dataframe.
        partitions : list of PartitionIdentifier, optional
            The partitions to apply the transformation to. If not given, the
            transformation is applied to all games.
        on_fail : {'raise', 'warn'}
            What to do when a transformation fails. If 'raise', an exception
            is raised as soon as the transformation fails for a game. If 'warn',
            a warning is issued and the game is skipped.

        Raises
        ------
        TransformException
            If a transformation fails and `on_fail` is set to 'raise'.

        Returns
        -------
        pandas.DataFrame, optional
            The transformed data, if `to_table` is not None.
        """
        all_games = self.games()
        todo_games = (
            cast(
                DataFrame[GameSchema],
                pd.concat(
                    [all_games.query(partition.to_clause("pandas")) for partition in partitions]
                ),
            )
            if partitions is not None and len(partitions) > 0
            else all_games
        )
        result = []
        todo_games_verbose = tqdm(
            todo_games.itertuples(), total=len(todo_games), desc="Transforming dataset"
        )
        for game in todo_games_verbose:
            try:
                if isinstance(from_table, str):
                    input = self.read_table(
                        from_table, partitions=[PartitionIdentifier(game_id=game.game_id)]
                    )
                    output = transformation(game, input)
                else:
                    inputs = [
                        self.read_table(t, partitions=[PartitionIdentifier(game_id=game.game_id)])
                        for t in from_table
                    ]
                    output = transformation(game, inputs)
                if to_table is not None:
                    self.insert_partition_table(
                        to_table,
                        output,
                        PartitionIdentifier(
                            competition_id=game.competition_id,
                            season_id=game.season_id,
                            game_id=game.game_id,
                        ),
                    )
                else:
                    result.append(output)
            except Exception as e:
                if on_fail == "warn":
                    warnings.warn(f"Failed for game with id={game.game_id}: {e}", stacklevel=2)
                else:
                    raise TransformException(game_id=game.game_id) from e
        if to_table is None:
            return pd.concat(result)
        return None
