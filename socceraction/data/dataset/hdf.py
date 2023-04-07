"""HDF store interface."""
import logging
from typing import Optional

import pandas as pd

from .base import Dataset

logger = logging.getLogger(__name__)


class HDFDataset(Dataset, pd.HDFStore):
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

    def _import_competitions(self, competitions: pd.DataFrame) -> None:
        competitions = competitions.copy()
        try:
            competitions = pd.concat([self["competitions"], competitions])
        except KeyError:
            pass
        competitions.drop_duplicates(
            subset=["competition_id", "season_id"], keep="last", inplace=True
        )
        self.put("competitions", competitions, format="table", data_columns=True)
        logger.debug("Imported %d competitions", len(competitions))

    def _import_games(self, games: pd.DataFrame) -> None:
        games = games.copy()
        try:
            games = pd.concat([self["games"], games])
        except KeyError:
            pass
        games.drop_duplicates(subset=["game_id"], keep="last", inplace=True)
        self.put("games", games, format="table", data_columns=True)
        logger.debug("Imported %d games", len(games))

    def _import_teams(self, teams: pd.DataFrame) -> None:
        teams = teams.copy()
        try:
            teams = pd.concat([self["teams"], teams])
        except KeyError:
            pass
        teams.drop_duplicates(subset=["team_id"], keep="last", inplace=True)
        self.put("teams", teams, format="table", data_columns=True)
        logger.debug("Imported %d teams", len(teams))

    def _import_players(self, players: pd.DataFrame) -> None:
        playerids = players.copy()
        try:
            playerids = pd.concat([self["players"], playerids])
        except KeyError:
            pass
        playerids.drop_duplicates(subset=["player_id"], keep="last", inplace=True)
        self.put("players", playerids, format="table", data_columns=True)
        logger.debug("Imported %d players", len(players))

        player_games = players.copy()
        try:
            player_games = pd.concat([self["player_games"], player_games])
        except KeyError:
            pass
        player_games.drop_duplicates(subset=["player_id", "game_id"], keep="last", inplace=True)
        self.put("player_games", player_games, format="table", data_columns=True)

    def _import_actions(self, actions: pd.DataFrame) -> None:
        actions = actions.copy()
        actions['original_event_id'] = actions['original_event_id'].astype('str')
        self.put(
            f"actions/game_{actions.game_id.iloc[0]}",
            actions,
            format="table",
            data_columns=True,
        )
        logger.debug("Imported %d actions", len(actions))

    def games(
        self, competition_id: Optional[int] = None, season_id: Optional[int] = None
    ) -> pd.DataFrame:
        games = self["games"]
        if competition_id is not None:
            games = games[games["competition_id"] == competition_id]
        if season_id is not None:
            games = games[games["season_id"] == season_id]
        return games.set_index("game_id")

    def teams(self) -> pd.DataFrame:
        teams = self["teams"]
        return teams.set_index("team_id")

    def players(self) -> pd.DataFrame:
        players = self["players"]
        return players.set_index(["game_id", "player_id"])

    def actions(self, game_id: int) -> pd.DataFrame:
        try:
            df_actions = self[f"actions/game_{game_id}"].set_index(["game_id", "action_id"])
        except KeyError:
            raise IndexError(f"No game found with ID={game_id}")
        return df_actions
