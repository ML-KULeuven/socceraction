"""Implements serializers for Wyscout data."""

import glob
import os
import re
import warnings
from pathlib import Path
from typing import Any, Callable, Optional, Union, cast
from urllib.error import HTTPError
from urllib.parse import urlparse
from urllib.request import urlopen, urlretrieve
from zipfile import ZipFile, is_zipfile

import pandas as pd  # type: ignore
from pandera.typing import DataFrame

from ..base import (
    EventDataLoader,
    JSONType,
    MissingDataError,
    ParseError,
    _auth_remoteloadjson,
    _expand_minute,
    _has_auth,
    _localloadjson,
    _remoteloadjson,
)
from .schema import (
    WyscoutCompetitionSchema,
    WyscoutEventSchema,
    WyscoutGameSchema,
    WyscoutPlayerSchema,
    WyscoutTeamSchema,
)


class PublicWyscoutLoader(EventDataLoader):
    """
    Load the public Wyscout dataset.

    This dataset is a public release of event stream data, collected by Wyscout
    (https://wyscout.com/) containing all matches of the 2017/18 season of the
    top-5 European leagues (La Liga, Serie A, Bundesliga, Premier League, Ligue
    1), the FIFA World Cup 2018, and UEFA Euro Cup 2016. For a detailed
    description, see Pappalardo et al. [1]_.

    Parameters
    ----------
    root : str
        Path where a local copy of the dataset is stored or where the
        downloaded dataset should be stored.
    download : bool
        Whether to force a redownload of the data.

    References
    ----------
    .. [1] Pappalardo, L., Cintia, P., Rossi, A. et al. A public data set of
        spatio-temporal match events in soccer competitions. Sci Data 6, 236
        (2019). https://doi.org/10.1038/s41597-019-0247-7
    """

    def __init__(self, root: Optional[str] = None, download: bool = False) -> None:
        if root is None:
            self.root = os.path.join(os.getcwd(), "wyscout_data")
            os.makedirs(self.root, exist_ok=True)
        else:
            self.root = root

        self.get = _localloadjson

        if download or len(os.listdir(self.root)) == 0:
            self._download_repo()

        self._index = pd.DataFrame(
            [
                {
                    "competition_id": 524,
                    "season_id": 181248,
                    "season_name": "2017/2018",
                    "db_matches": "matches_Italy.json",
                    "db_events": "events_Italy.json",
                },
                {
                    "competition_id": 364,
                    "season_id": 181150,
                    "season_name": "2017/2018",
                    "db_matches": "matches_England.json",
                    "db_events": "events_England.json",
                },
                {
                    "competition_id": 795,
                    "season_id": 181144,
                    "season_name": "2017/2018",
                    "db_matches": "matches_Spain.json",
                    "db_events": "events_Spain.json",
                },
                {
                    "competition_id": 412,
                    "season_id": 181189,
                    "season_name": "2017/2018",
                    "db_matches": "matches_France.json",
                    "db_events": "events_France.json",
                },
                {
                    "competition_id": 426,
                    "season_id": 181137,
                    "season_name": "2017/2018",
                    "db_matches": "matches_Germany.json",
                    "db_events": "events_Germany.json",
                },
                {
                    "competition_id": 102,
                    "season_id": 9291,
                    "season_name": "2016",
                    "db_matches": "matches_European_Championship.json",
                    "db_events": "events_European_Championship.json",
                },
                {
                    "competition_id": 28,
                    "season_id": 10078,
                    "season_name": "2018",
                    "db_matches": "matches_World_Cup.json",
                    "db_events": "events_World_Cup.json",
                },
            ]
        ).set_index(["competition_id", "season_id"])
        self._match_index = self._create_match_index().set_index("match_id")
        self._cache: Optional[dict[str, Any]] = None

    def _download_repo(self) -> None:
        dataset_urls = {
            "competitions": "https://ndownloader.figshare.com/files/15073685",
            "teams": "https://ndownloader.figshare.com/files/15073697",
            "players": "https://ndownloader.figshare.com/files/15073721",
            "matches": "https://ndownloader.figshare.com/files/14464622",
            "events": "https://ndownloader.figshare.com/files/14464685",
        }
        # download and unzip Wyscout open data
        for url in dataset_urls.values():
            url_obj = urlopen(url).geturl()
            path = Path(urlparse(url_obj).path)
            file_name = os.path.join(self.root, path.name)
            file_local, _ = urlretrieve(url_obj, file_name)
            if is_zipfile(file_local):
                with ZipFile(file_local) as zip_file:
                    zip_file.extractall(self.root)

    def _create_match_index(self) -> pd.DataFrame:
        df_matches = pd.concat(
            [pd.DataFrame(self.get(path)) for path in glob.iglob(f"{self.root}/matches_*.json")]
        )
        df_matches.rename(
            columns={
                "wyId": "match_id",
                "competitionId": "competition_id",
                "seasonId": "season_id",
            },
            inplace=True,
        )
        return pd.merge(
            df_matches[["match_id", "competition_id", "season_id"]],
            self._index,
            on=["competition_id", "season_id"],
            how="left",
        )

    def competitions(self) -> DataFrame[WyscoutCompetitionSchema]:
        """Return a dataframe with all available competitions and seasons.

        Returns
        -------
        pd.DataFrame
            A dataframe containing all available competitions and seasons. See
            :class:`~socceraction.spadl.wyscout.WyscoutCompetitionSchema` for the schema.
        """
        path = os.path.join(self.root, "competitions.json")
        df_competitions = pd.DataFrame(self.get(path))
        df_competitions.rename(
            columns={"wyId": "competition_id", "name": "competition_name"}, inplace=True
        )
        df_competitions["country_name"] = df_competitions.apply(
            lambda x: x.area["name"] if x.area["name"] != "" else "International", axis=1
        )
        df_competitions["competition_gender"] = "male"
        df_competitions = pd.merge(
            df_competitions,
            self._index.reset_index()[["competition_id", "season_id", "season_name"]],
            on="competition_id",
            how="left",
        )
        return cast(
            DataFrame[WyscoutCompetitionSchema],
            df_competitions.reset_index()[
                [
                    "competition_id",
                    "season_id",
                    "country_name",
                    "competition_name",
                    "competition_gender",
                    "season_name",
                ]
            ],
        )

    def games(self, competition_id: int, season_id: int) -> DataFrame[WyscoutGameSchema]:
        """Return a dataframe with all available games in a season.

        Parameters
        ----------
        competition_id : int
            The ID of the competition.
        season_id : int
            The ID of the season.

        Returns
        -------
        pd.DataFrame
            A dataframe containing all available games. See
            :class:`~socceraction.spadl.wyscout.WyscoutGameSchema` for the schema.
        """
        path = os.path.join(self.root, self._index.at[(competition_id, season_id), "db_matches"])
        df_matches = pd.DataFrame(self.get(path))
        return cast(DataFrame[WyscoutGameSchema], _convert_games(df_matches))

    def _lineups(self, game_id: int) -> list[dict[str, Any]]:
        competition_id, season_id = self._match_index.loc[game_id, ["competition_id", "season_id"]]
        path = os.path.join(self.root, self._index.at[(competition_id, season_id), "db_matches"])
        df_matches = pd.DataFrame(self.get(path)).set_index("wyId")
        return list(df_matches.at[game_id, "teamsData"].values())

    def teams(self, game_id: int) -> DataFrame[WyscoutTeamSchema]:
        """Return a dataframe with both teams that participated in a game.

        Parameters
        ----------
        game_id : int
            The ID of the game.

        Returns
        -------
        pd.DataFrame
            A dataframe containing both teams. See
            :class:`~socceraction.spadl.wyscout.WyscoutTeamSchema` for the schema.
        """
        path = os.path.join(self.root, "teams.json")
        df_teams = pd.DataFrame(self.get(path)).set_index("wyId")
        df_teams_match_id = pd.DataFrame(self._lineups(game_id))["teamId"]
        df_teams_match = df_teams.loc[df_teams_match_id].reset_index()
        return cast(DataFrame[WyscoutTeamSchema], _convert_teams(df_teams_match))

    def players(self, game_id: int) -> DataFrame[WyscoutPlayerSchema]:
        """Return a dataframe with all players that participated in a game.

        Parameters
        ----------
        game_id : int
            The ID of the game.

        Returns
        -------
        pd.DataFrame
            A dataframe containing all players. See
            :class:`~socceraction.spadl.wyscout.WyscoutPlayerSchema` for the schema.
        """
        path = os.path.join(self.root, "players.json")
        df_players = pd.DataFrame(self.get(path)).set_index("wyId")
        lineups = self._lineups(game_id)
        players_match = []
        for team in lineups:
            playerlist = team["formation"]["lineup"]
            if team["formation"]["substitutions"] != "null":
                for p in team["formation"]["substitutions"]:
                    try:
                        playerlist.append(
                            next(
                                item
                                for item in team["formation"]["bench"]
                                if item["playerId"] == p["playerIn"]
                            )
                        )
                    except StopIteration:
                        warnings.warn(
                            f'A player with ID={p["playerIn"]} was substituted '
                            f'in the {p["minute"]}th minute of game {game_id}, but '
                            "could not be found on the bench."
                        )
            df = pd.DataFrame(playerlist)
            df["side"] = team["side"]
            df["team_id"] = team["teamId"]
            players_match.append(df)
        df_players_match = (
            pd.concat(players_match)
            .rename(columns={"playerId": "wyId"})
            .set_index("wyId")
            .join(df_players, how="left")
        )
        df_players_match.reset_index(inplace=True)
        for c in ["shortName", "lastName", "firstName"]:
            df_players_match[c] = df_players_match[c].apply(
                lambda x: x.encode().decode("unicode-escape")
            )
        df_players_match = _convert_players(df_players_match)

        # get minutes played
        competition_id, season_id = self._match_index.loc[game_id, ["competition_id", "season_id"]]
        path = os.path.join(self.root, self._index.at[(competition_id, season_id), "db_events"])
        if self._cache is not None and self._cache["path"] == path:
            df_events = self._cache["events"]
        else:
            df_events = pd.DataFrame(self.get(path)).set_index("matchId")
            # avoid that this large json file has to be parsed again for
            # each game when loading a batch of games from the same season
            self._cache = {"path": path, "events": df_events}
        match_events = df_events.loc[game_id].reset_index().to_dict("records")
        mp = _get_minutes_played(lineups, match_events)
        df_players_match = pd.merge(df_players_match, mp, on="player_id", how="right")
        df_players_match["minutes_played"] = df_players_match.minutes_played.fillna(0)
        df_players_match["game_id"] = game_id
        return cast(DataFrame[WyscoutPlayerSchema], df_players_match)

    def events(self, game_id: int) -> DataFrame[WyscoutEventSchema]:
        """Return a dataframe with the event stream of a game.

        Parameters
        ----------
        game_id : int
            The ID of the game.

        Returns
        -------
        pd.DataFrame
            A dataframe containing the event stream. See
            :class:`~socceraction.spadl.wyscout.WyscoutEventSchema` for the schema.
        """
        competition_id, season_id = self._match_index.loc[game_id, ["competition_id", "season_id"]]
        path = os.path.join(self.root, self._index.at[(competition_id, season_id), "db_events"])
        if self._cache is not None and self._cache["path"] == path:
            df_events = self._cache["events"]
        else:
            df_events = pd.DataFrame(self.get(path)).set_index("matchId")
            # avoid that this large json file has to be parsed again for
            # each game when loading a batch of games from the same season
            self._cache = {"path": path, "events": df_events}
        return cast(
            DataFrame[WyscoutEventSchema], _convert_events(df_events.loc[game_id].reset_index())
        )


class WyscoutLoader(EventDataLoader):
    """Load event data either from a remote location or from a local folder.

    Parameters
    ----------
    root : str
        Root-path of the data.
    getter : str or callable, default: "remote"
        "remote", "local" or a function that returns loads JSON data from a path.
    feeds : dict(str, str)
        Glob pattern for each feed that should be parsed. The default feeds for
        a "remote" getter are::

            {
                'competitions': 'competitions',
                'seasons': 'competitions/{season_id}/seasons',
                'games': 'seasons/{season_id}/matches',
                'events': 'matches/{game_id}/events?fetch=teams,players,match,substitutions'
            }

        The default feeds for a "local" getter are::

            {
                'competitions': 'competitions.json',
                'seasons': 'seasons_{competition_id}.json',
                'games': 'matches_{season_id}.json',
                'events': 'matches/events_{game_id}.json',
            }

    creds: dict, optional
        Login credentials in the format {"user": "", "passwd": ""}. Only used
        when getter is "remote".
    """

    _wyscout_api: str = "https://apirest.wyscout.com/v2/"

    def __init__(
        self,
        root: str = _wyscout_api,
        getter: Union[str, Callable[[str], JSONType]] = "remote",
        feeds: Optional[dict[str, str]] = None,
        creds: Optional[dict[str, str]] = None,
    ) -> None:
        self.root = root

        # Init credentials
        if creds is None:
            creds = {
                "user": os.environ.get("WY_USERNAME", ""),
                "passwd": os.environ.get("WY_PASSWORD", ""),
            }

        # Init getter
        if getter == "remote":
            self.get = _remoteloadjson
            if _has_auth(creds):
                _auth_remoteloadjson(creds["user"], creds["passwd"])
        elif getter == "local":
            self.get = _localloadjson
        else:
            self.get = getter  # type: ignore

        # Set up feeds
        if feeds is not None:
            self.feeds = feeds
        elif getter == "remote":
            self.feeds = {
                "seasons": "competitions/{competition_id}/seasons?fetch=competition",
                "games": "seasons/{season_id}/matches",
                "events": "matches/{game_id}/events?fetch=teams,players,match,coaches,referees,formations,substitutions",  # noqa: B950
            }
        elif getter == "local":
            self.feeds = {
                "competitions": "competitions.json",
                "seasons": "seasons_{competition_id}.json",
                "games": "matches_{season_id}.json",
                "events": "matches/events_{game_id}.json",
            }
        else:
            raise ValueError("No feeds specified.")

    def _get_file_or_url(
        self,
        feed: str,
        competition_id: Optional[int] = None,
        season_id: Optional[int] = None,
        game_id: Optional[int] = None,
    ) -> list[str]:
        competition_id_glob = "*" if competition_id is None else competition_id
        season_id_glob = "*" if season_id is None else season_id
        game_id_glob = "*" if game_id is None else game_id
        glob_pattern = self.feeds[feed].format(
            competition_id=competition_id_glob, season_id=season_id_glob, game_id=game_id_glob
        )
        if "*" in glob_pattern:
            files = glob.glob(os.path.join(self.root, glob_pattern))
            if len(files) == 0:
                raise MissingDataError
            return files
        return [glob_pattern]

    def competitions(
        self, competition_id: Optional[int] = None
    ) -> DataFrame[WyscoutCompetitionSchema]:
        """Return a dataframe with all available competitions and seasons.

        Parameters
        ----------
        competition_id : int, optional
            The ID of the competition.

        Raises
        ------
        ParseError
            When the raw data does not adhere to the expected format.

        Returns
        -------
        pd.DataFrame
            A dataframe containing all available competitions and seasons. See
            :class:`~socceraction.spadl.wyscout.WyscoutCompetitionSchema` for the schema.
        """
        # Get all competitions
        if "competitions" in self.feeds:
            competitions_url = self._get_file_or_url("competitions")[0]
            path = os.path.join(self.root, competitions_url)
            obj = self.get(path)
            if not isinstance(obj, dict) or "competitions" not in obj:
                raise ParseError(f"{path} should contain a list of competitions")
            seasons_urls = [
                self._get_file_or_url("seasons", competition_id=c["wyId"])[0]
                for c in obj["competitions"]
            ]
        else:
            seasons_urls = self._get_file_or_url("seasons", competition_id=competition_id)
        # Get seasons in each competition
        competitions = []
        seasons = []
        for seasons_url in seasons_urls:
            try:
                path = os.path.join(self.root, seasons_url)
                obj = self.get(path)
                if not isinstance(obj, dict) or "competition" not in obj or "seasons" not in obj:
                    raise ParseError(
                        f"{path} should contain a list of competition and list of seasons"
                    )
                competitions.append(obj["competition"])
                seasons.extend([s["season"] for s in obj["seasons"]])
            except FileNotFoundError:
                warnings.warn(f"File not found: {seasons_url}")
        df_competitions = _convert_competitions(pd.DataFrame(competitions))
        df_seasons = _convert_seasons(pd.DataFrame(seasons))
        # Merge into a single dataframe
        return cast(
            DataFrame[WyscoutCompetitionSchema],
            pd.merge(df_competitions, df_seasons, on="competition_id"),
        )

    def games(self, competition_id: int, season_id: int) -> DataFrame[WyscoutGameSchema]:
        """Return a dataframe with all available games in a season.

        Parameters
        ----------
        competition_id : int
            The ID of the competition.
        season_id : int
            The ID of the season.

        Raises
        ------
        ParseError
            When the raw data does not adhere to the expected format.

        Returns
        -------
        pd.DataFrame
            A dataframe containing all available games. See
            :class:`~socceraction.spadl.wyscout.WyscoutGameSchema` for the schema.
        """
        # Get all games
        if "games" in self.feeds:
            games_url = self._get_file_or_url(
                "games", competition_id=competition_id, season_id=season_id
            )[0]
            path = os.path.join(self.root, games_url)
            obj = self.get(path)
            if not isinstance(obj, dict) or "matches" not in obj:
                raise ParseError(f"{path} should contain a list of matches")
            gamedetails_urls = [
                self._get_file_or_url(
                    "events",
                    competition_id=competition_id,
                    season_id=season_id,
                    game_id=g["matchId"],
                )[0]
                for g in obj["matches"]
            ]
        else:
            gamedetails_urls = self._get_file_or_url(
                "events", competition_id=competition_id, season_id=season_id
            )
        games = []
        for gamedetails_url in gamedetails_urls:
            try:
                path = os.path.join(self.root, gamedetails_url)
                obj = self.get(path)
                if not isinstance(obj, dict) or "match" not in obj:
                    raise ParseError(f"{path} should contain a match")
                games.append(obj["match"])
            except FileNotFoundError:
                warnings.warn(f"File not found: {gamedetails_url}")
            except HTTPError:
                warnings.warn(f"Resource not found: {gamedetails_url}")
        df_games = _convert_games(pd.DataFrame(games))
        return cast(DataFrame[WyscoutGameSchema], df_games)

    def teams(self, game_id: int) -> DataFrame[WyscoutTeamSchema]:
        """Return a dataframe with both teams that participated in a game.

        Parameters
        ----------
        game_id : int
            The ID of the game.

        Raises
        ------
        ParseError
            When the raw data does not adhere to the expected format.

        Returns
        -------
        pd.DataFrame
            A dataframe containing both teams. See
            :class:`~socceraction.spadl.wyscout.WyscoutTeamSchema` for the schema.
        """
        events_url = self._get_file_or_url("events", game_id=game_id)[0]
        path = os.path.join(self.root, events_url)
        obj = self.get(path)
        if not isinstance(obj, dict) or "teams" not in obj:
            raise ParseError(f"{path} should contain a list of matches")
        teams = [t["team"] for t in obj["teams"].values() if t.get("team")]
        df_teams = _convert_teams(pd.DataFrame(teams))
        return cast(DataFrame[WyscoutTeamSchema], df_teams)

    def players(self, game_id: int) -> DataFrame[WyscoutPlayerSchema]:
        """Return a dataframe with all players that participated in a game.

        Parameters
        ----------
        game_id : int
            The ID of the game.

        Raises
        ------
        ParseError
            When the raw data does not adhere to the expected format.

        Returns
        -------
        pd.DataFrame
            A dataframe containing all players. See
            :class:`~socceraction.spadl.wyscout.WyscoutPlayerSchema` for the schema.
        """
        events_url = self._get_file_or_url("events", game_id=game_id)[0]
        path = os.path.join(self.root, events_url)
        obj = self.get(path)
        if not isinstance(obj, dict) or "players" not in obj:
            raise ParseError(f"{path} should contain a list of players")
        players = [
            player["player"]
            for team in obj["players"].values()
            for player in team
            if player.get("player")
        ]
        df_players = _convert_players(pd.DataFrame(players).drop_duplicates("wyId"))
        df_players = pd.merge(
            df_players,
            _get_minutes_played(obj["match"]["teamsData"], obj["events"]),
            on="player_id",
            how="right",
        )
        df_players["minutes_played"] = df_players.minutes_played.fillna(0)
        df_players["game_id"] = game_id
        return cast(DataFrame[WyscoutPlayerSchema], df_players)

    def events(self, game_id: int) -> DataFrame[WyscoutEventSchema]:
        """Return a dataframe with the event stream of a game.

        Parameters
        ----------
        game_id : int
            The ID of the game.

        Raises
        ------
        ParseError
            When the raw data does not adhere to the expected format.

        Returns
        -------
        pd.DataFrame
            A dataframe containing the event stream. See
            :class:`~socceraction.spadl.wyscout.WyscoutEventSchema` for the schema.
        """
        events_url = self._get_file_or_url("events", game_id=game_id)[0]
        path = os.path.join(self.root, events_url)
        obj = self.get(path)
        if not isinstance(obj, dict) or "events" not in obj:
            raise ParseError(f"{path} should contain a list of events")
        df_events = _convert_events(pd.DataFrame(obj["events"]))
        return cast(DataFrame[WyscoutEventSchema], df_events)


def _convert_competitions(competitions: pd.DataFrame) -> pd.DataFrame:
    competitionsmapping = {
        "wyId": "competition_id",
        "name": "competition_name",
        "gender": "competition_gender",
    }
    cols = ["competition_id", "competition_name", "country_name", "competition_gender"]
    competitions["country_name"] = competitions.apply(
        lambda x: x.area["name"] if x.area["name"] != "" else "International", axis=1
    )
    competitions = competitions.rename(columns=competitionsmapping)[cols]
    return competitions


def _convert_seasons(seasons: pd.DataFrame) -> pd.DataFrame:
    seasonsmapping = {
        "wyId": "season_id",
        "name": "season_name",
        "competitionId": "competition_id",
    }
    cols = ["season_id", "season_name", "competition_id"]
    seasons = seasons.rename(columns=seasonsmapping)[cols]
    return seasons


def _convert_games(matches: pd.DataFrame) -> pd.DataFrame:
    gamesmapping = {
        "wyId": "game_id",
        "dateutc": "game_date",
        "competitionId": "competition_id",
        "seasonId": "season_id",
        "gameweek": "game_day",
    }
    cols = ["game_id", "competition_id", "season_id", "game_date", "game_day"]
    games = matches.rename(columns=gamesmapping)[cols]
    games["game_date"] = pd.to_datetime(games["game_date"])
    games["home_team_id"] = matches.teamsData.apply(lambda x: _get_team_id(x, "home"))
    games["away_team_id"] = matches.teamsData.apply(lambda x: _get_team_id(x, "away"))
    return games


def _get_team_id(teamsData: dict[int, Any], side: str) -> int:
    for team_id, data in teamsData.items():
        if data["side"] == side:
            return int(team_id)
    raise ValueError()


def _convert_players(players: pd.DataFrame) -> pd.DataFrame:
    playermapping = {
        "wyId": "player_id",
        "shortName": "nickname",
        "firstName": "firstname",
        "lastName": "lastname",
        "birthDate": "birth_date",
    }
    cols = ["player_id", "nickname", "firstname", "lastname", "birth_date"]
    df_players = players.rename(columns=playermapping)[cols]
    df_players["player_name"] = df_players[["firstname", "lastname"]].agg(" ".join, axis=1)
    df_players["birth_date"] = pd.to_datetime(df_players["birth_date"])
    return df_players


def _convert_teams(teams: pd.DataFrame) -> pd.DataFrame:
    teammapping = {
        "wyId": "team_id",
        "name": "team_name_short",
        "officialName": "team_name",
    }
    cols = ["team_id", "team_name_short", "team_name"]
    return teams.rename(columns=teammapping)[cols]


def _convert_events(raw_events: pd.DataFrame) -> pd.DataFrame:
    eventmapping = {
        "id": "event_id",
        "match_id": "game_id",
        "event_name": "type_name",
        "sub_event_name": "subtype_name",
    }
    cols = [
        "event_id",
        "game_id",
        "period_id",
        "milliseconds",
        "team_id",
        "player_id",
        "type_id",
        "type_name",
        "subtype_id",
        "subtype_name",
        "positions",
        "tags",
    ]
    events = raw_events.copy()
    # Camel case to snake case column names
    pattern = re.compile(r"(?<!^)(?=[A-Z])")
    events.columns = [pattern.sub("_", c).lower() for c in events.columns]
    #
    events["type_id"] = (
        pd.to_numeric(
            events["event_id"] if "event_id" in events.columns else None, errors="coerce"
        )
        .fillna(0)
        .astype(int)
    )
    del events["event_id"]
    events["subtype_id"] = (
        pd.to_numeric(
            events["sub_event_id"] if "sub_event_id" in events.columns else None, errors="coerce"
        )
        .fillna(0)
        .astype(int)
    )
    del events["sub_event_id"]
    events["period_id"] = events.match_period.apply(lambda x: wyscout_periods[x])
    events["milliseconds"] = events.event_sec * 1000
    return events.rename(columns=eventmapping)[cols]


def _get_minutes_played(
    teamsData: list[dict[str, Any]], events: list[dict[str, Any]]
) -> pd.DataFrame:
    # get duration of each period
    periods_ts = {i: [0] for i in range(6)}
    for e in events:
        period_id = wyscout_periods[e["matchPeriod"]]
        periods_ts[period_id].append(e["eventSec"])
    periods_duration = [
        round(max(periods_ts[i]) / 60) for i in range(5) if max(periods_ts[i]) != 0
    ]
    # get duration of entire match
    duration = sum(periods_duration)

    # get stats for each player
    playergames: dict[int, dict[str, Any]] = {}
    if isinstance(teamsData, dict):
        teamsData = list(teamsData.values())
    for teamData in teamsData:
        formation = teamData.get("formation", {})
        substitutions = formation.get("substitutions", [])
        red_cards = {
            player["playerId"]: _expand_minute(int(player["redCards"]), periods_duration)
            for key in ["bench", "lineup"]
            for player in formation.get(key, [])
            if player["redCards"] != "0"
        }
        pg = {
            player["playerId"]: {
                "team_id": teamData["teamId"],
                "player_id": player["playerId"],
                "jersey_number": player.get("shirtNumber", 0),
                "minutes_played": red_cards.get(player["playerId"], duration),
                "is_starter": True,
            }
            for player in formation.get("lineup", [])
        }

        # correct minutes played for substituted players
        if substitutions != "null":
            for substitution in substitutions:
                expanded_minute_sub = _expand_minute(substitution["minute"], periods_duration)
                substitute = {
                    "team_id": teamData["teamId"],
                    "player_id": substitution["playerIn"],
                    "jersey_number": next(
                        (
                            p.get("shirtNumber", 0)
                            for p in formation.get("bench", [])
                            if p["playerId"] == substitution["playerIn"]
                        ),
                        0,
                    ),
                    "minutes_played": duration - expanded_minute_sub,
                    "is_starter": False,
                }
                if substitution["playerIn"] in red_cards:
                    substitute["minutes_played"] = (
                        red_cards[substitution["playerIn"]] - expanded_minute_sub
                    )
                pg[substitution["playerIn"]] = substitute
                pg[substitution["playerOut"]]["minutes_played"] = expanded_minute_sub

        playergames = {**playergames, **pg}
    return pd.DataFrame(playergames.values())


wyscout_periods = {"1H": 1, "2H": 2, "E1": 3, "E2": 4, "P": 5}
