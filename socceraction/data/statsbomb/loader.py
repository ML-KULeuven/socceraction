"""Implements serializers for StatsBomb data."""
import os
import warnings
from typing import Any, Dict, List, Optional

import pandas as pd  # type: ignore
from pandera.typing import DataFrame

try:
    from statsbombpy import api_client, sb

    def my_has_auth(creds: Dict[str, str]) -> bool:
        """Monkeypatch to hide the repeated print messages."""
        if creds.get("user") in [None, ""] or creds.get("passwd") in [None, ""]:
            warnings.warn("credentials were not supplied. open data access only")
            return False
        return True

    api_client.has_auth = my_has_auth
except ImportError:
    sb = None

from socceraction.data.base import EventDataLoader, ParseError, _localloadjson

from .schema import (
    StatsBombCompetitionSchema,
    StatsBombEventSchema,
    StatsBombGameSchema,
    StatsBombPlayerSchema,
    StatsBombTeamSchema,
)


class StatsBombLoader(EventDataLoader):
    """Load Statsbomb data either from a remote location or from a local folder.

    To load remote data, this loader uses the `statsbombpy
    <https://github.com/statsbomb/statsbombpy>`__ package. Data can be retrieved
    from the StatsBomb API and from the `Open Data GitHub repo
    <https://github.com/statsbomb/open-data/>`__.
    API access is for paying customers only. Authentication can be done by
    setting environment variables named ``SB_USERNAME`` and ``SB_PASSWORD`` to
    your login credentials. Alternatively, pass your login credentials using
    the ``creds`` parameter.
    StatsBomb's open data can be accessed without the need of authentication
    but its use is subject to a `user agreement
    <https://github.com/statsbomb/open-data/blob/master/LICENSE.pdf>`__.

    To load local data, point ``root`` to the root folder of the data. This folder
    should use the same directory structure as used in the Open Data GitHub repo.

    Parameters
    ----------
    getter : str
        "remote" or "local"
    root : str, optional
        Root-path of the data. Only used when getter is "local".
    creds: dict, optional
        Login credentials in the format {"user": "", "passwd": ""}. Only used
        when getter is "remote".
    """

    def __init__(
        self,
        getter: str = "remote",
        root: Optional[str] = None,
        creds: Optional[Dict[str, str]] = None,
    ) -> None:
        if getter == "remote":
            if sb is None:
                raise ImportError(
                    """The 'statsbombpy' package is required. Install with 'pip install statsbombpy'."""
                )
            self._creds = creds or sb.DEFAULT_CREDS
            self._local = False
        elif getter == "local":
            if root is None:
                raise ValueError("""The 'root' parameter is required when loading local data.""")
            self._local = True
            self._root = root
        else:
            raise ValueError("Invalid getter specified")

    def competitions(self) -> DataFrame[StatsBombCompetitionSchema]:
        """Return a dataframe with all available competitions and seasons.

        Raises
        ------
        ParseError
            When the raw data does not adhere to the expected format.

        Returns
        -------
        pd.DataFrame
            A dataframe containing all available competitions and seasons. See
            :class:`~socceraction.spadl.statsbomb.StatsBombCompetitionSchema` for the schema.
        """
        cols = [
            "season_id",
            "competition_id",
            "competition_name",
            "country_name",
            "competition_gender",
            "season_name",
        ]
        if self._local:
            obj = _localloadjson(str(os.path.join(self._root, "competitions.json")))
        else:
            obj = list(sb.competitions(fmt="dict", creds=self._creds).values())
        if not isinstance(obj, list):
            raise ParseError("The retrieved data should contain a list of competitions")
        if len(obj) == 0:
            return pd.DataFrame(columns=cols).pipe(DataFrame[StatsBombCompetitionSchema])
        return pd.DataFrame(obj)[cols].pipe(DataFrame[StatsBombCompetitionSchema])

    def games(self, competition_id: int, season_id: int) -> DataFrame[StatsBombGameSchema]:
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
            :class:`~socceraction.spadl.statsbomb.StatsBombGameSchema` for the schema.
        """
        cols = [
            "game_id",
            "season_id",
            "competition_id",
            "competition_stage",
            "game_day",
            "game_date",
            "home_team_id",
            "away_team_id",
            "home_score",
            "away_score",
            "venue",
            "referee",
        ]
        if self._local:
            obj = _localloadjson(
                str(os.path.join(self._root, f"matches/{competition_id}/{season_id}.json"))
            )
        else:
            obj = list(
                sb.matches(competition_id, season_id, fmt="dict", creds=self._creds).values()
            )
        if not isinstance(obj, list):
            raise ParseError("The retrieved data should contain a list of games")
        if len(obj) == 0:
            return pd.DataFrame(columns=cols).pipe(DataFrame[StatsBombGameSchema])
        gamesdf = pd.DataFrame(_flatten(m) for m in obj)
        gamesdf["kick_off"] = gamesdf["kick_off"].fillna("12:00:00.000")
        gamesdf["match_date"] = pd.to_datetime(
            gamesdf[["match_date", "kick_off"]].agg(" ".join, axis=1)
        )
        gamesdf.rename(
            columns={
                "match_id": "game_id",
                "match_date": "game_date",
                "match_week": "game_day",
                "stadium_name": "venue",
                "referee_name": "referee",
                "competition_stage_name": "competition_stage",
            },
            inplace=True,
        )
        if "venue" not in gamesdf:
            gamesdf["venue"] = None
        if "referee" not in gamesdf:
            gamesdf["referee"] = None
        return gamesdf[cols].pipe(DataFrame[StatsBombGameSchema])

    def _lineups(self, game_id: int) -> List[Dict[str, Any]]:
        if self._local:
            obj = _localloadjson(str(os.path.join(self._root, f"lineups/{game_id}.json")))
        else:
            obj = list(sb.lineups(game_id, fmt="dict", creds=self._creds).values())
        if not isinstance(obj, list):
            raise ParseError("The retrieved data should contain a list of teams")
        if len(obj) != 2:
            raise ParseError("The retrieved data should contain two teams")
        return obj

    def teams(self, game_id: int) -> DataFrame[StatsBombTeamSchema]:
        """Return a dataframe with both teams that participated in a game.

        Parameters
        ----------
        game_id : int
            The ID of the game.

        Raises
        ------
        ParseError  # noqa: DAR402
            When the raw data does not adhere to the expected format.

        Returns
        -------
        pd.DataFrame
            A dataframe containing both teams. See
            :class:`~socceraction.spadl.statsbomb.StatsBombTeamSchema` for the schema.
        """
        cols = ["team_id", "team_name"]
        obj = self._lineups(game_id)
        return pd.DataFrame(obj)[cols].pipe(DataFrame[StatsBombTeamSchema])

    def players(self, game_id: int) -> DataFrame[StatsBombPlayerSchema]:
        """Return a dataframe with all players that participated in a game.

        Parameters
        ----------
        game_id : int
            The ID of the game.

        Raises
        ------
        ParseError  # noqa: DAR402
            When the raw data does not adhere to the expected format.

        Returns
        -------
        pd.DataFrame
            A dataframe containing all players. See
            :class:`~socceraction.spadl.statsbomb.StatsBombPlayerSchema` for the schema.
        """
        cols = [
            "game_id",
            "team_id",
            "player_id",
            "player_name",
            "nickname",
            "jersey_number",
            "is_starter",
            "starting_position_id",
            "starting_position_name",
            "minutes_played",
        ]

        obj = self._lineups(game_id)
        playersdf = pd.DataFrame(_flatten_id(p) for lineup in obj for p in lineup["lineup"])
        playergamesdf = extract_player_games(self.events(game_id))
        playersdf = pd.merge(
            playersdf,
            playergamesdf[
                ["player_id", "team_id", "position_id", "position_name", "minutes_played"]
            ],
            on="player_id",
        )
        playersdf["game_id"] = game_id
        playersdf["position_name"] = playersdf["position_name"].replace(0, "Substitute")
        playersdf["position_id"] = playersdf["position_id"].fillna(0).astype(int)
        playersdf["is_starter"] = playersdf["position_id"] != 0
        playersdf.rename(
            columns={
                "player_nickname": "nickname",
                "country_name": "country",
                "position_id": "starting_position_id",
                "position_name": "starting_position_name",
            },
            inplace=True,
        )
        return playersdf[cols].pipe(DataFrame[StatsBombPlayerSchema])

    def events(self, game_id: int, load_360: bool = False) -> DataFrame[StatsBombEventSchema]:
        """Return a dataframe with the event stream of a game.

        Parameters
        ----------
        game_id : int
            The ID of the game.
        load_360 : bool
            Whether to load the 360 data.

        Raises
        ------
        ParseError
            When the raw data does not adhere to the expected format.

        Returns
        -------
        pd.DataFrame
            A dataframe containing the event stream. See
            :class:`~socceraction.spadl.statsbomb.StatsBombEventSchema` for the schema.
        """
        cols = [
            "game_id",
            "event_id",
            "period_id",
            "team_id",
            "player_id",
            "type_id",
            "type_name",
            "event_id",
            "index",
            "timestamp",
            "minute",
            "second",
            "possession",
            "possession_team_id",
            "possession_team_name",
            "play_pattern_id",
            "play_pattern_name",
            "team_name",
            "duration",
            "extra",
            "related_events",
            "player_name",
            "position_id",
            "position_name",
            "location",
            "under_pressure",
            "counterpress",
        ]
        # Load the events
        if self._local:
            obj = _localloadjson(str(os.path.join(self._root, f"events/{game_id}.json")))
        else:
            obj = list(sb.events(game_id, fmt="dict", creds=self._creds).values())
        if not isinstance(obj, list):
            raise ParseError("The retrieved data should contain a list of events")
        if len(obj) == 0:
            return pd.DataFrame(columns=cols).pipe(DataFrame[StatsBombEventSchema])
        eventsdf = pd.DataFrame(_flatten_id(e) for e in obj)
        eventsdf["game_id"] = game_id
        eventsdf["timestamp"] = pd.to_datetime(eventsdf["timestamp"], format="%H:%M:%S.%f")
        eventsdf["related_events"] = eventsdf["related_events"].apply(
            lambda d: d if isinstance(d, list) else []
        )
        eventsdf["under_pressure"] = eventsdf["under_pressure"].fillna(False).astype(bool)
        eventsdf["counterpress"] = eventsdf["counterpress"].fillna(False).astype(bool)
        eventsdf.rename(
            columns={
                "id": "event_id",
                "period": "period_id",
            },
            inplace=True,
        )
        if not load_360:
            return eventsdf.pipe(DataFrame[StatsBombEventSchema])
        # Load the 360 data
        if self._local:
            obj = _localloadjson(str(os.path.join(self._root, f"three-sixty/{game_id}.json")))
        else:
            obj = sb.frames(game_id, fmt="dict", creds=self._creds)
        if not isinstance(obj, list):
            raise ParseError("The retrieved data should contain a list of frames")
        if len(obj) == 0:
            eventsdf["visible_area_360"] = None
            eventsdf["freeze_frame_360"] = None
            return eventsdf.pipe(DataFrame[StatsBombEventSchema])
        framesdf = pd.DataFrame(obj).rename(
            columns={
                "event_uuid": "event_id",
                "visible_area": "visible_area_360",
                "freeze_frame": "freeze_frame_360",
            },
        )[["event_id", "visible_area_360", "freeze_frame_360"]]
        return pd.merge(eventsdf, framesdf, on="event_id", how="left").pipe(
            DataFrame[StatsBombEventSchema]
        )


def extract_player_games(events: pd.DataFrame) -> pd.DataFrame:
    """Extract player games [player_id, game_id, minutes_played] from statsbomb match events.

    Parameters
    ----------
    events : pd.DataFrame
        DataFrame containing StatsBomb events of a single game.

    Returns
    -------
    player_games : pd.DataFrame
        A DataFrame with the number of minutes played by each player during the game.
    """
    game_minutes = max(events[events.type_name == "Half End"].minute)

    game_id = events.game_id.mode().values[0]
    players = {}
    for startxi in events[events.type_name == "Starting XI"].itertuples():
        team_id, team_name = startxi.team_id, startxi.team_name
        for player in startxi.extra["tactics"]["lineup"]:
            player = _flatten_id(player)
            player = {
                **player,
                **{
                    "game_id": game_id,
                    "team_id": team_id,
                    "team_name": team_name,
                    "minutes_played": game_minutes,
                },
            }
            players[player["player_id"]] = player
    for substitution in events[events.type_name == "Substitution"].itertuples():
        replacement = substitution.extra["substitution"]["replacement"]
        replacement = {
            "player_id": replacement["id"],
            "player_name": replacement["name"],
            "minutes_played": game_minutes - substitution.minute,
            "team_id": substitution.team_id,
            "game_id": game_id,
            "team_name": substitution.team_name,
        }
        players[replacement["player_id"]] = replacement
        # minutes_played = substitution.minute
        players[substitution.player_id]["minutes_played"] = substitution.minute
    pg = pd.DataFrame(players.values()).fillna(0)
    for col in pg.columns:
        if "_id" in col:
            pg[col] = pg[col].astype(int)  # pylint: disable=E1136,E1137
    return pg


def _flatten_id(d: Dict[str, Dict[str, Any]]) -> Dict[str, Any]:
    newd = {}
    extra = {}
    for k, v in d.items():
        if isinstance(v, dict):
            if "id" in v and "name" in v:
                newd[k + "_id"] = v["id"]
                newd[k + "_name"] = v["name"]
            else:
                extra[k] = v
        else:
            newd[k] = v
    newd["extra"] = extra
    return newd


def _flatten(d: Dict[str, Dict[str, Any]]) -> Dict[str, Any]:
    newd = {}
    for k, v in d.items():
        if isinstance(v, dict):
            if "id" in v and "name" in v:
                newd[k + "_id"] = v["id"]
                newd[k + "_name"] = v["name"]
                newd[k + "_extra"] = {l: w for (l, w) in v.items() if l in ("id", "name")}
            else:
                newd = {**newd, **_flatten(v)}
        else:
            newd[k] = v
    return newd
