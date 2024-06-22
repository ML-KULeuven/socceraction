"""Implements serializers for StatsBomb data."""

import os
from typing import Any, Optional, cast

import pandas as pd  # type: ignore
from pandera.typing import DataFrame

try:
    from statsbombpy import sb
except ImportError:
    sb = None

from socceraction.data.base import (
    EventDataLoader,
    ParseError,
    _expand_minute,
    _localloadjson,
)

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
        creds: Optional[dict[str, str]] = None,
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
            return cast(DataFrame[StatsBombCompetitionSchema], pd.DataFrame(columns=cols))
        return cast(DataFrame[StatsBombCompetitionSchema], pd.DataFrame(obj)[cols])

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
                str(os.path.join(self._root, "matches", f"{competition_id}", f"{season_id}.json"))
            )
        else:
            obj = list(
                sb.matches(competition_id, season_id, fmt="dict", creds=self._creds).values()
            )
        if not isinstance(obj, list):
            raise ParseError("The retrieved data should contain a list of games")
        if len(obj) == 0:
            return cast(DataFrame[StatsBombGameSchema], pd.DataFrame(columns=cols))
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
        return cast(DataFrame[StatsBombGameSchema], gamesdf[cols])

    def _lineups(self, game_id: int) -> list[dict[str, Any]]:
        if self._local:
            obj = _localloadjson(str(os.path.join(self._root, "lineups", f"{game_id}.json")))
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
        return cast(DataFrame[StatsBombTeamSchema], pd.DataFrame(obj)[cols])

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
        return cast(DataFrame[StatsBombPlayerSchema], playersdf[cols])

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
            obj = _localloadjson(str(os.path.join(self._root, "events", f"{game_id}.json")))
        else:
            obj = list(sb.events(game_id, fmt="dict", creds=self._creds).values())
        if not isinstance(obj, list):
            raise ParseError("The retrieved data should contain a list of events")
        if len(obj) == 0:
            return cast(DataFrame[StatsBombEventSchema], pd.DataFrame(columns=cols))

        eventsdf = pd.DataFrame(_flatten_id(e) for e in obj)
        eventsdf["match_id"] = game_id
        eventsdf["timestamp"] = pd.to_timedelta(eventsdf["timestamp"])
        eventsdf["related_events"] = eventsdf["related_events"].apply(
            lambda d: d if isinstance(d, list) else []
        )
        eventsdf["under_pressure"] = eventsdf["under_pressure"].fillna(False).astype(bool)
        eventsdf["counterpress"] = eventsdf["counterpress"].fillna(False).astype(bool)
        eventsdf.rename(
            columns={"id": "event_id", "period": "period_id", "match_id": "game_id"},
            inplace=True,
        )
        if not load_360:
            return cast(DataFrame[StatsBombEventSchema], eventsdf[cols])

        # Load the 360 data
        cols_360 = ["visible_area_360", "freeze_frame_360"]
        if self._local:
            obj = _localloadjson(str(os.path.join(self._root, "three-sixty", f"{game_id}.json")))
        else:
            obj = sb.frames(game_id, fmt="dict", creds=self._creds)
        if not isinstance(obj, list):
            raise ParseError("The retrieved data should contain a list of frames")
        if len(obj) == 0:
            eventsdf["visible_area_360"] = None
            eventsdf["freeze_frame_360"] = None
            return cast(DataFrame[StatsBombEventSchema], eventsdf[cols + cols_360])
        framesdf = pd.DataFrame(obj).rename(
            columns={
                "event_uuid": "event_id",
                "visible_area": "visible_area_360",
                "freeze_frame": "freeze_frame_360",
            },
        )[["event_id", "visible_area_360", "freeze_frame_360"]]
        return cast(
            DataFrame[StatsBombEventSchema],
            pd.merge(eventsdf, framesdf, on="event_id", how="left")[cols + cols_360],
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
    # get duration of each period
    periods = pd.DataFrame(
        [
            {"period_id": 1, "minute": 45},
            {"period_id": 2, "minute": 45},
            {"period_id": 3, "minute": 15},
            {"period_id": 4, "minute": 15},
            # Shoot-outs should not contritbute to minutes played
            # {"period_id": 5, "minute": 0},
        ]
    ).set_index("period_id")
    periods_minutes = (
        events.loc[events.type_name == "Half End", ["period_id", "minute"]]
        .drop_duplicates()
        .set_index("period_id")
        .sort_index()
        .subtract(periods.cumsum().shift(1).fillna(0))
        .minute.dropna()
        .astype(int)
        .tolist()
    )
    # get duration of entire match
    game_minutes = sum(periods_minutes)

    game_id = events.game_id.mode().values[0]
    players = {}
    # Red cards
    red_cards = events[
        events.apply(
            lambda x: any(
                e in x.extra
                and "card" in x.extra[e]
                and x.extra[e]["card"]["name"] in ["Second Yellow", "Red Card"]
                for e in ["foul_committed", "bad_behaviour"]
            ),
            axis=1,
        )
    ]
    # stats for starting XI
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
            player_red_card = red_cards[red_cards.player_id == player["player_id"]]
            if len(player_red_card) > 0:
                red_card_minute = player_red_card.iloc[0].minute
                player["minutes_played"] = _expand_minute(red_card_minute, periods_minutes)
            players[player["player_id"]] = player
    # stats for substitutions
    for substitution in events[events.type_name == "Substitution"].itertuples():
        exp_sub_minute = _expand_minute(substitution.minute, periods_minutes)
        replacement = {
            "player_id": substitution.extra["substitution"]["replacement"]["id"],
            "player_name": substitution.extra["substitution"]["replacement"]["name"],
            "minutes_played": game_minutes - exp_sub_minute,
            "team_id": substitution.team_id,
            "game_id": game_id,
            "team_name": substitution.team_name,
        }
        player_red_card = red_cards[red_cards.player_id == replacement["player_id"]]
        if len(player_red_card) > 0:
            red_card_minute = player_red_card.iloc[0].minute
            replacement["minutes_played"] = (
                _expand_minute(red_card_minute, periods_minutes) - exp_sub_minute
            )
        players[replacement["player_id"]] = replacement
        players[substitution.player_id]["minutes_played"] = exp_sub_minute
    pg = pd.DataFrame(players.values()).fillna(0)
    for col in pg.columns:
        if "_id" in col:
            pg[col] = pg[col].astype(int)  # pylint: disable=E1136,E1137
    return pg


def _flatten_id(d: dict[str, dict[str, Any]]) -> dict[str, Any]:
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


def _flatten(d: dict[str, dict[str, Any]]) -> dict[str, Any]:
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
