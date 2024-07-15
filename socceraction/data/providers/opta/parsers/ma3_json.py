"""JSON parser for Stats Perform MA3 feeds."""

from datetime import datetime
from typing import Any, Optional

import pandas as pd

from ...base import MissingDataError
from .base import OptaJSONParser, _get_end_x, _get_end_y, assertget


class MA3JSONParser(OptaJSONParser):
    """Extract data from a Stats Perform MA3 data stream.

    Parameters
    ----------
    path : str
        Path of the data file.
    """

    _position_map = {
        1: "Goalkeeper",
        2: "Defender",
        3: "Midfielder",
        4: "Forward",
        5: "Substitute",
    }

    def _get_match_info(self) -> dict[str, Any]:
        if "matchInfo" in self.root:
            return self.root["matchInfo"]
        raise MissingDataError

    def _get_live_data(self) -> dict[str, Any]:
        if "liveData" in self.root:
            return self.root["liveData"]
        raise MissingDataError

    def extract_competitions(self) -> dict[tuple[str, str], dict[str, Any]]:
        """Return a dictionary with all available competitions.

        Returns
        -------
        dict
            A mapping between competion IDs and the information available about
            each competition in the data stream.
        """
        match_info = self._get_match_info()
        season = assertget(match_info, "tournamentCalendar")
        competition = assertget(match_info, "competition")
        competition_id = assertget(competition, "id")
        season_id = assertget(season, "id")
        season = {
            # Fields required by the base schema
            "season_id": season_id,
            "season_name": assertget(season, "name"),
            "competition_id": competition_id,
            "competition_name": assertget(competition, "name"),
        }
        return {(competition_id, season_id): season}

    def extract_games(self) -> dict[str, dict[str, Any]]:
        """Return a dictionary with all available games.

        Returns
        -------
        dict
            A mapping between game IDs and the information available about
            each game in the data stream.
        """
        match_info = self._get_match_info()
        game_id = assertget(match_info, "id")
        season = assertget(match_info, "tournamentCalendar")
        competition = assertget(match_info, "competition")
        contestant = assertget(match_info, "contestant")
        game_date = assertget(match_info, "date")[0:10]
        game_time = assertget(match_info, "time")[0:8]
        game_datetime = f"{game_date}T{game_time}"
        venue = assertget(match_info, "venue")
        game_obj = {
            "game_id": game_id,
            "competition_id": assertget(competition, "id"),
            "season_id": assertget(season, "id"),
            "game_day": int(match_info["week"]) if "week" in match_info else None,
            "game_date": datetime.strptime(game_datetime, "%Y-%m-%dT%H:%M:%S"),
            "home_team_id": self._extract_team_id(contestant, "home"),
            "away_team_id": self._extract_team_id(contestant, "away"),
            "venue": assertget(venue, "shortName"),
        }
        live_data = self._get_live_data()
        if "matchDetails" in live_data:
            match_details = assertget(live_data, "matchDetails")
            if "matchLengthMin" in match_details:
                game_obj["duration"] = assertget(match_details, "matchLengthMin")
            if "scores" in match_details:
                scores = assertget(match_details, "scores")
                game_obj["home_score"] = assertget(scores, "total")["home"]
                game_obj["away_score"] = assertget(scores, "total")["away"]

        return {game_id: game_obj}

    def extract_teams(self) -> dict[str, dict[str, Any]]:
        """Return a dictionary with all available teams.

        Returns
        -------
        dict
            A mapping between team IDs and the information available about
            each team in the data stream.
        """
        match_info = self._get_match_info()
        contestants = assertget(match_info, "contestant")
        teams = {}
        for contestant in contestants:
            team_id = assertget(contestant, "id")
            team = {
                # Fields required by the base schema
                "team_id": team_id,
                "team_name": assertget(contestant, "name"),
            }
            teams[team_id] = team
        return teams

    def extract_players(self) -> dict[tuple[str, str], dict[str, Any]]:  # noqa: C901
        """Return a dictionary with all available players.

        Returns
        -------
        dict
            A mapping between (game ID, player ID) tuples and the information
            available about each player in the data stream.
        """
        match_info = self._get_match_info()
        game_id = assertget(match_info, "id")
        live_data = self._get_live_data()
        events = assertget(live_data, "event")

        game_duration = self._extract_duration()
        playerid_to_name = {}

        players_data: dict[str, list[Any]] = {
            "starting_position_id": [],
            "player_id": [],
            "team_id": [],
            "position_in_formation": [],
            "jersey_number": [],
        }
        red_cards = {}

        for event in events:
            event_type = assertget(event, "typeId")
            if event_type == 34:
                team_id = assertget(event, "contestantId")
                qualifiers = assertget(event, "qualifier")
                for q in qualifiers:
                    qualifier_id = assertget(q, "qualifierId")
                    value = assertget(q, "value")
                    value = value.split(", ")
                    if qualifier_id == 30:
                        players_data["player_id"] += value
                        team = [team_id for _ in range(len(value))]
                        players_data["team_id"] += team
                    elif qualifier_id == 44:
                        value = [int(v) for v in value]
                        players_data["starting_position_id"] += value
                    elif qualifier_id == 131:
                        value = [int(v) for v in value]
                        players_data["position_in_formation"] += value
                    elif qualifier_id == 59:
                        value = [int(v) for v in value]
                        players_data["jersey_number"] += value
            elif event_type == 17 and "playerId" in event:
                qualifiers = assertget(event, "qualifier")
                for q in qualifiers:
                    qualifier_id = assertget(q, "qualifierId")
                    if qualifier_id in [32, 33]:
                        red_cards[event["playerId"]] = event["timeMin"]

            player_id = event.get("playerId")
            if player_id is None:
                continue
            player_name = assertget(event, "playerName")
            if player_id not in playerid_to_name:
                playerid_to_name[player_id] = player_name

        df_players_data = pd.DataFrame.from_dict(players_data)  # type: ignore

        substitutions = list(self.extract_substitutions().values())
        substitutions_columns = ["player_id", "team_id", "minute_start", "minute_end"]
        df_substitutions = pd.DataFrame(substitutions, columns=substitutions_columns)
        df_substitutions = df_substitutions.groupby(["player_id", "team_id"]).max().reset_index()
        df_substitutions["minute_start"] = df_substitutions["minute_start"].fillna(0)
        df_substitutions["minute_end"] = df_substitutions["minute_end"].fillna(game_duration)

        if df_substitutions.empty:
            df_players_data["minute_start"] = 0
            df_players_data["minute_end"] = game_duration
        else:
            df_players_data = df_players_data.merge(
                df_substitutions, on=["team_id", "player_id"], how="left"
            )
        df_players_data["minute_end"] = df_players_data.apply(
            lambda row: red_cards[row["player_id"]]
            if row["player_id"] in red_cards
            else row["minute_end"],
            axis=1,
        )

        df_players_data["is_starter"] = df_players_data["position_in_formation"] > 0
        df_players_data.loc[
            df_players_data["is_starter"] & df_players_data["minute_start"].isnull(),
            "minute_start",
        ] = 0
        df_players_data.loc[
            df_players_data["is_starter"] & df_players_data["minute_end"].isnull(), "minute_end"
        ] = game_duration

        df_players_data["minutes_played"] = (
            (df_players_data["minute_end"] - df_players_data["minute_start"]).fillna(0).astype(int)
        )

        players = {}
        for _, player in df_players_data.iterrows():
            if player.minutes_played > 0:
                players[(game_id, player.player_id)] = {
                    # Fields required by the base schema
                    "game_id": game_id,
                    "team_id": player.team_id,
                    "player_id": player.player_id,
                    "player_name": playerid_to_name[player.player_id],
                    "is_starter": player.is_starter,
                    "minutes_played": player.minutes_played,
                    "jersey_number": player.jersey_number,
                    # Fields required by the opta schema
                    "starting_position": self._position_map.get(
                        player.starting_position_id, "Unknown"
                    ),
                }
        return players

    def extract_events(self) -> dict[tuple[str, int], dict[str, Any]]:
        """Return a dictionary with all available events.

        Returns
        -------
        dict
            A mapping between (game ID, event ID) tuples and the information
            available about each event in the data stream.
        """
        match_info = self._get_match_info()
        live_data = self._get_live_data()
        game_id = assertget(match_info, "id")

        events = {}
        for element in assertget(live_data, "event"):
            timestamp_string = assertget(element, "timeStamp")
            timestamp = self._convert_timestamp(timestamp_string)

            qualifiers = {
                int(q["qualifierId"]): q.get("value") for q in element.get("qualifier", [])
            }
            start_x = float(assertget(element, "x"))
            start_y = float(assertget(element, "y"))
            end_x = _get_end_x(qualifiers)
            end_y = _get_end_y(qualifiers)

            event_id = int(assertget(element, "id"))
            event = {
                # Fields required by the base schema
                "game_id": game_id,
                "event_id": event_id,
                "period_id": int(assertget(element, "periodId")),
                "team_id": assertget(element, "contestantId"),
                "player_id": element.get("playerId"),
                "type_id": int(assertget(element, "typeId")),
                # Fields required by the opta schema
                "timestamp": timestamp,
                "minute": int(assertget(element, "timeMin")),
                "second": int(assertget(element, "timeSec")),
                "outcome": bool(int(element.get("outcome", 1))),
                "start_x": start_x,
                "start_y": start_y,
                "end_x": end_x if end_x is not None else start_x,
                "end_y": end_y if end_y is not None else start_y,
                "qualifiers": qualifiers,
                # Optional fields
                "assist": bool(int(element.get("assist", 0))),
                "keypass": bool(int(element.get("keyPass", 0))),
            }
            events[(game_id, event_id)] = event
        return events

    def extract_substitutions(self) -> dict[int, dict[str, Any]]:
        """Return a dictionary with all substitution events.

        Returns
        -------
        dict
            A mapping between player IDs and the information available about
            each substitution in the data stream.
        """
        live_data = self._get_live_data()
        events = assertget(live_data, "event")

        subs = {}
        for e in events:
            event_type = assertget(e, "typeId")
            if event_type in (18, 19):
                sub_id = assertget(e, "playerId")
                substitution_data = {
                    "player_id": assertget(e, "playerId"),
                    "team_id": assertget(e, "contestantId"),
                }
                if event_type == 18:
                    substitution_data["minute_end"] = assertget(e, "timeMin")
                else:
                    substitution_data["minute_start"] = assertget(e, "timeMin")
                subs[sub_id] = substitution_data
        return subs

    def _extract_duration(self) -> int:
        live_data = self._get_live_data()
        events = assertget(live_data, "event")

        game_duration = 90

        for event in events:
            event_type = assertget(event, "typeId")
            if event_type == 30:
                # todo: add 1st half time
                qualifiers = assertget(event, "qualifier")
                for q in qualifiers:
                    qualifier = assertget(q, "qualifierId")
                    if qualifier == 209:
                        new_duration = assertget(event, "timeMin")
                        if new_duration > game_duration:
                            game_duration = new_duration

        return game_duration

    @staticmethod
    def _extract_team_id(teams: list[dict[str, str]], side: str) -> Optional[str]:
        for team in teams:
            team_side = assertget(team, "position")
            if team_side == side:
                team_id = assertget(team, "id")
                return team_id
        raise MissingDataError

    @staticmethod
    def _convert_timestamp(timestamp_string: str) -> datetime:
        try:
            return datetime.strptime(timestamp_string, "%Y-%m-%dT%H:%M:%S.%fZ")
        except ValueError:
            return datetime.strptime(timestamp_string, "%Y-%m-%dT%H:%M:%SZ")
