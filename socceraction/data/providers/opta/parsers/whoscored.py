"""JSON parser for WhoScored feeds."""

import json  # type: ignore
import re
from datetime import datetime, timedelta
from typing import Any, Optional

from ...base import MissingDataError
from .base import OptaParser, _get_end_x, _get_end_y, assertget


def _position_mapping(formation: str, x: float, y: float) -> str:
    if x == 0 and y == 5:
        return "GK"
    return "Unknown"


class WhoScoredParser(OptaParser):
    """Extract data from a JSON data stream scraped from WhoScored.

    Parameters
    ----------
    path : str
        Path of the data file.
    competition_id : int
        ID of the competition to which the provided data file belongs. If
        None, this information is extracted from a field 'competition_id' in
        the JSON.
    season_id : int
        ID of the season to which the provided data file belongs. If None,
        this information is extracted from a field 'season_id' in the JSON.
    game_id : int
        ID of the game to which the provided data file belongs. If None, this
        information is extracted from a field 'game_id' in the JSON.
    """

    def __init__(  # noqa: C901
        self,
        path: str,
        competition_id: Optional[int] = None,
        season_id: Optional[int] = None,
        game_id: Optional[int] = None,
    ) -> None:
        with open(path, encoding="utf-8") as fh:
            self.root = json.load(fh)

        if competition_id is None:
            try:
                competition_id = int(assertget(self.root, "competition_id"))
            except AssertionError as e:
                raise MissingDataError(
                    """Could not determine the competition id. Add it to the
                    file path or include a field 'competition_id' in the
                    JSON."""
                ) from e
        self.competition_id = competition_id

        if season_id is None:
            try:
                season_id = int(assertget(self.root, "season_id"))
            except AssertionError as e:
                raise MissingDataError(
                    """Could not determine the season id. Add it to the file
                    path or include a field 'season_id' in the JSON."""
                ) from e
        self.season_id = season_id

        if game_id is None:
            try:
                game_id = int(assertget(self.root, "game_id"))
            except AssertionError as e:
                raise MissingDataError(
                    """Could not determine the game id. Add it to the file
                    path or include a field 'game_id' in the JSON."""
                ) from e
        self.game_id = game_id

    def _get_period_id(self, event: dict[str, Any]) -> int:
        period = assertget(event, "period")
        period_id = int(assertget(period, "value"))
        return period_id

    def _get_period_milliseconds(self, event: dict[str, Any]) -> int:
        period_minute_limits = assertget(self.root, "periodMinuteLimits")
        period_id = self._get_period_id(event)
        if period_id == 16:  # Pre-match
            return 0
        if period_id == 14:  # Post-game
            return 0
        minute = int(assertget(event, "minute"))
        period_minute = minute
        if period_id > 1:
            period_minute = minute - period_minute_limits[str(period_id - 1)]
        period_second = period_minute * 60 + int(event.get("second", 0))
        return period_second * 1000

    def extract_games(self) -> dict[int, dict[str, Any]]:
        """Return a dictionary with all available games.

        Returns
        -------
        dict
            A mapping between game IDs and the information available about
            each game in the data stream.
        """
        team_home = assertget(self.root, "home")
        team_away = assertget(self.root, "away")
        game_dict = {
            # Fields required by the base schema
            "game_id": self.game_id,
            "season_id": self.season_id,
            "competition_id": self.competition_id,
            "game_day": None,  # Cannot be determined from the data stream
            "game_date": datetime.strptime(
                assertget(self.root, "startTime"), "%Y-%m-%dT%H:%M:%S"
            ),  # Dates are UTC
            "home_team_id": int(assertget(team_home, "teamId")),
            "away_team_id": int(assertget(team_away, "teamId")),
            # Optional fields
            "home_score": int(assertget(assertget(self.root["home"], "scores"), "running")),
            "away_score": int(assertget(assertget(self.root["away"], "scores"), "running")),
            "duration": int(self.root.get("expandedMaxMinute"))
            if "expandedMaxMinute" in self.root
            else None,
            "referee": self.root.get("referee", {}).get("name"),
            "venue": self.root.get("venueName"),
            "attendance": int(self.root.get("attendance")) if "attendance" in self.root else None,
            "home_manager": team_home.get("managerName"),
            "away_manager": team_away.get("managerName"),
        }
        return {self.game_id: game_dict}

    def extract_teams(self) -> dict[int, dict[str, Any]]:
        """Return a dictionary with all available teams.

        Returns
        -------
        dict
            A mapping between team IDs and the information available about
            each team in the data stream.
        """
        teams = {}
        for side in [self.root["home"], self.root["away"]]:
            team_id = int(assertget(side, "teamId"))
            teams[team_id] = {
                # Fields required by the base schema
                "team_id": team_id,
                "team_name": assertget(side, "name"),
            }
        return teams

    def extract_players(self) -> dict[tuple[int, int], dict[str, Any]]:
        """Return a dictionary with all available players.

        Returns
        -------
        dict
            A mapping between (game ID, player ID) tuples and the information
            available about each player in the data stream.
        """
        game_id = self.game_id
        player_gamestats = self.extract_playergamestats()
        players = {}
        for team in [self.root["home"], self.root["away"]]:
            team_id = int(assertget(team, "teamId"))
            for p in team["players"]:
                player_id = int(assertget(p, "playerId"))
                players[(game_id, player_id)] = {
                    # Fields required by the base schema
                    "game_id": game_id,
                    "team_id": team_id,
                    "player_id": player_id,
                    "player_name": assertget(p, "name"),
                    "is_starter": bool(p.get("isFirstEleven", False)),
                    "minutes_played": player_gamestats[(game_id, player_id)]["minutes_played"],
                    "jersey_number": player_gamestats[(game_id, player_id)]["jersey_number"],
                    # Fields required by the opta schema
                    "starting_position": player_gamestats[(game_id, player_id)]["position_code"],
                    # Optional fields
                    # WhoScored retrieves player details for the current date,
                    # not for the game date. Hence, we do not innclude this
                    # info.
                    # age=int(p["age"]),
                    # height=float(p.get("height", float("NaN"))),
                    # weight=float(p.get("weight", float("NaN"))),
                }
        return players

    def extract_events(self) -> dict[tuple[int, int], dict[str, Any]]:
        """Return a dictionary with all available events.

        Returns
        -------
        dict
            A mapping between (game ID, event ID) tuples and the information
            available about each event in the data stream.
        """
        events = {}

        time_start_str = assertget(self.root, "startTime")
        time_start = datetime.strptime(time_start_str, "%Y-%m-%dT%H:%M:%S")
        for attr in self.root["events"]:
            event_id = int(assertget(attr, "id" if "id" in attr else "eventId"))
            eventtype = attr.get("type", {})
            start_x = float(assertget(attr, "x"))
            start_y = float(assertget(attr, "y"))
            minute = int(assertget(attr, "expandedMinute"))
            second = int(attr.get("second", 0))
            qualifiers = {
                int(q["type"]["value"]): q.get("value", True) for q in attr.get("qualifiers", [])
            }
            end_x = attr.get("endX", _get_end_x(qualifiers))
            end_y = attr.get("endY", _get_end_y(qualifiers))
            events[(self.game_id, event_id)] = {
                # Fields required by the base schema
                "game_id": self.game_id,
                "event_id": event_id,
                "period_id": self._get_period_id(attr),
                "team_id": int(assertget(attr, "teamId")),
                "player_id": int(attr.get("playerId")) if "playerId" in attr else None,
                "type_id": int(assertget(eventtype, "value")),
                # type_name=assertget(eventtype, "displayName"),  # added in the opta loader
                # Fields required by the opta schema
                # Timestamp is not availe in the data stream. The returned
                # timestamp  is not accurate, but sufficient for camptability
                # with the other Opta data streams.
                "timestamp": (time_start + timedelta(seconds=(minute * 60 + second))),
                "minute": minute,
                "second": second,
                "outcome": bool(attr["outcomeType"].get("value"))
                if "outcomeType" in attr
                else None,
                "start_x": start_x,
                "start_y": start_y,
                "end_x": end_x if end_x is not None else start_x,
                "end_y": end_y if end_y is not None else start_y,
                "qualifiers": qualifiers,
                # Optional fields
                "related_player_id": int(attr.get("relatedPlayerId"))
                if "relatedPlayerId" in attr
                else None,
                "touch": bool(attr.get("isTouch", False)),
                "goal": bool(attr.get("isGoal", False)),
                "shot": bool(attr.get("isShot", False)),
                # assist=bool(attr.get('assist')) if "assist" in attr else None,
                # keypass=bool(attr.get('keypass')) if "keypass" in attr else None,
            }

        return events

    def extract_substitutions(self) -> dict[tuple[int, int], dict[str, Any]]:
        """Return a dictionary with all substitution events.

        Returns
        -------
        dict
            A mapping between (game ID, player ID) tuples and the information
            available about each substitution in the data stream.
        """
        subs = {}
        subonevents = [e for e in self.root["events"] if e["type"].get("value") == 19]
        for e in subonevents:
            sub_id = int(assertget(e, "playerId"))
            sub = {
                "game_id": self.game_id,
                "team_id": int(assertget(e, "teamId")),
                "period_id": self._get_period_id(e),
                "period_milliseconds": self._get_period_milliseconds(e),
                "player_in_id": int(assertget(e, "playerId")),
                "player_out_id": int(assertget(e, "relatedPlayerId")),
            }
            subs[(self.game_id, sub_id)] = sub
        return subs

    def extract_positions(self) -> dict[tuple[int, int, int], dict[str, Any]]:  # noqa: C901
        """Return a dictionary with each player's position during a game.

        Returns
        -------
        dict
            A mapping between (game ID, player ID, epoch ID) tuples and the
            information available about each player's position in the data stream.
        """
        positions = {}
        for t in [self.root["home"], self.root["away"]]:
            team_id = int(assertget(t, "teamId"))
            for f in assertget(t, "formations"):
                fpositions = assertget(f, "formationPositions")
                playersIds = assertget(f, "playerIds")
                formation = assertget(f, "formationName")

                period_end_minutes = assertget(self.root, "periodEndMinutes")
                period_minute_limits = assertget(self.root, "periodMinuteLimits")
                start_minute = int(assertget(f, "startMinuteExpanded"))
                end_minute = int(assertget(f, "endMinuteExpanded"))
                for period_id in sorted(period_end_minutes.keys()):
                    if period_end_minutes[period_id] > start_minute:
                        break
                period_id = int(period_id)
                period_minute = start_minute
                if period_id > 1:
                    period_minute = start_minute - period_minute_limits[str(period_id - 1)]

                for i, p in enumerate(fpositions):
                    player_id = int(playersIds[i])
                    x = float(assertget(p, "vertical"))
                    y = float(assertget(p, "horizontal"))
                    position_code = _position_mapping(formation, x, y)
                    positions[(self.game_id, player_id, start_minute)] = {
                        "game_id": self.game_id,
                        "team_id": team_id,
                        "player_id": player_id,
                        "period_id": period_id,
                        "period_milliseconds": (period_minute * 60 * 1000),
                        "start_milliseconds": (start_minute * 60 * 1000),
                        "end_milliseconds": (end_minute * 60 * 1000),
                        "formation_scheme": formation,
                        "player_position": position_code,
                        "player_position_x": x,
                        "player_position_y": y,
                    }
        return positions

    def extract_teamgamestats(self) -> dict[tuple[int, int], dict[str, Any]]:
        """Return some aggregated statistics of each team in a game.

        Returns
        -------
        list(dict)
            A dictionary with aggregated team statistics for each team.
        """
        teams_gamestats = {}
        teams = [self.root["home"], self.root["away"]]
        for team in teams:
            team_id = int(assertget(team, "teamId"))
            statsdict = {}
            for name in team["stats"]:
                if isinstance(team["stats"][name], dict):
                    statsdict[_camel_to_snake(name)] = sum(team["stats"][name].values())

            scores = assertget(team, "scores")
            teams_gamestats[(self.game_id, team_id)] = dict(
                game_id=self.game_id,
                team_id=team_id,
                side=assertget(team, "field"),
                score=assertget(scores, "fulltime"),
                shootout_score=scores.get("penalty"),
                **{k: statsdict[k] for k in statsdict if not k.endswith("Success")},
            )

        return teams_gamestats

    def extract_playergamestats(self) -> dict[tuple[int, int], dict[str, Any]]:  # noqa: C901
        """Return some aggregated statistics of each player in a game.

        Returns
        -------
        dict(dict)
            A dictionary with aggregated team statistics for each player.
        """
        players_gamestats = {}
        for team in [self.root["home"], self.root["away"]]:
            team_id = int(assertget(team, "teamId"))
            red_cards = {
                e["playerId"]: e["expandedMinute"]
                for e in team.get("incidentEvents", [])
                if "cardType" in e
                and e["cardType"]["displayName"] in ["Red", "SecondYellow"]
                and "playerId" in e  # not defined if a coach receives a red card
            }
            for player in team["players"]:
                statsdict = {
                    _camel_to_snake(name): sum(stat.values())
                    for name, stat in player["stats"].items()
                }
                stats = [k for k in statsdict if not k.endswith("success")]

                player_id = int(assertget(player, "playerId"))
                p = dict(
                    game_id=self.game_id,
                    team_id=team_id,
                    player_id=player_id,
                    is_starter=bool(player.get("isFirstEleven", False)),
                    position_code=player.get("position", None),
                    jersey_number=int(player.get("shirtNo", 0)),
                    mvp=bool(player.get("isManOfTheMatch", False)),
                    **{k: statsdict[k] for k in stats},
                )
                if "subbedInExpandedMinute" in player:
                    p["minute_start"] = player["subbedInExpandedMinute"]
                if "subbedOutExpandedMinute" in player:
                    p["minute_end"] = player["subbedOutExpandedMinute"]
                if player_id in red_cards:
                    p["minute_end"] = red_cards[player_id]

                # Did not play
                p["minutes_played"] = 0
                # Played the full game
                if p["is_starter"] and "minute_end" not in p:
                    p["minute_start"] = 0
                    p["minute_end"] = self.root["expandedMaxMinute"]
                    p["minutes_played"] = self.root["expandedMaxMinute"]
                # Started but substituted out
                elif p["is_starter"] and "minute_end" in p:
                    p["minute_start"] = 0
                    p["minutes_played"] = p["minute_end"]
                # Substitud in and played the remainder of the game
                elif "minute_start" in p and "minute_end" not in p:
                    p["minute_end"] = self.root["expandedMaxMinute"]
                    p["minutes_played"] = self.root["expandedMaxMinute"] - p["minute_start"]
                # Substitud in and out
                elif "minute_start" in p and "minute_end" in p:
                    p["minutes_played"] = p["minute_end"] - p["minute_start"]

                players_gamestats[(self.game_id, player_id)] = p
        return players_gamestats


def _camel_to_snake(name: str) -> str:
    s1 = re.sub("(.)([A-Z][a-z]+)", r"\1_\2", name)
    return re.sub("([a-z0-9])([A-Z])", r"\1_\2", s1).lower()
