"""JSON parser for Opta F24 feeds."""

from datetime import datetime
from typing import Any

from ...base import MissingDataError
from .base import OptaJSONParser, _get_end_x, _get_end_y, assertget


class F24JSONParser(OptaJSONParser):
    """Extract data from a Opta F24 data stream.

    Parameters
    ----------
    path : str
        Path of the data file.
    """

    def _get_doc(self) -> dict[str, Any]:
        for node in self.root:
            if "Games" in node["data"].keys():
                return node
        raise MissingDataError

    def extract_games(self) -> dict[int, dict[str, Any]]:
        """Return a dictionary with all available games.

        Returns
        -------
        dict
            A mapping between game IDs and the information available about
            each game in the data stream.
        """
        f24 = self._get_doc()

        data = assertget(f24, "data")
        games = assertget(data, "Games")
        game = assertget(games, "Game")
        attr = assertget(game, "@attributes")

        game_id = int(assertget(attr, "id"))
        game_dict = {
            game_id: {
                # Fields required by the base schema
                "game_id": game_id,
                "season_id": int(assertget(attr, "season_id")),
                "competition_id": int(assertget(attr, "competition_id")),
                "game_day": int(assertget(attr, "matchday")),
                "game_date": datetime.strptime(
                    assertget(assertget(attr, "game_date"), "locale"), "%Y-%m-%dT%H:%M:%S.%fZ"
                ).replace(tzinfo=None),
                "home_team_id": int(assertget(attr, "home_team_id")),
                "away_team_id": int(assertget(attr, "away_team_id")),
                # Fields required by the opta schema
                # home_score=?
                # away_score=?
                # duration=?
                # referee=?
                # venue=?,
                # attendance=?
                # Optional fields
                # home_manager=?
                # away_manager=?
            }
        }
        return game_dict

    def extract_events(self) -> dict[tuple[int, int], dict[str, Any]]:
        """Return a dictionary with all available events.

        Returns
        -------
        dict
            A mapping between (game ID, event ID) tuples and the information
            available about each event in the data stream.
        """
        f24 = self._get_doc()

        data = assertget(f24, "data")
        games = assertget(data, "Games")
        game = assertget(games, "Game")
        game_attr = assertget(game, "@attributes")
        game_id = int(assertget(game_attr, "id"))

        events = {}
        for element in assertget(game, "Event"):
            attr = element["@attributes"]
            timestamp = attr["TimeStamp"].get("locale") if attr.get("TimeStamp") else None
            timestamp = datetime.strptime(timestamp, "%Y-%m-%dT%H:%M:%S.%fZ")
            qualifiers = {
                int(q["@attributes"]["qualifier_id"]): q["@attributes"]["value"]
                for q in element.get("Q", [])
            }
            start_x = float(assertget(attr, "x"))
            start_y = float(assertget(attr, "y"))
            end_x = _get_end_x(qualifiers)
            end_y = _get_end_y(qualifiers)

            event_id = int(assertget(attr, "id"))
            events[(game_id, event_id)] = {
                # Fields required by the base schema
                "game_id": game_id,
                "event_id": event_id,
                "period_id": int(assertget(attr, "period_id")),
                "team_id": int(assertget(attr, "team_id")),
                "player_id": int(assertget(attr, "player_id")),
                "type_id": int(assertget(attr, "type_id")),
                # type_name=?, # added in the opta loader
                # Fields required by the opta schema
                "timestamp": timestamp,
                "minute": int(assertget(attr, "min")),
                "second": int(assertget(attr, "sec")),
                "outcome": bool(int(attr.get("outcome", 1))),
                "start_x": start_x,
                "start_y": start_y,
                "end_x": end_x if end_x is not None else start_x,
                "end_y": end_y if end_y is not None else start_y,
                "qualifiers": qualifiers,
                # Optional fields
                "assist": bool(int(attr.get("assist", 0))),
                "keypass": bool(int(attr.get("keypass", 0))),
            }
        return events
