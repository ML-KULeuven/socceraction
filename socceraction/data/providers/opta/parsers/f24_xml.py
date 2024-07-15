"""XML parser for Opta F24 feeds."""

from datetime import datetime
from typing import Any

from lxml import objectify

from .base import OptaXMLParser, _get_end_x, _get_end_y, assertget


class F24XMLParser(OptaXMLParser):
    """Extract data from a Opta F24 data stream.

    Parameters
    ----------
    path : str
        Path of the data file.
    """

    def _get_doc(self) -> objectify.ObjectifiedElement:
        return self.root

    def extract_games(self) -> dict[int, dict[str, Any]]:
        """Return a dictionary with all available games.

        Returns
        -------
        dict
            A mapping between game IDs and the information available about
            each game in the data stream.
        """
        optadocument = self._get_doc()
        game_elem = optadocument.find("Game")
        attr = game_elem.attrib
        game_id = int(assertget(attr, "id"))
        game_dict = {
            # Fields required by the base schema
            "game_id": game_id,
            "season_id": int(assertget(attr, "season_id")),
            "competition_id": int(assertget(attr, "competition_id")),
            "game_day": int(assertget(attr, "matchday")),
            "game_date": datetime.strptime(assertget(attr, "game_date"), "%Y-%m-%dT%H:%M:%S"),
            "home_team_id": int(assertget(attr, "home_team_id")),
            "away_team_id": int(assertget(attr, "away_team_id")),
            # Optional fields
            "home_score": int(assertget(attr, "home_score")),
            "away_score": int(assertget(attr, "away_score")),
            # duration=?
            # referee=?
            # venue=?
            # attendance=?
            # home_manager=?
            # away_manager=?
        }
        return {game_id: game_dict}

    def extract_events(self) -> dict[tuple[int, int], dict[str, Any]]:
        """Return a dictionary with all available events.

        Returns
        -------
        dict
            A mapping between (game ID, event ID) tuples and the information
            available about each event in the data stream.
        """
        optadocument = self._get_doc()
        game_elm = optadocument.find("Game")
        game_id = int(assertget(game_elm.attrib, "id"))
        events = {}
        for event_elm in game_elm.iterchildren("Event"):
            attr = dict(event_elm.attrib)
            event_id = int(assertget(attr, "id"))

            qualifiers = {
                int(qualifier_elm.attrib["qualifier_id"]): qualifier_elm.attrib.get("value")
                for qualifier_elm in event_elm.iterchildren("Q")
            }
            start_x = float(assertget(attr, "x"))
            start_y = float(assertget(attr, "y"))
            end_x = _get_end_x(qualifiers)
            end_y = _get_end_y(qualifiers)

            events[(game_id, event_id)] = {
                # Fields required by the base schema
                "game_id": game_id,
                "event_id": event_id,
                "period_id": int(assertget(attr, "period_id")),
                "team_id": int(assertget(attr, "team_id")),
                "player_id": int(attr["player_id"]) if "player_id" in attr else None,
                "type_id": int(assertget(attr, "type_id")),
                # type_name=?, # added in the opta loader
                # Fields required by the opta schema
                "timestamp": datetime.strptime(
                    assertget(attr, "timestamp"), "%Y-%m-%dT%H:%M:%S.%f"
                ),
                "minute": int(assertget(attr, "min")),
                "second": int(assertget(attr, "sec")),
                "outcome": bool(int(attr["outcome"])) if "outcome" in attr else None,
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
