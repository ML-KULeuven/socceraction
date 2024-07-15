"""JSON parser for Opta F1 feeds."""

from datetime import datetime
from typing import Any

from ...base import MissingDataError
from .base import OptaJSONParser, assertget


class F1JSONParser(OptaJSONParser):
    """Extract data from a Opta F1 data stream.

    Parameters
    ----------
    path : str
        Path of the data file.
    """

    def _get_feed(self) -> dict[str, Any]:
        for node in self.root:
            if "OptaFeed" in node["data"].keys():
                return node
        raise MissingDataError

    def _get_doc(self) -> dict[str, Any]:
        f1 = self._get_feed()
        data = assertget(f1, "data")
        optafeed = assertget(data, "OptaFeed")
        optadocument = assertget(optafeed, "OptaDocument")
        return optadocument

    def extract_competitions(self) -> dict[tuple[int, int], dict[str, Any]]:
        """Return a dictionary with all available competitions.

        Returns
        -------
        dict
            A mapping between (competion ID, season ID) tuples and the
            information available about each competition in the data stream.
        """
        optadocument = self._get_doc()
        attr = assertget(optadocument, "@attributes")
        competition_id = int(assertget(attr, "competition_id"))
        season_id = int(assertget(attr, "season_id"))
        competition = {
            # Fields required by the base schema
            "season_id": season_id,
            "season_name": str(assertget(attr, "season_id")),
            "competition_id": competition_id,
            "competition_name": assertget(attr, "competition_name"),
        }
        return {(competition_id, season_id): competition}

    def extract_games(self) -> dict[int, dict[str, Any]]:
        """Return a dictionary with all available games.

        Returns
        -------
        dict
            A mapping between game IDs and the information available about
            each game in the data stream.
        """
        optadocument = self._get_doc()
        attr = assertget(optadocument, "@attributes")
        matchdata = assertget(optadocument, "MatchData")
        matches = {}
        for match in matchdata:
            matchattr = assertget(match, "@attributes")
            matchinfo = assertget(match, "MatchInfo")
            matchinfoattr = assertget(matchinfo, "@attributes")
            game_id = int(assertget(matchattr, "uID")[1:])
            matches[game_id] = {
                # Fields required by the base schema
                "game_id": game_id,
                "competition_id": int(assertget(attr, "competition_id")),
                "season_id": int(assertget(attr, "season_id")),
                "game_day": int(assertget(matchinfoattr, "MatchDay")),
                "game_date": datetime.strptime(assertget(matchinfo, "Date"), "%Y-%m-%d %H:%M:%S"),
                # home_team_id=see below,
                # away_team_id=see below,
                # Optional fields
                # home_score=see below,
                # away_score=see below,
                # duration=?
                # referee=?
                # venue=?,
                # attendance=?
                # home_manager=?
                # away_manager=?
            }
            teamdata = assertget(match, "TeamData")
            for team in teamdata:
                teamattr = assertget(team, "@attributes")
                side = assertget(teamattr, "Side")
                teamid = assertget(teamattr, "TeamRef")
                score = assertget(teamattr, "Score")
                if side == "Home":
                    matches[game_id]["home_team_id"] = int(teamid[1:])
                    matches[game_id]["home_score"] = int(score)
                else:
                    matches[game_id]["away_team_id"] = int(teamid[1:])
                    matches[game_id]["away_score"] = int(score)
        return matches
