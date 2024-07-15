"""JSON parser for Stats Perform MA1 feeds."""

from datetime import datetime
from typing import Any, Optional

from ...base import MissingDataError
from .base import OptaJSONParser, assertget


class MA1JSONParser(OptaJSONParser):
    """Extract data from a Stats Perform MA1 data stream.

    Parameters
    ----------
    path : str
        Path of the data file.
    """

    def _get_matches(self) -> list[dict[str, Any]]:
        if "matchInfo" in self.root:
            return [self.root]
        if "match" in self.root:
            return self.root["match"]
        raise MissingDataError

    def _get_match_info(self, match: dict[str, Any]) -> dict[str, Any]:
        if "matchInfo" in match:
            return match["matchInfo"]
        raise MissingDataError

    def _get_live_data(self, match: dict[str, Any]) -> dict[str, Any]:
        if "liveData" in match:
            return match["liveData"]
        return {}

    def _get_name(self, obj: dict[str, Any]) -> Optional[str]:
        if "name" in obj:
            return assertget(obj, "name")
        if "firstName" in obj:
            return f"{assertget(obj, 'firstName')} {assertget(obj, 'lastName')}"
        return None

    @staticmethod
    def _extract_team_id(teams: list[dict[str, str]], side: str) -> Optional[str]:
        for team in teams:
            team_side = assertget(team, "position")
            if team_side == side:
                team_id = assertget(team, "id")
                return team_id
        raise MissingDataError

    def extract_competitions(self) -> dict[tuple[str, str], dict[str, Any]]:
        """Return a dictionary with all available competitions.

        Returns
        -------
        dict
            A mapping between (competion ID, season ID) tuples and the
            information available about each competition in the data stream.
        """
        competitions = {}
        for match in self._get_matches():
            match_info = self._get_match_info(match)
            season = assertget(match_info, "tournamentCalendar")
            season_id = assertget(season, "id")
            competition = assertget(match_info, "competition")
            competition_id = assertget(competition, "id")
            competitions[(competition_id, season_id)] = {
                "season_id": season_id,
                "season_name": assertget(season, "name"),
                "competition_id": competition_id,
                "competition_name": assertget(competition, "name"),
            }
        return competitions

    def extract_games(self) -> dict[str, dict[str, Any]]:
        """Return a dictionary with all available games.

        Returns
        -------
        dict
            A mapping between game IDs and the information available about
            each game in the data stream.
        """
        games = {}
        for match in self._get_matches():
            match_info = self._get_match_info(match)
            game_id = assertget(match_info, "id")
            season = assertget(match_info, "tournamentCalendar")
            competition = assertget(match_info, "competition")
            contestant = assertget(match_info, "contestant")
            game_date = assertget(match_info, "date")
            game_time = assertget(match_info, "time")
            game_datetime = f"{game_date} {game_time}"
            venue = assertget(match_info, "venue")
            games[game_id] = {
                # Fields required by the base schema
                "game_id": game_id,
                "competition_id": assertget(competition, "id"),
                "season_id": assertget(season, "id"),
                "game_day": int(match_info["week"]) if "week" in match_info else None,
                "game_date": datetime.strptime(game_datetime, "%Y-%m-%dZ %H:%M:%SZ"),
                "home_team_id": self._extract_team_id(contestant, "home"),
                "away_team_id": self._extract_team_id(contestant, "away"),
                # Optional fields
                # home_score=?,
                # away_score=?,
                # duration=?,
                # referee=?,
                "venue": venue["shortName"] if "shortName" in venue else None,
                # attendance=?,
                # home_manager=?,
                # away_manager=?,
            }
            live_data = self._get_live_data(match)
            if "matchDetails" in live_data:
                match_details = assertget(live_data, "matchDetails")
                if "matchLengthMin" in match_details:
                    games[game_id]["duration"] = assertget(match_details, "matchLengthMin")
                if "scores" in match_details:
                    scores = assertget(match_details, "scores")
                    games[game_id]["home_score"] = assertget(scores, "total")["home"]
                    games[game_id]["away_score"] = assertget(scores, "total")["away"]
                if "matchDetailsExtra" in live_data:
                    extra_match_details = assertget(live_data, "matchDetailsExtra")
                    if "attendance" in extra_match_details:
                        games[game_id]["attendance"] = int(
                            assertget(extra_match_details, "attendance")
                        )
                    if "matchOfficial" in extra_match_details:
                        for official in assertget(extra_match_details, "matchOfficial"):
                            if official["type"] == "Main":
                                games[game_id]["referee"] = self._get_name(official)
        return games

    def extract_teams(self) -> dict[str, dict[str, Any]]:
        """Return a dictionary with all available teams.

        Returns
        -------
        dict
            A mapping between team IDs and the information available about
            each team in the data stream.
        """
        teams = {}
        for match in self._get_matches():
            match_info = self._get_match_info(match)
            contestants = assertget(match_info, "contestant")
            for contestant in contestants:
                team_id = assertget(contestant, "id")
                team = {
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
            A mapping between player IDs and the information available about
            each player in the data stream.
        """
        players = {}
        subs = self.extract_substitutions()
        for match in self._get_matches():
            match_info = self._get_match_info(match)
            game_id = assertget(match_info, "id")
            live_data = self._get_live_data(match)
            if "lineUp" not in live_data:
                continue
            red_cards = {
                e["playerId"]: e["timeMin"]
                for e in live_data.get("card", [])
                if "type" in e
                and e["type"] in ["Y2C", "RC"]
                and "playerId" in e  # not defined if a coach receives a red card
            }
            lineups = assertget(live_data, "lineUp")
            for lineup in lineups:
                team_id = assertget(lineup, "contestantId")
                players_in_lineup = assertget(lineup, "player")
                for individual in players_in_lineup:
                    player_id = assertget(individual, "playerId")
                    players[(game_id, player_id)] = {
                        # Fields required by the base schema
                        "game_id": game_id,
                        "team_id": team_id,
                        "player_id": player_id,
                        "player_name": self._get_name(individual),
                        "is_starter": assertget(individual, "position") != "Substitute",
                        # minutes_played="?",
                        "jersey_number": assertget(individual, "shirtNumber"),
                        # Fields required by the opta schema
                        "starting_position": assertget(individual, "position"),
                    }
                    if "matchDetails" in live_data and "substitute" in live_data:
                        match_details = assertget(live_data, "matchDetails")
                        if "matchLengthMin" not in match_details:
                            continue
                        # Determine when player entered the pitch
                        is_starter = assertget(individual, "position") != "Substitute"
                        sub_in = [
                            s
                            for s in subs.values()
                            if s["game_id"] == game_id and s["player_in_id"] == player_id
                        ]
                        if is_starter:
                            minute_start = 0
                        elif len(sub_in) == 1:
                            minute_start = sub_in[0]["minute"]
                        else:
                            minute_start = None
                        # Determine when player left the pitch
                        sub_out = [
                            s
                            for s in subs.values()
                            if s["game_id"] == game_id and s["player_out_id"] == player_id
                        ]
                        duration = assertget(match_details, "matchLengthMin")
                        minute_end = duration
                        if len(sub_out) == 1:
                            minute_end = sub_out[0]["minute"]
                        elif player_id in red_cards:
                            minute_end = red_cards[player_id]
                        # Determin time on the pitch
                        if is_starter or minute_start is not None:
                            players[(game_id, player_id)]["minutes_played"] = (
                                minute_end - minute_start
                            )
                        else:
                            players[(game_id, player_id)]["minutes_played"] = 0
        return players

    def extract_substitutions(self) -> dict[tuple[int, int], dict[str, Any]]:
        """Return a dictionary with all substitution events.

        Returns
        -------
        dict
            A mapping between (game ID, player ID) tuples and the information
            available about each substitution in the data stream.
        """
        subs = {}
        for match in self._get_matches():
            match_info = self._get_match_info(match)
            game_id = assertget(match_info, "id")
            live_data = self._get_live_data(match)
            if "substitute" not in live_data:
                continue
            for e in assertget(live_data, "substitute"):
                sub_id = assertget(e, "playerOnId")
                subs[(game_id, sub_id)] = {
                    "game_id": game_id,
                    "team_id": assertget(e, "contestantId"),
                    "period_id": int(assertget(e, "periodId")),
                    "minute": int(assertget(e, "timeMin")),
                    "player_in_id": assertget(e, "playerOnId"),
                    "player_out_id": assertget(e, "playerOffId"),
                }
        return subs
