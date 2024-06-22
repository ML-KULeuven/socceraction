"""XML parser for Opta F7 feeds."""

from datetime import datetime
from typing import Any

from lxml import objectify

from .base import OptaXMLParser, assertget


class F7XMLParser(OptaXMLParser):
    """Extract data from a Opta F7 data stream.

    Parameters
    ----------
    path : str
        Path of the data file.
    """

    def _get_doc(self) -> objectify.ObjectifiedElement:
        optadocument = self.root.find("SoccerDocument")
        return optadocument

    def _get_stats(self, obj: objectify.ObjectifiedElement) -> dict[str, Any]:
        stats = {}
        for stat in obj.find("Stat"):
            stats[stat.attrib["Type"]] = stat.text
        return stats

    def _get_name(self, obj: objectify.ObjectifiedElement) -> str:
        if "Known" in obj:
            return obj.Known
        return obj.First + " " + obj.Last

    def extract_competitions(self) -> dict[tuple[int, int], dict[str, Any]]:
        """Return a dictionary with all available competitions.

        Returns
        -------
        dict
            A mapping between (competion ID, season ID) tuples and the
            information available about each competition in the data stream.
        """
        optadocument = self._get_doc()
        competition = optadocument.Competition
        competition_id = int(competition.attrib["uID"][1:])
        stats = self._get_stats(competition)
        season_id = int(assertget(stats, "season_id"))
        competition_dict = {
            # Fields required by the base schema
            "competition_id": competition_id,
            "season_id": season_id,
            "season_name": assertget(stats, "season_name"),
            "competition_name": competition.Name.text,
        }
        return {(competition_id, season_id): competition_dict}

    def extract_games(self) -> dict[int, dict[str, Any]]:
        """Return a dictionary with all available games.

        Returns
        -------
        dict
            A mapping between game IDs and the information available about
            each game in the data stream.
        """
        optadocument = self._get_doc()
        competition = optadocument.Competition
        competition_id = int(competition.attrib["uID"][1:])
        competition_stats = self._get_stats(competition)
        match_info = optadocument.MatchData.MatchInfo
        game_id = int(optadocument.attrib["uID"][1:])
        stats = self._get_stats(optadocument.MatchData)
        team_data_elms = {
            t.attrib["Side"]: t for t in optadocument.MatchData.iterchildren("TeamData")
        }
        team_officials = {}
        for t in optadocument.iterchildren("Team"):
            side = (
                "Home"
                if int(team_data_elms["Home"].attrib["TeamRef"][1:]) == int(t.attrib["uID"][1:])
                else "Away"
            )
            for m in t.iterchildren("TeamOfficial"):
                if m.attrib["Type"] == "Manager":
                    team_officials[side] = m

        game_dict = {
            # Fields required by the base schema
            "game_id": game_id,
            "season_id": int(assertget(competition_stats, "season_id")),
            "competition_id": competition_id,
            "game_day": int(competition_stats["matchday"])
            if "matchday" in competition_stats
            else None,
            "game_date": datetime.strptime(match_info.Date.text, "%Y%m%dT%H%M%S%z").replace(
                tzinfo=None
            ),
            "home_team_id": int(
                assertget(assertget(team_data_elms, "Home").attrib, "TeamRef")[1:]
            ),
            "away_team_id": int(
                assertget(assertget(team_data_elms, "Away").attrib, "TeamRef")[1:]
            ),
            # Optional fields
            "home_score": int(assertget(assertget(team_data_elms, "Home").attrib, "Score")),
            "away_score": int(assertget(assertget(team_data_elms, "Away").attrib, "Score")),
            "duration": int(stats["match_time"]),
            "referee": self._get_name(optadocument.MatchData.MatchOfficial.OfficialName),
            "venue": optadocument.Venue.Name.text,
            "attendance": int(match_info.Attendance),
            "home_manager": self._get_name(team_officials["Home"].PersonName)
            if "Home" in team_officials
            else None,
            "away_manager": self._get_name(team_officials["Away"].PersonName)
            if "Away" in team_officials
            else None,
        }
        return {game_id: game_dict}

    def extract_teams(self) -> dict[int, dict[str, Any]]:
        """Return a dictionary with all available teams.

        Returns
        -------
        dict
            A mapping between team IDs and the information available about
            each team in the data stream.
        """
        optadocument = self._get_doc()
        team_elms = list(optadocument.iterchildren("Team"))
        teams = {}
        for team_elm in team_elms:
            team_id = int(assertget(team_elm.attrib, "uID")[1:])
            teams[team_id] = {
                # Fields required by the base schema
                "team_id": team_id,
                "team_name": team_elm.Name.text,
            }
        return teams

    def extract_lineups(self) -> dict[int, dict[str, Any]]:
        """Return a dictionary with the lineup of each team.

        Returns
        -------
        dict
            A mapping between team IDs and the information available about
            each team's lineup in the data stream.
        """
        optadocument = self._get_doc()

        stats = {}
        for stat in optadocument.MatchData.find("Stat"):
            stats[stat.attrib["Type"]] = stat.text

        lineup_elms = optadocument.MatchData.iterchildren("TeamData")
        lineups = {}
        for team_elm in lineup_elms:
            # lineup attributes
            team_id = int(team_elm.attrib["TeamRef"][1:])
            lineups[team_id] = {
                "formation": team_elm.attrib["Formation"],
                "score": int(team_elm.attrib["Score"]),
                "side": team_elm.attrib["Side"],
                "players": {},
            }
            # substitutes
            subst_elms = team_elm.iterchildren("Substitution")
            subst = [subst_elm.attrib for subst_elm in subst_elms]
            # red_cards
            booking_elms = team_elm.iterchildren("Booking")
            red_cards = {
                int(booking_elm.attrib["PlayerRef"][1:]): int(booking_elm.attrib["Min"])
                for booking_elm in booking_elms
                if "CardType" in booking_elm.attrib
                and booking_elm.attrib["CardType"] in ["Red", "SecondYellow"]
                and "PlayerRef" in booking_elm.attrib  # not defined if a coach receives a red card
            }
            # players
            player_elms = team_elm.PlayerLineUp.iterchildren("MatchPlayer")
            for player_elm in player_elms:
                player_id = int(player_elm.attrib["PlayerRef"][1:])
                sub_on = int(
                    next(
                        (
                            item["Time"]
                            for item in subst
                            if "Retired" not in item and item["SubOn"] == f"p{player_id}"
                        ),
                        stats["match_time"] if player_elm.attrib["Status"] == "Sub" else 0,
                    )
                )
                sub_off = int(
                    next(
                        (item["Time"] for item in subst if item["SubOff"] == f"p{player_id}"),
                        stats["match_time"]
                        if player_id not in red_cards
                        else red_cards[player_id],
                    )
                )
                minutes_played = sub_off - sub_on
                lineups[team_id]["players"][player_id] = {
                    "starting_position_id": int(player_elm.attrib["Formation_Place"]),
                    "starting_position_name": player_elm.attrib["Position"],
                    "jersey_number": int(player_elm.attrib["ShirtNumber"]),
                    "is_starter": int(player_elm.attrib["Formation_Place"]) != 0,
                    "minutes_played": minutes_played,
                }
        return lineups

    def extract_players(self) -> dict[tuple[int, int], dict[str, Any]]:
        """Return a dictionary with all available players.

        Returns
        -------
        dict
            A mapping between (game ID, player ID) tuples and the information
            available about each player in the data stream.
        """
        optadocument = self._get_doc()
        game_id = int(optadocument.attrib["uID"][1:])
        lineups = self.extract_lineups()
        team_elms = list(optadocument.iterchildren("Team"))
        players = {}
        for team_elm in team_elms:
            team_id = int(team_elm.attrib["uID"][1:])
            for player_elm in team_elm.iterchildren("Player"):
                player_id = int(player_elm.attrib["uID"][1:])
                player = {
                    # Fields required by the base schema
                    "game_id": game_id,
                    "team_id": team_id,
                    "player_id": player_id,
                    "player_name": self._get_name(player_elm.PersonName),
                    "is_starter": lineups[team_id]["players"][player_id]["is_starter"],
                    "minutes_played": lineups[team_id]["players"][player_id]["minutes_played"],
                    "jersey_number": lineups[team_id]["players"][player_id]["jersey_number"],
                    # Fields required by the opta schema
                    "starting_position": lineups[team_id]["players"][player_id][
                        "starting_position_name"
                    ],
                    # Optional fields
                    # height="?",
                    # weight="?",
                    # age="?",
                }
                players[(game_id, player_id)] = player

        return players
