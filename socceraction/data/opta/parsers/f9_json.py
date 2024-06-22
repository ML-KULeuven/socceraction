"""JSON parser for Opta F9 feeds."""

from datetime import datetime
from typing import Any, Optional

from ...base import MissingDataError
from .base import OptaJSONParser, assertget


class F9JSONParser(OptaJSONParser):
    """Extract data from a Opta F9 data stream.

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
        f9 = self._get_feed()
        data = assertget(f9, "data")
        optafeed = assertget(data, "OptaFeed")
        optadocument = assertget(optafeed, "OptaDocument")[0]
        return optadocument

    def _get_stats(self, obj: dict[str, Any]) -> dict[str, Any]:
        if "Stat" not in obj:
            return {}

        stats = {}
        statobj = obj["Stat"] if isinstance(obj["Stat"], list) else [obj["Stat"]]
        for stat in statobj:
            stats[stat["@attributes"]["Type"]] = stat["@value"]
        return stats

    def _get_name(self, obj: dict[str, Any]) -> Optional[str]:
        if "Known" in obj and obj["Known"].strip():
            return obj["Known"]
        if "First" in obj and "Last" in obj and obj["Last"].strip() or obj["First"].strip():
            return (obj["First"] + " " + obj["Last"]).strip()
        return None

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
        competition = assertget(optadocument, "Competition")
        competitionstat = self._get_stats(competition)
        venue = assertget(optadocument, "Venue")
        matchofficial = assertget(matchdata, "MatchOfficial")
        matchinfo = assertget(matchdata, "MatchInfo")
        matchstat = self._get_stats(matchdata)
        teamdata = assertget(matchdata, "TeamData")
        scores = {}
        for t in teamdata:
            scores[t["@attributes"]["Side"]] = t["@attributes"]["Score"]

        game_id = int(assertget(attr, "uID")[1:])
        game_dict = {
            # Fields required by the base schema
            "game_id": game_id,
            "competition_id": int(assertget(assertget(competition, "@attributes"), "uID")[1:]),
            "season_id": assertget(competitionstat, "season_id"),
            "game_day": competitionstat["matchday"] if "matchday" in competitionstat else None,
            "game_date": datetime.strptime(
                assertget(matchinfo, "Date"), "%Y%m%dT%H%M%S%z"
            ).replace(tzinfo=None),
            # home_team_id=see below
            # away_team_id=see below
            # Optional fields
            "home_score": int(scores["Home"]),
            "away_score": int(scores["Away"]),
            "duration": int(assertget(matchstat, "match_time")),
            "referee": self._get_name(matchofficial["OfficialName"])
            if "OfficialName" in matchofficial
            else None,
            "venue": venue["Name"] if "Name" in venue else None,
            "attendance": int(matchinfo["Attendance"]) if "Attendance" in matchinfo else None,
            # home_manager=see below
            # away_manager=see below
        }
        for team in teamdata:
            teamattr = assertget(team, "@attributes")
            side = assertget(teamattr, "Side")
            teamid = assertget(teamattr, "TeamRef")
            score = assertget(teamattr, "Score")
            manager = (
                self._get_name(team["TeamOfficial"]["PersonName"])
                if "TeamOfficial" in team
                else None
            )
            if side == "Home":
                game_dict["home_team_id"] = int(teamid[1:])
                game_dict["home_score"] = int(score)
                game_dict["home_manager"] = manager
            else:
                game_dict["away_team_id"] = int(teamid[1:])
                game_dict["away_score"] = int(score)
                game_dict["away_manager"] = manager
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
        root_teams = assertget(optadocument, "Team")

        teams = {}
        for team in root_teams:
            if "id" in team.keys():
                nameobj = team.get("nameObj")
                team_id = int(team["id"])
                teams[team_id] = {
                    # Fields required by the base schema
                    "team_id": team_id,
                    "team_name": nameobj.get("name"),
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
        optadocument = self._get_doc()
        attr = assertget(optadocument, "@attributes")
        game_id = int(assertget(attr, "uID")[1:])
        root_teams = assertget(optadocument, "Team")
        lineups = self.extract_lineups()

        players = {}
        for team in root_teams:
            team_id = int(team["@attributes"]["uID"].replace("t", ""))
            for player in team["Player"]:
                player_id = int(player["@attributes"]["uID"].replace("p", ""))

                assert "nameObj" in player["PersonName"]
                nameobj = player["PersonName"]["nameObj"]
                if not nameobj.get("is_unknown"):
                    player = {
                        # Fields required by the base schema
                        "game_id": game_id,
                        "team_id": team_id,
                        "player_id": player_id,
                        "player_name": self._get_name(player["PersonName"]),
                        # is_starter=
                        # minutes_played=
                        # jersey_number=
                        # Fields required by the opta schema
                        # starting_position=
                        # Optional fields
                        # height="?",
                        # weight="?",
                        # age="?",
                    }
                    if player_id in lineups[team_id]["players"]:
                        player = dict(
                            **player,
                            jersey_number=lineups[team_id]["players"][player_id]["jersey_number"],
                            starting_position=lineups[team_id]["players"][player_id][
                                "starting_position_name"
                            ],
                            is_starter=lineups[team_id]["players"][player_id]["is_starter"],
                            minutes_played=lineups[team_id]["players"][player_id][
                                "minutes_played"
                            ],
                        )
                    players[(game_id, player_id)] = player
        return players

    def extract_lineups(self) -> dict[int, dict[str, Any]]:
        """Return a dictionary with the lineup of each team.

        Raises
        ------
        MissingDataError
            If teams data is not available in the stream.

        Returns
        -------
        dict
            A mapping between team IDs and the information available about
            each team's lineup in the data stream.
        """
        optadocument = self._get_doc()
        attr = assertget(optadocument, "@attributes")

        try:
            rootf9 = optadocument["MatchData"]["TeamData"]
        except KeyError as e:
            raise MissingDataError from e
        matchstats = optadocument["MatchData"]["Stat"]
        matchstats = [matchstats] if isinstance(matchstats, dict) else matchstats
        matchstatsdict = {stat["@attributes"]["Type"]: stat["@value"] for stat in matchstats}

        lineups: dict[int, dict[str, Any]] = {}
        for team in rootf9:
            # lineup attributes
            team_id = int(team["@attributes"]["TeamRef"].replace("t", ""))
            lineups[team_id] = {"players": {}}
            # substitutes
            subst = [s["@attributes"] for s in team["Substitution"]]
            # red cards
            red_cards = {
                int(e["@attributes"]["PlayerRef"].replace("p", "")): e["@attributes"]["Time"]
                for e in team.get("Booking", [])
                if "CardType" in e["@attributes"]
                and e["@attributes"]["CardType"] in ["Red", "SecondYellow"]
                and "PlayerRef" in e["@attributes"]  # not defined if a coach receives a red card
            }
            for player in team["PlayerLineUp"]["MatchPlayer"]:
                attr = player["@attributes"]
                player_id = int(attr["PlayerRef"].replace("p", ""))
                playerstatsdict = {
                    stat["@attributes"]["Type"]: stat["@value"] for stat in player["Stat"]
                }
                sub_on = next(
                    (
                        item["Time"]
                        for item in subst
                        if "Retired" not in item and item["SubOn"] == f"p{player_id}"
                    ),
                    matchstatsdict["match_time"] if attr["Status"] == "Sub" else 0,
                )
                sub_off = next(
                    (item["Time"] for item in subst if item["SubOff"] == f"p{player_id}"),
                    matchstatsdict["match_time"]
                    if player_id not in red_cards
                    else red_cards[player_id],
                )
                minutes_played = sub_off - sub_on
                lineups[team_id]["players"][player_id] = dict(
                    jersey_number=attr["ShirtNumber"],
                    starting_position_name=attr["Position"],
                    starting_position_id=attr["position_id"],
                    is_starter=attr["Status"] == "Start",
                    minutes_played=minutes_played,
                    **playerstatsdict,
                )
        return lineups

    def extract_teamgamestats(self) -> list[dict[str, Any]]:
        """Return some aggregated statistics of each team.

        Raises
        ------
        MissingDataError
            If teams data is not available in the stream.

        Returns
        -------
        list(dict)
            A dictionary with aggregated team statistics for each team.
        """
        optadocument = self._get_doc()
        attr = assertget(optadocument, "@attributes")
        game_id = int(assertget(attr, "uID")[1:])

        try:
            rootf9 = optadocument["MatchData"]["TeamData"]
        except KeyError as e:
            raise MissingDataError from e
        teams_gamestats = []
        for team in rootf9:
            attr = team["@attributes"]
            statsdict = self._get_stats(team)

            team_gamestats = dict(
                game_id=game_id,
                team_id=int(attr["TeamRef"].replace("t", "")),
                side=attr["Side"],
                score=attr["Score"],
                shootout_score=attr["ShootOutScore"],
                **statsdict,
            )

            teams_gamestats.append(team_gamestats)
        return teams_gamestats
