"""JSON parser for Stats Perform MA1 feeds."""
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd

from ...base import MissingDataError
from .base import OptaJSONParser, assertget


class MA12JSONParser(OptaJSONParser):
    """Extract data from a Stats Perform MA12 data stream.

    Parameters
    ----------
    path : str
        Path of the data file.
    """

    def _get_matches(self) -> List[Dict[str, Any]]:
        if 'matchInfo' in self.root:
            return [self.root]
        if 'match' in self.root:
            return self.root['match']
        raise MissingDataError

    def _get_match_info(self) -> Dict[str, Any]:
        if "matchInfo" in self.root:
            return self.root["matchInfo"]
        raise MissingDataError

    def _get_live_data(self) -> Dict[str, Any]:
        if "liveData" in self.root:
            return self.root["liveData"]
        raise MissingDataError

    def _get_name(self, obj: Dict[str, Any]) -> Optional[str]:
        if "name" in obj:
            return assertget(obj, "name")
        if "firstName" in obj:
            return f"{assertget(obj, 'firstName')} {assertget(obj, 'lastName')}"
        return None

    @staticmethod
    def _extract_team_id(teams: List[Dict[str, str]], side: str) -> Optional[str]:
        for team in teams:
            team_side = assertget(team, "position")
            if team_side == side:
                team_id = assertget(team, "id")
                return team_id
        raise MissingDataError

    def extract_players(self) -> Dict[Tuple[str, str], Dict[str, Any]]:  # noqa: C901
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
        line_up = assertget(live_data, "lineUp")

        players_data: Dict[str, List[Any]] = {
            # "game_id": [],
            "team_id": [],
            "player_id": [],
            "player_name": [],
            # "is_starter": [],
            "starting_position": [],
            "minutes_played": [],
            "jersey_number": [],
            "position_side": [],
            "xG_non_penalty": [],
        }

        team_info = assertget(match_info, "contestant")
        home_team_info = team_info[0]
        away_team_info = team_info[1]

        home_team_id = assertget(home_team_info, "id")
        away_team_id = assertget(away_team_info, "id")

        team_home = line_up[0]
        team_away = line_up[1]
        for player in assertget(team_home, "player"):
            # players_data["game_id"] += [game_id]
            players_data["team_id"] += [home_team_id]
            players_data["player_id"] += [assertget(player, "playerId")]
            players_data["player_name"] += [assertget(player, "matchName")]

            starting_position = assertget(player, "position")
            players_data["starting_position"] += [starting_position]
            # players_data["is_starter"] += (starting_position != "Substitute")

            players_data["jersey_number"] += [assertget(player, "shirtNumber")]

            if starting_position != "Substitute":
                players_data["position_side"] += [assertget(player, "positionSide")]
            else:
                players_data["position_side"] += [""]

            xG_non_penalty = 0.0
            minutesPlayed = 0
            if "stat" in player and player["stat"] is not None:
                player_stats = assertget(player, "stat")
                for stat in player_stats:
                    stat_type = assertget(stat, "type")
                    if stat_type == "expectedGoalsNonpenalty":
                        xG_non_penalty = assertget(stat, "value")
                        break
                    elif stat_type == "minsPlayed":
                        minutesPlayed = assertget(stat, "value")
            players_data["xG_non_penalty"] += [xG_non_penalty]
            players_data["minutes_played"] += [minutesPlayed]

        for player in assertget(team_away, "player"):
            # players_data["game_id"] += game_id
            players_data["team_id"] += [away_team_id]
            players_data["player_id"] += [assertget(player, "playerId")]
            players_data["player_name"] += [assertget(player, "matchName")]

            starting_position = assertget(player, "position")
            players_data["starting_position"] += [starting_position]
            # players_data["is_starter"] += (starting_position != "Substitute")

            players_data["jersey_number"] += [assertget(player, "shirtNumber")]

            if starting_position != "Substitute":
                players_data["position_side"] += [assertget(player, "positionSide")]
            else:
                players_data["position_side"] += [""]
            xG_non_penalty = 0.0
            minutesPlayed = 0
            if "stat" in player:
                player_stats = assertget(player, "stat")
                for stat in player_stats:
                    stat_type = assertget(stat, "type")
                    if stat_type == "expectedGoalsNonpenalty":
                        xG_non_penalty = assertget(stat, "value")
                        break
                    elif stat_type == "minsPlayed":
                        minutesPlayed = assertget(stat, "value")
            players_data["xG_non_penalty"] += [xG_non_penalty]
            players_data["minutes_played"] += [minutesPlayed]

        df_players_data = pd.DataFrame.from_dict(players_data)

        players = {}
        for _, player in df_players_data.iterrows():
            is_starter = player.starting_position != "Substitute"
            players[(game_id, player.player_id)] = {
                # Fields required by the base schema
                "game_id": game_id,
                "team_id": player.team_id,
                "player_id": player.player_id,
                "player_name": player.player_name,
                "is_starter": is_starter,
                "minutes_played": int(player.minutes_played),
                "jersey_number": player.jersey_number,
                # Fields required by the opta schema
                "starting_position": player.starting_position,
                "position_side": player.position_side,
                "xG_non_penalty": player.xG_non_penalty,
            }
        return players
