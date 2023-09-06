"""JSON parser for Stats Perform MA5 feeds."""
from datetime import datetime
from typing import Any, Dict, List

from ...base import MissingDataError
from .base import OptaJSONParser, assertget


class MA5JSONParser(OptaJSONParser):
    """Extract data from a Stats Perform MA5 data stream.

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

    def extract_games(self) -> Dict[str, Dict[str, Any]]:
        """Return a dictionary with all available games.

        Returns
        -------
        dict
            A mapping between game IDs and the information available about
            each game in the data stream.
        """
        match_info = self._get_match_info()
        live_data = self._get_live_data()
        game_id = assertget(match_info, "id")
        season = assertget(match_info, "tournamentCalendar")
        competition = assertget(match_info, "competition")

        contestants = assertget(match_info, "contestant")
        home_team = contestants[0]
        away_team = contestants[1]
        home_team_id = assertget(home_team, "id")
        away_team_id = assertget(away_team, "id")

        possession_data = assertget(live_data, "possession")
        possession_wave_data = assertget(possession_data, "possessionWave")
        overall_percentage = assertget(possession_wave_data[0], "overall")
        away_possession = assertget(overall_percentage, "away")
        home_possession = assertget(overall_percentage, "home")

        game_date = assertget(match_info, "date")[0:10]
        game_time = assertget(match_info, "time")[0:8]
        game_datetime = f"{game_date}T{game_time}"
        return {
            game_id: dict(
                game_id=game_id,
                season_id=assertget(season, "id"),
                competition_id=assertget(competition, "id"),
                game_day=int(match_info["week"]) if "week" in match_info else None,
                game_date=datetime.strptime(game_datetime, "%Y-%m-%dT%H:%M:%S"),
                home_team_id=home_team_id,
                away_team_id=away_team_id,
                home_possession=float(home_possession),
                away_possession=float(away_possession),
            )
        }
