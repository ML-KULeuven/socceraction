"""JSON parser for Stats Perform MA1 feeds."""
from typing import Any, Dict

import unidecode  # type: ignore

from ...base import MissingDataError
from .base import OptaJSONParser, assertget


class MA1JSONParser(OptaJSONParser):
    """Extract data from a Stats Perform MA1 data stream.

    Parameters
    ----------
    path : str
        Path of the data file.
    """

    def _get_match_info(self) -> Dict[str, Any]:
        if 'matchInfo' in self.root:
            return self.root['matchInfo']
        raise MissingDataError

    def _get_live_data(self) -> Dict[str, Any]:
        if 'liveData' in self.root:
            return self.root['liveData']
        raise MissingDataError

    def extract_competitions(self) -> Dict[int, Dict[str, Any]]:
        """Return a dictionary with all available competitions.

        Returns
        -------
        dict
            A mapping between competion IDs and the information available about
            each competition in the data stream.
        """
        match_info = self._get_match_info()
        tournament_calender = assertget(match_info, 'tournamentCalendar')
        competition = assertget(match_info, 'competition')
        season_id = assertget(tournament_calender, 'id')
        season = dict(
            season_id=assertget(tournament_calender, 'id'),
            season_name=assertget(tournament_calender, 'name'),
            competition_id=assertget(competition, 'id'),
            competition_name=assertget(competition, 'name'),
        )
        return {season_id: season}

    def extract_teams(self) -> Dict[int, Dict[str, Any]]:
        """Return a dictionary with all available teams.

        Returns
        -------
        dict
            A mapping between team IDs and the information available about
            each team in the data stream.
        """
        match_info = self._get_match_info()
        contestants = assertget(match_info, 'contestant')
        teams = {}
        for contestant in contestants:
            team_id = assertget(contestant, 'id')
            team = dict(
                team_id=team_id,
                team_name=assertget(contestant, 'name'),
            )
            teams[team_id] = team
        return teams

    def extract_players(self) -> Dict[int, Dict[str, Any]]:
        """Return a dictionary with all available players.

        Returns
        -------
        dict
            A mapping between player IDs and the information available about
            each player in the data stream.
        """
        live_data = self._get_live_data()
        lineups = assertget(live_data, 'lineUp')
        players = {}
        for lineup in lineups:
            team_id = assertget(lineup, 'contestantId')
            players_in_lineup = assertget(lineup, 'player')
            for individual in players_in_lineup:
                player_id = assertget(individual, 'playerId')
                player = dict(
                    team_id=team_id,
                    player_id=player_id,
                    firstname=assertget(individual, 'firstName').strip() or None,
                    lastname=assertget(individual, 'lastName').strip() or None,
                    nickname=assertget(individual, 'matchName').strip() or None,
                )
                for name_field in ['firstname', 'lastname', 'nickname']:
                    if player[name_field]:
                        player[name_field] = unidecode.unidecode(player[name_field])
                players[player_id] = player
        return players
