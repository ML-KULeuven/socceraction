"""JSON parser for Opta F1 feeds."""
from datetime import datetime
from typing import Any, Dict

from ...base import MissingDataError
from .base import OptaJSONParser, assertget


class F1JSONParser(OptaJSONParser):
    """Extract data from a Opta F1 data stream.

    Parameters
    ----------
    path : str
        Path of the data file.
    """

    def _get_feed(self) -> Dict[str, Any]:
        for node in self.root:
            if 'OptaFeed' in node['data'].keys():
                return node
        raise MissingDataError

    def _get_doc(self) -> Dict[str, Any]:
        f1 = self._get_feed()
        data = assertget(f1, 'data')
        optafeed = assertget(data, 'OptaFeed')
        optadocument = assertget(optafeed, 'OptaDocument')
        return optadocument

    def extract_competitions(self) -> Dict[int, Dict[str, Any]]:
        """Return a dictionary with all available competitions.

        Returns
        -------
        dict
            A mapping between competion IDs and the information available about
            each competition in the data stream.
        """
        optadocument = self._get_doc()
        attr = assertget(optadocument, '@attributes')
        competition_id = int(assertget(attr, 'competition_id'))
        competition = dict(
            season_id=int(assertget(attr, 'season_id')),
            season_name=str(assertget(attr, 'season_id')),
            competition_id=competition_id,
            competition_name=assertget(attr, 'competition_name'),
        )
        return {competition_id: competition}

    def extract_games(self) -> Dict[int, Dict[str, Any]]:
        """Return a dictionary with all available games.

        Returns
        -------
        dict
            A mapping between game IDs and the information available about
            each game in the data stream.
        """
        optadocument = self._get_doc()
        attr = assertget(optadocument, '@attributes')
        matchdata = assertget(optadocument, 'MatchData')
        matches = {}
        for match in matchdata:
            match_dict: Dict[str, Any] = {}
            match_dict['competition_id'] = int(assertget(attr, 'competition_id'))
            match_dict['season_id'] = int(assertget(attr, 'season_id'))
            matchattr = assertget(match, '@attributes')
            match_dict['game_id'] = int(assertget(matchattr, 'uID')[1:])
            matchinfo = assertget(match, 'MatchInfo')
            matchinfoattr = assertget(matchinfo, '@attributes')
            match_dict['game_day'] = int(assertget(matchinfoattr, 'MatchDay'))
            match_dict['venue'] = str(assertget(matchinfoattr, 'Venue_id'))
            match_dict['game_date'] = datetime.strptime(
                assertget(matchinfo, 'Date'), '%Y-%m-%d %H:%M:%S'
            )
            teamdata = assertget(match, 'TeamData')
            for team in teamdata:
                teamattr = assertget(team, '@attributes')
                side = assertget(teamattr, 'Side')
                teamid = assertget(teamattr, 'TeamRef')
                if side == 'Home':
                    match_dict['home_team_id'] = int(teamid[1:])
                else:
                    match_dict['away_team_id'] = int(teamid[1:])
            matches[match_dict['game_id']] = match_dict
        return matches
