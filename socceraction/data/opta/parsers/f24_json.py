"""JSON parser for Opta F24 feeds."""
from datetime import datetime
from typing import Any, Dict

from ...base import MissingDataError
from .base import OptaJSONParser, _get_end_x, _get_end_y, assertget


class F24JSONParser(OptaJSONParser):
    """Extract data from a Opta F24 data stream.

    Parameters
    ----------
    path : str
        Path of the data file.
    """

    def _get_doc(self) -> Dict[str, Any]:
        for node in self.root:
            if 'Games' in node['data'].keys():
                return node
        raise MissingDataError

    def extract_games(self) -> Dict[int, Dict[str, Any]]:
        """Return a dictionary with all available games.

        Returns
        -------
        dict
            A mapping between game IDs and the information available about
            each game in the data stream.
        """
        f24 = self._get_doc()

        data = assertget(f24, 'data')
        games = assertget(data, 'Games')
        game = assertget(games, 'Game')
        attr = assertget(game, '@attributes')

        game_id = int(assertget(attr, 'id'))
        game_dict = {
            game_id: dict(
                competition_id=int(assertget(attr, 'competition_id')),
                game_id=game_id,
                season_id=int(assertget(attr, 'season_id')),
                game_day=int(assertget(attr, 'matchday')),
                home_team_id=int(assertget(attr, 'home_team_id')),
                away_team_id=int(assertget(attr, 'away_team_id')),
            )
        }
        return game_dict

    def extract_events(self) -> Dict[int, Dict[str, Any]]:
        """Return a dictionary with all available events.

        Returns
        -------
        dict
            A mapping between event IDs and the information available about
            each event in the data stream.
        """
        f24 = self._get_doc()

        data = assertget(f24, 'data')
        games = assertget(data, 'Games')
        game = assertget(games, 'Game')
        game_attr = assertget(game, '@attributes')
        game_id = int(assertget(game_attr, 'id'))

        events = {}
        for element in assertget(game, 'Event'):
            attr = element['@attributes']
            timestamp = attr['TimeStamp'].get('locale') if attr.get('TimeStamp') else None
            timestamp = datetime.strptime(timestamp, '%Y-%m-%dT%H:%M:%S.%fZ')
            qualifiers = {
                int(q['@attributes']['qualifier_id']): q['@attributes']['value']
                for q in element.get('Q', [])
            }
            start_x = float(assertget(attr, 'x'))
            start_y = float(assertget(attr, 'y'))
            end_x = _get_end_x(qualifiers)
            end_y = _get_end_y(qualifiers)
            if end_x is None:
                end_x = start_x
            if end_y is None:
                end_y = start_y

            event_id = int(assertget(attr, 'event_id'))
            event = dict(
                game_id=game_id,
                event_id=event_id,
                type_id=int(assertget(attr, 'type_id')),
                period_id=int(assertget(attr, 'period_id')),
                minute=int(assertget(attr, 'min')),
                second=int(assertget(attr, 'sec')),
                timestamp=timestamp,
                player_id=int(assertget(attr, 'player_id')),
                team_id=int(assertget(attr, 'team_id')),
                outcome=bool(int(attr.get('outcome', 1))),
                start_x=start_x,
                start_y=start_y,
                end_x=end_x,
                end_y=end_y,
                assist=bool(int(attr.get('assist', 0))),
                keypass=bool(int(attr.get('keypass', 0))),
                qualifiers=qualifiers,
            )
            events[event_id] = event
        return events
