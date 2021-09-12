"""XML parser for Opta F24 feeds."""
from datetime import datetime
from typing import Any, Dict, Type

from lxml import objectify

from .base import OptaXMLParser, _get_end_x, _get_end_y, assertget


class F24XMLParser(OptaXMLParser):
    """Extract data from a Opta F24 data stream.

    Parameters
    ----------
    path : str
        Path of the data file.
    """

    def _get_doc(self) -> Type[objectify.ObjectifiedElement]:
        return self.root

    def extract_games(self) -> Dict[int, Dict[str, Any]]:
        """Return a dictionary with all available games.

        Returns
        -------
        dict
            A mapping between game IDs and the information available about
            each game in the data stream.
        """
        optadocument = self._get_doc()
        game_elem = optadocument.find('Game')
        attr = game_elem.attrib
        game_id = int(assertget(attr, 'id'))
        game_dict = dict(
            game_id=game_id,
            competition_id=int(assertget(attr, 'competition_id')),
            season_id=int(assertget(attr, 'season_id')),
            game_day=int(assertget(attr, 'matchday')),
            game_date=datetime.strptime(assertget(attr, 'game_date'), '%Y-%m-%dT%H:%M:%S'),
            home_team_id=int(assertget(attr, 'home_team_id')),
            home_score=int(assertget(attr, 'home_score')),
            away_team_id=int(assertget(attr, 'away_team_id')),
            away_score=int(assertget(attr, 'away_score')),
        )
        return {game_id: game_dict}

    def extract_events(self) -> Dict[int, Dict[str, Any]]:
        """Return a dictionary with all available events.

        Returns
        -------
        dict
            A mapping between event IDs and the information available about
            each event in the data stream.
        """
        optadocument = self._get_doc()
        game_elm = optadocument.find('Game')
        attr = game_elm.attrib
        game_id = int(assertget(attr, 'id'))
        events = {}
        for event_elm in game_elm.iterchildren('Event'):
            attr = dict(event_elm.attrib)
            event_id = int(attr['id'])

            qualifiers = {
                int(qualifier_elm.attrib['qualifier_id']): qualifier_elm.attrib.get('value')
                for qualifier_elm in event_elm.iterchildren('Q')
            }
            start_x = float(assertget(attr, 'x'))
            start_y = float(assertget(attr, 'y'))
            end_x = _get_end_x(qualifiers)
            end_y = _get_end_y(qualifiers)
            if end_x is None:
                end_x = start_x
            if end_y is None:
                end_y = start_y

            event = dict(
                game_id=game_id,
                event_id=event_id,
                type_id=int(assertget(attr, 'type_id')),
                period_id=int(assertget(attr, 'period_id')),
                minute=int(assertget(attr, 'min')),
                second=int(assertget(attr, 'sec')),
                timestamp=datetime.strptime(assertget(attr, 'timestamp'), '%Y-%m-%dT%H:%M:%S.%f'),
                player_id=int(attr.get('player_id', 0)),
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
