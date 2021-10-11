"""JSON parser for WhoScored feeds."""
import json  # type: ignore
import re
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

import unidecode  # type: ignore

from ...base import MissingDataError
from .base import OptaParser, _get_end_x, _get_end_y, assertget


class WhoScoredParser(OptaParser):
    """Extract data from a JSON data stream scraped from WhoScored.

    Parameters
    ----------
    path : str
        Path of the data file.
    competition_id : int
        ID of the competition to which the provided data file belongs. If
        None, this information is extracted from a field 'competition_id' in
        the JSON.
    season_id : int
        ID of the season to which the provided data file belongs. If None,
        this information is extracted from a field 'season_id' in the JSON.
    game_id : int
        ID of the game to which the provided data file belongs. If None, this
        information is extracted from a field 'game_id' in the JSON.
    """

    def __init__(  # noqa: C901
        self,
        path: str,
        competition_id: Optional[int] = None,
        season_id: Optional[int] = None,
        game_id: Optional[int] = None,
    ) -> None:
        with open(path, 'rt', encoding='utf-8') as fh:
            self.root = json.load(fh)
            self.position_mapping = lambda formation, x, y: 'Unknown'

        if competition_id is None:
            try:
                competition_id = int(assertget(self.root, 'competition_id'))
            except AssertionError as e:
                raise MissingDataError(
                    """Could not determine the competition id. Add it to the
                    file path or include a field 'competition_id' in the
                    JSON."""
                ) from e
        self.competition_id = competition_id

        if season_id is None:
            try:
                season_id = int(assertget(self.root, 'season_id'))
            except AssertionError as e:
                raise MissingDataError(
                    """Could not determine the season id. Add it to the file
                    path or include a field 'season_id' in the JSON."""
                ) from e
        self.season_id = season_id

        if game_id is None:
            try:
                game_id = int(assertget(self.root, 'game_id'))
            except AssertionError as e:
                raise MissingDataError(
                    """Could not determine the game id. Add it to the file
                    path or include a field 'game_id' in the JSON."""
                ) from e
        self.game_id = game_id

    def _get_period_id(self, event: Dict[str, Any]) -> int:
        period = assertget(event, 'period')
        period_id = int(assertget(period, 'value'))
        return period_id

    def _get_period_milliseconds(self, event: Dict[str, Any]) -> int:
        period_minute_limits = assertget(self.root, 'periodMinuteLimits')
        period_id = self._get_period_id(event)
        if period_id == 16:  # Pre-match
            return 0
        if period_id == 14:  # Post-game
            return 0
        minute = int(assertget(event, 'minute'))
        period_minute = minute
        if period_id > 1:
            period_minute = minute - period_minute_limits[str(period_id - 1)]
        period_second = period_minute * 60 + int(event.get('second', 0))
        return period_second * 1000

    def extract_games(self) -> Dict[int, Dict[str, Any]]:
        """Return a dictionary with all available games.

        Returns
        -------
        dict
            A mapping between game IDs and the information available about
            each game in the data stream.
        """
        team_home = assertget(self.root, 'home')
        team_away = assertget(self.root, 'away')
        game_id = self.game_id
        game_dict = dict(
            game_id=game_id,
            season_id=self.season_id,
            competition_id=self.competition_id,
            game_day=0,  # TODO: not defined in the JSON object
            game_date=datetime.strptime(
                assertget(self.root, 'startTime'), '%Y-%m-%dT%H:%M:%S'
            ),  # Dates are UTC
            home_team_id=int(assertget(team_home, 'teamId')),
            away_team_id=int(assertget(team_away, 'teamId')),
            # is_regular=None, # TODO
            # is_extra_time=None, # TODO
            # is_penalties=None, # TODO
            # is_golden_goal=None, # TODO
            # is_silver_goal=None, # TODO
            # Optional fields
            home_score=int(assertget(assertget(self.root['home'], 'scores'), 'fulltime')),
            away_score=int(assertget(assertget(self.root['away'], 'scores'), 'fulltime')),
            attendance=int(self.root.get('attendance', 0)),
            venue=str(self.root.get('venueName')),
            referee_id=int(self.root.get('referee', {}).get('officialId', 0)),
            duration=int(self.root.get('expandedMaxMinute')),
        )
        return {game_id: game_dict}

    def extract_teams(self) -> Dict[int, Dict[str, Any]]:
        """Return a dictionary with all available teams.

        Returns
        -------
        dict
            A mapping between team IDs and the information available about
            each team in the data stream.
        """
        teams = {}
        for t in [self.root['home'], self.root['away']]:
            team_id = int(assertget(t, 'teamId'))
            team = dict(
                team_id=team_id,
                team_name=assertget(t, 'name'),
            )
            for f in ['team_name']:
                if team[f]:
                    team[f] = unidecode.unidecode(team[f])

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
        player_gamestats = self.extract_playergamestats()
        game_id = self.game_id
        players = {}
        for team in [self.root['home'], self.root['away']]:
            team_id = int(assertget(team, 'teamId'))
            for p in team['players']:
                player_id = int(assertget(p, 'playerId'))
                player = dict(
                    game_id=game_id,
                    team_id=team_id,
                    player_id=int(assertget(p, 'playerId')),
                    is_starter=bool(p.get('isFirstEleven', False)),
                    player_name=str(assertget(p, 'name')),
                    age=int(p['age']),
                    # nation_code=None,
                    # line_code=str(assertget(p, "position")),
                    # preferred_foot=None,
                    # gender=None,
                    height=float(p.get('height', float('NaN'))),
                    weight=float(p.get('weight', float('NaN'))),
                    minutes_played=player_gamestats[player_id]['minutes_played'],
                    jersey_number=player_gamestats[player_id]['jersey_number'],
                    starting_position_id=0,  # TODO
                    starting_position_name=player_gamestats[player_id]['position_code'],
                )
                for f in ['player_name']:
                    if player[f]:
                        player[f] = unidecode.unidecode(player[f])
                players[player_id] = player
        return players

    def extract_events(self) -> Dict[int, Dict[str, Any]]:
        """Return a dictionary with all available events.

        Returns
        -------
        dict
            A mapping between event IDs and the information available about
            each event in the data stream.
        """
        events = {}

        game_id = self.game_id
        time_start_str = str(assertget(self.root, 'startTime'))
        time_start = datetime.strptime(time_start_str, '%Y-%m-%dT%H:%M:%S')
        for attr in self.root['events']:
            qualifiers = {}
            qualifiers = {
                int(q['type']['value']): q.get('value', True) for q in attr.get('qualifiers', [])
            }
            start_x = float(assertget(attr, 'x'))
            start_y = float(assertget(attr, 'y'))
            end_x = _get_end_x(qualifiers)
            end_y = _get_end_y(qualifiers)
            if end_x is None:
                end_x = start_x
            if end_y is None:
                end_y = start_y
            eventtype = attr.get('type', {})
            period = attr.get('period', {})
            outcome = attr.get('outcomeType', {'value': 1})
            eventIdKey = 'eventId'
            if 'id' in attr:
                eventIdKey = 'id'
            minute = int(assertget(attr, 'expandedMinute'))
            second = int(attr.get('second', 0))
            event_id = int(assertget(attr, eventIdKey))
            event = dict(
                game_id=game_id,
                event_id=event_id,
                type_id=int(assertget(eventtype, 'value')),
                period_id=int(assertget(period, 'value')),
                minute=minute,
                second=second,
                timestamp=(time_start + timedelta(seconds=(minute * 60 + second))),
                player_id=int(attr.get('playerId', 0)),
                team_id=int(assertget(attr, 'teamId')),
                outcome=bool(int(outcome.get('value', 1))),
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

    def extract_substitutions(self) -> Dict[int, Dict[str, Any]]:
        """Return a dictionary with all substitution events.

        Returns
        -------
        dict
            A mapping between player IDs and the information available about
            each substitution in the data stream.
        """
        game_id = self.game_id

        subs = {}
        subonevents = [e for e in self.root['events'] if e['type'].get('value') == 19]
        for e in subonevents:
            sub_id = int(assertget(e, 'playerId'))
            sub = dict(
                game_id=game_id,
                team_id=int(assertget(e, 'teamId')),
                period_id=int(assertget(assertget(e, 'period'), 'value')),
                period_milliseconds=self._get_period_milliseconds(e),
                player_in_id=int(assertget(e, 'playerId')),
                player_out_id=int(assertget(e, 'relatedPlayerId')),
            )
            subs[sub_id] = sub
        return subs

    def extract_positions(self) -> Dict[int, Dict[str, Any]]:  # noqa: C901
        """Return a dictionary with each player's position during a game.

        Returns
        -------
        dict
            A mapping between team IDs and the information available about
            each player's position in the data stream.
        """
        game_id = self.game_id

        positions = {}
        for t in [self.root['home'], self.root['away']]:
            team_id = int(assertget(t, 'teamId'))
            for f in assertget(t, 'formations'):
                fpositions = assertget(f, 'formationPositions')
                playersIds = assertget(f, 'playerIds')
                formation = assertget(f, 'formationName')

                period_end_minutes = assertget(self.root, 'periodEndMinutes')
                period_minute_limits = assertget(self.root, 'periodMinuteLimits')
                start_minute = int(assertget(f, 'startMinuteExpanded'))
                end_minute = int(assertget(f, 'endMinuteExpanded'))
                for period_id in sorted(period_end_minutes.keys()):
                    if period_end_minutes[period_id] > start_minute:
                        break
                period_id = int(period_id)
                period_minute = start_minute
                if period_id > 1:
                    period_minute = start_minute - period_minute_limits[str(period_id - 1)]

                for i, p in enumerate(fpositions):
                    x = float(assertget(p, 'vertical'))
                    y = float(assertget(p, 'horizontal'))
                    try:
                        position_code = self.position_mapping(formation, x, y)
                    except KeyError:
                        position_code = 'Unknown'
                    pos = dict(
                        game_id=game_id,
                        team_id=team_id,
                        period_id=period_id,
                        period_milliseconds=(period_minute * 60 * 1000),
                        start_milliseconds=(start_minute * 60 * 1000),
                        end_milliseconds=(end_minute * 60 * 1000),
                        formation_scheme=formation,
                        player_id=int(playersIds[i]),
                        player_position=position_code,
                        player_position_x=x,
                        player_position_y=y,
                    )
                    positions[team_id] = pos
        return positions

    def extract_referee(self) -> Dict[int, Dict[str, Any]]:
        """Return a dictionary with all referees.

        Returns
        -------
        dict
            A mapping between referee IDs and the information available about
            each referee in the data stream.
        """
        if 'referee' not in self.root:
            return {
                0: dict(referee_id=0, first_name='Unkown', last_name='Unkown', short_name='Unkown')
            }

        r = self.root['referee']
        referee_id = int(assertget(r, 'officialId'))
        referee = dict(
            referee_id=referee_id,
            first_name=r.get('firstName'),
            last_name=r.get('lastName'),
            short_name=r.get('name'),
        )
        for f in ['first_name', 'last_name', 'short_name']:
            if referee[f]:
                referee[f] = unidecode.unidecode(referee[f])
        return {referee_id: referee}

    def extract_teamgamestats(self) -> List[Dict[str, Any]]:
        """Return some aggregated statistics of each team.

        Returns
        -------
        list(dict)
            A dictionary with aggregated team statistics for each team.
        """
        game_id = self.game_id

        teams_gamestats = []
        teams = [self.root['home'], self.root['away']]
        for team in teams:
            statsdict = {}
            for name in team['stats']:
                if isinstance(team['stats'][name], dict):
                    statsdict[_camel_to_snake(name)] = sum(team['stats'][name].values())

            scores = assertget(team, 'scores')
            team_gamestats = dict(
                game_id=game_id,
                team_id=int(assertget(team, 'teamId')),
                side=assertget(team, 'field'),
                score=assertget(scores, 'fulltime'),
                shootout_score=scores.get('penalty', 0),
                **{k: statsdict[k] for k in statsdict if not k.endswith('Success')},
            )

            teams_gamestats.append(team_gamestats)
        return teams_gamestats

    def extract_playergamestats(self) -> Dict[int, Dict[str, Any]]:  # noqa: C901
        """Return some aggregated statistics of each player.

        Returns
        -------
        dict(dict)
            A dictionary with aggregated team statistics for each player.
        """
        game_id = self.game_id

        players_gamestats = {}
        for team in [self.root['home'], self.root['away']]:
            team_id = int(assertget(team, 'teamId'))
            for player in team['players']:
                statsdict = {
                    _camel_to_snake(name): sum(stat.values())
                    for name, stat in player['stats'].items()
                }
                stats = [k for k in statsdict if not k.endswith('Success')]

                player_id = int(assertget(player, 'playerId'))
                p = dict(
                    game_id=game_id,
                    team_id=team_id,
                    player_id=player_id,
                    is_starter=bool(player.get('isFirstEleven', False)),
                    position_code=player.get('position', None),
                    # optional fields
                    jersey_number=int(player.get('shirtNo', 0)),
                    mvp=bool(player.get('isManOfTheMatch', False)),
                    **{k: statsdict[k] for k in stats},
                )
                if 'subbedInExpandedMinute' in player:
                    p['minute_start'] = player['subbedInExpandedMinute']
                if 'subbedOutExpandedMinute' in player:
                    p['minute_end'] = player['subbedOutExpandedMinute']

                # Did not play
                p['minutes_played'] = 0
                # Played the full game
                if p['is_starter'] and 'minute_end' not in p:
                    p['minute_start'] = 0
                    p['minute_end'] = self.root['expandedMaxMinute']
                    p['minutes_played'] = self.root['expandedMaxMinute']
                # Started but substituted out
                elif p['is_starter'] and 'minute_end' in p:
                    p['minute_start'] = 0
                    p['minutes_played'] = p['minute_end']
                # Substitud in and played the remainder of the game
                elif 'minute_start' in p and 'minute_end' not in p:
                    p['minute_end'] = self.root['expandedMaxMinute']
                    p['minutes_played'] = self.root['expandedMaxMinute'] - p['minute_start']
                # Substitud in and out
                elif 'minute_start' in p and 'minute_end' in p:
                    p['minutes_played'] = p['minute_end'] - p['minute_start']

                players_gamestats[player_id] = p
        return players_gamestats


def _camel_to_snake(name: str) -> str:
    s1 = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', name)
    return re.sub('([a-z0-9])([A-Z])', r'\1_\2', s1).lower()
