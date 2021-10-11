"""JSON parser for Opta F9 feeds."""
from datetime import datetime
from typing import Any, Dict, List

import unidecode  # type: ignore

from ...base import MissingDataError
from .base import OptaJSONParser, assertget


class F9JSONParser(OptaJSONParser):
    """Extract data from a Opta F9 data stream.

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
        f9 = self._get_feed()
        data = assertget(f9, 'data')
        optafeed = assertget(data, 'OptaFeed')
        optadocument = assertget(optafeed, 'OptaDocument')[0]
        return optadocument

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
        venue = assertget(optadocument, 'Venue')
        matchdata = assertget(optadocument, 'MatchData')
        matchofficial = assertget(matchdata, 'MatchOfficial')
        matchinfo = assertget(matchdata, 'MatchInfo')
        stat = assertget(matchdata, 'Stat')
        assert stat['@attributes']['Type'] == 'match_time'
        teamdata = assertget(matchdata, 'TeamData')
        scores = {}
        for t in teamdata:
            scores[t['@attributes']['Side']] = t['@attributes']['Score']

        game_id = int(assertget(attr, 'uID')[1:])
        game_dict = {
            game_id: dict(
                game_id=game_id,
                venue=str(
                    venue['@attributes']['uID']
                ),  # The venue's name is not included in this stream
                referee_id=int(matchofficial['@attributes']['uID'].replace('o', '')),
                game_date=datetime.strptime(
                    assertget(matchinfo, 'Date'), '%Y%m%dT%H%M%S%z'
                ).replace(tzinfo=None),
                attendance=int(matchinfo.get('Attendance', 0)),
                duration=int(stat['@value']),
                home_score=int(scores['Home']),
                away_score=int(scores['Away']),
            )
        }
        return game_dict

    def extract_teams(self) -> Dict[int, Dict[str, Any]]:
        """Return a dictionary with all available teams.

        Returns
        -------
        dict
            A mapping between team IDs and the information available about
            each team in the data stream.
        """
        optadocument = self._get_doc()
        root_teams = assertget(optadocument, 'Team')

        teams = {}
        for team in root_teams:
            if 'id' in team.keys():
                nameobj = team.get('nameObj')
                team_id = int(team['id'])
                team = dict(
                    team_id=team_id,
                    team_name=nameobj.get('name'),
                )
                for f in ['team_name']:
                    team[f] = unidecode.unidecode(team[f]) if f in team else team[f]
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
        optadocument = self._get_doc()
        root_teams = assertget(optadocument, 'Team')
        lineups = self.extract_lineups()

        players = {}
        for team in root_teams:
            team_id = int(team['@attributes']['uID'].replace('t', ''))
            for player in team['Player']:
                player_id = int(player['@attributes']['uID'].replace('p', ''))

                assert 'nameObj' in player['PersonName']
                nameobj = player['PersonName']['nameObj']
                if not nameobj.get('is_unknown'):
                    player = dict(
                        team_id=team_id,
                        player_id=player_id,
                        firstname=nameobj.get('first').strip() or None,
                        lastname=nameobj.get('last').strip() or None,
                        player_name=nameobj.get('full').strip() or None,
                        nickname=nameobj.get('known') or nameobj.get('full').strip() or None,
                    )
                    if player_id in lineups[team_id]['players']:
                        player = dict(
                            **player,
                            jersey_number=lineups[team_id]['players'][player_id]['jersey_number'],
                            starting_position_name=lineups[team_id]['players'][player_id][
                                'starting_position_name'
                            ],
                            starting_position_id=lineups[team_id]['players'][player_id][
                                'starting_position_id'
                            ],
                            is_starter=lineups[team_id]['players'][player_id]['is_starter'],
                            minutes_played=lineups[team_id]['players'][player_id][
                                'minutes_played'
                            ],
                        )
                    for f in ['firstname', 'lastname', 'player_name', 'nickname']:
                        if player[f]:
                            player[f] = unidecode.unidecode(player[f])
                    players[player_id] = player
        return players

    def extract_lineups(self) -> Dict[int, Dict[str, Any]]:
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
        attr = assertget(optadocument, '@attributes')

        try:
            rootf9 = optadocument['MatchData']['TeamData']
        except KeyError as e:
            raise MissingDataError from e
        matchstats = optadocument['MatchData']['Stat']
        matchstats = [matchstats] if isinstance(matchstats, dict) else matchstats
        matchstatsdict = {stat['@attributes']['Type']: stat['@value'] for stat in matchstats}

        lineups: Dict[int, Dict[str, Any]] = {}
        for team in rootf9:
            # lineup attributes
            team_id = int(team['@attributes']['TeamRef'].replace('t', ''))
            lineups[team_id] = dict(players=dict())
            # substitutes
            subst = [s['@attributes'] for s in team['Substitution']]
            for player in team['PlayerLineUp']['MatchPlayer']:
                attr = player['@attributes']
                player_id = int(attr['PlayerRef'].replace('p', ''))
                playerstatsdict = {
                    stat['@attributes']['Type']: stat['@value'] for stat in player['Stat']
                }
                sub_on = next(
                    (
                        item['Time']
                        for item in subst
                        if 'Retired' not in item and item['SubOn'] == f'p{player_id}'
                    ),
                    matchstatsdict['match_time'] if attr['Status'] == 'Sub' else 0,
                )
                sub_off = next(
                    (item['Time'] for item in subst if item['SubOff'] == f'p{player_id}'),
                    matchstatsdict['match_time'],
                )
                minutes_played = sub_off - sub_on
                lineups[team_id]['players'][player_id] = dict(
                    jersey_number=attr['ShirtNumber'],
                    starting_position_name=attr['Position'],
                    starting_position_id=attr['position_id'],
                    is_starter=attr['Status'] == 'Start',
                    minutes_played=minutes_played,
                    **playerstatsdict,
                )
        return lineups

    def extract_referee(self) -> Dict[int, Dict[str, Any]]:
        """Return a dictionary with all referees.

        Raises
        ------
        MissingDataError
            If referee data is not available in the stream.

        Returns
        -------
        dict
            A mapping between referee IDs and the information available about
            each referee in the data stream.
        """
        optadocument = self._get_doc()

        try:
            rootf9 = optadocument['MatchData']['MatchOfficial']
        except KeyError as e:
            raise MissingDataError from e

        name = rootf9['OfficialName']
        nameobj = name['nameObj']
        referee_id = int(rootf9['@attributes']['uID'].replace('o', ''))
        referee = dict(
            referee_id=referee_id,
            referee_firstname=name.get('First') or nameobj.get('first'),
            referee_lastname=name.get('Last') or nameobj.get('last'),
        )
        for f in ['referee_firstname', 'referee_lastname']:
            if referee[f]:
                referee[f] = unidecode.unidecode(referee[f])
        return {referee_id: referee}

    def extract_teamgamestats(self) -> List[Dict[str, Any]]:
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
        attr = assertget(optadocument, '@attributes')
        game_id = int(assertget(attr, 'uID')[1:])

        try:
            rootf9 = optadocument['MatchData']['TeamData']
        except KeyError as e:
            raise MissingDataError from e
        teams_gamestats = []
        for team in rootf9:
            attr = team['@attributes']
            statsdict = {stat['@attributes']['Type']: stat['@value'] for stat in team['Stat']}

            team_gamestats = dict(
                game_id=game_id,
                team_id=int(attr['TeamRef'].replace('t', '')),
                side=attr['Side'],
                score=attr['Score'],
                shootout_score=attr['ShootOutScore'],
                **statsdict,
            )

            teams_gamestats.append(team_gamestats)
        return teams_gamestats
