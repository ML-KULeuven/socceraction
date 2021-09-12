"""XML parser for Opta F7 feeds."""
from datetime import datetime
from typing import Any, Dict, Type

from lxml import objectify

from .base import OptaXMLParser, assertget


class F7XMLParser(OptaXMLParser):
    """Extract data from a Opta F7 data stream.

    Parameters
    ----------
    path : str
        Path of the data file.
    """

    def _get_doc(self) -> Type[objectify.ObjectifiedElement]:
        optadocument = self.root.find('SoccerDocument')
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
        competition = optadocument.Competition

        stats = {}
        for stat in competition.find('Stat'):
            stats[stat.attrib['Type']] = stat.text
        competition_id = int(competition.attrib['uID'][1:])
        competition_dict = dict(
            competition_id=competition_id,
            season_id=int(assertget(stats, 'season_id')),
            season_name=assertget(stats, 'season_name'),
            competition_name=competition.Name.text,
        )
        return {competition_id: competition_dict}

    def extract_games(self) -> Dict[int, Dict[str, Any]]:
        """Return a dictionary with all available games.

        Returns
        -------
        dict
            A mapping between game IDs and the information available about
            each game in the data stream.
        """
        optadocument = self._get_doc()
        match_info = optadocument.MatchData.MatchInfo
        game_id = int(optadocument.attrib['uID'][1:])
        stats = {}
        for stat in optadocument.MatchData.find('Stat'):
            stats[stat.attrib['Type']] = stat.text

        game_dict = dict(
            game_id=game_id,
            venue=optadocument.Venue.Name.text,
            referee_id=int(optadocument.MatchData.MatchOfficial.attrib['uID'][1:]),
            game_date=datetime.strptime(match_info.Date.text, '%Y%m%dT%H%M%S%z').replace(
                tzinfo=None
            ),
            attendance=int(match_info.Attendance),
            duration=int(stats['match_time']),
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
        optadocument = self._get_doc()
        team_elms = list(optadocument.iterchildren('Team'))
        teams = {}
        for team_elm in team_elms:
            team_id = int(assertget(team_elm.attrib, 'uID')[1:])
            team = dict(
                team_id=team_id,
                team_name=team_elm.Name.text,
            )
            teams[team_id] = team
        return teams

    def extract_lineups(self) -> Dict[int, Dict[str, Any]]:
        """Return a dictionary with the lineup of each team.

        Returns
        -------
        dict
            A mapping between team IDs and the information available about
            each team's lineup in the data stream.
        """
        optadocument = self._get_doc()

        stats = {}
        for stat in optadocument.MatchData.find('Stat'):
            stats[stat.attrib['Type']] = stat.text

        lineup_elms = optadocument.MatchData.iterchildren('TeamData')
        lineups = {}
        for team_elm in lineup_elms:
            # lineup attributes
            team_id = int(team_elm.attrib['TeamRef'][1:])
            lineups[team_id] = dict(
                formation=team_elm.attrib['Formation'],
                score=int(team_elm.attrib['Score']),
                side=team_elm.attrib['Side'],
                players=dict(),
            )
            # substitutes
            subst_elms = team_elm.iterchildren('Substitution')
            subst = [subst_elm.attrib for subst_elm in subst_elms]
            # players
            player_elms = team_elm.PlayerLineUp.iterchildren('MatchPlayer')
            for player_elm in player_elms:
                player_id = int(player_elm.attrib['PlayerRef'][1:])
                sub_on = int(
                    next(
                        (
                            item['Time']
                            for item in subst
                            if 'Retired' not in item and item['SubOn'] == f'p{player_id}'
                        ),
                        stats['match_time'] if player_elm.attrib['Status'] == 'Sub' else 0,
                    )
                )
                sub_off = int(
                    next(
                        (item['Time'] for item in subst if item['SubOff'] == f'p{player_id}'),
                        stats['match_time'],
                    )
                )
                minutes_played = sub_off - sub_on
                lineups[team_id]['players'][player_id] = dict(
                    starting_position_id=int(player_elm.attrib['Formation_Place']),
                    starting_position_name=player_elm.attrib['Position'],
                    jersey_number=int(player_elm.attrib['ShirtNumber']),
                    is_starter=int(player_elm.attrib['Formation_Place']) != 0,
                    minutes_played=minutes_played,
                )
        return lineups

    def extract_players(self) -> Dict[int, Dict[str, Any]]:
        """Return a dictionary with all available players.

        Returns
        -------
        dict
            A mapping between player IDs and the information available about
            each player in the data stream.
        """
        optadocument = self._get_doc()
        lineups = self.extract_lineups()
        team_elms = list(optadocument.iterchildren('Team'))
        players = {}
        for team_elm in team_elms:
            team_id = int(team_elm.attrib['uID'][1:])
            for player_elm in team_elm.iterchildren('Player'):
                player_id = int(player_elm.attrib['uID'][1:])
                firstname = str(player_elm.find('PersonName').find('First'))
                lastname = str(player_elm.find('PersonName').find('Last'))
                nickname = str(player_elm.find('PersonName').find('Known'))
                player = dict(
                    team_id=team_id,
                    player_id=player_id,
                    player_name=' '.join([firstname, lastname]),
                    firstname=firstname,
                    lastname=lastname,
                    nickname=nickname,
                    **lineups[team_id]['players'][player_id],
                )
                players[player_id] = player

        return players
