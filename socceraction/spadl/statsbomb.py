# -*- coding: utf-8 -*-
"""StatsBomb event stream data to SPADL converter."""
import os
from typing import Any, Dict, List, Tuple

import pandas as pd  # type: ignore
import pandera as pa
from pandera.typing import DataFrame, DateTime, Series

from . import config as spadlconfig
from .base import (
    CompetitionSchema,
    EventDataLoader,
    EventSchema,
    GameSchema,
    ParseError,
    PlayerSchema,
    TeamSchema,
    _add_dribbles,
    _fix_clearances,
    _fix_direction_of_play,
)

__all__ = [
    'StatsBombLoader',
    'convert_to_actions',
    'StatsBombCompetitionSchema',
    'StatsBombGameSchema',
    'StatsBombPlayerSchema',
    'StatsBombTeamSchema',
    'StatsBombEventSchema',
]


class StatsBombCompetitionSchema(CompetitionSchema):
    """Definition of a dataframe containing a list of competitions and seasons."""

    country_name: Series[str]
    competition_gender: Series[str]


class StatsBombGameSchema(GameSchema):
    """Definition of a dataframe containing a list of games."""

    competition_stage: Series[str]
    home_score: Series[int]
    away_score: Series[int]
    venue: Series[str] = pa.Field(nullable=True)
    referee_id: Series[int] = pa.Field(nullable=True)


class StatsBombPlayerSchema(PlayerSchema):
    """Definition of a dataframe containing the list of teams of a game."""

    nickname: Series[str] = pa.Field(nullable=True)
    starting_position_id: Series[int]
    starting_position_name: Series[str]


class StatsBombTeamSchema(TeamSchema):
    """Definition of a dataframe containing the list of players of a game."""


class StatsBombEventSchema(EventSchema):
    """Definition of a dataframe containing event stream data of a game."""

    event_id: Series[object]
    index: Series[int]
    timestamp: Series[DateTime]
    minute: Series[int]
    second: Series[int] = pa.Field(ge=0, le=59)
    possession: Series[int]
    possession_team_id: Series[int]
    possession_team_name: Series[str]
    play_pattern_id: Series[int]
    play_pattern_name: Series[str]
    team_name: Series[str]
    duration: Series[float] = pa.Field(nullable=True)
    extra: Series[object]
    related_events: Series[object]
    player_name: Series[str] = pa.Field(nullable=True)
    position_id: Series[int] = pa.Field(nullable=True)
    position_name: Series[str] = pa.Field(nullable=True)
    location: Series[object] = pa.Field(nullable=True)
    under_pressure: Series[bool] = pa.Field(nullable=True)
    counterpress: Series[bool] = pa.Field(nullable=True)


class StatsBombLoader(EventDataLoader):
    """Load Statsbomb data either from a remote location or from a local folder.

    This is a temporary class until `statsbombpy <https://github.com/statsbomb/statsbombpy>`__
    becomes compatible with socceraction

    Parameters
    ----------
    root : str
        Root-path of the data. Defaults to the open source data at
        "https://raw.githubusercontent.com/statsbomb/open-data/master/data/"
    getter : str
        "remote" or "local"

    """

    _free_open_data: str = 'https://raw.githubusercontent.com/statsbomb/open-data/master/data/'

    def __init__(self, root: str = _free_open_data, getter: str = 'remote'):
        super().__init__(root, getter)

    def competitions(self) -> DataFrame[StatsBombCompetitionSchema]:
        """Return a dataframe with all available competitions and seasons.

        Returns
        -------
        pd.DataFrame
            A dataframe containing all available competitions and seasons. See
            :class:`~socceraction.spadl.statsbomb.StatsBombCompetitionSchema` for the schema.
        """
        path = os.path.join(self.root, 'competitions.json')
        obj = self.get(path)
        if not isinstance(obj, list):
            raise ParseError('{} should contain a list of competitions'.format(path))
        return pd.DataFrame(obj)[
            [
                'season_id',
                'competition_id',
                'competition_name',
                'country_name',
                'competition_gender',
                'season_name',
            ]
        ]

    def games(self, competition_id: int, season_id: int) -> DataFrame[StatsBombGameSchema]:
        """Return a dataframe with all available games in a season.

        Parameters
        ----------
        competition_id : int
            The ID of the competition.
        season_id : int
            The ID of the season.

        Returns
        -------
        pd.DataFrame
            A dataframe containing all available games. See
            :class:`~socceraction.spadl.statsbomb.StatsBombGameSchema` for the schema.
        """
        cols = [
            'game_id',
            'season_id',
            'competition_id',
            'competition_stage',
            'game_day',
            'game_date',
            'home_team_id',
            'away_team_id',
            'home_score',
            'away_score',
            'venue',
            'referee_id',
        ]
        path = os.path.join(self.root, f'matches/{competition_id}/{season_id}.json')
        obj = self.get(path)
        if not isinstance(obj, list):
            raise ParseError('{} should contain a list of games'.format(path))
        if not len(obj):
            return pd.DataFrame(columns=cols)
        gamesdf = pd.DataFrame(_flatten(m) for m in obj)
        gamesdf['kick_off'] = gamesdf['kick_off'].fillna('12:00:00.000')
        gamesdf['match_date'] = pd.to_datetime(
            gamesdf[['match_date', 'kick_off']].agg(' '.join, axis=1)
        )
        gamesdf.rename(
            columns={
                'match_id': 'game_id',
                'match_date': 'game_date',
                'match_week': 'game_day',
                'stadium_name': 'venue',
                'competition_stage_name': 'competition_stage',
            },
            inplace=True,
        )
        if 'venue' not in gamesdf:
            gamesdf['venue'] = None
        if 'referee_id' not in gamesdf:
            gamesdf['referee_id'] = None
        return gamesdf[cols]

    def _lineups(self, game_id: int) -> List[Dict[str, Any]]:
        path = os.path.join(self.root, f'lineups/{game_id}.json')
        obj = self.get(path)
        if not isinstance(obj, list):
            raise ParseError('{} should contain a list of teams'.format(path))
        return obj

    def teams(self, game_id: int) -> DataFrame[StatsBombTeamSchema]:
        """Return a dataframe with both teams that participated in a game.

        Parameters
        ----------
        game_id : int
            The ID of the game.

        Returns
        -------
        pd.DataFrame
            A dataframe containing both teams. See
            :class:`~socceraction.spadl.statsbomb.StatsBombTeamSchema` for the schema.
        """
        return pd.DataFrame(self._lineups(game_id))[['team_id', 'team_name']]

    def players(self, game_id: int) -> DataFrame[StatsBombPlayerSchema]:
        """Return a dataframe with all players that participated in a game.

        Parameters
        ----------
        game_id : int
            The ID of the game.

        Returns
        -------
        pd.DataFrame
            A dataframe containing all players. See
            :class:`~socceraction.spadl.statsbomb.StatsBombPlayerSchema` for the schema.
        """
        playersdf = pd.DataFrame(
            _flatten_id(p) for lineup in self._lineups(game_id) for p in lineup['lineup']
        )
        playergamesdf = extract_player_games(self.events(game_id))
        playersdf = pd.merge(
            playersdf,
            playergamesdf[
                ['player_id', 'team_id', 'position_id', 'position_name', 'minutes_played']
            ],
            on='player_id',
        )
        playersdf['game_id'] = game_id
        playersdf['position_name'] = playersdf['position_name'].replace(0, 'Substitute')
        playersdf['is_starter'] = playersdf['position_id'] != 0
        playersdf.rename(
            columns={
                'player_nickname': 'nickname',
                'country_name': 'country',
                'position_id': 'starting_position_id',
                'position_name': 'starting_position_name',
            },
            inplace=True,
        )
        return playersdf[
            [
                'game_id',
                'team_id',
                'player_id',
                'player_name',
                'nickname',
                'jersey_number',
                'is_starter',
                'starting_position_id',
                'starting_position_name',
                'minutes_played',
            ]
        ]

    def events(self, game_id: int) -> DataFrame[StatsBombEventSchema]:
        """Return a dataframe with the event stream of a game.

        Parameters
        ----------
        game_id : int
            The ID of the game.

        Returns
        -------
        pd.DataFrame
            A dataframe containing the event stream. See
            :class:`~socceraction.spadl.statsbomb.StatsBombEventSchema` for the schema.
        """
        path = os.path.join(self.root, f'events/{game_id}.json')
        obj = self.get(path)
        if not isinstance(obj, list):
            raise ParseError('{} should contain a list of events'.format(path))
        eventsdf = pd.DataFrame(_flatten_id(e) for e in obj)
        eventsdf['game_id'] = game_id
        eventsdf['timestamp'] = pd.to_datetime(eventsdf['timestamp'], format='%H:%M:%S.%f')
        eventsdf['related_events'] = eventsdf['related_events'].apply(
            lambda d: d if isinstance(d, list) else []
        )
        eventsdf['under_pressure'] = eventsdf['under_pressure'].fillna(False).astype(bool)
        eventsdf['counterpress'] = eventsdf['counterpress'].fillna(False).astype(bool)
        eventsdf.rename(
            columns={
                'id': 'event_id',
                'period': 'period_id',
            },
            inplace=True,
        )
        return eventsdf


def _flatten_id(d: Dict[str, Dict[str, Any]]) -> Dict[str, Any]:
    newd = {}
    extra = {}
    for k, v in d.items():
        if isinstance(v, dict):
            if 'id' in v and 'name' in v:
                newd[k + '_id'] = v['id']
                newd[k + '_name'] = v['name']
            else:
                extra[k] = v
        else:
            newd[k] = v
    newd['extra'] = extra
    return newd


def _flatten(d: Dict[str, Dict[str, Any]]) -> Dict[str, Any]:
    newd = {}
    for k, v in d.items():
        if isinstance(v, dict):
            if 'id' in v and 'name' in v:
                newd[k + '_id'] = v['id']
                newd[k + '_name'] = v['name']
                newd[k + '_extra'] = {l: w for (l, w) in v.items() if l in ('id', 'name')}
            else:
                newd = {**newd, **_flatten(v)}
        else:
            newd[k] = v
    return newd


def extract_player_games(events: pd.DataFrame) -> pd.DataFrame:
    """
    Extract player games [player_id, game_id, minutes_played] from statsbomb match events.

    Parameters
    ----------
    events : pd.DataFrame
        DataFrame containing StatsBomb events of a single game.

    Returns
    -------
    player_games : pd.DataFrame
        A DataFrame with the number of minutes played by each player during the game.
    """
    game_minutes = max(events[events.type_name == 'Half End'].minute)

    game_id = events.game_id.mode().values[0]
    players = {}
    for startxi in events[events.type_name == 'Starting XI'].itertuples():
        team_id, team_name = startxi.team_id, startxi.team_name
        for player in startxi.extra['tactics']['lineup']:
            player = _flatten_id(player)
            player = {
                **player,
                **{
                    'game_id': game_id,
                    'team_id': team_id,
                    'team_name': team_name,
                    'minutes_played': game_minutes,
                },
            }
            players[player['player_id']] = player
    for substitution in events[events.type_name == 'Substitution'].itertuples():
        replacement = substitution.extra['substitution']['replacement']
        replacement = {
            'player_id': replacement['id'],
            'player_name': replacement['name'],
            'minutes_played': game_minutes - substitution.minute,
            'team_id': substitution.team_id,
            'game_id': game_id,
            'team_name': substitution.team_name,
        }
        players[replacement['player_id']] = replacement
        # minutes_played = substitution.minute
        players[substitution.player_id]['minutes_played'] = substitution.minute
    pg = pd.DataFrame(players.values()).fillna(0)
    for col in pg.columns:
        if '_id' in col:
            pg[col] = pg[col].astype(int)
    return pg


def convert_to_actions(events: pd.DataFrame, home_team_id: int) -> pd.DataFrame:
    """
    Convert StatsBomb events to SPADL actions.

    Parameters
    ----------
    events : pd.DataFrame
        DataFrame containing StatsBomb events from a single game.
    home_team_id : int
        ID of the home team in the corresponding game.

    Returns
    -------
    actions : pd.DataFrame
        DataFrame with corresponding SPADL actions.

    """
    actions = pd.DataFrame()

    events['extra'] = events['extra'].fillna({})
    events = events.fillna(0)

    actions['game_id'] = events.game_id
    actions['original_event_id'] = events.event_id
    actions['period_id'] = events.period_id

    actions['time_seconds'] = (
        60 * events.minute
        + events.second
        - ((events.period_id > 1) * 45 * 60)
        - ((events.period_id > 2) * 45 * 60)
        - ((events.period_id > 3) * 15 * 60)
        - ((events.period_id > 4) * 15 * 60)
    )
    actions['team_id'] = events.team_id
    actions['player_id'] = events.player_id

    actions['start_x'] = events.location.apply(lambda x: x[0] if x else 1)
    actions['start_y'] = events.location.apply(lambda x: x[1] if x else 1)
    actions['start_x'] = ((actions['start_x'] - 1) / 119) * spadlconfig.field_length
    actions['start_y'] = 68 - ((actions['start_y'] - 1) / 79) * spadlconfig.field_width

    end_location = events[['location', 'extra']].apply(_get_end_location, axis=1)
    actions['end_x'] = end_location.apply(lambda x: x[0] if x else 1)
    actions['end_y'] = end_location.apply(lambda x: x[1] if x else 1)
    actions['end_x'] = ((actions['end_x'] - 1) / 119) * spadlconfig.field_length
    actions['end_y'] = 68 - ((actions['end_y'] - 1) / 79) * spadlconfig.field_width

    actions[['type_id', 'result_id', 'bodypart_id']] = events[['type_name', 'extra']].apply(
        _parse_event, axis=1, result_type='expand'
    )

    actions = (
        actions[actions.type_id != spadlconfig.actiontypes.index('non_action')]
        .sort_values(['game_id', 'period_id', 'time_seconds'])
        .reset_index(drop=True)
    )
    actions = _fix_direction_of_play(actions, home_team_id)
    actions = _fix_clearances(actions)

    actions['action_id'] = range(len(actions))
    actions = _add_dribbles(actions)

    for col in [c for c in actions.columns.values if c != 'original_event_id']:
        if '_id' in col:
            actions[col] = actions[col].astype(int)
    return actions


Location = Tuple[float, float]


def _get_end_location(q: Tuple[Location, Dict[str, Any]]) -> Location:
    start_location, extra = q
    for event in ['pass', 'shot', 'carry']:
        if event in extra and 'end_location' in extra[event]:
            return extra[event]['end_location']
    return start_location


def _parse_event(q: Tuple[str, Dict[str, Any]]) -> Tuple[int, int, int]:
    t, x = q
    events = {
        'Pass': _parse_pass_event,
        'Dribble': _parse_dribble_event,
        'Carry': _parse_carry_event,
        'Foul Committed': _parse_foul_event,
        'Duel': _parse_duel_event,
        'Interception': _parse_interception_event,
        'Shot': _parse_shot_event,
        'Own Goal Against': _parse_own_goal_event,
        'Goal Keeper': _parse_goalkeeper_event,
        'Clearance': _parse_clearance_event,
        'Miscontrol': _parse_miscontrol_event,
    }
    parser = events.get(t, _parse_event_as_non_action)
    a, r, b = parser(x)
    actiontype = spadlconfig.actiontypes.index(a)
    result = spadlconfig.results.index(r)
    bodypart = spadlconfig.bodyparts.index(b)
    return actiontype, result, bodypart


def _parse_event_as_non_action(extra: Dict[str, Any]) -> Tuple[str, str, str]:
    a = 'non_action'
    r = 'success'
    b = 'foot'
    return a, r, b


def _parse_pass_event(extra: Dict[str, Any]) -> Tuple[str, str, str]:  # noqa: C901

    a = 'pass'  # default
    p = extra.get('pass', {})
    ptype = p.get('type', {}).get('name')
    height = p.get('height', {}).get('name')
    cross = p.get('cross')
    if ptype == 'Free Kick':
        if height == 'High Pass' or cross:
            a = 'freekick_crossed'
        else:
            a = 'freekick_short'
    elif ptype == 'Corner':
        if height == 'High Pass' or cross:
            a = 'corner_crossed'
        else:
            a = 'corner_short'
    elif ptype == 'Goal Kick':
        a = 'goalkick'
    elif ptype == 'Throw-in':
        a = 'throw_in'
    elif cross:
        a = 'cross'
    else:
        a = 'pass'

    pass_outcome = extra.get('pass', {}).get('outcome', {}).get('name')
    if pass_outcome in ['Incomplete', 'Out']:
        r = 'fail'
    elif pass_outcome == 'Pass Offside':
        r = 'offside'
    else:
        r = 'success'

    bp = extra.get('pass', {}).get('body_part', {}).get('name')
    if bp is None:
        b = 'foot'
    elif 'Head' in bp:
        b = 'head'
    elif 'Foot' in bp or bp == 'Drop Kick':
        b = 'foot'
    else:
        b = 'other'

    return a, r, b


def _parse_dribble_event(extra: Dict[str, Any]) -> Tuple[str, str, str]:
    a = 'take_on'

    dribble_outcome = extra.get('dribble', {}).get('outcome', {}).get('name')
    if dribble_outcome == 'Incomplete':
        r = 'fail'
    elif dribble_outcome == 'Complete':
        r = 'success'
    else:
        r = 'success'

    b = 'foot'

    return a, r, b


def _parse_carry_event(extra: Dict[str, Any]) -> Tuple[str, str, str]:
    a = 'dribble'
    r = 'success'
    b = 'foot'
    return a, r, b


def _parse_foul_event(extra: Dict[str, Any]) -> Tuple[str, str, str]:
    a = 'foul'

    foul_card = extra.get('foul_committed', {}).get('card', {}).get('name', '')
    if 'Yellow' in foul_card:
        r = 'yellow_card'
    elif 'Red' in foul_card:
        r = 'red_card'
    else:
        r = 'success'

    b = 'foot'

    return a, r, b


def _parse_duel_event(extra: Dict[str, Any]) -> Tuple[str, str, str]:
    if extra.get('duel', {}).get('type', {}).get('name') == 'Tackle':
        a = 'tackle'
        duel_outcome = extra.get('duel', {}).get('outcome', {}).get('name')
        if duel_outcome in ['Lost In Play', 'Lost Out']:
            r = 'fail'
        elif duel_outcome in ['Success in Play', 'Won']:
            r = 'success'
        else:
            r = 'success'

        b = 'foot'
        return a, r, b
    return _parse_event_as_non_action(extra)


def _parse_interception_event(extra: Dict[str, Any]) -> Tuple[str, str, str]:
    a = 'interception'
    interception_outcome = extra.get('interception', {}).get('outcome', {}).get('name')
    if interception_outcome in ['Lost In Play', 'Lost Out']:
        r = 'fail'
    elif interception_outcome == 'Won':
        r = 'success'
    else:
        r = 'success'
    b = 'foot'
    return a, r, b


def _parse_shot_event(extra: Dict[str, Any]) -> Tuple[str, str, str]:
    extra_type = extra.get('shot', {}).get('type', {}).get('name')
    if extra_type == 'Free Kick':
        a = 'shot_freekick'
    elif extra_type == 'Penalty':
        a = 'shot_penalty'
    else:
        a = 'shot'

    shot_outcome = extra.get('shot', {}).get('outcome', {}).get('name')
    if shot_outcome == 'Goal':
        r = 'success'
    elif shot_outcome in ['Blocked', 'Off T', 'Post', 'Saved', 'Wayward']:
        r = 'fail'
    else:
        r = 'fail'

    bp = extra.get('shot', {}).get('body_part', {}).get('name')
    if bp is None:
        b = 'foot'
    elif 'Head' in bp:
        b = 'head'
    elif 'Foot' in bp or bp == 'Drop Kick':
        b = 'foot'
    else:
        b = 'other'

    return a, r, b


def _parse_own_goal_event(extra: Dict[str, Any]) -> Tuple[str, str, str]:
    a = 'shot'
    r = 'owngoal'
    b = 'foot'
    return a, r, b


def _parse_goalkeeper_event(extra: Dict[str, Any]) -> Tuple[str, str, str]:  # noqa: C901
    extra_type = extra.get('goalkeeper', {}).get('type', {}).get('name')
    if extra_type == 'Shot Saved':
        a = 'keeper_save'
    elif extra_type in ('Collected', 'Keeper Sweeper'):
        a = 'keeper_claim'
    elif extra_type == 'Punch':
        a = 'keeper_punch'
    else:
        a = 'non_action'

    goalkeeper_outcome = extra.get('goalkeeper', {}).get('outcome', {}).get('name', 'x')
    if goalkeeper_outcome in [
        'Claim',
        'Clear',
        'Collected Twice',
        'In Play Safe',
        'Success',
        'Touched Out',
    ]:
        r = 'success'
    elif goalkeeper_outcome in ['In Play Danger', 'No Touch']:
        r = 'fail'
    else:
        r = 'success'

    bp = extra.get('goalkeeper', {}).get('body_part', {}).get('name')
    if bp is None:
        b = 'foot'
    elif 'Head' in bp:
        b = 'head'
    elif 'Foot' in bp or bp == 'Drop Kick':
        b = 'foot'
    else:
        b = 'other'

    return a, r, b


def _parse_clearance_event(extra: Dict[str, Any]) -> Tuple[str, str, str]:
    a = 'clearance'
    r = 'success'
    b = 'foot'
    return a, r, b


def _parse_miscontrol_event(extra: Dict[str, Any]) -> Tuple[str, str, str]:
    a = 'bad_touch'
    r = 'fail'
    b = 'foot'
    return a, r, b
