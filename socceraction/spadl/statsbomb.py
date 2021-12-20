# -*- coding: utf-8 -*-
"""StatsBomb event stream data to SPADL converter."""
from typing import Any, Dict, Tuple

import pandas as pd  # type: ignore
from pandera.typing import DataFrame

from . import config as spadlconfig
from .base import _add_dribbles, _fix_clearances, _fix_direction_of_play
from .schema import SPADLSchema


def convert_to_actions(events: pd.DataFrame, home_team_id: int) -> DataFrame[SPADLSchema]:
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

    events = events.copy()
    events['extra'].fillna({}, inplace=True)
    events.fillna(0, inplace=True)

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

    actions['start_x'] = events.location.apply(lambda x: x[0] if x else 1).clip(1, 120)
    actions['start_y'] = events.location.apply(lambda x: x[1] if x else 1).clip(1, 80)
    actions['start_x'] = ((actions['start_x'] - 1) / 119) * spadlconfig.field_length
    actions['start_y'] = 68 - ((actions['start_y'] - 1) / 79) * spadlconfig.field_width

    end_location = events[['location', 'extra']].apply(_get_end_location, axis=1)
    actions['end_x'] = end_location.apply(lambda x: x[0] if x else 1).clip(1, 120)
    actions['end_y'] = end_location.apply(lambda x: x[1] if x else 1).clip(1, 80)
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
    return actions.pipe(DataFrame[SPADLSchema])


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


def _parse_event_as_non_action(_extra: Dict[str, Any]) -> Tuple[str, str, str]:
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


def _parse_carry_event(_extra: Dict[str, Any]) -> Tuple[str, str, str]:
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


def _parse_own_goal_event(_extra: Dict[str, Any]) -> Tuple[str, str, str]:
    a = 'bad_touch'
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


def _parse_clearance_event(_extra: Dict[str, Any]) -> Tuple[str, str, str]:
    a = 'clearance'
    r = 'success'
    b = 'foot'
    return a, r, b


def _parse_miscontrol_event(_extra: Dict[str, Any]) -> Tuple[str, str, str]:
    a = 'bad_touch'
    r = 'fail'
    b = 'foot'
    return a, r, b


def StatsBombLoader(*args, **kwargs):  # type: ignore # noqa
    from warnings import warn

    from socceraction.data.statsbomb import StatsBombLoader  # type: ignore

    warn(
        """socceraction.spadl.statsbomb.StatsBombLoader is depecated,
        use socceraction.data.statsbomb.StatsBombLoader instead""",
        DeprecationWarning,
    )
    return StatsBombLoader(*args, **kwargs)


def extract_player_games(events: pd.DataFrame) -> pd.DataFrame:  # noqa
    from warnings import warn

    from socceraction.data.statsbomb import extract_player_games

    warn(
        """socceraction.spadl.statsbomb.extract_player_games is depecated,
        use socceraction.data.statsbomb.extract_player_games instead""",
        DeprecationWarning,
    )
    return extract_player_games(events)


def StatsBombCompetitionSchema(*args, **kwargs):  # type: ignore # noqa
    from warnings import warn

    from socceraction.data.statsbomb import StatsBombCompetitionSchema

    warn(
        """socceraction.spadl.statsbomb.StatsBombCompetitionSchema is depecated,
        use socceraction.data.statsbomb.StatsBombCompetitionSchema instead""",
        DeprecationWarning,
    )
    return StatsBombCompetitionSchema(*args, **kwargs)


def StatsBombGameSchema(*args, **kwargs):  # type: ignore # noqa
    from warnings import warn

    from socceraction.data.statsbomb import StatsBombGameSchema

    warn(
        """socceraction.spadl.statsbomb.StatsBombGameSchema is depecated,
        use socceraction.data.statsbomb.StatsBombGameSchema instead""",
        DeprecationWarning,
    )
    return StatsBombGameSchema(*args, **kwargs)


def StatsBombPlayerSchema(*args, **kwargs):  # type: ignore # noqa
    from warnings import warn

    from socceraction.data.statsbomb import StatsBombPlayerSchema

    warn(
        """socceraction.spadl.statsbomb.StatsBombPlayerSchema is depecated,
        use socceraction.data.statsbomb.StatsBombPlayerSchema instead""",
        DeprecationWarning,
    )
    return StatsBombPlayerSchema(*args, **kwargs)


def StatsBombTeamSchema(*args, **kwargs):  # type: ignore # noqa
    from warnings import warn

    from socceraction.data.statsbomb import StatsBombTeamSchema

    warn(
        """socceraction.spadl.statsbomb.StatsBombTeamSchema is depecated,
        use socceraction.data.statsbomb.StatsBombTeamSchema instead""",
        DeprecationWarning,
    )
    return StatsBombTeamSchema(*args, **kwargs)


def StatsBombEventSchema(*args, **kwargs):  # type: ignore # noqa
    from warnings import warn

    from socceraction.data.statsbomb import StatsBombEventSchema

    warn(
        """socceraction.spadl.statsbomb.StatsBombEventSchema is depecated,
        use socceraction.data.statsbomb.StatsBombEventSchema instead""",
        DeprecationWarning,
    )
    return StatsBombEventSchema(*args, **kwargs)
