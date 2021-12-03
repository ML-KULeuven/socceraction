# -*- coding: utf-8 -*-
"""Opta event stream data to SPADL converter."""
from typing import Any, Dict, Tuple

import pandas as pd  # type: ignore
from pandera.typing import DataFrame

from . import config as spadlconfig
from .base import _add_dribbles, _fix_clearances, _fix_direction_of_play
from .schema import SPADLSchema


def convert_to_actions(events: pd.DataFrame, home_team_id: int) -> DataFrame[SPADLSchema]:
    """
    Convert Opta events to SPADL actions.

    Parameters
    ----------
    events : pd.DataFrame
        DataFrame containing Opta events from a single game.
    home_team_id : int
        ID of the home team in the corresponding game.

    Returns
    -------
    actions : pd.DataFrame
        DataFrame with corresponding SPADL actions.

    """
    actions = pd.DataFrame()

    actions['game_id'] = events.game_id
    actions['original_event_id'] = events.event_id.astype(object)
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

    for col in ['start_x', 'end_x']:
        actions[col] = events[col] / 100 * spadlconfig.field_length
    for col in ['start_y', 'end_y']:
        actions[col] = events[col] / 100 * spadlconfig.field_width

    actions['type_id'] = events[['type_name', 'outcome', 'qualifiers']].apply(_get_type_id, axis=1)
    actions['result_id'] = events[['type_name', 'outcome', 'qualifiers']].apply(
        _get_result_id, axis=1
    )
    actions['bodypart_id'] = events.qualifiers.apply(_get_bodypart_id)

    actions = (
        actions[actions.type_id != spadlconfig.actiontypes.index('non_action')]
        .sort_values(['game_id', 'period_id', 'time_seconds'])
        .reset_index(drop=True)
    )
    actions = _fix_owngoals(actions)
    actions = _fix_direction_of_play(actions, home_team_id)
    actions = _fix_clearances(actions)
    actions['action_id'] = range(len(actions))
    actions = _add_dribbles(actions)

    return actions.pipe(DataFrame[SPADLSchema])


def _get_bodypart_id(qualifiers: Dict[int, Any]) -> int:
    if 15 in qualifiers:
        b = 'head'
    elif 21 in qualifiers:
        b = 'other'
    else:
        b = 'foot'
    return spadlconfig.bodyparts.index(b)


def _get_result_id(args: Tuple[str, bool, Dict[int, Any]]) -> int:
    e, outcome, q = args
    if e == 'offside pass':
        r = 'offside'  # offside
    elif e == 'foul':
        r = 'fail'
    elif e in ['attempt saved', 'miss', 'post']:
        r = 'fail'
    elif e == 'goal':
        if 28 in q:
            r = 'owngoal'  # own goal, x and y must be switched
        else:
            r = 'success'
    elif e == 'ball touch':
        r = 'fail'
    elif outcome:
        r = 'success'
    else:
        r = 'fail'
    return spadlconfig.results.index(r)


def _get_type_id(args: Tuple[str, bool, Dict[int, Any]]) -> int:  # noqa: C901
    eventname, outcome, q = args
    if eventname in ('pass', 'offside pass'):
        cross = 2 in q
        freekick = 5 in q
        corner = 6 in q
        throw_in = 107 in q
        goalkick = 124 in q
        if throw_in:
            a = 'throw_in'
        elif freekick and cross:
            a = 'freekick_crossed'
        elif freekick:
            a = 'freekick_short'
        elif corner and cross:
            a = 'corner_crossed'
        elif corner:
            a = 'corner_short'
        elif cross:
            a = 'cross'
        elif goalkick:
            a = 'goalkick'
        else:
            a = 'pass'
    elif eventname == 'take on':
        a = 'take_on'
    elif eventname == 'foul' and outcome is False:
        a = 'foul'
    elif eventname == 'tackle':
        a = 'tackle'
    elif eventname in ('interception', 'blocked pass'):
        a = 'interception'
    elif eventname in ['miss', 'post', 'attempt saved', 'goal']:
        if 9 in q:
            a = 'shot_penalty'
        elif 26 in q:
            a = 'shot_freekick'
        else:
            a = 'shot'
    elif eventname == 'save':
        a = 'keeper_save'
    elif eventname == 'claim':
        a = 'keeper_claim'
    elif eventname == 'punch':
        a = 'keeper_punch'
    elif eventname == 'keeper pick-up':
        a = 'keeper_pick_up'
    elif eventname == 'clearance':
        a = 'clearance'
    elif eventname == 'ball touch' and outcome is False:
        a = 'bad_touch'
    else:
        a = 'non_action'
    return spadlconfig.actiontypes.index(a)


def _fix_owngoals(actions: pd.DataFrame) -> pd.DataFrame:
    owngoals_idx = (actions.result_id == spadlconfig.results.index('owngoal')) & (
        actions.type_id == spadlconfig.actiontypes.index('shot')
    )
    actions.loc[owngoals_idx, 'end_x'] = (
        spadlconfig.field_length - actions[owngoals_idx].end_x.values
    )
    actions.loc[owngoals_idx, 'end_y'] = (
        spadlconfig.field_width - actions[owngoals_idx].end_y.values
    )
    actions.loc[owngoals_idx, 'type_id'] = spadlconfig.actiontypes.index('bad_touch')
    return actions


def OptaLoader(*args, **kwargs):  # type: ignore # noqa
    from warnings import warn

    from socceraction.data.opta import OptaLoader  # type: ignore

    warn(
        """socceraction.spadl.opta.OptaLoader is depecated,
        use socceraction.data.opta.OptaLoader instead""",
        DeprecationWarning,
    )
    return OptaLoader(*args, **kwargs)


def OptaCompetitionSchema(*args, **kwargs):  # type: ignore # noqa
    from warnings import warn

    from socceraction.data.opta import OptaCompetitionSchema

    warn(
        """socceraction.spadl.opta.OptaCompetitionSchema is depecated,
        use socceraction.data.opta.OptaCompetitionSchema instead""",
        DeprecationWarning,
    )
    return OptaCompetitionSchema(*args, **kwargs)


def OptaGameSchema(*args, **kwargs):  # type: ignore # noqa
    from warnings import warn

    from socceraction.data.opta import OptaGameSchema

    warn(
        """socceraction.spadl.opta.OptaGameSchema is depecated,
        use socceraction.data.opta.OptaGameSchema instead""",
        DeprecationWarning,
    )
    return OptaGameSchema(*args, **kwargs)


def OptaPlayerSchema(*args, **kwargs):  # type: ignore # noqa
    from warnings import warn

    from socceraction.data.opta import OptaPlayerSchema

    warn(
        """socceraction.spadl.opta.OptaPlayerSchema is depecated,
        use socceraction.data.opta.OptaPlayerSchema instead""",
        DeprecationWarning,
    )
    return OptaPlayerSchema(*args, **kwargs)


def OptaTeamSchema(*args, **kwargs):  # type: ignore # noqa
    from warnings import warn

    from socceraction.data.opta import OptaTeamSchema

    warn(
        """socceraction.spadl.opta.OptaTeamSchema is depecated,
        use socceraction.data.opta.OptaTeamSchema instead""",
        DeprecationWarning,
    )
    return OptaTeamSchema(*args, **kwargs)


def OptaEventSchema(*args, **kwargs):  # type: ignore # noqa
    from warnings import warn

    from socceraction.data.opta import OptaEventSchema

    warn(
        """socceraction.spadl.opta.OptaEventSchema is depecated,
        use socceraction.data.opta.OptaEventSchema instead""",
        DeprecationWarning,
    )
    return OptaEventSchema(*args, **kwargs)
