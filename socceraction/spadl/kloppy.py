"""Kloppy EventDataset to SPADL converter."""

import warnings
from typing import Optional, Union, cast

import kloppy
import pandas as pd  # type: ignore
from kloppy.domain import (
    BodyPart,
    CardType,
    CarryEvent,
    ClearanceEvent,
    CoordinateSystem,
    Dimension,
    DuelEvent,
    DuelResult,
    DuelType,
    Event,
    EventDataset,
    EventType,
    FoulCommittedEvent,
    GoalkeeperActionType,
    GoalkeeperEvent,
    InterceptionResult,
    MetricPitchDimensions,
    MiscontrolEvent,
    Orientation,
    Origin,
    PassEvent,
    PassResult,
    PassType,
    PitchDimensions,
    Provider,
    Qualifier,
    RecoveryEvent,
    SetPieceType,
    ShotEvent,
    ShotResult,
    TakeOnEvent,
    TakeOnResult,
    VerticalOrientation,
)
from packaging import version
from pandera.typing import DataFrame

from . import config as spadlconfig
from .base import _add_dribbles, _fix_clearances
from .schema import SPADLSchema

_KLOPPY_VERSION = version.parse(kloppy.__version__)
_SUPPORTED_PROVIDERS = {
    Provider.STATSBOMB: version.parse("3.15.0"),
    # Provider.OPTA: version.parse("3.15.0"),
}


def convert_to_actions(
    dataset: EventDataset, game_id: Optional[Union[str, int]] = None
) -> DataFrame[SPADLSchema]:
    """Convert a Kloppy event data set to SPADL actions.

    Parameters
    ----------
    dataset : EventDataset
        A Kloppy event data set.
    game_id : str or int, optional
        The identifier of the game. If not provided, the game id will not be
        set in the SPADL DataFrame.

    Returns
    -------
    actions : pd.DataFrame
        DataFrame with corresponding SPADL actions.

    """
    # Check if Kloppy is installed and if the version is supported
    if dataset.metadata.provider not in _SUPPORTED_PROVIDERS:
        warnings.warn(
            f"Converting {dataset.metadata.provider} data is not yet supported. "
            f"The result may be incorrect or incomplete. "
            f"Supported providers are: {', '.join([p.value for p in _SUPPORTED_PROVIDERS.keys()])}"
        )
    elif _KLOPPY_VERSION < _SUPPORTED_PROVIDERS[dataset.metadata.provider]:
        warnings.warn(
            f"Converting {dataset.metadata.provider} data is only supported from "
            f"Kloppy version {_SUPPORTED_PROVIDERS[dataset.metadata.provider]} (you have {_KLOPPY_VERSION}). "
            f"The result may be incorrect or incomplete."
        )

    # Convert the dataset to the SPADL coordinate system
    new_dataset = dataset.transform(
        to_orientation=Orientation.HOME_AWAY,
        to_coordinate_system=_SoccerActionCoordinateSystem(
            pitch_length=dataset.metadata.coordinate_system.pitch_length,
            pitch_width=dataset.metadata.coordinate_system.pitch_width,
        ),
    )

    # Convert the events to SPADL actions
    actions = []
    for event in new_dataset.events:
        action = dict(
            game_id=game_id,
            original_event_id=event.event_id,
            period_id=event.period.id,
            time_seconds=event.timestamp.total_seconds(),
            team_id=event.team.team_id if event.team else None,
            player_id=event.player.player_id if event.player else None,
            start_x=event.coordinates.x if event.coordinates else None,
            start_y=event.coordinates.y if event.coordinates else None,
            **_get_end_location(event),
            **_parse_event(event),
        )
        actions.append(action)

    # Create the SPADL actions DataFrame
    df_actions = (
        pd.DataFrame(actions)
        .sort_values(["game_id", "period_id", "time_seconds"], kind="mergesort")
        .reset_index(drop=True)
    )
    df_actions = df_actions[df_actions.type_id != spadlconfig.actiontypes.index("non_action")]

    df_actions = _fix_clearances(df_actions)

    df_actions["action_id"] = range(len(df_actions))
    df_actions = _add_dribbles(df_actions)

    return cast(DataFrame[SPADLSchema], df_actions)


class _SoccerActionCoordinateSystem(CoordinateSystem):
    @property
    def provider(self) -> Provider:
        return "SoccerAction"

    @property
    def origin(self) -> Origin:
        return Origin.BOTTOM_LEFT

    @property
    def vertical_orientation(self) -> VerticalOrientation:
        return VerticalOrientation.BOTTOM_TO_TOP

    @property
    def pitch_dimensions(self) -> PitchDimensions:
        return MetricPitchDimensions(
            x_dim=Dimension(0, spadlconfig.field_length),
            y_dim=Dimension(0, spadlconfig.field_width),
            pitch_length=self.pitch_length,
            pitch_width=self.pitch_width,
            standardized=True,
        )


def _get_end_location(event: Event) -> dict[str, Optional[float]]:
    if isinstance(event, PassEvent):
        if event.receiver_coordinates:
            return {
                "end_x": event.receiver_coordinates.x,
                "end_y": event.receiver_coordinates.y,
            }
    elif isinstance(event, CarryEvent):
        if event.end_coordinates:
            return {
                "end_x": event.end_coordinates.x,
                "end_y": event.end_coordinates.y,
            }
    elif isinstance(event, ShotEvent):
        if event.result_coordinates:
            return {
                "end_x": event.result_coordinates.x,
                "end_y": event.result_coordinates.y,
            }
    if event.coordinates:
        return {"end_x": event.coordinates.x, "end_y": event.coordinates.y}
    return {"end_x": None, "end_y": None}


def _parse_event(event: Event) -> dict[str, int]:
    events = {
        EventType.PASS: _parse_pass_event,
        EventType.SHOT: _parse_shot_event,
        EventType.TAKE_ON: _parse_take_on_event,
        EventType.CARRY: _parse_carry_event,
        EventType.FOUL_COMMITTED: _parse_foul_event,
        EventType.DUEL: _parse_duel_event,
        EventType.CLEARANCE: _parse_clearance_event,
        EventType.MISCONTROL: _parse_miscontrol_event,
        EventType.GOALKEEPER: _parse_goalkeeper_event,
        EventType.INTERCEPTION: _parse_interception_event,
        # other non-action events
        # EventType.GENERIC: _parse_event_as_non_action,
        # EventType.RECOVERY: _parse_event_as_non_action,
        # EventType.SUBSTITUTION: _parse_event_as_non_action,
        # EventType.CARD: _parse_event_as_non_action,
        # EventType.PLAYER_ON: _parse_event_as_non_action,
        # EventType.PLAYER_OFF: _parse_event_as_non_action,
        # EventType.BALL_OUT: _parse_event_as_non_action,
        # EventType.FORMATION_CHANGE:_parse_event_as_non_action,
    }
    parser = events.get(event.event_type, _parse_event_as_non_action)
    a, r, b = parser(event)
    return {
        "type_id": spadlconfig.actiontypes.index(a),
        "result_id": spadlconfig.results.index(r),
        "bodypart_id": spadlconfig.bodyparts.index(b),
    }


def _qualifiers(event: Event) -> list[Qualifier]:
    if event.qualifiers:
        return [q.value for q in event.qualifiers]
    return []


def _parse_bodypart(qualifiers: list[Qualifier], default: str = "foot") -> str:
    if BodyPart.HEAD in qualifiers:
        b = "head"
    elif BodyPart.RIGHT_FOOT in qualifiers:
        b = "foot_right"
    elif BodyPart.LEFT_FOOT in qualifiers:
        b = "foot_left"
    elif BodyPart.CHEST in qualifiers or BodyPart.OTHER in qualifiers:
        b = "other"
    elif BodyPart.HEAD_OTHER in qualifiers:
        b = "head/other"
    else:
        b = default
    return b


def _parse_event_as_non_action(event: Event) -> tuple[str, str, str]:
    a = "non_action"
    r = "success"
    b = "foot"
    return a, r, b


def _parse_pass_event(event: PassEvent) -> tuple[str, str, str]:  # noqa: C901
    qualifiers = _qualifiers(event)
    b = _parse_bodypart(qualifiers)

    a = "pass"  # default
    r = None
    if SetPieceType.FREE_KICK in qualifiers:
        if (
            PassType.CHIPPED_PASS in qualifiers
            or PassType.CROSS in qualifiers
            or PassType.HIGH_PASS in qualifiers
            or PassType.LONG_BALL in qualifiers
        ):
            a = "freekick_crossed"
        else:
            a = "freekick_short"
    elif SetPieceType.CORNER_KICK in qualifiers:
        if (
            PassType.CHIPPED_PASS in qualifiers
            or PassType.CROSS in qualifiers
            or PassType.HIGH_PASS in qualifiers
            or PassType.LONG_BALL in qualifiers
        ):
            a = "corner_crossed"
        else:
            a = "corner_short"
    elif SetPieceType.GOAL_KICK in qualifiers:
        a = "goalkick"
    elif SetPieceType.THROW_IN in qualifiers:
        a = "throw_in"
        b = "other"
    elif PassType.CROSS in qualifiers:
        a = "cross"
    else:
        a = "pass"

    if BodyPart.KEEPER_ARM in qualifiers:
        b = "other"

    if r is None:
        if event.result in [PassResult.INCOMPLETE, PassResult.OUT]:
            r = "fail"
        elif event.result == PassResult.OFFSIDE:
            r = "offside"
        elif event.result == PassResult.COMPLETE:
            r = "success"
        else:
            # discard interrupted events
            a = "non_action"
            r = "success"

    return a, r, b


def _parse_shot_event(event: ShotEvent) -> tuple[str, str, str]:
    qualifiers = _qualifiers(event)
    b = _parse_bodypart(qualifiers)

    if SetPieceType.FREE_KICK in qualifiers:
        a = "shot_freekick"
    elif SetPieceType.PENALTY in qualifiers:
        a = "shot_penalty"
    else:
        a = "shot"

    if event.result == ShotResult.GOAL:
        r = "success"
    elif event.result == ShotResult.OWN_GOAL:
        a = "bad_touch"
        r = "owngoal"
    else:
        r = "fail"

    return a, r, b


def _parse_take_on_event(event: TakeOnEvent) -> tuple[str, str, str]:
    a = "take_on"

    if event.result == TakeOnResult.COMPLETE:
        r = "success"
    else:
        r = "fail"

    b = "foot"

    return a, r, b


def _parse_carry_event(_e: CarryEvent) -> tuple[str, str, str]:
    a = "dribble"
    r = "success"
    b = "foot"
    return a, r, b


def _parse_interception_event(event: RecoveryEvent) -> tuple[str, str, str]:
    a = "interception"
    qualifiers = _qualifiers(event)
    b = _parse_bodypart(qualifiers, default="foot")

    if event.result == InterceptionResult.LOST or event.result == InterceptionResult.OUT:
        r = "fail"
    else:
        r = "success"

    return a, r, b


def _parse_foul_event(event: FoulCommittedEvent) -> tuple[str, str, str]:
    a = "foul"
    r = "fail"
    b = "foot"

    qualifiers = _qualifiers(event)
    if CardType.FIRST_YELLOW in qualifiers:
        r = "yellow_card"
    elif CardType.SECOND_YELLOW in qualifiers:
        r = "red_card"
    elif CardType.RED in qualifiers:
        r = "red_card"

    return a, r, b


def _parse_duel_event(event: DuelEvent) -> tuple[str, str, str]:
    qualifiers = _qualifiers(event)

    a = "non_action"
    b = "foot"
    if DuelType.GROUND in qualifiers and DuelType.LOOSE_BALL not in qualifiers:
        a = "tackle"
        b = "foot"

    if event.result == DuelResult.LOST:
        r = "fail"
    else:
        r = "success"

    return a, r, b


def _parse_clearance_event(event: ClearanceEvent) -> tuple[str, str, str]:
    a = "clearance"
    r = "success"
    qualifiers = _qualifiers(event)
    b = _parse_bodypart(qualifiers)
    return a, r, b


def _parse_miscontrol_event(event: MiscontrolEvent) -> tuple[str, str, str]:
    a = "bad_touch"
    r = "fail"
    b = "foot"
    return a, r, b


def _parse_goalkeeper_event(event: GoalkeeperEvent) -> tuple[str, str, str]:
    a = "non_action"
    r = "success"
    qualifiers = _qualifiers(event)
    b = _parse_bodypart(qualifiers, default="other")

    if GoalkeeperActionType.SAVE in qualifiers:
        a = "keeper_save"
        r = "success"
    # if GoalkeeperActionType.SAVE_ATTEMPT in qualifiers:
    #     a = "keeper_save"
    #     r = "fail"
    if GoalkeeperActionType.CLAIM in qualifiers:
        a = "keeper_claim"
    if GoalkeeperActionType.SMOTHER in qualifiers:
        a = "keeper_claim"
    if GoalkeeperActionType.PUNCH in qualifiers:
        a = "keeper_punch"
    if GoalkeeperActionType.PICK_UP in qualifiers:
        a = "keeper_pick_up"
    if GoalkeeperActionType.REFLEX in qualifiers:
        pass

    return a, r, b
