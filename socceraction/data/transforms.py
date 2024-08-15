"""Common data transformations."""

from abc import ABC, abstractmethod
from concurrent.futures import ThreadPoolExecutor, as_completed
from functools import partial
from typing import Any, Callable, Generic, Optional, TypeVar, Union, cast

import pandas as pd
from pandera.typing import DataFrame, Series
from typing_extensions import override

from socceraction import spadl
from socceraction.atomic import spadl as atomic_spadl
from socceraction.atomic.spadl.schema import AtomicSPADLSchema
from socceraction.data.providers.statsbomb import StatsBombEventSchema
from socceraction.data.schema import EventSchema, GameSchema
from socceraction.features.utils import _FEATURE_REGISTRY
from socceraction.spadl.schema import SPADLSchema
from socceraction.types import Features, GameStates, Mask, SPADLActions


class TransformException(Exception):
    """Exception raised when a transormation fails."""

    def __init__(self, game_id: str) -> None:
        self.game_id = game_id
        super().__init__(f"Transformation failed for game with ID={game_id}")


T = TypeVar("T")


class Transform(ABC, Generic[T]):
    """Constructs a transformer for a game dataset."""

    @abstractmethod
    def __call__(self, game: Series[GameSchema], data: T) -> DataFrame[Any]:
        """Transform the data for a game."""
        pass

    def __repr__(self) -> str:
        """Return a string representation of the transformer."""
        return self.__class__.__name__


class PlayActionsLeftToRight(Transform[DataFrame[SPADLSchema]]):
    """Transforms the orientation of actions such that they are executed from left to right."""

    @override
    def __call__(
        self, game: Series[GameSchema], actions: DataFrame[SPADLSchema]
    ) -> DataFrame[SPADLSchema]:
        return cast(
            DataFrame[SPADLSchema], spadl.utils.play_left_to_right(actions, game.home_team_id)
        )


class PlayAtomicActionsLeftToRight(Transform[DataFrame[AtomicSPADLSchema]]):
    """Transforms the orientation of actions such that they are executed from left to right."""

    @override
    def __call__(
        self, game: Series[GameSchema], actions: DataFrame[AtomicSPADLSchema]
    ) -> DataFrame[AtomicSPADLSchema]:
        return cast(
            DataFrame[AtomicSPADLSchema],
            atomic_spadl.utils.play_left_to_right(actions, game.home_team_id),
        )


class EventsToActions(Transform[DataFrame[EventSchema]], ABC):
    """Transforms events to SPADL actions."""

    def __init__(
        self,
        fn_convert_to_actions: Callable[[DataFrame[EventSchema], int], DataFrame[SPADLSchema]],
    ) -> None:
        self.fn_convert_to_actions = fn_convert_to_actions

    @override
    def __call__(
        self, game: Series[GameSchema], events: DataFrame[EventSchema]
    ) -> DataFrame[SPADLSchema]:
        actions = self.fn_convert_to_actions(events, game.home_team_id)
        return actions


class StatsBombEventsToActions(EventsToActions):
    """Convert StatsBomb events to SPADL actions."""

    def __init__(
        self,
        xy_fidelity_version: Optional[int] = None,
        shot_fidelity_version: Optional[int] = None,
    ) -> None:
        fn_convert_to_actions = partial(
            spadl.statsbomb.convert_to_actions,
            xy_fidelity_version=xy_fidelity_version,
            shot_fidelity_version=shot_fidelity_version,
        )
        super().__init__(fn_convert_to_actions)


class OptaEventsToActions(EventsToActions):
    """Convert Opta events to SPADL actions."""

    def __init__(self) -> None:
        super().__init__(spadl.opta.convert_to_actions)


class WyscoutEventsToActions(EventsToActions):
    """Convert Wyscout events to SPADL actions."""

    def __init__(self) -> None:
        super().__init__(spadl.wyscout.convert_to_actions)


class ActionsToAtomic(Transform[DataFrame[SPADLSchema]]):
    """Convert SPADL actions to atomic SPADL actions."""

    def __init__(
        self,
        fn_convert_to_atomic: Optional[
            Callable[[DataFrame[SPADLSchema]], DataFrame[AtomicSPADLSchema]]
        ] = None,
    ) -> None:
        self.fn_convert_to_atomic = (
            fn_convert_to_atomic
            if fn_convert_to_atomic is not None
            else atomic_spadl.convert_to_atomic
        )

    @override
    def __call__(
        self, game: Series[GameSchema], actions: DataFrame[SPADLSchema]
    ) -> DataFrame[AtomicSPADLSchema]:
        atomic_actions = self.fn_convert_to_atomic(actions)
        return atomic_actions


class ActionsToFeatures(Transform[DataFrame[SPADLSchema]]):
    """Convert SPADL actions to features."""

    def __init__(
        self,
        xfns: list[Callable[[SPADLActions | GameStates, Mask], Features] | str],
        mask_fn: Optional[Callable[[Series[SPADLSchema]], bool]] = None,
        convert_ltr: bool = True,
        nb_prev_actions: int = 3,
    ) -> None:
        self.xfns = xfns
        self.mask_fn = mask_fn if mask_fn is not None else lambda a: True
        self.convert_ltr = convert_ltr
        self.nb_prev_actions = nb_prev_actions

    @override
    def __call__(  # noqa: C901
        self,
        game: Series[GameSchema],
        actions: DataFrame[SPADLSchema],
    ) -> Features:
        features_idx_cols = ["game_id", "action_id", "original_event_id"]

        # create mask
        mask = actions.apply(lambda a: self.mask_fn(a), axis=1).values

        # get feature generators
        fns: dict[
            Callable[[Union[SPADLActions, GameStates], Mask], Features], Optional[list[str]]
        ] = {}
        for fn in self.xfns:
            if isinstance(fn, str):
                if fn not in _FEATURE_REGISTRY:
                    raise ValueError(
                        f"Unkown feature '{fn}'. Valid feature names are [{', '.join(_FEATURE_REGISTRY)}]"
                    )
                _fn = _FEATURE_REGISTRY[fn]
                if _fn not in fns:
                    fns[_fn] = [fn]
                else:
                    fns[_fn].append(fn)  # type: ignore
            else:
                fns[fn] = None

        # handle inputs with no data or no features
        if sum(mask) == 0:
            # TODO: create the expected columns
            return cast(
                DataFrame[Any], pd.DataFrame(index=pd.MultiIndex([], names=features_idx_cols))
            )
        if len(fns) == 0:
            return cast(DataFrame[Any], pd.DataFrame(index=actions.loc[:, features_idx_cols]))

        # prepare actions for applying feature generators
        actions = spadl.utils.add_names(actions)
        # convert actions to ltr orientation
        if self.convert_ltr:
            actions = cast(
                DataFrame[SPADLSchema], spadl.utils.play_left_to_right(actions, game.home_team_id)
            )
        # convert actions to ltr shot gamestates
        gamestates = spadl.utils.to_gamestates(actions, nb_prev_actions=self.nb_prev_actions)
        gamestates_ltr = spadl.utils.play_left_to_right(gamestates, game.home_team_id)
        masked_gamestates_ltr = [states.loc[mask].copy() for states in gamestates_ltr]

        # compute features
        def _apply_generator(
            fn: Callable[[Union[SPADLActions, GameStates], Mask], Features],
            cols: list[str] | None,
        ) -> Features:
            try:
                features = None
                ftype = getattr(fn, "ftype", None)
                if ftype == "actions":
                    features = fn(actions, mask)
                elif ftype == "gamestates":
                    features = fn(masked_gamestates_ltr)
                elif ftype is None:
                    raise ValueError("The feature generator does not have and 'ftype'.")
                else:
                    raise ValueError(f"This transform cannot compute features of type '{ftype}'.")
                if cols is not None:
                    return features[cols]
                return features
            except Exception as e:
                raise Exception(f"Failed to apply feature generator '{fn.__name__}'.") from e

        with ThreadPoolExecutor() as executor:
            future_to_fn = {
                executor.submit(_apply_generator, fn, cols): fn for fn, cols in fns.items()
            }
            attrs = []
            for future in as_completed(future_to_fn):
                result = future.result()
                if not result.empty:
                    attrs.append(result)

        features_idx = pd.MultiIndex.from_frame(actions.loc[mask, features_idx_cols])
        df_attrs = pd.concat(attrs, axis=1).set_index(features_idx)

        # Fill missing values
        missing_bool = df_attrs.select_dtypes(include=["boolean"]).columns
        df_attrs[missing_bool] = df_attrs[missing_bool].fillna(False).astype(bool)
        df_attrs.sort_index(axis=1, inplace=True)

        return cast(DataFrame[Any], df_attrs.reset_index())


class StatsBombEventsToFeatures(Transform[DataFrame[StatsBombEventSchema]]):
    """Convert StatsBomb events to features."""

    def __init__(
        self,
        xfns: list[Callable[[DataFrame[StatsBombEventSchema], Mask], Features] | str],
        mask_fn: Optional[Callable[[Series[StatsBombEventSchema]], bool]] = None,
    ) -> None:
        self.xfns = xfns
        self.mask_fn = mask_fn if mask_fn is not None else lambda a: True

    @override
    def __call__(  # noqa: C901
        self, game: Series[GameSchema], events: DataFrame[StatsBombEventSchema]
    ) -> Features:
        # create mask
        mask = events.apply(lambda e: self.mask_fn(e), axis=1)

        # get feature generators
        fns: dict[
            Callable[[DataFrame[StatsBombEventSchema], Mask], Features], Optional[list[str]]
        ] = {}
        for fn in self.xfns:
            if isinstance(fn, str):
                if fn not in _FEATURE_REGISTRY:
                    raise ValueError(
                        f"Unkown feature '{fn}'. Valid feature names are [{', '.join(_FEATURE_REGISTRY)}]"
                    )
                _fn = _FEATURE_REGISTRY[fn]
                if _fn not in fns:
                    fns[_fn] = [fn]
                else:
                    fns[_fn].append(fn)  # type: ignore
            else:
                fns[fn] = None

        # handle inputs with no data or no features
        if sum(mask) == 0:
            # TODO: create the expected columns
            return cast(
                DataFrame[Any], pd.DataFrame(index=pd.MultiIndex([], names=["original_event_id"]))
            )
        features_idx = pd.MultiIndex.from_frame(
            events.loc[mask, ["event_id"]], names=["original_event_id"]
        )
        if len(self.xfns) == 0:
            return cast(DataFrame[Any], pd.DataFrame(index=features_idx))

        # compute features
        def _apply_generator(
            fn: Callable[[DataFrame[StatsBombEventSchema], Mask], Features], cols: list[str] | None
        ) -> Features:
            try:
                features = None
                ftype = getattr(fn, "ftype", None)
                if ftype == "events":
                    features = fn(events, mask)
                elif ftype is None:
                    raise ValueError("The feature generator does not have and 'ftype'.")
                else:
                    raise ValueError(f"This transform cannot compute features of type '{ftype}'.")
                if cols is not None:
                    return features[cols]
                return features
            except Exception as e:
                raise Exception(f"Failed to apply feature generator '{fn.__name__}'.") from e

        with ThreadPoolExecutor() as executor:
            future_to_fn = {
                executor.submit(_apply_generator, fn, cols): fn for fn, cols in fns.items()
            }
            attrs = []
            for future in as_completed(future_to_fn):
                result = future.result()
                if not result.empty:
                    attrs.append(result)

        df_attrs = pd.concat(attrs, axis=1).set_index(features_idx)

        # Fill missing values
        missing_bool = df_attrs.select_dtypes(include=["boolean"]).columns
        df_attrs[missing_bool] = df_attrs[missing_bool].fillna(False).astype(bool)
        df_attrs.sort_index(axis=1, inplace=True)

        return cast(DataFrame[Any], df_attrs.reset_index())
