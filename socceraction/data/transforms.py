"""Transforms are common data transformations."""

from abc import ABC, abstractmethod
from functools import partial
from typing import Any, Callable, Dict, Optional

from pandera.typing import DataFrame, Series

from socceraction import spadl
from socceraction.atomic import spadl as atomic_spadl
from socceraction.atomic.spadl.schema import AtomicSPADLSchema
from socceraction.data.schema import EventSchema, GameSchema
from socceraction.spadl.schema import SPADLSchema


class Transform(ABC):
    """Constructs a transformer for a game dataset."""

    @abstractmethod
    def __call__(self, game: Series[GameSchema], data: Any) -> Any:
        pass

    def __repr__(self) -> str:
        return self.__class__.__name__


class Compose(Transform):
    def __init__(self, transforms: dict[str, Transform]) -> None:
        self.transforms = transforms

    def __call__(self, game: Series[GameSchema], data: Any) -> Any:
        for name, transform in self.transforms.items():
            data = transform(game, data)
        return data

    def __repr__(self):
        format_string = self.__class__.__name__ + "("
        for t in self.transforms:
            format_string += "\n"
            format_string += f"    {t}"
        format_string += "\n)"
        return format_string


class EventsToActions(Transform):
    """Transforms events to SPADL actions."""

    def __init__(self, fn_convert_to_actions: Callable) -> None:
        self.fn_convert_to_actions = fn_convert_to_actions

    def __call__(
        self, game: Series[GameSchema], events: DataFrame[EventSchema]
    ) -> DataFrame[SPADLSchema]:
        actions = self.fn_convert_to_actions(events, game.home_team_id)
        return actions


class StatsBombEventsToActions(EventsToActions):
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
    def __init__(self) -> None:
        super().__init__(spadl.opta.convert_to_actions)


class WyscoutEventsToActions(EventsToActions):
    def __init__(self) -> None:
        super().__init__(spadl.wyscout.convert_to_actions)


class ActionsToAtomic(Transform):
    def __init__(
        self,
        fn_convert_to_atomic: Optional[Callable] = None,
    ):
        self.fn_convert_to_atomic = (
            fn_convert_to_atomic
            if fn_convert_to_atomic is not None
            else atomic_spadl.convert_to_atomic
        )

    def __call__(
        self, game: Series[GameSchema], actions: DataFrame[SPADLSchema]
    ) -> DataFrame[AtomicSPADLSchema]:
        atomic_actions = self.fn_convert_to_atomic(actions)
        return atomic_actions


# class FeatureTransformer(DatasetTransformer):
#     def __init__(self, dataset: Dataset):
#         super().__init__(dataset)
#
#     def transform(self, game):
#         """Prepare a dataset for training and validation.
#
#         Parameters
#         ----------
#         dataset : Dataset
#             The dataset to use.
#         game_ids : list of ints (default=None)
#             Only use data from the games in this list. By default, all games
#             in the dataset are used.
#         on_fail: 'raise' or 'warn'
#             What to do if a feature or label function fails on a specific game.
#
#         Returns
#         -------
#         X : pd.DataFrame
#             A dataframe containing the features.
#         y : pd.DataFrame
#             A dataframe containing the labels.
#         """
#         game_actions = self.actions(game.game_id)
#         game_events = self.events(game.game_id)
#         _X, _y = fs.compute_attributes(
#             game,
#             game_actions,
#             events=game_events,
#             xfns=self.xfns,
#             yfns=self.yfns,
#             shotfilter=self.shotfilter,
#             nb_prev_actions=self.nb_prev_actions,
#         )
