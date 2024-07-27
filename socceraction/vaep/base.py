"""Tools for creating and analyzing VAEP models."""

from typing import Any, Literal

import pandas as pd
from pandera.typing import DataFrame
from sklearn.base import clone
from sklearn.pipeline import Pipeline
from sklearn.utils.validation import NotFittedError

from socceraction.base import BaseEstimator
from socceraction.data import Dataset, PartitionIdentifier
from socceraction.data.transforms import Transform
from socceraction.types import Features
from socceraction.vaep import formula


class VAEP(BaseEstimator):
    """
    An implementation of the VAEP framework.

    VAEP (Valuing Actions by Estimating Probabilities) [1]_ defines the
    problem of valuing a soccer player's contributions within a match as
    a binary classification problem and rates actions by estimating its effect
    on the short-term probablities that a team will both score and concede.

    Parameters
    ----------
    xfns : list
        List of feature transformers (see :mod:`socceraction.vaep.features`)
        used to describe the game states. Uses :attr:`~socceraction.vaep.base.xfns_default`
        if None.
    nb_prev_actions : int, default=3  # noqa: DAR103
        Number of previous actions used to decscribe the game state.


    References
    ----------
    .. [1] Tom Decroos, Lotte Bransen, Jan Van Haaren, and Jesse Davis.
        "Actions speak louder than goals: Valuing player actions in soccer." In
        Proceedings of the 25th ACM SIGKDD International Conference on Knowledge
        Discovery & Data Mining, pp. 1851-1861. 2019.
    """

    def __init__(
        self,
        dataset_transformer: Transform,
        pipeline: Pipeline | tuple[Pipeline, Pipeline],
        target_colnames: tuple[str, str] = ("scores", "concedes"),
        column_descriptions: dict[str, str] | None = None,
    ) -> None:
        super().__init__(dataset_transformer, column_descriptions)
        self.dataset_transformer = dataset_transformer
        if isinstance(pipeline, Pipeline):
            self.pipelines = (pipeline, clone(pipeline))
        else:
            self.pipelines = pipeline
        self.target_colnames = target_colnames

    def train(
        self,
        source_data: Dataset | Features,
        partitions: list[PartitionIdentifier] | None = None,
    ) -> "VAEP":
        """Train the model."""
        df_source_data = self._prepare_and_check_source_data(source_data, partitions)

        for pipeline, target_colname in zip(self.pipelines, self.target_colnames):
            target_col = df_source_data.loc[:, target_colname]
            feature_cols = df_source_data.drop(target_colname, axis=1)
            pipeline.fit(feature_cols, target_col.squeeze())
        self._fitted = True
        return self

    def validate(
        self,
        source_data: Dataset | Features,
        target_colname: str = "goal_from_shot",
        partitions: list[PartitionIdentifier] | None = None,
        bins: int | list[float] = 10,
        bin_strategy: Literal["quantile", "uniform"] = "quantile",
        plot: bool = True,
    ) -> dict:
        """Validate the model."""
        return {}

    def estimate(
        self,
        source_data: Dataset | Features,
        partitions: list[PartitionIdentifier] | None = None,
    ) -> DataFrame[Any]:
        """Estimate the VAEP values for all actions in a set of games."""
        if not self._fitted:
            raise NotFittedError("Must fit model before predicting WP.")

        df_source_data = self._prepare_and_check_source_data(source_data, partitions)

        if isinstance(source_data, Dataset):
            df_source_idx = pd.MultiIndex.from_frame(
                df_source_data[["game_id", "action_id", "original_event_id"]]
            )
        else:
            df_source_idx = source_data.index

        Y_hat = pd.DataFrame()
        for col, pipeline in zip(["scores", "concedes"], self.pipelines):
            Y_hat[col] = pipeline.predict_proba(df_source_data)[:, 1]

        p_scores, p_concedes = Y_hat.scores, Y_hat.concedes
        vaep_values = formula.value(df_actions, p_scores, p_concedes)

        return vaep_values.set_index(df_source_idx)
