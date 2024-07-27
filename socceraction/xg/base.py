"""Tools for creating and analyzing xG models."""

import os
from collections.abc import Set
from pathlib import Path
from typing import Any, Callable, Literal

import joblib
import numpy as np
import numpy.typing as npt
import pandas as pd
from pandera.typing import DataFrame, Series
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import brier_score_loss, roc_auc_score
from sklearn.pipeline import Pipeline, make_pipeline
from sklearn.utils.validation import NotFittedError
from typing_extensions import override

from socceraction import features as fs
from socceraction import spadl
from socceraction.base import BaseEstimator
from socceraction.data import Dataset, PartitionIdentifier
from socceraction.data.transforms import ActionsToFeatures, Transform
from socceraction.ml import evaluation
from socceraction.ml.pipeline import InteractionFeature
from socceraction.ml.preprocessing import simple_proc_for_linear_algoritms
from socceraction.spadl.schema import SPADLSchema
from socceraction.types import Features
from socceraction.xg import labels as lab


class XGModel(BaseEstimator):
    """A wrapper around a pipeline for computing xG values.

    Parameters
    ----------
    dataset_transformer : Transform
        A dataset transformer to convert a dataset to features.
    pipeline : Pipeline
        Scikit-Learn pipeline to use for the model.
    target_colname : str or integer (default=``"goal_from_shot"``)
        The name of the target variable column.
    column_descriptions : dict, optional
        A dictionary whose keys are the names of the columns used in the
        model, and the values are string descriptions of what the columns
        mean.

    Attributes
    ----------
    training_seasons : list of ``PartitionIdentifier``, or ``None`` (default=``None``)
        If the model was trained using data from a Dataset, a list of
        dataset partitions used to train the model. If no Dataset was used,
        an empty list. If no model has been trained yet, ``None``.
    validation_seasons :  list of ``PartitionIdentifier``, or ``None`` (default=``None``)
        Same as ``training_seasons``, but for validation data.
    sample_probabilities : A numpy array of floats or ``None`` (default=``None``)
        After the model has been validated, contains the sampled predicted
        probabilities used to compute the validation statistic.
    predicted_goal_percents : A numpy array of floats or ``None`` (default=``None``)
        After the model has been validated, contains the actual probabilities
        in the test set at each probability in ``sample_probabilities``.
    num_shots_used : A numpy array of floats or ``None`` (default=``None``)
        After the model has been validated, contains the number of shots used
        to compute each element of ``predicted_goal_percents``.
    model_directory : string
        The directory where all models will be saved to or loaded from.
    """

    def __init__(
        self,
        dataset_transformer: Transform,
        pipeline: Pipeline,
        target_colname: str = "goal_from_shot",
        column_descriptions: dict[str, str] | None = None,
    ) -> None:
        super().__init__(dataset_transformer, column_descriptions)
        self.pipeline = pipeline
        self.target_colname = target_colname

        self._sample_probabilities = None
        self._predicted_goal_percents = None
        self._num_shots_used = None

    @property
    def sample_probabilities(self) -> npt.NDArray[np.float64] | None:
        return self._sample_probabilities

    @property
    def predicted_goal_percents(self) -> npt.NDArray[np.float64] | None:
        return self._predicted_goal_percents

    @property
    def num_shots_used(self) -> npt.NDArray[np.int64] | None:
        return self._num_shots_used

    def train(
        self,
        source_data: Dataset | Features,
        partitions: list[PartitionIdentifier] | None = None,
    ) -> "XGModel":
        """Train the model.

        Once a modeling pipeline is set up (either the default or something
        custom-generated), historical data needs to be fed into it in order to
        "fit" the model so that it can then be used to predict future results.
        This method implements a simple wrapper around the core Scikit-learn
        functionality which does this.

        The default is to use data from a Dataset object, however that can be
        changed to a simple Pandas DataFrame with precomputed features and
        labels if desired.

        There is no particular output from this function, rather the
        parameters governing the fit of the model are saved inside the model
        object itself. If you want to get an estimate of the quality of the
        fit, use the ``validate_model`` method after running this method.

        Parameters
        ----------
        source_data : ``Dataset`` or a Pandas DataFrame
            The data to be used to train the model. If an instance of
            ``Dataset`` is given, will query the database for the training data.
        partitions : list of ``PartitionIdentifier``
            What seasons to use to train the model if getting data from a Dataset instance.
            If ``source_data`` is not a ``Dataset``, this argument will be ignored.
            **NOTE:** it is critical not to use all possible data in order to train the
            model - some will need to be reserved for a final validation (see the
            ``validate_model`` method). A good dataset to reserve for validation
            is the most recent one or two seasons.
        """
        df_source_data = self._prepare_and_check_source_data(source_data, partitions)

        target_col = df_source_data.loc[:, self.target_colname]
        feature_cols = df_source_data.drop(self.target_colname, axis=1)
        self.pipeline.fit(feature_cols, target_col.squeeze())
        self._fitted = True
        return self

    def validate(
        self,
        source_data: Dataset | Features,
        partitions: list[PartitionIdentifier] | None = None,
        bins: int | list[float] = 10,
        bin_strategy: Literal["quantile", "uniform"] = "quantile",
        plot: bool = True,
    ) -> dict:
        """Validate the model.

        Once a modeling pipeline is trained, a different dataset must be fed
        into the trained model to validate the quality of the fit. This method
        implements a simple wrapper around the core Scikit-learn functionality
        which does this.

        The default is to use data from a Dataset object, however that can be
        changed to a simple Pandas DataFrame with precomputed features and
        labels if desired.

        The output of this method is a dictionary with relevant error metrics
        (see ``soccer_xg.metrics``).

        Parameters
        ----------
        source_data : ``Dataset`` or a Pandas DataFrame
            The data to be used to validate the model. If an instance of
            ``Dataset`` is given, will query the api database for the training
            data.
        partitions : list of ``PartitionIdentifier``
            What seasons to use to validated the model if getting data from
            a Dataset instance. If ``source_data`` is not a ``Dataset``, this
            argument will be ignored. **NOTE:** it is critical not to use the
            same data to validate the model as was used in the fit. Generally
            a good data set to use for validation is one from a time period
            more recent than was used to train the model.
        bins : int or list of floats
            Number of bins to create in the scores' space, or list of bin
            boundaries. More bins require more data.
        bin_strategy : {'uniform', 'quantile'}, default='uniform'
            Strategy used to define the widths of the bins.
            uniform
                The bins have identical widths.
            quantile
                The bins have the same number of samples and depend on `y_prob`.
        plot: bool (default=true)
            Whether to plot the AUROC and probability calibration curves.

        Returns
        -------
        dict
            Error metrics on the validation data.

        Raises
        ------
        NotFittedError
            If the model hasn't been fit.
        """
        if not self._fitted:
            raise NotFittedError("Must fit model before validating.")

        df_source_data = self._prepare_and_check_source_data(source_data, partitions)

        feature_cols = df_source_data.drop(self.target_colname, axis=1)
        target_col = df_source_data.loc[:, self.target_colname]

        predicted_probabilities = self.estimate(feature_cols)["xG"]
        target_col = target_col.squeeze()

        (
            self._predicted_goal_percents,
            self._sample_probabilities,
            _,
            self._num_shots_used,
        ) = evaluation.calibration_curve(target_col, predicted_probabilities, bins, bin_strategy)

        # Compute the maximal deviation from a perfect prediction as well as the area under the
        # curve of the residual between |predicted - perfect|:
        max_deviation = evaluation.max_deviation(
            self.sample_probabilities, self.predicted_goal_percents
        )
        residual_area = evaluation.residual_area(
            self.sample_probabilities, self.predicted_goal_percents
        )
        roc = roc_auc_score(target_col, predicted_probabilities)
        brier = brier_score_loss(target_col, predicted_probabilities)
        ece = evaluation.expected_calibration_error(
            target_col, predicted_probabilities, bins, "uniform"
        )
        ace = evaluation.expected_calibration_error(
            target_col, predicted_probabilities, bins, "quantile"
        )

        if plot:
            import matplotlib.pyplot as plt

            fig, ax = plt.subplots(1, 2, figsize=(10, 5))
            evaluation.plot_roc_curve(target_col, predicted_probabilities, fig=fig, ax=ax[0])
            evaluation.plot_reliability_diagram(
                target_col,
                predicted_probabilities,
                fig=fig,
                ax=ax[1],
                bayesian=False,
                bins=bins,
                bin_strategy=bin_strategy,
                show_counts=False,
                fmt="s-",
                min_samples=100,
                show_histogram=False,
                overlay_histogram=False,
                invert_histogram=False,
                ci=0.95,
                shaded_ci=True,
                show_gaps=False,
                show_bars=False,
            )
            ax[0].set_title("ROC curve")
            ax[1].set_title("Reliability diagram")
            plt.tight_layout()
            plt.show()

        return {
            "max_dev": max_deviation,
            "residual_area": residual_area,
            "roc": roc,
            "brier": brier,
            "ece": ece,
            "ace": ace,
        }

    def estimate(
        self,
        source_data: Dataset | Features,
        partitions: list[PartitionIdentifier] | None = None,
    ) -> DataFrame[Any]:
        """Estimate the xG values for all shots in a set of games.

        The default is to use data from a Dataset object, however that can be changed
        to a simple Pandas DataFrame with precomputed features and labels if desired.

        Parameters
        ----------
        source_data : ``Dataset`` or a Pandas DataFrame
            The data to be used to apply the model. If an instance of
            ``Dataset`` is given, will query the database for the inference data.
        partitions : list of ``PartitionIdentifier`` (default=None)
            Only xG values for the games in these partitions are returned. By default,
            xG values are computed for all games in the source data.
            If ``source_data`` is not a ``Dataset``, this argument will be ignored.

        Returns
        -------
        pd.DataFrame
            A dataframe with a column 'xG', containing the predicted xG value
            of each shot in the given data, indexed by (game_id, action_id, original_event_id) of
            the corresponding shot.

        Raises
        ------
        NotFittedError
            If the model hasn't been fit.
        """
        if not self._fitted:
            raise NotFittedError("Must fit model before predicting WP.")

        if isinstance(source_data, Dataset):
            df_source_data = source_data.transform(
                self.dataset_transformer,
                from_table="actions",
                to_table=None,
                partitions=partitions,
            )
            df_source_idx = pd.MultiIndex.from_frame(
                df_source_data[["game_id", "action_id", "original_event_id"]]
            )
        else:
            df_source_data = source_data.copy()
            df_source_idx = source_data.index
        assert df_source_data is not None

        xg = self.pipeline.predict_proba(df_source_data)[:, 1]
        return pd.DataFrame({"xG": xg}, index=df_source_idx)

    def save_model(self, filepath: Path | str, overwrite: bool = True) -> None:
        """Save the XGModel instance to disk.

        Parameters
        ----------
        filepath : Path (default=None):
            The filename to use for the saved model.
        overwrite : bool
            Whether to silently overwrite any existing file at the target
            location.

        Returns
        -------
        ``None``
        """
        # If file exists and should not be overwritten:
        if not overwrite and os.path.isfile(filepath):
            raise ValueError(
                'save_model got overwrite="False", but a file '
                f"({filepath}) exists already. No model was saved."
            )

        joblib.dump(self, filepath)


def load_model(path: str | Path) -> XGModel:
    """Load a saved XGModel.

    Parameters
    ----------
    path : string
        Filepath to load the model from.

    Returns
    -------
    ``socceraction.xg.XGModel`` instance.
    """
    return joblib.load(path)


class PenaltyXGModel(XGModel):
    """An xG model for penalties.

    This model gives each penalty and xG value of 0.792453
    """

    def __init__(self) -> None:
        dataset_transformer = ActionsToFeatures(
            xfns=[lab.goal_from_shot],
            mask_fn=create_shot_mask({"shot_penalty"}),
        )
        pipeline = Pipeline([])
        super().__init__(dataset_transformer, pipeline, column_descriptions={})
        self._fitted = True

    @override
    def train(
        self,
        source_data: Dataset | Features,
        partitions: list[PartitionIdentifier] | None = None,
    ) -> "PenaltyXGModel":
        return self

    @override
    def estimate(
        self, source_data: Dataset | Features, partitions: list[PartitionIdentifier] | None = None
    ) -> DataFrame[Any]:
        if isinstance(source_data, Dataset):
            df_source_data = source_data.transform(
                self.dataset_transformer,
                from_table="actions",
                to_table=None,
                partitions=partitions,
            )
        else:
            df_source_data = source_data.copy()
        assert df_source_data is not None

        xg = pd.DataFrame(index=df_source_data.index)
        xg["xG"] = 0.792453

        return xg


class FreekickXGModel(XGModel):
    """An xG model for set pieces.

    This model estimates the xG value of a direct free kicks based on the
    distance and angle to the goal using a logistic regression model.
    """

    def __init__(self) -> None:
        dataset_transformer = ActionsToFeatures(
            xfns=[fs.shot_dist, fs.shot_visible_angle, lab.goal_from_shot],
            mask_fn=create_shot_mask({"shot_freekick"}),
        )
        pipeline = self._build_pipeline()
        super().__init__(dataset_transformer, pipeline)

    def _build_pipeline(self) -> Pipeline:
        dist_colname = "dist_shot"
        angle_colname = "visible_angle_shot"
        dist_x_angle_colname = "dist_x_visible_angle_shot"

        self.column_descriptions = {
            dist_colname: "Distance to goal",
            angle_colname: "Angle to goal",
            dist_x_angle_colname: "Distance * angle to goal",
        }

        feature_pipeline = InteractionFeature([dist_colname, angle_colname], dist_x_angle_colname)
        preprocess_pipeline = simple_proc_for_linear_algoritms(
            numeric_features=[dist_colname, angle_colname, dist_x_angle_colname],
            categoric_features=[],
        )
        base_model = LogisticRegression(max_iter=10000, solver="lbfgs", fit_intercept=True)
        return make_pipeline(feature_pipeline, preprocess_pipeline, base_model)


class BasicOpenplayXGModel(XGModel):
    """An basic xG model for shots from open play.

    This model estimates the xG value of a shot based on the bodypart used
    to take the shot, the distance and angle to the goal, and the
    distance x angle interaction using a logistic regression model.
    """

    def __init__(self) -> None:
        dataset_transformer = ActionsToFeatures(
            xfns=[fs.shot_dist, fs.shot_visible_angle, fs.bodypart, lab.goal_from_shot],
            mask_fn=create_shot_mask({"shot"}),
        )
        pipeline = self._build_pipeline()
        super().__init__(dataset_transformer, pipeline)

    def _build_pipeline(self) -> Pipeline:
        bodypart_colname = "bodypart"
        dist_colname = "dist_shot"
        angle_colname = "visible_angle_shot"
        dist_x_angle_colname = "dist_x_visible_angle_shot"

        self.column_descriptions = {
            bodypart_colname: "Bodypart used for the shot (head, foot or other)",
            dist_colname: "Distance to goal",
            angle_colname: "Angle to goal",
            dist_x_angle_colname: "Distance * angle to goal",
        }

        feature_pipeline = InteractionFeature([dist_colname, angle_colname], dist_x_angle_colname)
        preprocess_pipeline = simple_proc_for_linear_algoritms(
            numeric_features=[dist_colname, angle_colname, dist_x_angle_colname],
            categoric_features=[bodypart_colname],
        )
        base_model = LogisticRegression(max_iter=10000, solver="lbfgs", fit_intercept=False)
        return make_pipeline(feature_pipeline, preprocess_pipeline, base_model)


class AdvancedOpenplayXGModel(XGModel):
    """An advanced xG model for shots from open play.

    This model estimates the xG value of a shot based on the bodypart used
    to take the shot, the distance and angle to the goal, ... using a xgboost model.

    This model requires more training data than the ``BasicOpenplayXGModel``.
    """

    def __init__(self) -> None:
        dataset_transformer = ActionsToFeatures(
            xfns=[fs.shot_dist, fs.shot_visible_angle, fs.bodypart, lab.goal_from_shot],
            mask_fn=create_shot_mask({"shot"}),
        )
        pipeline = self._build_pipeline()
        super().__init__(dataset_transformer, pipeline)

    def _build_pipeline(self) -> Pipeline:
        raise NotImplementedError("This model is not yet implemented.")


class StatsBombOpenplayXGModel(XGModel):
    """An advanced xG model for shots from open play that uses the detailed info in StatsBomb event data.

    This model estimates the xG value of a shot based on the bodypart used
    to take the shot, the distance and angle to the goal, ... using a xgboost model.

    """

    def __init__(self) -> None:
        dataset_transformer = ActionsToFeatures(
            xfns=[fs.shot_dist, fs.shot_visible_angle, fs.bodypart, lab.goal_from_shot],
            mask_fn=create_shot_mask({"shot"}),
        )
        pipeline = self._build_pipeline()
        super().__init__(dataset_transformer, pipeline)

    def _build_pipeline(self) -> Pipeline:
        raise NotImplementedError("This model is not yet implemented.")


class XGModelEnsemble(BaseEstimator):
    """An ensemble of xG models.

    This model is an ensemble of different xG models, each specialized for a specific type of shot.

    Parameters
    ----------
    models : list of ``XGModel`` instances
        The xG models to include in the ensemble.
    """

    def __init__(self, models: list[XGModel] | None = None) -> None:
        if models is None:
            models = [BasicOpenplayXGModel(), FreekickXGModel(), PenaltyXGModel()]
        self.models = models
        self.column_descriptions = {m.__class__.__name__: m.column_descriptions for m in models}

    def train(
        self,
        source_data: Dataset | Features,
        partitions: list[PartitionIdentifier] | None = None,
    ) -> "XGModelEnsemble":
        """See :meth:`XGModel.train`."""
        for model in self.models:
            model.train(source_data, target_colname, partitions)
        return self

    def validate(
        self,
        source_data: Dataset | Features,
        partitions: list[PartitionIdentifier] | None = None,
        bins: int | list[float] = 10,
        bin_strategy: Literal["quantile", "uniform"] = "quantile",
        plot: bool = True,
    ) -> dict:
        """See :meth:`XGModel.validate`."""
        results = {}
        for model in self.models:
            results.update(model.validate(source_data, partitions, bins, bin_strategy, plot))
        return results

    def estimate(
        self,
        source_data: Dataset | Features,
        partitions: list[PartitionIdentifier] | None = None,
    ) -> DataFrame[Any]:
        """See :meth:`XGModel.estimate`."""
        xg = []
        for model in self.models:
            xg.append(model.estimate(source_data, partitions))
        return pd.concat(xg).sort_index()


def create_shot_mask(
    shot_types: Set[str] = set({"shot", "shot_penalty", "shot_freekick"}),
    exclude_owngoals: bool = True,
) -> Callable[[Series[SPADLSchema]], bool]:
    """Return a boolean mask indicating which shots to handle.

    This method is used to filter out shots that should not be used
    for training, validation, or prediction. By default, it filters
    out own-goals only.

    Parameters
    ----------
    shot_types : set of strings (default={"shot", "shot_penalty", "shot_freekick"})
        The types of shots to include.
    exclude_owngoals : bool (default=True)
        Whether to exclude own-goals.

    Returns
    -------
    pd.Series -> bool
        A boolean mask indicating which shots to handle.
    """
    type_ids = [spadl.config.actiontypes.index(t) for t in shot_types]
    result_ids = [spadl.config.results.index(r) for r in ["fail", "success"]]

    def fn(action: Series[SPADLSchema]) -> bool:
        if exclude_owngoals:
            return (action.type_id in type_ids) and (action.result_id in result_ids)
        else:
            return action.type_id in type_ids

    return fn
