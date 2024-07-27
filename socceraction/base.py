"""Base classes for all estimators and various utility functions."""

from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional

from pandera.typing import DataFrame

from socceraction.data.dataset import Dataset, PartitionIdentifier
from socceraction.data.transforms import Transform


class BaseEstimator(ABC):
    """Base class for all action value estimation models in socceraction."""

    def __init__(
        self,
        dataset_transformer: Transform,
        column_descriptions: Optional[dict[str, str]] = None,
    ) -> None:
        self.dataset_transformer = dataset_transformer
        self.column_descriptions = column_descriptions

        self._fitted = False
        self._training_partitions = None
        self._validation_partitions = None

    @property
    def training_partitions(self) -> Optional[list[PartitionIdentifier]]:
        return self._training_partitions

    @property
    def validation_partitions(self) -> Optional[list[PartitionIdentifier]]:
        return self._validation_partitions

    def _prepare_and_check_source_data(
        self,
        source_data: Dataset | DataFrame[Any],
        partitions: Optional[list[PartitionIdentifier]] = None,
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
        return df_source_data

    @abstractmethod
    def train(
        self,
        source_data: Dataset | DataFrame[Any],
        partitions: Optional[list[PartitionIdentifier]] = None,
    ) -> "BaseEstimator":
        """Train the model."""

    @abstractmethod
    def validate(
        self,
        source_data: Dataset | DataFrame[Any],
        partitions: Optional[list[PartitionIdentifier]] = None,
    ) -> dict:
        """Validate the model."""

    @abstractmethod
    def estimate(
        self,
        source_data: Dataset | DataFrame[Any],
        partitions: Optional[list[PartitionIdentifier]] = None,
    ) -> DataFrame[Any]:
        """Estimate the value of actions in a set of games."""
