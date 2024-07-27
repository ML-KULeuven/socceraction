"""Custom transformers for data preprocessing.

This module provides a collection of custom transformers for data preprocessing,
designed to be used with scikit-learn's pipeline and transformer interfaces.
"""

from typing import Any, Optional

import pandas as pd
from sklearn.base import BaseEstimator, TransformerMixin


class ColumnsSelector(BaseEstimator, TransformerMixin):
    """Transformer that selects specified columns from a DataFrame.

    Parameters
    ----------
    columns : list of str
        The columns to be selected.
    """

    def __init__(self, columns: list[str]) -> None:
        assert isinstance(columns, list)
        self.columns = columns

    def fit(self, X: pd.DataFrame, y: Optional[Any] = None) -> "ColumnsSelector":
        """Fit the transformer on the DataFrame.

        Parameters
        ----------
        X : pd.DataFrame
            Input DataFrame.
        y : None, optional
            Ignored.

        Returns
        -------
        self : ColumnsSelector
            Fitted transformer.
        """
        return self

    def transform(self, X: pd.DataFrame) -> pd.DataFrame:
        """Transform the DataFrame by selecting the specified columns.

        Parameters
        ----------
        X : pd.DataFrame
            Input DataFrame.

        Returns
        -------
        pd.DataFrame
            Transformed DataFrame with only the selected columns.
        """
        return X[self.columns]


class AsString(BaseEstimator, TransformerMixin):
    """Transformer that converts all values in a DataFrame to strings."""

    def fit(self, X: pd.DataFrame, y: Optional[Any] = None) -> "AsString":
        """Fit the transformer on the DataFrame.

        Parameters
        ----------
        X : pd.DataFrame
            Input DataFrame.
        y : None, optional
            Ignored.

        Returns
        -------
        self : AsString
            Fitted transformer.
        """
        return self

    def transform(self, X: pd.DataFrame) -> pd.DataFrame:
        """Transform the DataFrame by converting all values to strings.

        Parameters
        ----------
        X : pd.DataFrame
            Input DataFrame.

        Returns
        -------
        pd.DataFrame
            Transformed DataFrame with all values as strings.
        """
        return X.astype(str)


class OrdinalEncoder(BaseEstimator, TransformerMixin):
    """Encode categorical values by natural numbers based on alphabetical order.

    N/A values are encoded to -2 and rare values to -1.
    Very similar to TolerantLabelEncoder.

    Parameters
    ----------
    min_support : int
        The minimum support threshold for encoding a category.
    """

    def __init__(self, min_support: int) -> None:
        self.min_support = min_support
        self.vc: dict[str, pd.Series] = {}

    def _mapping(self, vc: pd.Series) -> dict[str, int]:
        """Create a mapping for encoding the categories.

        Parameters
        ----------
        vc : pd.Series
            Value counts of a column.

        Returns
        -------
        mapping : dict
            Mapping of category to encoded value.
        """
        mapping = {}
        for i, v in enumerate(vc[vc >= self.min_support].index):
            mapping[v] = i
        for v in vc.index[vc < self.min_support]:
            mapping[v] = -1
        mapping["nan"] = -2
        return mapping

    def _transform_column(self, x: pd.Series) -> pd.DataFrame:
        """Transform a single column using the encoding mapping.

        Parameters
        ----------
        x : pd.Series
            Input column.

        Returns
        -------
        pd.DataFrame
            Transformed column as a DataFrame.
        """
        x = x.astype(str)
        vc = self.vc[x.name]

        mapping = self._mapping(vc)

        output = pd.DataFrame()
        output[x.name] = x.map(lambda a: mapping[a] if a in mapping else -3)
        output.index = x.index
        return output.astype(int)

    def fit(self, X: pd.DataFrame, y: Optional[Any] = None) -> "OrdinalEncoder":
        """Fit the transformer on the DataFrame.

        Parameters
        ----------
        X : pd.DataFrame
            Input DataFrame.
        y : None, optional
            Ignored.

        Returns
        -------
        self : OrdinalEncoder
            Fitted transformer.
        """
        X = X.astype(str)
        self.vc = {c: X[c].value_counts() for c in X.columns}
        return self

    def transform(self, X: pd.DataFrame) -> pd.DataFrame:
        """Transform the DataFrame using the fitted encoder.

        Parameters
        ----------
        X : pd.DataFrame
            Input DataFrame.

        Returns
        -------
        pd.DataFrame
            Transformed DataFrame with encoded categorical values.
        """
        if len(X[X.index.duplicated()]):
            print(X[X.index.duplicated()].index)
            raise ValueError("Input contains duplicate index")
        dfs = [self._transform_column(X[c]) for c in X.columns]
        out = pd.DataFrame(index=X.index)
        for df in dfs:
            out = out.join(df)
        return out.values


class InteractionFeature(BaseEstimator, TransformerMixin):
    """Transformer that creates an interaction feature by multiplying specified columns.

    Parameters
    ----------
    columns_to_multiply : list of str
        List of column names to be multiplied together.
    new_column_name : str, optional
        Name of the new column. If not provided, the name will be generated by joining the column names with '_x_'.
    """

    def __init__(
        self, columns_to_multiply: list[str], new_column_name: Optional[str] = None
    ) -> None:
        self.columns_to_multiply = columns_to_multiply
        if new_column_name is None:
            self.new_column_name = "_x_".join(columns_to_multiply)
        else:
            self.new_column_name = new_column_name

    def fit(self, X: pd.DataFrame, y: Optional[Any] = None) -> "InteractionFeature":
        """
        Fit the transformer on the DataFrame.

        Parameters
        ----------
        X : pd.DataFrame
            Input DataFrame.
        y : None, optional
            Ignored.

        Returns
        -------
        self : InteractionFeature
            Fitted transformer.
        """
        return self

    def transform(self, X: pd.DataFrame) -> pd.DataFrame:
        """
        Transform the DataFrame by creating the interaction feature.

        Parameters
        ----------
        X : pd.DataFrame
            Input DataFrame.

        Returns
        -------
        pd.DataFrame
            Transformed DataFrame with the interaction feature added.
        """
        X_copy = X.copy()
        X_copy[self.new_column_name] = X_copy[self.columns_to_multiply[0]]

        for col in self.columns_to_multiply[1:]:
            X_copy[self.new_column_name] *= X_copy[col]

        return X_copy
