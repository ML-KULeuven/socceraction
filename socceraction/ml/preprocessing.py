"""Preprocessing pipelines for machine learning models.

This module provides functions to create preprocessing pipelines for
scikit-learn-based modeling pipelines. The pipelines handle both numeric and
categorical features, applying appropriate transformations for each type.
"""

from typing import Union

from sklearn.impute import SimpleImputer
from sklearn.pipeline import FeatureUnion, Pipeline, make_pipeline, make_union
from sklearn.preprocessing import OneHotEncoder

from .pipeline import AsString, ColumnsSelector, OrdinalEncoder


def simple_proc_for_tree_algorithms(
    numeric_features: list[str], categoric_features: list[str]
) -> Union[Pipeline, FeatureUnion]:
    """
    Create a simple preprocessing pipeline for tree-based algorithms.

    Parameters
    ----------
    numeric_features : list of str
        List of names of numeric features.
    categoric_features : list of str
        List of names of categorical features.

    Returns
    -------
    Union[Pipeline, FeatureUnion]
        A scikit-learn pipeline or feature union for preprocessing.

    Raises
    ------
    Exception
        If both variable lists are empty.
    """
    catpipe = make_pipeline(
        ColumnsSelector(categoric_features),
        # AsString(),
        OrdinalEncoder(min_support=5),
        # ColumnApplier(FillNaN('nan')),
        # ColumnApplier(TolerantLabelEncoder())
    )
    numpipe = make_pipeline(
        ColumnsSelector(numeric_features),
        SimpleImputer(strategy="mean"),
    )
    if numeric_features and categoric_features:
        return make_union(catpipe, numpipe)
    elif numeric_features:
        return numpipe
    elif categoric_features:
        return catpipe
    raise Exception("Both variable lists are empty")


def simple_proc_for_linear_algorithms(
    numeric_features: list[str], categoric_features: list[str]
) -> Union[Pipeline, FeatureUnion]:
    """
    Create a simple preprocessing pipeline for linear algorithms.

    Parameters
    ----------
    numeric_features : list of str
        List of names of numeric features.
    categoric_features : list of str
        List of names of categorical features.

    Returns
    -------
    Union[Pipeline, FeatureUnion]
        A scikit-learn pipeline or feature union for preprocessing.

    Raises
    ------
    Exception
        If both variable lists are empty.
    """
    catpipe = make_pipeline(
        ColumnsSelector(categoric_features),
        AsString(),
        OneHotEncoder(),
        # ColumnApplier(FillNaN('nan')),
        # ColumnApplier(TolerantLabelEncoder())
    )
    numpipe = make_pipeline(
        ColumnsSelector(numeric_features),
        SimpleImputer(strategy="mean"),
        # StandardScaler(),
    )
    if numeric_features and categoric_features:
        return make_union(catpipe, numpipe)
    elif numeric_features:
        return numpipe
    elif categoric_features:
        return catpipe
    raise Exception("Both variable lists are empty")
