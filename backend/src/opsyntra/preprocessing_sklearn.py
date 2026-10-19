"""Raw Telco conversion and learned preprocessing, persisted inside each model."""

import numpy as np
import pandas as pd
from sklearn.base import BaseEstimator, TransformerMixin
from sklearn.compose import ColumnTransformer
from sklearn.impute import SimpleImputer
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder, StandardScaler
from sklearn.utils.validation import check_is_fitted

from .data_validation import FEATURE_COLUMNS, NUMERIC_COLUMNS

CATEGORICAL_COLUMNS = tuple(column for column in FEATURE_COLUMNS if column not in NUMERIC_COLUMNS)


class NormalizeTelcoInputs(TransformerMixin, BaseEstimator):
    """Validate predictors and convert raw values without learning statistics.

    Identifiers, labels and event metadata are never returned as predictors.
    Unknown categorical strings are supported by the downstream encoder.
    """

    def fit(self, X, y=None):
        self._normalize(X)
        self.feature_names_in_ = np.asarray(FEATURE_COLUMNS, dtype=object)
        self.n_features_in_ = len(FEATURE_COLUMNS)
        return self

    def transform(self, X):
        check_is_fitted(self, "feature_names_in_")
        return self._normalize(X)

    def get_feature_names_out(self, input_features=None):
        check_is_fitted(self, "feature_names_in_")
        return self.feature_names_in_.copy()

    @staticmethod
    def _normalize(X):
        if not isinstance(X, pd.DataFrame):
            raise ValueError("Predictor input must be a pandas DataFrame with named columns.")
        if X.empty or X.columns.duplicated().any():
            raise ValueError("Input must contain records and unique column names.")
        missing = sorted(set(FEATURE_COLUMNS) - set(X.columns))
        if missing:
            raise ValueError(f"Missing required predictor columns: {missing}")
        clean = pd.DataFrame(index=X.index)
        for column in FEATURE_COLUMNS:
            values = X[column].astype("string").str.strip().replace("", pd.NA)
            if column in NUMERIC_COLUMNS:
                converted = pd.to_numeric(values, errors="coerce")
                if (values.notna() & converted.isna()).any():
                    raise ValueError(f"{column}: invalid numeric value.")
                numeric = converted.to_numpy(dtype=float, na_value=np.nan)
                if np.isinf(numeric).any() or (numeric < 0).any():
                    raise ValueError(f"{column}: values must be finite and nonnegative, or missing.")
                if column in ("SeniorCitizen", "tenure"):
                    present = numeric[~np.isnan(numeric)]
                    if np.any(present % 1 != 0):
                        raise ValueError(f"{column}: values must be integers.")
                    if column == "SeniorCitizen" and not np.isin(present, [0, 1]).all():
                        raise ValueError("SeniorCitizen: values must be 0 or 1.")
                clean[column] = numeric
            else:
                # sklearn imputers expect np.nan rather than pandas' pd.NA.
                clean[column] = values.astype(object).where(values.notna(), np.nan)
        return clean


def build_preprocessor(*, scale_numeric: bool) -> ColumnTransformer:
    numeric_steps = [("imputer", SimpleImputer(strategy="median", keep_empty_features=True))]
    if scale_numeric:
        numeric_steps.append(("scaler", StandardScaler()))
    categorical = Pipeline([
        ("imputer", SimpleImputer(strategy="constant", fill_value="__MISSING__", keep_empty_features=True)),
        ("encoder", OneHotEncoder(handle_unknown="ignore", sparse_output=False)),
    ])
    return ColumnTransformer([
        ("numeric", Pipeline(numeric_steps), list(NUMERIC_COLUMNS)),
        ("categorical", categorical, list(CATEGORICAL_COLUMNS)),
    ], remainder="drop")
