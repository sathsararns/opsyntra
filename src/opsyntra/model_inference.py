"""Load a complete fitted pipeline and predict raw customer records."""

from pathlib import Path

import joblib
import pandas as pd
from sklearn.pipeline import Pipeline
from sklearn.utils.validation import check_is_fitted


def load_pipeline(path: Path) -> Pipeline:
    pipeline = joblib.load(path)
    if not isinstance(pipeline, Pipeline) or not hasattr(pipeline, "opsyntra_metadata_"):
        raise ValueError("Expected an Opsyntra fitted sklearn Pipeline artifact.")
    check_is_fitted(pipeline)
    if list(pipeline.classes_) != [0, 1]:
        raise ValueError("Artifact must use binary churn labels [0, 1].")
    return pipeline


def predict_records(pipeline: Pipeline, records) -> pd.DataFrame:
    if isinstance(records, dict):
        records = [records]
    data = records.copy() if isinstance(records, pd.DataFrame) else pd.DataFrame(records)
    probabilities = pipeline.predict_proba(data)[:, 1]
    predictions = pipeline.predict(data)
    output = pd.DataFrame(index=range(len(data)))
    if "customerID" in data:
        output["customerID"] = data["customerID"].astype("string").str.strip().to_numpy()
    output["churn_probability"] = probabilities
    output["prediction"] = ["Yes" if value == 1 else "No" for value in predictions]
    output["model_version"] = pipeline.opsyntra_metadata_["model_version"]
    return output
