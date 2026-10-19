"""Raw customer inference through a complete saved Spark PipelineModel."""

from pathlib import Path

import pandas as pd
from pyspark.ml import PipelineModel


def load_spark_pipeline(path):
    model = PipelineModel.load(Path(path).resolve().as_uri())
    from .spark_pipeline import SparkChurnDecision, NormalizeSparkTelco
    if not isinstance(model.stages[0], NormalizeSparkTelco) or not isinstance(model.stages[-1], SparkChurnDecision):
        raise ValueError("Expected a complete Opsyntra Spark pipeline with a saved decision threshold.")
    return model


def predict_spark_frame(model, frame):
    columns = (["customerID"] if "customerID" in frame.columns else []) + ["churn_probability", "prediction", "model_version"]
    rows = model.transform(frame).select(*columns).collect()
    if not rows:
        raise ValueError("Input must contain customer records.")
    result = pd.DataFrame([row.asDict() for row in rows], columns=columns)
    if "customerID" in result:
        result = result.sort_values("customerID").reset_index(drop=True)
    return result
