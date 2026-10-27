"""Load one complete selected model once per Kafka consumer process."""
import json
from pathlib import Path

from opsyntra.data.validation import FEATURE_COLUMNS
from opsyntra.orchestration.models import model_digest


class PredictionModel:
    def __init__(self, settings, engine="sklearn", model_path=None, runtime_directory=None):
        self.engine, self.spark = engine, None
        configuration = settings["model_config"]
        if engine == "sklearn":
            from opsyntra.ml.inference import load_pipeline
            self.path = Path(model_path or configuration["sklearn_model"]).resolve()
            self.model = load_pipeline(self.path)
            self.version = self.model.opsyntra_metadata_["model_version"]
        elif engine == "spark":
            if model_path is None:
                pointer = json.loads(configuration["spark_pointer"].read_text())
                model_path = configuration["backend"] / pointer["pipeline_path"]
            self.path = Path(model_path).resolve()
            from opsyntra.spark.runtime import create_spark
            from opsyntra.spark.inference import load_spark_pipeline
            self.spark = create_spark(configuration["spark_settings"], Path(runtime_directory))
            try:
                self.model = load_spark_pipeline(self.path)
                self.version = self.model.stages[-1].getOrDefault("modelVersion")
            except BaseException:
                self.spark.stop()
                raise
        else:
            raise ValueError("engine must be sklearn or spark")
        self.sha256 = model_digest(self.path)

    def predict(self, events):
        # An explicit feature list keeps labels, IDs and metadata outside inference.
        if self.engine == "sklearn":
            from opsyntra.ml.inference import predict_records
            frame = predict_records(self.model, [{name: event[name] for name in FEATURE_COLUMNS} for event in events])
        else:
            from pyspark.sql.types import StringType, StructField, StructType
            from opsyntra.spark.inference import predict_spark_frame
            columns = ["customerID", *FEATURE_COLUMNS]
            rows = [(f"INFERENCE-{index:012d}", *[None if event[name] is None else str(event[name]) for name in FEATURE_COLUMNS])
                    for index, event in enumerate(events)]
            schema = StructType([StructField(name, StringType(), True) for name in columns])
            frame = predict_spark_frame(self.model, self.spark.createDataFrame(rows, schema))
        return frame.to_dict(orient="records")

    def close(self):
        if self.spark is not None:
            self.spark.stop()
