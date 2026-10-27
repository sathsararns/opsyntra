"""Local HTTP interface to Opsyntra's existing reports and saved ML pipeline."""

import io
import json
import logging
import os
from functools import lru_cache
from pathlib import Path

import pandas as pd
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, ConfigDict, Field

from opsyntra.config import load_config, load_training_config
from opsyntra.data.validation import FEATURE_COLUMNS
from opsyntra.data.pipeline import sha256_file
from opsyntra.ml.inference import load_pipeline, predict_records

ROOT = Path(__file__).resolve().parents[3]
CONFIG_PATH = Path(os.environ.get("OPSYNTRA_CONFIG", ROOT / "config.yaml")).resolve()
logger = logging.getLogger(__name__)
MAX_BATCH_ROWS = 10_000


class CustomerRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")
    record: dict


class BatchRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")
    csv_text: str = Field(min_length=1, max_length=2_000_000)


def read_overview(config_path: Path = CONFIG_PATH) -> dict:
    data_config = load_config(config_path)
    training_config = load_training_config(config_path)
    quality = json.loads(data_config.report_path.read_text(encoding="utf-8"))
    training = json.loads(training_config.report_path.read_text(encoding="utf-8"))
    source_hash = sha256_file(data_config.raw_path)
    if source_hash != quality["source_sha256"] or source_hash != training["provenance"]["source_sha256"]:
        raise ValueError("Dataset and saved reports refer to different source data.")
    data = pd.read_csv(data_config.raw_path)
    data["TotalCharges"] = pd.to_numeric(data["TotalCharges"], errors="coerce")
    data["churned"] = data["Churn"].eq("Yes")
    tenure_groups = pd.cut(
        data["tenure"], bins=[-1, 12, 24, 36, 48, 60, 72],
        labels=["0–12", "13–24", "25–36", "37–48", "49–60", "61–72"],
    )

    def summarize(frame: pd.DataFrame, label: str) -> dict:
        churn_rate = float(frame["churned"].mean()) * 100 if len(frame) else 0
        return {
            "label": label, "customers": len(frame),
            "churn_rate": round(churn_rate, 2), "retention_rate": round(100 - churn_rate, 2),
            "monthly_charges": round(float(frame["MonthlyCharges"].mean()), 2) if len(frame) else 0,
        }

    return {
        "dataset": {
            "customers": len(data), "features": len(FEATURE_COLUMNS),
            "churned": int(data["churned"].sum()),
            "churn_rate": round(float(data["churned"].mean()) * 100, 2),
            "monthly_charges": round(float(data["MonthlyCharges"].mean()), 2),
        },
        "quality": quality,
        "model": {
            "name": training["selected_model"],
            "version": training["selected_model_version"],
            "metrics": training["selected_test_metrics"],
            "partition_rows": training["partition_rows"],
            "selection_metric": training["selection_metric"],
            "decision_threshold": training.get("decision_threshold", .5),
            "tracking": training.get("tracking"),
            "candidates": [
                {"name": name, "selected": name == training["selected_model"],
                 **{key: result[key] for key in ("best_parameters", "cv_mean_score", "training_seconds", "validation", "model_version")},
                 "decision_threshold": result.get("decision_threshold", .5),
                 "mlflow_run_id": result.get("mlflow_run_id")}
                for name, result in training["candidates"].items()
            ],
        },
        "tenure_segments": [summarize(frame, str(label)) for label, frame in data.groupby(tenure_groups, observed=True)],
        "contract_segments": [summarize(frame, str(label)) for label, frame in data.groupby("Contract", sort=True)],
        "source": "Local Telco Customer Churn dataset and saved evaluation reports",
    }


@lru_cache(maxsize=1)
def get_model():
    path = load_training_config(CONFIG_PATH).artifact_dir / "best_pipeline.joblib"
    pipeline = load_pipeline(path)
    # A small local request does not need parallel tree worker setup.
    classifier = pipeline.named_steps["classifier"]
    if hasattr(classifier, "estimator_"):
        classifier = classifier.estimator_.estimator
    if "n_jobs" in classifier.get_params():
        classifier.set_params(n_jobs=1)
    return pipeline


app = FastAPI(title="Opsyntra Local API", version="0.1.0")
from opsyntra.analytics.read_api import router as analytics_router
app.include_router(analytics_router)


@app.get("/api/health")
def health():
    try:
        model = get_model()
        return {"status": "ready", "model_version": model.opsyntra_metadata_["model_version"]}
    except (OSError, ValueError, ImportError) as error:
        logger.warning("Model unavailable: %s", error)
        raise HTTPException(503, "Train the model before starting predictions.") from error


@app.get("/api/overview")
def overview():
    try:
        result = read_overview()
        model = get_model()
        if model.opsyntra_metadata_["model_version"] != result["model"]["version"]:
            raise HTTPException(503, "Restart the local API to load the newly trained model.")
        result["prediction_ready"] = True
        result["loaded_model_version"] = model.opsyntra_metadata_["model_version"]
        return result
    except (OSError, ValueError, KeyError, ImportError) as error:
        logger.warning("Project data unavailable: %s", error)
        raise HTTPException(503, "Run the data and training pipelines to prepare your workspace.") from error


def run_predictions(data: pd.DataFrame) -> dict:
    try:
        model = get_model()
    except (OSError, ValueError, ImportError) as error:
        raise HTTPException(503, "The saved prediction model is unavailable.") from error
    try:
        results = predict_records(model, data)
    except (ValueError, TypeError) as error:
        raise HTTPException(422, str(error)) from error
    return {
        "predictions": json.loads(results.to_json(orient="records", double_precision=15)),
        "model_version": model.opsyntra_metadata_["model_version"],
        "summary": {
            "total": len(results), "predicted_churn": int(results["prediction"].eq("Yes").sum()),
            "average_probability": float(results["churn_probability"].mean()),
        },
    }


@app.post("/api/predict")
def predict(request: CustomerRequest):
    return run_predictions(pd.DataFrame([request.record]))


@app.post("/api/predict/batch")
def predict_batch(request: BatchRequest):
    if len(request.csv_text.encode("utf-8")) > 2_000_000:
        raise HTTPException(413, "Choose a CSV smaller than 2 MB.")
    try:
        data = pd.read_csv(io.StringIO(request.csv_text), dtype="string", keep_default_na=False)
    except (pd.errors.ParserError, pd.errors.EmptyDataError, UnicodeError) as error:
        raise HTTPException(422, "The uploaded file is not a readable customer CSV.") from error
    if not 1 <= len(data) <= MAX_BATCH_ROWS:
        raise HTTPException(422, "Upload between 1 and 10,000 customer rows.")
    return run_predictions(data)
