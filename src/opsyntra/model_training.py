"""Train on saved memberships, tune in folds, select on validation, assess test."""

import hashlib
import importlib.metadata
import json
import logging
import platform
import time
from pathlib import Path

import joblib
import numpy as np
import pandas as pd
from sklearn.model_selection import GridSearchCV, StratifiedKFold

from .config import load_config, load_training_config
from .data_pipeline import sha256_file, write_json
from .data_splitting import apply_memberships
from .data_validation import FEATURE_COLUMNS, NUMERIC_COLUMNS, TARGET
from .model_building import build_model_pipeline
from .model_evaluation import evaluate_model

logger = logging.getLogger(__name__)


def load_training_partitions(config_path: Path):
    config = load_config(config_path)
    manifest_path = config.output_dir / "manifest.json"
    if not manifest_path.is_file():
        raise FileNotFoundError("Run python -m pipelines.data_pipeline first.")
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    expected = [
        (config.raw_path, manifest["source_sha256"]),
        (config.normalized_path, manifest["normalized_sha256"]),
        (config.output_dir / "memberships.csv", manifest["membership_sha256"]),
    ]
    for path, digest in expected:
        if sha256_file(path) != digest:
            raise ValueError(f"Data checksum mismatch: {path}. Regenerate and review Step 1 artifacts.")
    data = pd.read_csv(config.normalized_path, dtype="string", keep_default_na=False)
    memberships = pd.read_csv(config.output_dir / "memberships.csv", dtype="string")
    if not data[TARGET].isin(["No", "Yes"]).all():
        raise ValueError("Training labels must be No or Yes.")
    partitions = apply_memberships(data, memberships)
    for name, part in partitions.items():
        if len(part) != manifest["splits"][name]["rows"]:
            raise ValueError(f"{name}: membership count differs from the saved manifest.")
    return partitions, manifest


def persist_pipeline(pipeline, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    joblib.dump(pipeline, temporary, compress=3)
    temporary.replace(path)


def run_training(config_path: Path) -> dict:
    config_path = Path(config_path)
    config = load_training_config(config_path)
    partitions, data_manifest = load_training_partitions(config_path)
    train, validation = partitions["train"], partitions["validation"]
    X_train = train.loc[:, list(FEATURE_COLUMNS)]
    y_train = train[TARGET].map({"No": 0, "Yes": 1}).astype(int)
    X_validation = validation.loc[:, list(FEATURE_COLUMNS)]
    y_validation = validation[TARGET].map({"No": 0, "Yes": 1}).astype(int)
    if y_train.value_counts().min() < config.cv_folds:
        raise ValueError("Training class counts are smaller than cv_folds.")
    # Construct every candidate up front so a missing dependency fails before fitting.
    candidates = {
        name: build_model_pipeline(name, seed=config.seed, jobs=config.estimator_jobs)
        for name in config.models
    }
    folds = StratifiedKFold(n_splits=config.cv_folds, shuffle=True, random_state=config.seed)
    fitted = {}
    results = {}
    for name, pipeline in candidates.items():
        logger.info("Training %s with %s-fold CV", name, config.cv_folds)
        started = time.perf_counter()
        search = GridSearchCV(
            pipeline, config.param_grids.get(name, {}), scoring=config.selection_metric,
            cv=folds, n_jobs=1, refit=True, error_score="raise", return_train_score=False,
        )
        search.fit(X_train, y_train)
        fitted[name] = search.best_estimator_
        results[name] = {
            "best_parameters": search.best_params_,
            "cv_mean_score": float(search.best_score_),
            "cv_std_score": float(search.cv_results_["std_test_score"][search.best_index_]),
            "search_candidates": len(search.cv_results_["params"]),
            "training_seconds": round(time.perf_counter() - started, 4),
            "validation": evaluate_model(search.best_estimator_, X_validation, y_validation),
        }
        logger.info("%s validation %s: %.4f", name, config.selection_metric, results[name]["validation"][config.selection_metric])
    selected_name = max(config.models, key=lambda name: results[name]["validation"][config.selection_metric])
    selected = fitted[selected_name]
    # Only the selected candidate is assessed on the held-out test partition.
    test = partitions["test"]
    test_metrics = evaluate_model(
        selected, test.loc[:, list(FEATURE_COLUMNS)], test[TARGET].map({"No": 0, "Yes": 1}).astype(int),
    )
    versions = {"python": platform.python_version()}
    for package in ("pandas", "numpy", "scikit-learn", "joblib", "xgboost", "scipy"):
        try:
            versions[package] = importlib.metadata.version(package)
        except importlib.metadata.PackageNotFoundError:
            versions[package] = "not installed"
    provenance = {
        "source_sha256": data_manifest["source_sha256"],
        "membership_sha256": data_manifest["membership_sha256"],
        "seed": config.seed, "cv_folds": config.cv_folds,
        "selection_metric": config.selection_metric,
        "decision_rule": "Use the fitted classifier predict method; no threshold tuning.",
        "positive_class": 1, "target_mapping": {"No": 0, "Yes": 1},
        "feature_columns": list(FEATURE_COLUMNS), "runtime": versions,
        "fit_partition": "train", "selection_partition": "validation",
    }
    for name, pipeline in fitted.items():
        identity = {
            **provenance, "model_name": name,
            "parameters": results[name]["best_parameters"], "pipeline_definition": repr(pipeline),
        }
        version = hashlib.sha256(json.dumps(identity, sort_keys=True).encode()).hexdigest()[:16]
        pipeline.opsyntra_metadata_ = {**identity, "model_version": version}
        artifact_path = config.artifact_dir / "candidates" / f"{name}.joblib"
        persist_pipeline(pipeline, artifact_path)
        results[name]["artifact"] = str(artifact_path.relative_to(config.artifact_dir))
        results[name]["artifact_sha256"] = sha256_file(artifact_path)
        results[name]["model_version"] = version
    selected_path = config.artifact_dir / "best_pipeline.joblib"
    persist_pipeline(selected, selected_path)
    # Inference must work after an actual serialization round trip.
    loaded = joblib.load(selected_path)
    loaded_probabilities = loaded.predict_proba(X_validation)
    original_probabilities = selected.predict_proba(X_validation)
    # Parallel tree reductions can differ at machine precision after reload.
    np.testing.assert_allclose(loaded_probabilities, original_probabilities, rtol=0, atol=1e-12)
    np.testing.assert_array_equal(loaded.predict(X_validation), selected.predict(X_validation))
    preprocessor = selected.named_steps["preprocessing"]
    numeric_imputer = preprocessor.named_transformers_["numeric"].named_steps["imputer"]
    report = {
        "status": "PASS", "selected_model": selected_name,
        "selected_model_version": selected.opsyntra_metadata_["model_version"],
        "selection_metric": config.selection_metric,
        "selection_rule": "Highest validation score after training-only cross-validation; configured model order breaks ties.",
        "test_used_for_selection": False,
        "partition_rows": {name: len(part) for name, part in partitions.items()},
        "candidates": results, "selected_test_metrics": test_metrics,
        "provenance": provenance,
        "preprocessing": {
            "numeric_imputation": "median fitted on training data only",
            "categorical_imputation": "constant __MISSING__",
            "unseen_categories": "OneHotEncoder(handle_unknown='ignore')",
            "numeric_scaling": "StandardScaler for Logistic Regression; omitted for trees and dummy",
            "selected_numeric_imputer_statistics": dict(zip(NUMERIC_COLUMNS, numeric_imputer.statistics_.tolist())),
            "selected_encoded_feature_count": len(preprocessor.get_feature_names_out()),
        },
        "artifact": {
            "filename": selected_path.name, "sha256": sha256_file(selected_path),
            "reload_probabilities_match": True, "reload_absolute_tolerance": 1e-12,
            "reload_max_absolute_difference": float(np.max(np.abs(loaded_probabilities - original_probabilities))),
            "reload_classifications_identical": True,
        },
        "confusion_matrix_labels": ["No", "Yes"],
        "metric_note": "F1/precision/recall refer to churn=Yes. Average precision is the non-interpolated PR summary, not trapezoidal PR-AUC.",
    }
    write_json(config.artifact_dir / "model_metadata.json", report)
    write_json(config.report_path, report)
    comparison = []
    for name, result in results.items():
        comparison.append({
            "model": name, "selected": name == selected_name,
            "cv_mean_score": result["cv_mean_score"], "training_seconds": result["training_seconds"],
            **{f"validation_{key}": value for key, value in result["validation"].items() if key != "confusion_matrix"},
        })
    config.comparison_path.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(comparison).to_csv(config.comparison_path, index=False, lineterminator="\n")
    logger.info("Selected %s; saved complete pipeline to %s", selected_name, selected_path)
    logger.info("Test accuracy %.4f; recall %.4f; F1 %.4f; ROC-AUC %.4f", test_metrics["accuracy"], test_metrics["recall"], test_metrics["f1"], test_metrics["roc_auc"])
    return report
