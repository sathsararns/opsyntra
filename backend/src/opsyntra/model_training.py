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
from sklearn.model_selection import GridSearchCV, StratifiedKFold, FixedThresholdClassifier
from sklearn.frozen import FrozenEstimator

from .config import load_training_config
from .data_pipeline import sha256_file, write_json
from .training_data import load_training_partitions
from .data_validation import FEATURE_COLUMNS, NUMERIC_COLUMNS, TARGET
from .model_building import build_model_pipeline
from .model_evaluation import evaluate_model, select_threshold
from .experiment_tracking import record_experiments, log_summary_files

logger = logging.getLogger(__name__)


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
        training_seconds = round(time.perf_counter() - started, 4)
        if name == "random_forest":
            # Stable sums at the threshold boundary, including after serialization
            # and in the single-threaded API. CV fitting still uses configured jobs.
            fitted[name].named_steps["classifier"].set_params(n_jobs=1)
        evaluation_started = time.perf_counter()
        default_metrics = evaluate_model(fitted[name], X_validation, y_validation)
        classifier_parameters = fitted[name].named_steps["classifier"].get_params(deep=False)
        # XGBoost's default missing=np.nan is configuration, not a JSON metric.
        classifier_parameters = {key: str(value) if isinstance(value, float) and not np.isfinite(value) else value
                                 for key, value in classifier_parameters.items()}
        if name == "dummy":
            threshold_search = {"threshold": .5, "policy": "Keep most-frequent baseline unchanged"}
        else:
            threshold_search = select_threshold(y_validation, fitted[name].predict_proba(X_validation)[:, 1])
            # Freeze the already-fitted classifier. This step never fits on validation data.
            classifier = FixedThresholdClassifier(
                FrozenEstimator(fitted[name].named_steps["classifier"]),
                threshold=threshold_search["threshold"], response_method="predict_proba",
            )
            classifier.fit(fitted[name][:-1].transform(X_train), y_train)
            fitted[name].steps[-1] = ("classifier", classifier)
        preprocessor = fitted[name].named_steps["preprocessing"]
        results[name] = {
            "best_parameters": search.best_params_,
            "cv_mean_score": float(search.best_score_),
            "cv_std_score": float(search.cv_results_["std_test_score"][search.best_index_]),
            "search_candidates": len(search.cv_results_["params"]),
            "training_seconds": training_seconds,
            "validation": evaluate_model(fitted[name], X_validation, y_validation),
            "validation_default": default_metrics,
            "validation_seconds": round(time.perf_counter() - evaluation_started, 4),
            "decision_threshold": threshold_search["threshold"],
            "threshold_selection": threshold_search,
            "classifier_parameters": classifier_parameters,
            "cv_results": {"parameters": search.cv_results_["params"],
                           "mean_scores": search.cv_results_["mean_test_score"].tolist(),
                           "std_scores": search.cv_results_["std_test_score"].tolist()},
            "preprocessing_configuration": {
                "definition": repr(fitted[name][:-1]),
                "feature_columns": list(FEATURE_COLUMNS),
                "encoded_feature_names": preprocessor.get_feature_names_out().tolist(),
                "numeric_medians": preprocessor.named_transformers_["numeric"].named_steps["imputer"].statistics_.tolist(),
                "categorical_values": [values.tolist() for values in
                    preprocessor.named_transformers_["categorical"].named_steps["encoder"].categories_],
            },
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
    for package in ("pandas", "numpy", "scikit-learn", "joblib", "xgboost", "scipy", "mlflow", "matplotlib"):
        try:
            versions[package] = importlib.metadata.version(package)
        except importlib.metadata.PackageNotFoundError:
            versions[package] = "not installed"
    provenance = {
        "source_sha256": data_manifest["source_sha256"],
        "membership_sha256": data_manifest["membership_sha256"],
        "seed": config.seed, "cv_folds": config.cv_folds,
        "selection_metric": config.selection_metric,
        "decision_rule": "Predict Yes when probability >= validation-F1 threshold. Ties prefer closest to 0.5, then higher threshold. Dummy keeps its native baseline rule.",
        "threshold_selection_partition": "validation",
        "positive_class": 1, "target_mapping": {"No": 0, "Yes": 1},
        "feature_columns": list(FEATURE_COLUMNS), "runtime": versions,
        "fit_partition": "train", "selection_partition": "validation",
    }
    for name, pipeline in fitted.items():
        identity = {
            **provenance, "model_name": name,
            "decision_threshold": results[name]["decision_threshold"],
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
        "decision_threshold": results[selected_name]["decision_threshold"],
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
        "metric_note": "F1/precision/recall refer to churn=Yes. Average precision is the non-interpolated PR summary, not trapezoidal PR-AUC. Validation metrics are used for selection and are optimistic; test metrics are the held-out assessment.",
    }
    record_experiments(config, config_path, fitted, report, partitions, data_manifest)
    write_json(config.artifact_dir / "model_metadata.json", report)
    write_json(config.report_path, report)
    comparison = []
    for name, result in results.items():
        comparison.append({
            "model": name, "selected": name == selected_name,
            "cv_mean_score": result["cv_mean_score"], "training_seconds": result["training_seconds"],
            "decision_threshold": result["decision_threshold"], "mlflow_run_id": result["mlflow_run_id"],
            **{f"validation_{key}": value for key, value in result["validation"].items() if key != "confusion_matrix"},
        })
    config.comparison_path.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(comparison).to_csv(config.comparison_path, index=False, lineterminator="\n")
    write_selection_explanation(config, report)
    log_summary_files(config, report)
    logger.info("Selected %s; saved complete pipeline to %s", selected_name, selected_path)
    logger.info("Test accuracy %.4f; recall %.4f; F1 %.4f; ROC-AUC %.4f", test_metrics["accuracy"], test_metrics["recall"], test_metrics["f1"], test_metrics["roc_auc"])
    return report


def write_selection_explanation(config, report):
    selected = report["candidates"][report["selected_model"]]
    lines = ["# Opsyntra model selection", "",
             f"Selected **{report['selected_model']}**, version `{report['selected_model_version']}`.", "",
             "Hyperparameters were tuned with stratified training-only cross-validation. "
             "Models were ranked by validation average precision (AP). Each non-dummy model's "
             "threshold maximizes validation F1; ties prefer proximity to 0.5, then a higher threshold. "
             "The dummy keeps its most-frequent baseline rule. The test set was used only after selection.", "",
             "| Model | Validation AP | Threshold | Precision | Recall | F1 |", "|---|---:|---:|---:|---:|---:|"]
    for name, result in report["candidates"].items():
        m = result["validation"]
        lines.append(f"| {name} | {m['average_precision']:.4f} | {result['decision_threshold']:.6f} | {m['precision']:.4f} | {m['recall']:.4f} | {m['f1']:.4f} |")
    lines += ["", f"The selected threshold is **{selected['decision_threshold']:.6f}**. "
              f"Validation F1 changes from {selected['validation_default']['f1']:.4f} at the native decision rule "
              f"to {selected['validation']['f1']:.4f}. This prioritizes balancing precision and recall; "
              "it is not a business-cost optimization. Revisit the objective when retention costs are known.", "",
              "Validation scores are selection evidence and can be optimistic. Final held-out test metrics:", ""]
    for key, value in report["selected_test_metrics"].items():
        lines.append(f"- {key}: {value if isinstance(value, list) else f'{value:.6f}'}")
    lines += ["", "Average precision summarizes the precision–recall curve; it is not trapezoidal PR-AUC.", "",
              f"Dataset SHA256: `{report['provenance']['source_sha256']}`.",
              f"Split SHA256: `{report['provenance']['membership_sha256']}`.",
              f"MLflow experiment: `{report['tracking']['experiment_name']}`. Batch: `{report['tracking']['batch_id']}`.",
              f"Selected run: `{selected['mlflow_run_id']}`.", "",
              f"Plots: `{selected['plots_directory']}/`. UI screenshots: `../../docs/screenshots/mlflow-*.png`.", ""]
    (config.report_path.parent / "model_selection.md").write_text("\n".join(lines), encoding="utf-8")
