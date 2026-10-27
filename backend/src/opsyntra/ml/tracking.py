"""Local MLflow evidence for complete pipelines; no tracking server is required."""

import uuid
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import mlflow
import mlflow.sklearn
from mlflow.models import infer_signature
from sklearn.metrics import ConfusionMatrixDisplay, PrecisionRecallDisplay, RocCurveDisplay

from opsyntra.data.validation import FEATURE_COLUMNS, TARGET


def tracking_uri(config):
    return "sqlite:///" + config.tracking_database.as_posix()


def run_description(config, report, name):
    result = report["candidates"][name]
    choice = "Selected: highest" if name == report["selected_model"] else "Compared using"
    return (f"{choice} validation {config.selection_metric}: {result['validation'][config.selection_metric]:.4f}. "
            f"Decision threshold {result['decision_threshold']:.6f}; validation F1 {result['validation']['f1']:.4f} "
            f"(native rule: {result['validation_default']['f1']:.4f}). "
            "Thresholds use validation only; test metrics are logged only for the selected model. "
            "The saved pipeline includes preprocessing and the decision rule.")


def log_summary_files(config, report):
    client = mlflow.MlflowClient(tracking_uri(config))
    for run_id in report["tracking"]["runs"].values():
        client.log_artifact(run_id, str(config.comparison_path), "comparison")
        client.log_artifact(run_id, str(config.report_path.parent / "model_selection.md"), "selection")


def save_plots(pipeline, X, y, destination, prefix, threshold_search=None):
    destination.mkdir(parents=True, exist_ok=True)
    probabilities = pipeline.predict_proba(X)[:, 1]
    plots = [
        ("roc", lambda ax: RocCurveDisplay.from_predictions(y, probabilities, ax=ax)),
        ("precision_recall", lambda ax: PrecisionRecallDisplay.from_predictions(y, probabilities, ax=ax)),
        ("confusion_matrix", lambda ax: ConfusionMatrixDisplay.from_predictions(
            y, pipeline.predict(X), labels=[0, 1], display_labels=["No", "Yes"], ax=ax, colorbar=False)),
    ]
    for name, draw in plots:
        fig, ax = plt.subplots(figsize=(7, 5))
        draw(ax)
        ax.set_title(f"{prefix.replace('_', ' ').title()} — {name.replace('_', ' ')}")
        fig.tight_layout()
        fig.savefig(destination / f"{prefix}_{name}.png", dpi=140)
        plt.close(fig)
    if threshold_search:
        fig, ax = plt.subplots(figsize=(7, 5))
        ax.plot(threshold_search["thresholds"], threshold_search["f1_scores"])
        ax.axvline(threshold_search["threshold"], color="seagreen", linestyle="--",
                   label=f"Selected: {threshold_search['threshold']:.4f}")
        ax.set(xlabel="Decision threshold", ylabel="Validation F1", title="Validation threshold selection")
        ax.legend()
        fig.tight_layout()
        fig.savefig(destination / "validation_threshold.png", dpi=140)
        plt.close(fig)


def record_experiments(config, config_path, fitted, report, partitions, manifest):
    """Record one comparable run per candidate, plus test evidence for the winner only."""
    previous_uri = mlflow.get_tracking_uri()
    if mlflow.active_run():
        raise RuntimeError("Finish the active MLflow run before running the training pipeline.")
    config.tracking_database.parent.mkdir(parents=True, exist_ok=True)
    config.tracking_artifacts.mkdir(parents=True, exist_ok=True)
    uri = tracking_uri(config)
    batch_id = uuid.uuid4().hex
    try:
        mlflow.set_tracking_uri(uri)
        experiment = mlflow.get_experiment_by_name(config.experiment_name)
        experiment_id = experiment.experiment_id if experiment else mlflow.create_experiment(
            config.experiment_name, artifact_location=config.tracking_artifacts.as_uri())
        report["tracking"] = {"uri": uri, "experiment_name": config.experiment_name,
                              "experiment_id": experiment_id, "batch_id": batch_id, "runs": {}}
        validation = partitions["validation"]
        X = validation.loc[:, list(FEATURE_COLUMNS)]
        y = validation[TARGET].map({"No": 0, "Yes": 1}).astype(int)
        for name, pipeline in fitted.items():
            result = report["candidates"][name]
            selected = name == report["selected_model"]
            with mlflow.start_run(experiment_id=experiment_id, run_name=name,
                                  description=run_description(config, report, name), tags={
                "engine": "sklearn", "batch_id": batch_id, "selected": str(selected).lower(),
                "model_version": result["model_version"], "fit_partition": "train",
                "selection_partition": "validation", "test_used_for_selection": "false",
                "dataset_sha256": manifest["source_sha256"],
                "split_sha256": manifest["membership_sha256"],
            }) as run:
                run_id = run.info.run_id
                report["tracking"]["runs"][name] = run_id
                result["mlflow_run_id"] = run_id
                mlflow.log_params({
                    "model": name, "seed": config.seed, "cv_folds": config.cv_folds,
                    "training_estimator_jobs": config.estimator_jobs,
                    "selection_metric": config.selection_metric, "threshold_metric": "validation_f1",
                    "decision_threshold": result["decision_threshold"],
                    "dataset_sha256": manifest["source_sha256"], "split_sha256": manifest["membership_sha256"],
                    "numeric_imputation": "median", "categorical_imputation": "__MISSING__",
                    "categorical_encoding": "OneHotEncoder(ignore unknown)",
                    "numeric_scaling": "StandardScaler" if name == "logistic_regression" else "none",
                    **{f"classifier.{k}": v for k, v in result["classifier_parameters"].items()},
                    **{f"rows.{k}": len(v) for k, v in partitions.items()},
                })
                for prefix, metrics in [("validation", result["validation"]),
                                        ("validation_default", result["validation_default"])]:
                    mlflow.log_metrics({f"{prefix}_{k}": v for k, v in metrics.items() if k != "confusion_matrix"})
                mlflow.log_metrics({k: result[k] for k in
                                    ("cv_mean_score", "cv_std_score", "training_seconds", "validation_seconds")})
                mlflow.log_dict(manifest, "data/split_manifest.json")
                mlflow.log_dict(report["provenance"], "data/provenance.json")
                mlflow.log_dict(result["preprocessing_configuration"], "preprocessing/configuration.json")
                mlflow.log_dict(result["cv_results"], "search/cv_results.json")
                mlflow.log_dict(result["threshold_selection"], "selection/threshold_search.json")
                mlflow.log_dict(result["validation"], "metrics/validation.json")
                mlflow.log_artifact(str(config_path), "configuration")
                mlflow.log_artifact(str(config.artifact_dir / result["artifact"]), "joblib")
                example = partitions["train"].loc[:, list(FEATURE_COLUMNS)].head(3).astype(str)
                info = mlflow.sklearn.log_model(
                    sk_model=pipeline, name="pipeline", input_example=example,
                    serialization_format="cloudpickle",
                    signature=infer_signature(example, pipeline.predict(example)),
                    code_paths=[str(Path(__file__).resolve().parents[1])],
                    pip_requirements=[f"{k}=={v}" for k, v in report["provenance"]["runtime"].items()
                                      if k not in ("python", "mlflow", "matplotlib") and v != "not installed"],
                )
                result["mlflow_model_uri"] = info.model_uri
                # Batch-specific directories prevent old test plots leaking into
                # a later non-selected candidate's run.
                plot_dir = config.report_path.parent / "plots" / batch_id / name
                result["plots_directory"] = plot_dir.relative_to(config.report_path.parent).as_posix()
                save_plots(pipeline, X, y, plot_dir, "validation",
                           None if name == "dummy" else result["threshold_selection"])
                if selected:
                    mlflow.log_metrics({f"test_{k}": v for k, v in report["selected_test_metrics"].items() if k != "confusion_matrix"})
                    mlflow.log_dict(report["selected_test_metrics"], "metrics/test.json")
                    test = partitions["test"]
                    save_plots(pipeline, test.loc[:, list(FEATURE_COLUMNS)],
                               test[TARGET].map({"No": 0, "Yes": 1}).astype(int), plot_dir, "test")
                    mlflow.log_dict({"model": name, "threshold": result["decision_threshold"],
                                     "rule": report["selection_rule"],
                                     "threshold_rule": report["provenance"]["decision_rule"]}, "selection/decision.json")
                mlflow.log_artifacts(str(plot_dir), "plots")
        # Attach the final comparison and explanation to every run in this batch.
        client = mlflow.MlflowClient()
        for run_id in report["tracking"]["runs"].values():
            client.log_dict(run_id, report, "comparison/model_training_report.json")
    finally:
        mlflow.set_tracking_uri(previous_uri)
