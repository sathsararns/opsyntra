"""Log native Spark artifacts without importing PySpark into the Windows host."""

import json
import hashlib
import zipfile
from pathlib import Path, PurePosixPath

import pandas as pd
import yaml

from opsyntra.config import load_training_config
from opsyntra.data.pipeline import write_json


def log_source_files(client, run_id, root):
    # Preserve the package layout, including aliases for historical stage paths.
    package = root / "src/opsyntra"
    for source in sorted(package.rglob("*.py")):
        relative = source.parent.relative_to(package)
        destination = "code/opsyntra"
        if relative.parts:
            destination += "/" + relative.as_posix()
        client.log_artifact(run_id, str(source), destination)
    for filename in ("data.txt", "spark.txt"):
        client.log_artifact(run_id, str(root / "requirements" / filename), "environment")


def archive_pipeline(pipeline_path, archive_path):
    """Keep every native model file while avoiding Windows CopyFile2 limits."""
    from opsyntra.orchestration.models import filesystem_path, model_digest
    pipeline_path, archive_path = Path(pipeline_path).resolve(), Path(archive_path).resolve()
    if archive_path.is_relative_to(pipeline_path):
        raise ValueError("Pipeline archive must live outside the native model directory")
    source = filesystem_path(pipeline_path)
    expected = model_digest(source)
    destination = filesystem_path(archive_path)
    destination.parent.mkdir(parents=True, exist_ok=True)
    files = sorted((p for p in source.rglob("*") if p.is_file()), key=lambda p: p.relative_to(source).parts)
    with zipfile.ZipFile(destination, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        for file in files:
            archive.write(file, file.relative_to(source).as_posix())
    with zipfile.ZipFile(destination) as archive:
        checksum = hashlib.sha256()
        for name in sorted(archive.namelist(), key=lambda name: PurePosixPath(name).parts):
            checksum.update(name.encode())
            checksum.update(archive.read(name))
        if checksum.hexdigest() != expected or model_digest(source) != expected:
            raise ValueError("Native pipeline archive is incomplete or its source changed")
    return {"format": "native_spark_pipeline_zip", "native_pipeline_sha256": expected,
            "archive_sha256": hashlib.sha256(destination.read_bytes()).hexdigest(), "files": len(files)}


def log_spark_runs(config_path):
    import mlflow
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    from sklearn.metrics import ConfusionMatrixDisplay, PrecisionRecallDisplay, RocCurveDisplay
    from opsyntra.ml.tracking import tracking_uri

    config_path = Path(config_path).resolve()
    root = config_path.parent
    settings = yaml.safe_load(config_path.read_text(encoding="utf-8"))["spark"]
    report_path = root / settings["report_path"]
    report = json.loads(report_path.read_text(encoding="utf-8"))
    config = load_training_config(config_path)
    if report.get("tracking", {}).get("runs"):
        client = mlflow.MlflowClient(tracking_uri(config))
        for run_id in report["tracking"]["runs"].values():
            log_source_files(client, run_id, root)
        return report["tracking"]
    previous = mlflow.get_tracking_uri()
    if mlflow.active_run():
        raise RuntimeError("Finish the active MLflow run before logging Spark runs.")
    try:
        mlflow.set_tracking_uri(tracking_uri(config))
        experiment = mlflow.get_experiment_by_name(settings["experiment_name"])
        experiment_id = experiment.experiment_id if experiment else mlflow.create_experiment(
            settings["experiment_name"], artifact_location=config.tracking_artifacts.as_uri())
        runs = {}
        artifacts = root / Path(report["selected_pipeline_path"]).parent
        for name, result in report["candidates"].items():
            selected = name == report["selected_model"]
            with mlflow.start_run(experiment_id=experiment_id, run_name=f"spark_{name}", tags={
                "engine": "pyspark", "batch_id": report["batch_id"], "model_name": name,
                "selected": str(selected).lower(), "model_version": result["model_version"],
                "test_used_for_selection": "false",
            }, description=(f"{'Selected by highest' if selected else 'Compared using'} validation average precision. "
                            f"Threshold {result['decision_threshold']:.6f} maximizes validation churn-class F1. "
                            "Native Spark pipeline includes cleaning, imputation, encoding, features, classifier and decision rule.")) as run:
                runs[name] = run.info.run_id
                mlflow.log_params({"model": name, "decision_threshold": result["decision_threshold"],
                                   "dataset_sha256": report["provenance"]["source_sha256"],
                                   "split_sha256": report["provenance"]["membership_sha256"],
                                   "selection_metric": "average_precision", "threshold_metric": "validation_f1",
                                   "seed": report["provenance"]["seed"], "cv_folds": report["provenance"]["cv_folds"],
                                   **result["parameters"]})
                mlflow.log_metrics({f"validation_{k}": v for k, v in result["validation"].items() if k != "confusion_matrix"})
                mlflow.log_metrics({**result["timing"], "cv_seconds": result["cv_seconds"]})
                mlflow.log_dict(result, "evaluation/candidate.json")
                mlflow.log_dict(report["provenance"], "data/provenance.json")
                mlflow.log_artifact(str(config_path), "configuration")
                archive = artifacts / "archives" / f"{name}_pipeline.zip"
                archive_info = archive_pipeline(root / result["pipeline_path"], archive)
                mlflow.log_artifact(str(archive), "spark_pipeline")
                mlflow.log_dict(archive_info, "spark_pipeline/manifest.json")
                mlflow.set_tag("pipeline_format", "native_spark_pipeline_zip")
                log_source_files(mlflow.MlflowClient(), run.info.run_id, root)
                validation = pd.read_csv(artifacts / f"{name}_validation_scores.csv")
                plot_directory = artifacts / "plots" / name
                plot_directory.mkdir(parents=True, exist_ok=True)
                subsets = [("validation", validation.label, validation.churn_probability)]
                if selected:
                    paired = pd.read_csv((root / settings["comparison_path"]).parent / "engine_test_predictions.csv")
                    subsets.append(("test", paired.Churn.map({"No": 0, "Yes": 1}), paired.spark_probability))
                    mlflow.log_metrics({f"test_{k}": v for k, v in report["selected_test_metrics"].items() if k != "confusion_matrix"})
                for prefix, labels, scores in subsets:
                    for title, draw in [
                        ("roc", lambda ax: RocCurveDisplay.from_predictions(labels, scores, ax=ax)),
                        ("precision_recall", lambda ax: PrecisionRecallDisplay.from_predictions(labels, scores, ax=ax)),
                        ("confusion_matrix", lambda ax: ConfusionMatrixDisplay.from_predictions(labels, (scores >= result["decision_threshold"]).astype(int), ax=ax, display_labels=["No", "Yes"], colorbar=False)),
                    ]:
                        figure, axis = plt.subplots(figsize=(7, 5))
                        draw(axis)
                        axis.set_title(f"Spark {name}: {prefix} {title}")
                        figure.tight_layout()
                        figure.savefig(plot_directory / f"{prefix}_{title}.png", dpi=140)
                        plt.close(figure)
                mlflow.log_artifacts(str(plot_directory), "plots")
                for path in [root / settings["comparison_path"], (root / settings["comparison_path"]).with_suffix(".md")]:
                    mlflow.log_artifact(str(path), "comparison")
        report["tracking"] = {"experiment_name": settings["experiment_name"], "experiment_id": experiment_id, "runs": runs}
        write_json(report_path, report)
        write_json(artifacts / "metadata.json", report)
        return report["tracking"]
    finally:
        mlflow.set_tracking_uri(previous)
