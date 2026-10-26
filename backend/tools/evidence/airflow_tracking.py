"""Record a successful Airflow run in the host's existing MLflow store."""
import argparse
import json
import hashlib
import sys
import zipfile
from pathlib import Path, PurePosixPath

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "src"))


def track(engine):
    import mlflow
    from opsyntra.airflow_workflow import WorkflowContext
    from opsyntra.config import load_training_config
    from opsyntra.experiment_tracking import tracking_uri
    from opsyntra.orchestration_models import filesystem_path, model_digest

    evidence = json.loads((ROOT / ".airflow-runtime" / f"last_success_{engine}.json").read_text())
    if evidence["state"] != "success" or any(t["state"] != "success" for t in evidence["tasks"]):
        raise ValueError("Only a complete successful Airflow run may be logged")
    ctx = WorkflowContext(ROOT, evidence["run_id"], engine)
    summary = ctx.read("results/summary.json")
    evaluation = ctx.read("evaluation.json")
    if (ctx.run / "mlflow.json").is_file():
        return ctx.read("mlflow.json")
    config = load_training_config(ROOT / "config.yaml")
    previous = mlflow.get_tracking_uri()
    try:
        mlflow.set_tracking_uri(tracking_uri(config))
        experiment = mlflow.get_experiment_by_name("Opsyntra-Airflow")
        experiment_id = experiment.experiment_id if experiment else mlflow.create_experiment(
            "Opsyntra-Airflow", artifact_location=config.tracking_artifacts.as_uri())
        with mlflow.start_run(experiment_id=experiment_id, run_name=f"airflow_{engine}", tags={
            "dag_id": summary["dag_id"], "dag_run_id": summary["run_id"], "engine": engine,
            "dag_state": "success", "test_used_for_selection": "false",
        }) as parent:
            mlflow.log_dict(evidence, "workflow/airflow_run.json")
            mlflow.log_dict(summary, "workflow/results.json")
            mlflow.log_params({"selected_model": summary["selected_model"], "decision_threshold": summary["decision_threshold"],
                               "dataset_sha256": summary["source_sha256"], "split_sha256": summary["membership_sha256"]})
            mlflow.log_metrics({f"test_{k}": v for k, v in summary["test_metrics"].items() if k != "confusion_matrix"})
            mlflow.log_artifacts(str(ctx.run / "logs"), "workflow/task_logs")
            for name in ("approval.json", "selected_model.json", "training.json", "evaluation.json", "config.yaml", "snapshot.json"):
                mlflow.log_artifact(str(ctx.run / name), "workflow")
            mlflow.log_artifacts(str(ctx.run / "results"), "results")
            runs = {}
            for name, candidate in evaluation["candidates"].items():
                with mlflow.start_run(experiment_id=experiment_id, run_name=f"{engine}_{name}", nested=True,
                                      tags={"engine": engine, "dag_run_id": summary["run_id"],
                                            "selected": str(name == summary["selected_model"]).lower()}) as child:
                    runs[name] = child.info.run_id
                    mlflow.log_params({**candidate["parameters"], "decision_threshold": candidate["decision_threshold"],
                                       "model_version": candidate["model_version"], "dataset_sha256": summary["source_sha256"],
                                       "split_sha256": summary["membership_sha256"]})
                    mlflow.log_metrics({f"validation_{k}": v for k, v in candidate["validation"].items() if k != "confusion_matrix"})
                    mlflow.log_metrics({"training_seconds": candidate["training_seconds"],
                                        "cv_mean_average_precision": candidate["cv_mean_average_precision"]})
                    if name == summary["selected_model"]:
                        mlflow.log_metrics({f"test_{k}": v for k, v in summary["test_metrics"].items() if k != "confusion_matrix"})
                    mlflow.log_dict(candidate, "evaluation.json")
                    path = filesystem_path(ctx.resolve(candidate["path"]))
                    if model_digest(path) != candidate["sha256"]:
                        raise ValueError("Model changed before MLflow logging")
                    if path.is_dir():
                        # Windows CopyFile2 rejects deeply nested Spark paths. A ZIP
                        # retains the exact native pipeline tree without long copies.
                        archive = ctx.run / "tracking" / f"{name}_pipeline.zip"
                        archive.parent.mkdir(exist_ok=True)
                        with zipfile.ZipFile(archive, "w", compression=zipfile.ZIP_DEFLATED) as bundle:
                            for file in sorted(p for p in path.rglob("*") if p.is_file()):
                                bundle.write(file, file.relative_to(path).as_posix())
                        with zipfile.ZipFile(archive) as bundle:
                            if bundle.testzip() is not None:
                                raise ValueError("Native Spark pipeline archive is corrupt")
                            digest = hashlib.sha256()
                            for member in sorted(bundle.namelist(), key=lambda name: PurePosixPath(name).parts):
                                digest.update(member.encode())
                                digest.update(bundle.read(member))
                            if digest.hexdigest() != candidate["sha256"]:
                                raise ValueError("Native Spark pipeline archive is incomplete")
                        mlflow.log_artifact(str(archive), "pipeline")
                        from opsyntra.spark_tracking import log_source_files
                        log_source_files(mlflow.MlflowClient(), child.info.run_id, ROOT)
                    else:
                        mlflow.log_artifact(str(path), "pipeline")
            result = {"experiment_name": "Opsyntra-Airflow", "experiment_id": experiment_id,
                      "workflow_run_id": parent.info.run_id, "candidate_runs": runs, "dag_run_id": summary["run_id"]}
        ctx.write("mlflow.json", result)
        return result
    finally:
        mlflow.set_tracking_uri(previous)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--engine", choices=["sklearn", "spark"], default="sklearn")
    args = parser.parse_args()
    print(json.dumps(track(args.engine)))
