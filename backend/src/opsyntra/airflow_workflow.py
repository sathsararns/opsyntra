"""Eight file-based ML tasks; task retries share an immutable run snapshot."""

import hashlib
import json
import logging
import math
import re
import shutil
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import yaml

from .config import load_config
from .data_pipeline import sha256_file, write_json
from .data_splitting import apply_memberships
from .data_validation import read_raw_data, validate_and_normalize
from .orchestration_models import model_digest

TASKS = ("validate_data", "prepare_splits", "tune_and_train", "evaluate", "validate_candidate",
         "save_selected_model", "predict", "save_results")


def atomic_json(path, value):
    temporary = path.with_suffix(".tmp.json")
    write_json(temporary, value)
    temporary.replace(path)


def atomic_copy(source, destination):
    destination.parent.mkdir(parents=True, exist_ok=True)
    temporary = destination.with_suffix(destination.suffix + ".tmp")
    shutil.copyfile(source, temporary)
    temporary.replace(destination)


class WorkflowContext:
    def __init__(self, backend, run_id, engine="sklearn", prediction_input=None, config_path=None):
        if engine not in ("sklearn", "spark"):
            raise ValueError("engine must be sklearn or spark")
        self.backend = Path(backend).resolve()
        self.source_config = Path(config_path or self.backend / "config.yaml").resolve()
        self.source_settings = yaml.safe_load(self.source_config.read_text(encoding="utf-8"))
        self._policy = self.source_settings["orchestration"]
        self.engine, self.run_id = engine, run_id
        key = re.sub(r"[^A-Za-z0-9_.-]", "_", run_id)[:70].strip(".") or "run"
        key += "-" + hashlib.sha256(run_id.encode()).hexdigest()[:12]
        self.artifact_root = (self.backend / self.policy["artifact_dir"]).resolve()
        if not self.artifact_root.is_relative_to(self.backend) or self.artifact_root == self.backend:
            raise ValueError("orchestration.artifact_dir must be a project subdirectory")
        self.run = self.artifact_root / "runs" / engine / key
        self.run.mkdir(parents=True, exist_ok=True)
        self.config_path = self.run / "config.yaml"
        self.log = logging.getLogger("opsyntra.airflow")
        self.prediction_input = prediction_input or self.policy["prediction_input"]

    @property
    def policy(self):
        if hasattr(self, "config_path") and self.config_path.is_file():
            return self.settings["orchestration"]
        return self._policy

    @property
    def settings(self):
        return yaml.safe_load(self.config_path.read_text(encoding="utf-8"))

    @property
    def snapshot(self):
        return self.read("snapshot.json")

    def relative(self, path):
        return Path(path).resolve().relative_to(self.run).as_posix()

    def resolve(self, relative):
        path = (self.run / relative).resolve()
        if not path.is_relative_to(self.run):
            raise ValueError("Artifact path escapes its DAG run directory")
        return path

    def read(self, relative):
        return json.loads(self.resolve(relative).read_text(encoding="utf-8"))

    def write(self, relative, value):
        atomic_json(self.resolve(relative), value)

    def verify_snapshot(self):
        snapshot = self.snapshot
        for relative, digest in snapshot["files"].items():
            if sha256_file(self.resolve(relative)) != digest:
                raise ValueError(f"Run snapshot changed: {relative}")
        if snapshot["run_id"] != self.run_id or snapshot["engine"] != self.engine:
            raise ValueError("Run identity differs from its snapshot")


def validate_data(ctx):
    if (ctx.run / "snapshot.json").exists():
        ctx.verify_snapshot()
        return {"snapshot_reused": True}
    source = load_config(ctx.source_config)
    manifest = json.loads((source.output_dir / "manifest.json").read_text(encoding="utf-8"))
    for path, expected in [(source.raw_path, manifest["source_sha256"]),
                            (source.normalized_path, manifest["normalized_sha256"]),
                            (source.output_dir / "memberships.csv", manifest["membership_sha256"])]:
        if sha256_file(path) != expected:
            raise ValueError("Canonical data artifacts drifted. Review and regenerate Step 1 before scheduling ML.")
    prediction = (ctx.backend / ctx.prediction_input).resolve()
    if not prediction.is_relative_to(ctx.backend) or prediction.suffix.lower() != ".csv":
        raise ValueError("prediction_input must be a CSV inside backend")
    files = {"source/raw.csv": source.raw_path, "source/memberships.csv": source.output_dir / "memberships.csv",
             "source/canonical_manifest.json": source.output_dir / "manifest.json", "source/prediction_input.csv": prediction}
    for destination, original in files.items():
        atomic_copy(original, ctx.resolve(destination))
    _, report = validate_and_normalize(read_raw_data(ctx.run / "source/raw.csv"))
    ctx.write("reports/data_validation.json", report)
    settings = ctx.source_settings
    settings["data"]["raw_path"] = str(ctx.run / "source/raw.csv")
    settings["data"]["normalized_path"] = str(ctx.run / "data/processed.csv")
    settings["splitting"]["output_dir"] = str(ctx.run / "data/splits")
    settings["report_path"] = str(ctx.run / "reports/data_validation.json")
    ctx.config_path.write_text(yaml.safe_dump(settings, sort_keys=False), encoding="utf-8")
    ctx.write("snapshot.json", {"run_id": ctx.run_id, "engine": ctx.engine,
                                "dag_id": ctx.policy["dag_id"], "source_sha256": manifest["source_sha256"],
                                "membership_sha256": manifest["membership_sha256"],
                                "files": {**{p: sha256_file(ctx.resolve(p)) for p in files}, "config.yaml": sha256_file(ctx.config_path)}})
    ctx.log.info("Validated %s customers; immutable snapshot %s", report["rows"], ctx.run)
    return {"rows": report["rows"], "status": report["status"]}


def prepare_splits(ctx):
    ctx.verify_snapshot()
    config = load_config(ctx.config_path)
    clean, _ = validate_and_normalize(read_raw_data(config.raw_path))
    memberships = pd.read_csv(ctx.run / "source/memberships.csv", dtype="string")
    partitions = apply_memberships(clean, memberships)
    config.normalized_path.parent.mkdir(parents=True, exist_ok=True)
    clean.to_csv(config.normalized_path, index=False, lineterminator="\n")
    config.output_dir.mkdir(parents=True, exist_ok=True)
    atomic_copy(ctx.run / "source/memberships.csv", config.output_dir / "memberships.csv")
    manifest = ctx.read("source/canonical_manifest.json")
    manifest["normalized_sha256"] = sha256_file(config.normalized_path)
    for name, part in partitions.items():
        if len(part) != manifest["splits"][name]["rows"]:
            raise ValueError("Saved membership count changed")
        part.to_csv(config.output_dir / f"{name}.csv", index=False, lineterminator="\n")
        manifest["splits"][name]["sha256"] = sha256_file(config.output_dir / f"{name}.csv")
    ctx.write("data/splits/manifest.json", manifest)
    ctx.log.info("Reused saved memberships: %s", {name: len(p) for name, p in partitions.items()})
    return {"partition_rows": {name: len(p) for name, p in partitions.items()}, "new_random_split": False}


def tune_and_train(ctx):
    ctx.verify_snapshot()
    from .orchestration_models import train_sklearn, train_spark
    result = (train_sklearn if ctx.engine == "sklearn" else train_spark)(ctx)
    ctx.write("training.json", result)
    return {"models": list(result["candidates"])}


def evaluate(ctx):
    ctx.verify_snapshot()
    from .orchestration_models import evaluate_sklearn, evaluate_spark
    result = (evaluate_sklearn if ctx.engine == "sklearn" else evaluate_spark)(ctx, ctx.read("training.json"))
    ctx.write("evaluation.json", result)
    ctx.log.info("Selected %s using validation AP; test F1 %.4f", result["selected_model"], result["selected_test_metrics"]["f1"])
    return {"selected_model": result["selected_model"], "test_used_for_selection": False}


def validate_candidate(ctx):
    ctx.verify_snapshot()
    evaluation = ctx.read("evaluation.json")
    candidate = evaluation["candidates"][evaluation["selected_model"]]
    score = candidate["validation"]["average_precision"]
    checks = {
        "selected_is_not_dummy": evaluation["selected_model"] != "dummy",
        "validation_ap_above_minimum": score >= ctx.policy["minimum_validation_average_precision"],
        "improves_dummy_validation_ap": (not ctx.policy["require_improvement_over_dummy"] or
                                          score > evaluation["dummy_validation_average_precision"]),
        "finite_validation_metrics": all(math.isfinite(v) for k, v in candidate["validation"].items() if k != "confusion_matrix"),
        "valid_threshold": 0 <= candidate["decision_threshold"] <= 1,
        "reload_matches": candidate["reload_matches"], "test_not_used_for_selection": evaluation["test_used_for_selection"] is False,
        "artifact_hash_matches": model_digest(ctx.resolve(candidate["path"])) == candidate["sha256"],
        "same_dataset": evaluation["source_sha256"] == ctx.snapshot["source_sha256"],
        "same_memberships": evaluation["membership_sha256"] == ctx.snapshot["membership_sha256"],
    }
    approval = {"status": "PASS" if all(checks.values()) else "FAIL", "checks": checks,
                "candidate_sha256": candidate["sha256"], "evaluation_sha256": sha256_file(ctx.run / "evaluation.json"),
                "selection_basis": "Validation scores and integrity only. Test scores are not an approval gate."}
    ctx.write("approval.json", approval)
    if approval["status"] != "PASS":
        raise ValueError("Candidate rejected: " + ", ".join(k for k, passed in checks.items() if not passed))
    return {"status": "PASS", "checks": len(checks)}


def save_selected_model(ctx):
    ctx.verify_snapshot()
    evaluation, approval = ctx.read("evaluation.json"), ctx.read("approval.json")
    candidate = evaluation["candidates"][evaluation["selected_model"]]
    if (approval["status"] != "PASS" or sha256_file(ctx.run / "evaluation.json") != approval["evaluation_sha256"] or
            model_digest(ctx.resolve(candidate["path"])) != approval["candidate_sha256"]):
        raise ValueError("A matching successful approval is required to save a selected model")
    source = ctx.resolve(candidate["path"])
    destination = ctx.run / "selected" / ("pipeline.joblib" if ctx.engine == "sklearn" else candidate["sha256"])
    if ctx.engine == "sklearn":
        atomic_copy(source, destination)
    elif not destination.exists():
        # A failed copy must not publish a partial native model. A retry uses
        # a new temporary directory and only exposes a checksum-verified tree.
        temporary = destination.with_name(destination.name + ".tmp-" + uuid.uuid4().hex)
        temporary.parent.mkdir(parents=True, exist_ok=True)
        shutil.copytree(source, temporary)
        if model_digest(temporary) != approval["candidate_sha256"]:
            raise ValueError("Temporary selected model failed integrity validation")
        temporary.replace(destination)
    if model_digest(destination) != approval["candidate_sha256"]:
        raise ValueError("Selected artifact copy failed integrity validation")
    ctx.write("selected_model.json", {**candidate, "selected_model": evaluation["selected_model"],
                                      "path": ctx.relative(destination), "engine": ctx.engine})
    return {"path": ctx.relative(destination), "model_version": candidate["model_version"]}


def predict(ctx):
    ctx.verify_snapshot()
    selected = ctx.read("selected_model.json")
    model_path = ctx.resolve(selected["path"])
    if model_digest(model_path) != selected["sha256"]:
        raise ValueError("Selected model changed before prediction")
    raw_path = ctx.run / "source/prediction_input.csv"
    if ctx.engine == "sklearn":
        from .model_inference import load_pipeline, predict_records
        results = predict_records(load_pipeline(model_path), pd.read_csv(raw_path, dtype="string", keep_default_na=False))
    else:
        from .spark_runtime import create_spark
        from .spark_inference import load_spark_pipeline, predict_spark_frame
        spark = create_spark(ctx.settings["spark"], ctx.run)
        try:
            frame = spark.read.option("header", True).csv(raw_path.as_uri())
            results = predict_spark_frame(load_spark_pipeline(model_path), frame)
        finally:
            spark.stop()
    expected = pd.read_csv(raw_path, dtype="string", keep_default_na=False)
    if len(results) != len(expected) or not results.churn_probability.between(0, 1).all():
        raise ValueError("Prediction coverage or probability validation failed")
    destination = ctx.run / "predictions/pending.csv"
    destination.parent.mkdir(parents=True, exist_ok=True)
    temporary = destination.with_suffix(".tmp.csv")
    results.to_csv(temporary, index=False, lineterminator="\n")
    temporary.replace(destination)
    ctx.write("prediction.json", {"rows": len(results), "path": ctx.relative(destination),
                                  "sha256": sha256_file(destination), "model_version": selected["model_version"]})
    return {"prediction_rows": len(results)}


def save_results(ctx):
    ctx.verify_snapshot()
    selected, prediction = ctx.read("selected_model.json"), ctx.read("prediction.json")
    pending = ctx.resolve(prediction["path"])
    if sha256_file(pending) != prediction["sha256"]:
        raise ValueError("Prediction artifact changed before result saving")
    final = ctx.run / "results/predictions.csv"
    atomic_copy(pending, final)
    evaluation = ctx.read("evaluation.json")
    report = {"status": "PASS", "dag_id": ctx.policy["dag_id"], "run_id": ctx.run_id, "engine": ctx.engine,
              "run_directory": str(ctx.run), "selected_model": selected["selected_model"],
              "model_version": selected["model_version"], "decision_threshold": selected["decision_threshold"],
              "selected_model_path": selected["path"], "model_sha256": selected["sha256"],
              "prediction_rows": prediction["rows"], "predictions_path": ctx.relative(final),
              "predictions_sha256": sha256_file(final), "validation": selected["validation"],
              "test_metrics": evaluation["selected_test_metrics"], "test_used_for_selection": False,
              "source_sha256": ctx.snapshot["source_sha256"], "membership_sha256": ctx.snapshot["membership_sha256"],
              "stages": list(TASKS), "completed_at": datetime.now(timezone.utc).isoformat()}
    ctx.write("results/summary.json", report)
    lines = ["# Airflow ML workflow results", "", f"Run: `{ctx.run_id}`. Engine: **{ctx.engine}**.", "",
             f"Selected **{selected['selected_model']}**; threshold **{selected['decision_threshold']:.6f}**.", "",
             f"Saved {prediction['rows']} raw-record predictions to `predictions.csv`.", "",
             "The candidate passed validation-score and artifact-integrity gates. Test scores did not select or approve the model.", "",
             "| Held-out test metric | Value |", "|---|---:|"]
    for key, value in report["test_metrics"].items():
        lines.append(f"| {key} | {value if isinstance(value, list) else f'{value:.6f}'} |")
    lines += ["", f"Dataset SHA256: `{report['source_sha256']}`.", f"Membership SHA256: `{report['membership_sha256']}`.", ""]
    (ctx.run / "results/summary.md").write_text("\n".join(lines), encoding="utf-8")
    # Only a fully saved result may become the latest successful workflow output.
    atomic_json(ctx.artifact_root / f"latest_success_{ctx.engine}.json", report)
    ctx.log.info("SUCCESS: %s; %s predictions; result directory %s", ctx.run_id, prediction["rows"], ctx.run / "results")
    return {"results_path": str(ctx.run / "results"), "rows": prediction["rows"]}


def execute_task(task, ctx, attempt=1):
    if task not in TASKS:
        raise ValueError("Unknown workflow task")
    log_directory = ctx.run / "logs"
    log_directory.mkdir(exist_ok=True)
    handler = logging.FileHandler(log_directory / f"{task}.log", encoding="utf-8")
    handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(message)s"))
    ctx.log.addHandler(handler)
    started = time.perf_counter()
    event = {"task": task, "attempt": attempt, "started_at": datetime.now(timezone.utc).isoformat(),
             "run_id": ctx.run_id, "engine": ctx.engine}
    try:
        ctx.log.info("Starting %s; run=%s; attempt=%s", task, ctx.run_id, attempt)
        result = globals()[task](ctx)
        event.update(status="success", result=result)
        ctx.log.info("Completed %s: %s", task, result)
        return result
    except Exception as error:
        event.update(status="failed", error=str(error))
        ctx.log.exception("Task %s failed", task)
        raise
    finally:
        event.update(finished_at=datetime.now(timezone.utc).isoformat(), duration_seconds=time.perf_counter() - started)
        atomic_json(log_directory / f"{task}_attempt_{attempt}.json", event)
        ctx.log.removeHandler(handler)
        handler.close()
