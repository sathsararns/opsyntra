"""Build a hash-indexed rubric/evidence map from actual reports and screenshots."""
import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
PROJECT = ROOT.parent


def read(path):
    return json.loads((PROJECT / path).read_text(encoding="utf-8"))


def main():
    host = read("backend/reports/test_results.json")
    spark = read("backend/reports/test_results_spark.json")
    for report in (host, spark):
        if report["status"] != "PASS" or report["failures"] or report["errors"]:
            raise ValueError("Tests did not pass; do not label the evidence complete")
    skipped = {t["test"] for t in host["tests"] if t["status"] == "skipped"}
    passed_spark = {t["test"] for t in spark["tests"] if t["status"] == "passed"}
    if not skipped <= passed_spark or spark["skipped"]:
        raise ValueError("Host-skipped Spark tests must pass in the real Spark runtime")
    training = read("backend/reports/model_training_report.json")
    screenshots = read("docs/screenshots/mlflow-evidence.json")
    if screenshots["model_version"] != training["selected_model_version"] or screenshots["batch_id"] != training["tracking"]["batch_id"]:
        raise ValueError("MLflow screenshots do not match the current training report")
    if len(training["tracking"]["runs"]) != 4:
        raise ValueError("Expected four comparable model runs")
    required_ml = read("backend/reports/airflow_workflow_report.json")
    ml_captures = read("docs/screenshots/airflow-evidence.json")
    for engine, result in required_ml["runs"].items():
        if result["state"] != "success" or len(result["tasks"]) != 8 or any(t["state"] != "success" for t in result["tasks"]):
            raise ValueError("Required eight-task ML DAG did not finish")
        if not any(c["run_id"] == result["run_id"] for c in ml_captures["runs"]):
            raise ValueError("Required ML DAG screenshots and report do not match")
    kafka_captures = read("docs/screenshots/kafka-evidence.json")
    bonus = {}
    for kind in ("batch", "streaming"):
        scheduler = read(f"backend/.airflow-runtime/last_success_kafka_{kind}.json")
        run_key = hashlib.sha256(scheduler["run_id"].encode()).hexdigest()[:20]
        result = read(f"backend/artifacts/kafka/airflow/{kind}/{run_key}/summary.json")
        if (scheduler["state"] != "success" or len(scheduler["tasks"]) != 4 or any(t["state"] != "success" for t in scheduler["tasks"])
                or result["status"] != "PASS" or scheduler["run_id"] != result["run_id"]):
            raise ValueError("Bonus DAG evidence is incomplete or mismatched")
        if not any(c.get("run_id") == scheduler["run_id"] for c in kafka_captures["captures"]):
            raise ValueError("Bonus screenshot provenance does not match its run")
        result_csv = Path(result["predictions_csv"])
        # WSL paths in task outputs map to the same shared workspace on Windows.
        if not result_csv.exists():
            relative = result["predictions_csv"].split("/backend/", 1)[1]
            result_csv = ROOT / relative
        if not result_csv.is_file():
            raise ValueError("Successful DAG has no persisted prediction CSV")
        bonus[kind] = {"scheduler": scheduler, "result": result,
                       "predictions_csv_sha256": hashlib.sha256(result_csv.read_bytes()).hexdigest()}
    publishing = read("backend/reports/kafka_publishing_report.json")
    consumption = read("backend/reports/kafka_consumption_report.json")
    if publishing["status"] != "PASS" or consumption["status"] != "PASS":
        raise ValueError("Real-broker publishing/recovery evidence is missing")
    required = ["mlflow-comparison.png", "mlflow-selected-model.png", "airflow-grid-sklearn.png", "airflow-grid-spark.png",
                "kafka-input-output.png", "kafka-deadletter.png", "kafka-recovery.png", "kafka-replay-window.png"]
    required += [f"kafka-airflow-{kind}-{view}.png" for kind in ("batch", "streaming") for view in ("grid", "graph", "log")]
    for name in required:
        if not (PROJECT / "docs/screenshots" / name).is_file():
            raise ValueError("Missing screenshot: " + name)
    rows = [
        ("Missing values, unseen categories, leakage and reload", "backend/reports/test_results.json", "backend/tests/unit/test_model_pipeline.py"),
        ("Single, CSV, batch Kafka and streaming Kafka probabilities match", "backend/reports/test_results.json", "backend/tests/integration/test_kafka_consumer.py"),
        ("Native Spark models, unseen categories, persistence and fresh-process inference", "backend/reports/test_results_spark.json", "backend/tests/integration/test_spark_workflow.py"),
        ("Comparable MLflow runs and validation-based selection", "backend/reports/model_selection.md", "docs/screenshots/mlflow-comparison.png"),
        ("Same held-out customers and honest engine timings", "backend/reports/engine_comparison.md", "backend/reports/engine_comparison.json"),
        ("Required eight-task ML DAG, both engines", "backend/reports/airflow_workflow_report.md", "docs/screenshots/airflow-evidence.json"),
        ("Batch/streaming producer and checkpoint resume", "backend/reports/kafka_publishing_report.md", "docs/screenshots/kafka-input-output.png"),
        ("Invalid records, deduplication and real crash recovery", "backend/reports/kafka_consumption_report.md", "docs/screenshots/kafka-recovery.png"),
        ("Remaining output-ack/ledger replay window", "docs/evidence/restart_after_output_ack.log", "docs/screenshots/kafka-replay-window.png"),
        ("Bonus scheduled batch Kafka DAG", "backend/dags/opsyntra_kafka_workflows.py", "docs/screenshots/kafka-airflow-batch-grid.png"),
        ("Bonus managed streaming DAG, heartbeats and graceful stop", "backend/dags/opsyntra_kafka_workflows.py", "docs/screenshots/kafka-airflow-streaming-log.png"),
        ("Reproducible setup and operating limits", "docs/VERIFICATION.md", "backend/KAFKA.md"),
    ]
    phase1 = None
    if (ROOT / "reports/clean_clone_verification.json").exists():
        clean = read("backend/reports/clean_clone_verification.json")
        frontend = read("backend/reports/frontend_verification.json")
        if clean["status"] != "PASS" or not clean.get("includes_fresh_airflow_runs") or frontend["status"] != "PASS":
            raise ValueError("Phase 1 clean-clone/frontend verification is incomplete")
        if frontend["browser"]["stats"]["unexpected"] or frontend["browser"]["stats"]["skipped"]:
            raise ValueError("Frontend verification contains failed/skipped flows")
        phase1 = {"source_commit": clean["source_commit"], "completed_at": clean["completed_at"],
                  "source_patches": clean.get("source_patches", []),
                  "clean_clone_status": clean["status"], "fresh_airflow_runs": clean["fresh_airflow_runs"],
                  "frontend": clean["frontend"], "reused_prerequisites": clean["reused_prerequisites"]}
        rows += [
            ("Independent clean clone, fresh model training and scheduler runs", "backend/reports/clean_clone_verification.md", "backend/reports/clean_clone_verification.json"),
            ("Frontend typecheck/build and six real browser flows", "backend/reports/frontend_verification.json", "docs/screenshots/frontend-prediction.png"),
            ("Submission demonstration and optional saved-model setup", "docs/DEMO_GUIDE.md", "docs/VERIFICATION.md"),
            ("Checksum-verified saved-model restore and both engines' inference", "backend/reports/model_bundle_verification.json", "backend/pipelines/model_bundle.py"),
            ("Isolated native-Linux environment setup and repeat setup", "backend/reports/spark_environment_verification.json", "backend/pipelines/spark_environment.py"),
        ]
    paths = {path for _, *files in rows for path in files}
    paths.update("docs/screenshots/" + name for name in required)
    paths.update(p.relative_to(PROJECT).as_posix() for p in (PROJECT / "docs/evidence").glob("*") if p.is_file())
    paths.update(p.relative_to(PROJECT).as_posix() for p in (PROJECT / "docs/evidence/clean-clone").glob("*") if p.is_file())
    paths.update(p.relative_to(PROJECT).as_posix() for p in (PROJECT / "docs/evidence/saved-model").glob("*.txt"))
    paths.update(p.relative_to(PROJECT).as_posix() for p in (PROJECT / "docs/evidence/history").rglob("manifest.json"))
    for historical in (PROJECT / "docs/evidence/history").rglob("manifest.json"):
        for member in json.loads(historical.read_text(encoding="utf-8"))["files"]:
            archived = (historical.parent / member["path"]).resolve()
            if not archived.is_relative_to(PROJECT / "docs/evidence/history") or hashlib.sha256(archived.read_bytes()).hexdigest() != member["sha256"]:
                raise ValueError("Archived evidence was changed: " + member["path"])
            paths.add(archived.relative_to(PROJECT).as_posix())
    paths.update({"backend/reports/test_results.log", "backend/reports/test_results_spark.log", "docs/screenshots/mlflow-evidence.json", "docs/screenshots/kafka-evidence.json"})
    paths.update({"backend/src/opsyntra/kafka_workflow.py", "backend/src/opsyntra/kafka_consumption.py", "backend/config.yaml",
                  "backend/producer.py", "backend/consumer.py", "start-airflow.ps1", "backend/pipelines/verification.py"})
    if phase1:
        paths.update({"backend/pipelines/clean_clone_check.py", "backend/pipelines/phase1_evidence.py", "backend/pipelines/model_bundle.py",
                      "backend/requirements/worker.txt", "start-spark.ps1", "backend/src/opsyntra/spark_tracking.py",
                      "backend/tests/unit/test_spark_archive.py", "backend/tests/unit/test_model_bundle.py",
                      "docs/DEMO_GUIDE.md", "docs/screenshots/frontend-desktop.png", "docs/screenshots/frontend-mobile.png"})
    artifacts = []
    for relative in sorted(paths):
        path = PROJECT / relative
        data = path.read_bytes()
        artifacts.append({"path": relative, "bytes": len(data), "sha256": hashlib.sha256(data).hexdigest()})
    report = {"status": "PASS", "captured_at": datetime.now(timezone.utc).isoformat(),
              "host_tests": {k: host[k] for k in ("tests_run", "passed", "skipped", "failures", "errors")},
              "spark_tests": {k: spark[k] for k in ("tests_run", "passed", "skipped", "failures", "errors")},
              "unique_tests_passed_across_runtimes": host["passed"] + spark["passed"],
              "model_version": training["selected_model_version"], "source_sha256": training["provenance"]["source_sha256"],
              "split_sha256": training["provenance"]["membership_sha256"], "bonus_dags": bonus,
              "rubric": [{"requirement": name, "evidence": files} for name, *files in rows], "artifacts": artifacts,
              "limits": ["Single local WSL broker/LocalExecutor; not a multi-host benchmark", "At-least-once delivery; no exactly-once claim",
                         "Model accepts unknown categories; Kafka deliberately enforces the documented categorical schema",
                         "Screenshots are dated evidence; recorded CLI transcript pages are explicitly labelled"]}
    if phase1:
        report["phase1"] = phase1
    (ROOT / "reports/submission_evidence.json").write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    lines = ["# Opsyntra verification and evidence", "", f"Generated: {report['captured_at']}", "",
             f"Host: {host['passed']} passed, {host['skipped']} Spark checks skipped. Native WSL Spark: {spark['passed']} passed, no skips. "
             f"All {report['unique_tests_passed_across_runtimes']} unique tests passed across the two runtimes.", "",
             f"Model: `{report['model_version']}`. Dataset: `{report['source_sha256']}`. Splits: `{report['split_sha256']}`.", "",
             "| Requirement | Evidence / implementation | Screenshot / check |", "|---|---|---|"]
    for name, first, second in rows:
        lines.append(f"| {name} | [{Path(first).name}](../../{first}) | [{Path(second).name}](../../{second}) |")
    lines += ["", "## Real bonus scheduler runs", ""]
    for kind, info in bonus.items():
        result = info["result"]
        lines += [f"- **{kind}**: `{result['run_id']}`; all four tasks successful; {result['predictions']} confirmed prediction rows; model `{result['model_version']}`."]
    if phase1:
        lines += ["", "## Phase 1 clean-clone handoff", "",
                  f"A separate local clone of `{phase1['source_commit']}` rebuilt both engines in fresh ML environments, passed six real browser flows, "
                  "and completed both required eight-task DAGs using fresh clone-specific Airflow metadata. "
                  "Host WSL/Java/Node/Edge, the Kafka broker and the Airflow tool installation were reused explicitly. "
                  "The linked report records the exact portability patch applied to its baseline commit. "
                  "See that report and retained command logs for the tested candidate, commands and prerequisites."]
    lines += ["", "The JSON index records SHA-256 and sizes of the evidence files, exact scheduler task states, run/group IDs, model/source/split hashes and output CSV hashes. "
              "Raw source data, saved models, service metadata and run ledgers stay ignored by Git; recreate them using the documented commands. "
              "Recovery transcripts and test logs needed for assessment are retained in the repository.", "",
              "See [reproduction commands](../../docs/VERIFICATION.md). Delivery remains at-least-once: the tested ack-before-ledger crash can produce two physical messages with one stable prediction ID. "
              "Separate groups may also emit the same prediction; downstream sinks should upsert by prediction_id.", ""]
    (ROOT / "reports/submission_evidence.md").write_text("\n".join(lines), encoding="utf-8")
    print(f"PASS: evidence indexed for {report['unique_tests_passed_across_runtimes']} tests and both bonus DAGs")


if __name__ == "__main__":
    main()
