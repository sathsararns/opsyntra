"""Build reviewable evidence from real successful scheduler runs and saved outputs."""
import json
import sys
from pathlib import Path
from datetime import datetime, timezone

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "src"))


def main():
    from opsyntra.orchestration.workflow import WorkflowContext, TASKS
    from opsyntra.data.pipeline import sha256_file, write_json
    from opsyntra.orchestration.models import model_digest
    import pandas as pd
    import yaml

    runs = {}
    for engine in ("sklearn", "spark"):
        evidence = json.loads((ROOT / ".airflow-runtime" / f"last_success_{engine}.json").read_text())
        if (evidence["state"] != "success" or len(evidence["tasks"]) != len(TASKS)
                or {t["task_id"] for t in evidence["tasks"]} != set(TASKS)
                or any(t["state"] != "success" for t in evidence["tasks"])):
            raise ValueError(f"{engine} does not have eight successful scheduled tasks")
        ctx = WorkflowContext(ROOT, evidence["run_id"], engine)
        ctx.verify_snapshot()
        summary, approval = ctx.read("results/summary.json"), ctx.read("approval.json")
        if approval["status"] != "PASS" or not all(approval["checks"].values()):
            raise ValueError("Candidate approval did not pass")
        if model_digest(ctx.resolve(summary["selected_model_path"])) != summary["model_sha256"]:
            raise ValueError("Saved selected model changed")
        output = ctx.resolve(summary["predictions_path"])
        if sha256_file(output) != summary["predictions_sha256"]:
            raise ValueError("Saved predictions changed")
        if sha256_file(ctx.run / "source/memberships.csv") != sha256_file(ROOT / "data/splits/memberships.csv"):
            raise ValueError("Memberships differ from the authoritative map")
        results = pd.read_csv(output)
        if len(results) != summary["prediction_rows"] or not results.churn_probability.between(0, 1).all():
            raise ValueError("Prediction coverage or probabilities are invalid")
        events = {name: ctx.read(f"logs/{name}_attempt_1.json") for name in TASKS}
        if any(event["status"] != "success" for event in events.values()):
            raise ValueError("Task event logs do not show success")
        runs[engine] = {**evidence, "summary": summary, "approval": approval,
                        "run_directory": ctx.run.relative_to(ROOT).as_posix(), "task_events": events,
                        "mlflow": ctx.read("mlflow.json"), "predictions": results.to_dict(orient="records")}
    if runs["sklearn"]["summary"]["membership_sha256"] != runs["spark"]["summary"]["membership_sha256"]:
        raise ValueError("Engines did not reuse the same memberships")
    settings = yaml.safe_load((ROOT / "config.yaml").read_text())
    report = {"captured_at": datetime.now(timezone.utc).isoformat(), "dag_id": "opsyntra_ml_workflow",
              "evidence_source": "Airflow scheduler CLI states plus hash-verified persisted task artifacts",
              "orchestration_policy": settings["orchestration"], "runs": runs}
    write_json(ROOT / "reports/airflow_workflow_report.json", report)
    lines = ["# Airflow ML workflow checkpoint", "", "Both engines completed all eight tasks through the actual Airflow 3.3.2 scheduler and LocalExecutor in Ubuntu WSL.",
             "These are scheduled DagRuns, with saved task states, native model artifacts and actual batch predictions.", "",
             "```text", "validate_data -> prepare_splits -> tune_and_train -> evaluate", "             -> validate_candidate -> save_selected_model -> predict -> save_results", "```", "",
             "| Engine | DAG state | Successful tasks | Selected model | Validation AP | Threshold | Test F1 | Saved predictions |",
             "|---|---|---:|---|---:|---:|---:|---:|"]
    for engine, run in runs.items():
        s = run["summary"]
        lines.append(f"| {engine} | success | 8/8 | {s['selected_model']} | {s['validation']['average_precision']:.6f} | {s['decision_threshold']:.6f} | {s['test_metrics']['f1']:.6f} | {s['prediction_rows']} |")
    lines += ["", "Model ranking uses validation average precision; thresholds maximize validation F1. Only the selected model receives held-out test scores. Test scores are not an approval gate.", "",
              "## Completed runs and saved outputs", ""]
    for engine, run in runs.items():
        directory = run["run_directory"]
        lines += [f"### {engine}", "", f"Run ID: `{run['run_id']}`.",
                  f"Started: {run['dag_run']['start_date']}; finished: {run['dag_run']['end_date']} (UTC).", "",
                  f"- [Final prediction CSV](../{directory}/results/predictions.csv)",
                  f"- [Result summary](../{directory}/results/summary.md)",
                  f"- [Candidate approval checks](../{directory}/approval.json)",
                  f"- [Selected model metadata](../{directory}/selected_model.json)",
                  f"- [Per-task logs](../{directory}/logs/)",
                  f"- MLflow experiment: **Opsyntra-Airflow**; workflow run `{run['mlflow']['workflow_run_id']}`.", "",
                  f"![Actual successful {engine} task grid](../../docs/screenshots/airflow-grid-{engine}.png)", "",
                  f"![Actual successful {engine} task graph](../../docs/screenshots/airflow-graph-{engine}.png)", "",
                  f"[Successful final task log](../../docs/screenshots/airflow-log-{engine}.png)", ""]
    lines += ["## Scheduled task duration", "", "These durations include subprocess startup and orchestration overhead. They are not the engine fit/inference benchmark from Step 4.", "",
              "| Task | sklearn seconds | Spark seconds |", "|---|---:|---:|"]
    for name in TASKS:
        durations = []
        for engine in ("sklearn", "spark"):
            task = next(t for t in runs[engine]["tasks"] if t["task_id"] == name)
            durations.append((datetime.fromisoformat(task["end_date"]) - datetime.fromisoformat(task["start_date"])).total_seconds())
        lines.append(f"| {name} | {durations[0]:.3f} | {durations[1]:.3f} |")
    lines += ["", "## Reproducibility and operational checks", "",
              "- Frozen raw data, prediction input, configuration and membership files are checksum-verified by downstream tasks.",
              "- Existing 4,225/1,409/1,409 customer memberships are reused; training folds alone fit imputation, encoding, scaling and classifiers.",
              "- Both approved models passed save/reload probability and label checks. The predict task loaded the approved artifact in a separate process and accepted CSV rows without Churn.",
              "- Candidate gates check validation metrics, dummy improvement, threshold range, data identity and artifact integrity. Approved artifacts are copied into the run's selected directory.",
              "- Retries: 2, with a 20-second delay. Per-task timeout: 30 minutes; DAG timeout: 120 minutes. One active run and one active task avoid overlapping local training.",
              "- Every task writes a log and per-attempt status/timing JSON. Failed upstream tasks block downstream outputs; retries replace final CSVs rather than appending duplicates.",
              "- Run artifacts are isolated by engine and sanitized, hashed run ID. The application frontend's model is not automatically replaced.",
              "- The workflow and all seven candidate runs are recorded in the existing Windows MLflow store, keeping artifact paths accessible from its UI.", "",
              f"Dataset SHA256: `{runs['sklearn']['summary']['source_sha256']}`.",
              f"Membership SHA256: `{runs['sklearn']['summary']['membership_sha256']}`.", "",
              "The JSON report retains scheduler task states, attempt events, approvals, model/data identifiers, predictions and MLflow IDs. Screenshots show the real local UI and are linked to scheduler run IDs in `docs/screenshots/airflow-evidence.json`.", "",
              "See [DAG setup and commands](../dags/README.md). Airflow's local automatic admin login is bound to loopback for development; shared deployment requires a separate deployment/authentication configuration.", ""]
    (ROOT / "reports/airflow_workflow_report.md").write_text("\n".join(lines), encoding="utf-8")
    print("Verified both real successful DagRuns and saved reports/airflow_workflow_report.{json,md}")


if __name__ == "__main__":
    main()
