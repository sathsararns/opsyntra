"""Retain clean-clone command logs and publish the Phase 1 handoff report."""
import hashlib
import json
import shutil
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]


def main():
    source = ROOT / "backend/reports/clean_clone_verification.json"
    report = json.loads(source.read_text(encoding="utf-8"))
    frontend = json.loads((ROOT / "backend/reports/frontend_verification.json").read_text(encoding="utf-8"))
    if report["status"] != "PASS" or not report.get("includes_fresh_airflow_runs") or frontend["status"] != "PASS":
        raise ValueError("Full clean-clone, frontend and fresh clone Airflow verification must pass first")
    required = {"clone", "clean_checkout", "windows_dependencies", "data_import", "sklearn_training", "sklearn_single", "sklearn_batch",
                "spark_dependencies", "spark_training", "spark_single", "spark_batch", "backend_tests", "spark_tests",
                "npm_install", "frontend_typecheck", "frontend_build", "frontend_browser", "kafka_publishing", "kafka_consumption", "airflow_sklearn", "airflow_spark"}
    passed = {stage["name"] for stage in report["stages"] if stage["status"] == "PASS"}
    if not required <= passed:
        raise ValueError("Missing required verification stages: " + str(required - passed))
    if frontend["source_commit"] != report["source_commit"]:
        raise ValueError("Frontend and clean-clone evidence refer to different commits")
    if not frontend.get("fresh_managed_servers"):
        raise ValueError("Browser verification must force fresh API/frontend servers")
    for patch in report.get("source_patches", []):
        clone_source = Path(report["checkout"]) / patch["path"]
        if hashlib.sha256(clone_source.read_bytes()).hexdigest() != patch["sha256"] or hashlib.sha256((ROOT / patch["path"]).read_bytes()).hexdigest() != patch["sha256"]:
            raise ValueError("Recorded portability patch no longer matches the tested candidate")
        patch["patch_sha256"] = hashlib.sha256((ROOT / patch["patch_file"]).read_bytes()).hexdigest()
    retained = ROOT / "docs/evidence/clean-clone"
    retained.mkdir(parents=True, exist_ok=True)
    for stage in report["stages"]:
        if not stage.get("log"):
            continue
        original = ROOT / stage["log"]
        if hashlib.sha256(original.read_bytes()).hexdigest() != stage["log_sha256"]:
            raise ValueError("Command log changed after verification: " + stage["name"])
        destination = retained / (original.stem + ".txt")
        shutil.copyfile(original, destination)
        stage["retained_log"] = destination.relative_to(ROOT).as_posix()
    # Preserve actual scheduler evidence from the isolated clone even after its
    # scheduler is stopped and the original project service has been restored.
    clone = Path(report["checkout"])
    report["fresh_airflow_runs"] = {}
    for engine in ("sklearn", "spark"):
        evidence = json.loads((clone / f"backend/.airflow-runtime/last_success_{engine}.json").read_text())
        if evidence["state"] != "success" or len(evidence["tasks"]) != 8 or any(t["state"] != "success" for t in evidence["tasks"]):
            raise ValueError("Fresh clone Airflow DAG did not complete all eight tasks")
        report["fresh_airflow_runs"][engine] = evidence
    report["evidence_retained_at"] = datetime.now(timezone.utc).isoformat()
    setup_path = ROOT / "backend/reports/spark_environment_verification.json"
    if setup_path.exists():
        setup = json.loads(setup_path.read_text(encoding="utf-8"))
        if setup["status"] != "PASS":
            raise ValueError("New Spark setup helper verification did not pass")
        report["new_setup_smoke_test"] = setup
    bundle_path = ROOT / "backend/reports/model_bundle_verification.json"
    if bundle_path.exists():
        bundle = json.loads(bundle_path.read_text(encoding="utf-8"))
        if bundle["status"] != "PASS" or len(bundle.get("commands", [])) != 4 or any(c["exit_code"] for c in bundle["commands"]):
            raise ValueError("Optional saved-model demonstration did not finish both engines' single/CSV inference")
        if hashlib.sha256(Path(bundle["bundle"]).read_bytes()).hexdigest() != bundle["bundle_sha256"]:
            raise ValueError("The saved-model handoff bundle changed after verification")
        report["saved_model_demonstration"] = {"status": bundle["status"], "bundle_sha256": bundle["bundle_sha256"],
                                                "checkout": bundle["checkout"], "models_trained_in_this_checkout": False}
    source.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    host = report["results"]["test_results.json"]
    spark = report["results"]["test_results_spark.json"]
    training = report["results"]["model_training_report.json"]
    lines = ["# Clean-clone verification", "", f"Baseline application commit: `{report['source_commit']}`.", "",
             "Status: **PASS**. This is a separate local Git clone with newly created Windows and Linux ML environments. "
             "No model artifacts, virtual environments, node_modules or raw data were copied from the original workspace. "
             "The supplied archive was imported as the external dataset input. Earlier failed attempts are retained below; "
             "verification completed after the recorded portability fix and successful retry.", "",
             f"Archive SHA-256: `{report['archive_sha256']}`.",
             f"Dataset SHA-256: `{training['provenance']['source_sha256']}`.",
             f"Membership SHA-256: `{training['provenance']['membership_sha256']}`.", "",
             f"Fresh clone tests: {host['passed']} host checks passed; {host['skipped']} Spark checks skipped there; "
             f"{spark['passed']} native Spark checks passed. Browser: {report['frontend']['passed']} passed, zero failed/skipped.", "",
             "## Commands and results", "", "| Stage | Result | Command output |", "|---|---|---|"]
    for stage in report["stages"]:
        retained_path = stage.get("retained_log")
        link = f"[log](../../{retained_path})" if retained_path else "Initial Git/ignored-artifact assertions"
        lines.append(f"| {stage['name']} | {stage['status']} | {link} |")
    lines += ["", "## Fresh scheduler runs", ""]
    for engine, run in report["fresh_airflow_runs"].items():
        lines.append(f"- {engine}: `{run['run_id']}`; eight tasks successful in fresh clone Airflow metadata.")
    if report.get("source_patches"):
        lines += ["", "## Verified portability fixes applied after cloning", ""]
        for patch in report["source_patches"]:
            lines += [f"- `{patch['path']}`: {patch['reason']}. [Exact applied patch](../../{patch['patch_file']}); patched-file SHA-256 `{patch['sha256']}`."]
    lines += ["", "## Reused prerequisites and limits", ""]
    lines += ["- " + value for value in report["reused_prerequisites"]]
    lines += ["", "The Git transport was local and its baseline commit is recorded above. Only the explicitly listed portability fixes were applied to the cloned application source; "
              "the patch and final source hashes identify the tested candidate without committing changes to the user's repository. Pending documentation/verification tools were not overlaid on the clone. "
              "This verifies a fresh application checkout on this computer, not provisioning a blank operating system. "
              "Kafka used the existing broker with isolated diagnostic groups/windows; Airflow used its existing tool installation with a new clone-specific metadata directory. "
              "The original project's Airflow service was restored afterwards.", "",
              "Generated models/reports in the clone have their own experiment/run IDs. They do not replace the original project's deployed models or MLflow database. "
              "See [the JSON evidence](clean_clone_verification.json), [frontend results](frontend_verification.json) and [demo guide](../../docs/DEMO_GUIDE.md).", ""]
    if report.get("saved_model_demonstration"):
        lines += ["## Separate saved-model setup", "",
                  "A second checkout imported the matching dataset and restored a checksummed model bundle. Both engines predicted single/CSV inputs without training there. "
                  "This faster setup explicitly reused installed runtimes and original deployed models; it is separate from the clean retraining proof above. "
                  "The guarded restore supports long Windows native Spark paths and rejects mismatched data or conflicting models. "
                  "See [bundle evidence](model_bundle_verification.json).", ""]
    (ROOT / "backend/reports/clean_clone_verification.md").write_text("\n".join(lines), encoding="utf-8")
    print("PASS: retained clean-clone logs, frontend checks and fresh Airflow scheduler evidence")


if __name__ == "__main__":
    main()
