"""Rebuild a local Git clone without copying models or environments into it."""
import argparse
import hashlib
import json
import os
import shutil
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4

ROOT = Path(__file__).resolve().parents[2]
REPORT = ROOT / "backend/reports/clean_clone_verification.json"


def utc():
    return datetime.now(timezone.utc).isoformat()


def digest(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def save(path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(".tmp")
    temporary.write_text(json.dumps(value, indent=2, allow_nan=False) + "\n", encoding="utf-8")
    temporary.replace(path)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--archive", type=Path, required=True)
    parser.add_argument("--resume", type=Path, help="Verification directory to resume after a failed stage")
    parser.add_argument("--include-airflow", action="store_true", help="Run fresh clone DAGs, temporarily stopping/restoring the main local Airflow service")
    args = parser.parse_args()
    if os.name != "nt":
        parser.error("This verifier exercises the documented Windows + Ubuntu WSL setup")
    folder = args.resume.resolve() if args.resume else ROOT / ".verification" / (datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ") + "-" + uuid4().hex[:6])
    if not folder.is_relative_to(ROOT / ".verification"):
        parser.error("Verification directory must stay under this workspace's .verification/")
    folder.mkdir(parents=True, exist_ok=True)
    checkout = folder / "checkout"
    python = folder / "windows-python/Scripts/python.exe"
    logs = folder / "logs"
    logs.mkdir(exist_ok=True)
    state = folder / "verification.json"
    if args.resume:
        report = json.loads(state.read_text())
        if report["source_commit"] != subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=ROOT, text=True).strip():
            raise ValueError("Source commit changed; start a new verification")
        if report["archive_sha256"] != digest(args.archive):
            raise ValueError("Dataset archive changed; start a new verification")
    else:
        report = {"status": "RUNNING", "started_at": utc(), "source_commit": subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=ROOT, text=True).strip(),
                  "source_worktree_changes": subprocess.check_output(["git", "status", "--short"], cwd=ROOT, text=True).splitlines(),
                  "directory": str(folder), "checkout": str(checkout), "archive": str(args.archive.resolve()),
                  "archive_sha256": digest(args.archive), "stages": [], "copied_model_artifacts": False,
                  "reused_prerequisites": ["Installed Windows Python interpreter (fresh isolated venv)", "Installed Node/npm and Microsoft Edge",
                    "Ubuntu WSL, python3 and Java 21 (fresh isolated Spark venv)", "Existing local Kafka broker/data and downloaded Kafka installation",
                    "Existing Airflow 3.3.2 tool installation; clone metadata/state newly created when --include-airflow is used",
                    "Package download caches; no existing Python environments/node_modules copied"]}
    report["status"] = "RUNNING"
    if report.get("error"):
        report.setdefault("previous_failures", []).append({"error": report.pop("error"), "resumed_at": utc()})
    environment = {**os.environ, "PATH": str(python.parent) + os.pathsep + os.environ.get("PATH", ""),
                   "PYTHONIOENCODING": "utf-8", "PYTHONUNBUFFERED": "1", "MLFLOW_DISABLE_AGENT_HINT": "1"}

    def persist():
        report["updated_at"] = utc()
        save(state, report)
        save(REPORT, report)

    def run(name, command, *, cwd=None, timeout=1800, force=False, env_overrides=None):
        previous = next((s for s in report["stages"] if s["name"] == name and s["status"] == "PASS"), None)
        if previous and not force:
            print("REUSED completed verification stage:", name, flush=True)
            return logs / (name + ".log")
        attempt = 1 + sum(s["name"] == name for s in report["stages"])
        stage = {"name": name, "attempt": attempt, "command": [str(x) for x in command], "cwd": str(cwd or checkout), "started_at": utc(), "status": "RUNNING"}
        if env_overrides:
            stage["environment_overrides"] = env_overrides
        report["stages"].append(stage)
        persist()
        print("START:", name, flush=True)
        started = time.perf_counter()
        path = logs / (name + f"-attempt-{attempt}.log")
        with path.open("w", encoding="utf-8") as output:
            try:
                process = subprocess.run([str(x) for x in command], cwd=cwd or checkout, env={**environment, **(env_overrides or {})},
                                         stdout=output, stderr=subprocess.STDOUT, timeout=timeout)
                stage["exit_code"] = process.returncode
                stage["status"] = "PASS" if process.returncode == 0 else "FAIL"
            except Exception as error:
                stage.update(status="FAIL", error=str(error))
            stage.update(finished_at=utc(), seconds=time.perf_counter() - started,
                         log=path.relative_to(ROOT).as_posix(), log_sha256=digest(path))
            persist()
        if stage["status"] != "PASS":
            raise RuntimeError(name + " failed: " + path.read_text(encoding="utf-8", errors="replace")[-5000:])
        print(f"PASS: {name} ({stage['seconds']:.1f}s)", flush=True)
        return path

    airflow_switched = False
    try:
        run("clone", ["git", "clone", "--no-hardlinks", str(ROOT), str(checkout)], cwd=folder)
        if not any(s["name"] == "clean_checkout" for s in report["stages"]):
            tracked = subprocess.check_output(["git", "status", "--porcelain"], cwd=checkout, text=True).strip()
            assert not tracked, tracked
            forbidden = ["backend/artifacts", "backend/.venv-spark", "frontend/node_modules", "backend/mlflow.db", "backend/data/raw/WA_Fn-UseC_-Telco-Customer-Churn.csv"]
            assert not any((checkout / p).exists() for p in forbidden), "Ignored inputs unexpectedly present in clone"
            report["stages"].append({"name": "clean_checkout", "status": "PASS", "ignored_paths_absent": forbidden})
            persist()
        run("windows_environment", [sys.executable, "-m", "venv", str(python.parent.parent)])
        run("windows_dependencies", [python, "-m", "pip", "install", "-r", "requirements/web.txt", "-r", "requirements/kafka.txt"], cwd=checkout / "backend")
        run("data_import", [python, "-m", "pipelines.data_pipeline", "--archive", args.archive.resolve()], cwd=checkout / "backend")
        run("sklearn_training", [python, "-m", "pipelines.training_pipeline"], cwd=checkout / "backend")
        run("sklearn_single", [python, "-m", "pipelines.inference_pipeline", "--record", "examples/customer.json", "--output", "artifacts/predictions/clean_single.json"], cwd=checkout / "backend")
        run("sklearn_batch", [python, "-m", "pipelines.inference_pipeline", "--input", "examples/customers.csv", "--output", "artifacts/predictions/clean_batch.csv"], cwd=checkout / "backend")
        linux_backend = subprocess.check_output(["wsl", "-d", "Ubuntu", "--exec", "wslpath", "-a", str(checkout / "backend").replace("\\", "/")], text=True).strip()
        wsl = ["wsl", "-d", "Ubuntu", "--cd", linux_backend, "--exec"]
        environment_helper = subprocess.check_output(["wsl", "-d", "Ubuntu", "--exec", "wslpath", "-a",
            str(ROOT / "backend/pipelines/spark_environment.py").replace("\\", "/")], text=True).strip()
        run("spark_environment", [*wsl, "python3", environment_helper, "--backend", linux_backend])
        run("spark_dependencies", [*wsl, ".venv-spark/bin/python", "-m", "pip", "install", "-r", "requirements/spark.txt",
                                   "mlflow==3.17.1", "matplotlib==3.11.2", "confluent-kafka==2.16.0"])
        run("spark_training", [*wsl, ".venv-spark/bin/python", "-m", "pipelines.spark_training_pipeline", "--skip-tracking"])
        run("spark_tracking", [python, "-m", "pipelines.spark_tracking"], cwd=checkout / "backend")
        run("spark_single", [*wsl, ".venv-spark/bin/python", "-m", "pipelines.spark_inference_pipeline", "--record", "examples/customer.json", "--output", "artifacts/predictions/clean_spark_single.json"])
        run("spark_batch", [*wsl, ".venv-spark/bin/python", "-m", "pipelines.spark_inference_pipeline", "--input", "examples/customers.csv", "--output", "artifacts/predictions/clean_spark_batch.csv"])
        run("backend_tests", [python, "-m", "pipelines.verification"], cwd=checkout / "backend")
        run("spark_tests", [*wsl, ".venv-spark/bin/python", "-m", "pipelines.verification", "--spark-only"])
        run("npm_install", ["npm.cmd", "ci"], cwd=checkout / "frontend")
        run("frontend_typecheck", ["npm.cmd", "run", "typecheck"], cwd=checkout / "frontend")
        run("frontend_build", ["npm.cmd", "run", "build"], cwd=checkout / "frontend")
        force_browser = not any(s["name"] == "frontend_browser" and s["status"] == "PASS" and s.get("environment_overrides", {}).get("CI") == "1" for s in report["stages"])
        frontend_log = run("frontend_browser", ["node", "node_modules/@playwright/test/cli.js", "test", "--reporter=json"],
                           cwd=checkout / "frontend", env_overrides={"CI": "1"}, force=force_browser)
        browser_text = frontend_log.read_text(encoding="utf-8", errors="replace")
        browser = None
        decoder = json.JSONDecoder()
        for offset, char in enumerate(browser_text):
            if char == "{":
                try:
                    value, _ = decoder.raw_decode(browser_text[offset:])
                    if isinstance(value, dict) and "stats" in value and "suites" in value:
                        browser = value
                        break
                except ValueError:
                    pass
        if browser is None or browser["stats"]["unexpected"] or browser["stats"]["skipped"] or browser["stats"]["expected"] != 6:
            raise ValueError("Expected all six real browser flows to pass without skips")
        frontend_report = {"status": "PASS", "checked_at": utc(), "source_commit": report["source_commit"],
                           "checkout": str(checkout), "fresh_node_modules": True, "fresh_managed_servers": True, "browser": browser,
                           "stages": [s for s in report["stages"] if s["name"].startswith(("npm_", "frontend_"))]}
        save(ROOT / "backend/reports/frontend_verification.json", frontend_report)
        for name in ("frontend-desktop.png", "frontend-mobile.png", "frontend-prediction.png"):
            shutil.copyfile(checkout / "docs/screenshots" / name, ROOT / "docs/screenshots" / name)
        run("kafka_publishing", [python, "-m", "pipelines.kafka_publishing_check"], cwd=checkout / "backend")
        run("kafka_consumption", [python, "-m", "pipelines.kafka_consumption_check", "--include-spark"], cwd=checkout / "backend")
        if args.include_airflow:
            run("main_airflow_stop", ["powershell", "-NoProfile", "-File", ROOT / "scripts/windows/start-airflow.ps1", "-Task", "stop"], cwd=ROOT, force=True)
            airflow_switched = True
            for engine in ("sklearn", "spark"):
                run("airflow_" + engine, ["powershell", "-NoProfile", "-File", checkout / "scripts/windows/start-airflow.ps1", "-Task", "run", "-Engine", engine])
            run("clone_airflow_stop", ["powershell", "-NoProfile", "-File", checkout / "scripts/windows/start-airflow.ps1", "-Task", "stop"], force=True)
            run("main_airflow_restore", ["powershell", "-NoProfile", "-File", ROOT / "scripts/windows/start-airflow.ps1", "-Task", "start"], cwd=ROOT, force=True)
            airflow_switched = False
        else:
            report["limits"] = ["Fresh clone Airflow DAG execution not requested; use --include-airflow for complete Phase 1"]
        for name in ("model_training_report.json", "spark_training_report.json", "test_results.json", "test_results_spark.json", "kafka_publishing_report.json", "kafka_consumption_report.json"):
            data = json.loads((checkout / "backend/reports" / name).read_text(encoding="utf-8"))
            report.setdefault("results", {})[name] = data
        report["frontend"] = {"passed": browser["stats"]["expected"], "failed": browser["stats"]["unexpected"], "skipped": browser["stats"]["skipped"]}
        report.update(status="PASS", completed_at=utc(), includes_fresh_airflow_runs=args.include_airflow)
        persist()
        print("PASS: clean-clone verification complete; report:", REPORT, flush=True)
    except BaseException as error:
        report.update(status="FAIL", error=str(error))
        persist()
        raise
    finally:
        if airflow_switched:
            subprocess.run(["powershell", "-NoProfile", "-File", checkout / "scripts/windows/start-airflow.ps1", "-Task", "stop"], cwd=checkout, env=environment)
            subprocess.run(["powershell", "-NoProfile", "-File", ROOT / "scripts/windows/start-airflow.ps1", "-Task", "start"], cwd=ROOT, env=environment)


if __name__ == "__main__":
    main()
