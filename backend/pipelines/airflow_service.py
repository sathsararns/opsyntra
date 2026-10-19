"""Manage a project-specific local Airflow service in Ubuntu WSL."""

import argparse
import hashlib
import json
import os
import signal
import subprocess
import sys
import time
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
VENV = Path.home() / ".local/share/opsyntra/airflow-venv"
AIRFLOW = VENV / "bin/airflow"
IDENTITY = hashlib.sha256(str(ROOT).encode()).hexdigest()[:10]
STATE = Path.home() / f".local/share/opsyntra/airflow-home-{IDENTITY}"
LOCAL = ROOT / ".airflow-runtime"
DAG_ID = "opsyntra_ml_workflow"


def environment():
    return {**os.environ, "PATH": str(VENV / "bin") + os.pathsep + os.environ.get("PATH", ""),
            "VIRTUAL_ENV": str(VENV), "AIRFLOW_HOME": str(STATE), "AIRFLOW__CORE__DAGS_FOLDER": str(ROOT / "dags"),
            "AIRFLOW__CORE__LOAD_EXAMPLES": "false", "AIRFLOW__CORE__EXECUTOR": "LocalExecutor",
            "AIRFLOW__CORE__PARALLELISM": "2", "AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION": "false",
            "AIRFLOW__CORE__SIMPLE_AUTH_MANAGER_ALL_ADMINS": "true", "AIRFLOW__API__HOST": "127.0.0.1",
            "AIRFLOW__API__PORT": "8080", "AIRFLOW__API__WORKERS": "1",
            "AIRFLOW__API__BASE_URL": "http://localhost:8080", "AIRFLOW__CORE__EXECUTION_API_SERVER_URL": "http://127.0.0.1:8080/execution/",
            "AIRFLOW__DAG_PROCESSOR__REFRESH_INTERVAL": "10", "AIRFLOW__DAG_PROCESSOR__MIN_FILE_PROCESS_INTERVAL": "10",
            "AIRFLOW__LOGGING__LOGGING_LEVEL": "INFO", "PYTHONUNBUFFERED": "1"}


def cli(*arguments, check=True):
    return subprocess.run([str(AIRFLOW), *arguments], env=environment(), cwd=ROOT,
                          text=True, capture_output=True, check=check)


def healthy():
    try:
        with urllib.request.urlopen("http://127.0.0.1:8080/api/v2/monitor/health", timeout=3) as response:
            data = json.load(response)
            return data.get("scheduler", {}).get("status") == "healthy" and data.get("dag_processor", {}).get("status") == "healthy"
    except Exception:
        return False


def start():
    if healthy():
        print("Airflow is already running at http://localhost:8080", flush=True)
        return
    if not AIRFLOW.is_file():
        raise RuntimeError("Airflow environment missing. Run start-airflow.ps1 -Task setup.")
    STATE.mkdir(parents=True, exist_ok=True)
    LOCAL.mkdir(exist_ok=True)
    with (STATE / "standalone.log").open("a") as log:
        process = subprocess.Popen([str(AIRFLOW), "standalone"], cwd=ROOT, env=environment(),
                                   stdin=subprocess.DEVNULL, stdout=log, stderr=subprocess.STDOUT, start_new_session=True)
    (STATE / "service.pid").write_text(str(process.pid))
    (LOCAL / "service.json").write_text(json.dumps({"home": str(STATE), "venv": str(VENV),
                                                   "pid": process.pid, "url": "http://localhost:8080"}, indent=2))
    for _ in range(120):
        if healthy():
            print("Airflow ready at http://localhost:8080", flush=True)
            return
        if process.poll() is not None:
            raise RuntimeError(f"Airflow exited. Inspect {STATE / 'standalone.log'}")
        time.sleep(1)
    raise TimeoutError(f"Airflow startup timed out. Inspect {STATE / 'standalone.log'}")


def stop():
    pid_file = STATE / "service.pid"
    if pid_file.exists():
        pid = int(pid_file.read_text())
        command = Path(f"/proc/{pid}/cmdline")
        if command.exists() and b"airflow" in command.read_bytes() and b"standalone" in command.read_bytes():
            os.killpg(pid, signal.SIGTERM)
        pid_file.unlink(missing_ok=True)
    print("Stopped this project's Airflow service.")


def parse_json(output):
    # Airflow can emit notices before CLI JSON; never parse notice lines as data.
    for index, char in enumerate(output):
        if char in "[{":
            try:
                return json.loads(output[index:])
            except json.JSONDecodeError:
                pass
    raise ValueError("Airflow command did not return JSON: " + output[-2000:])


def run(engine, prediction_input):
    start()
    for _ in range(60):
        dags = cli("dags", "list", "--output", "json", check=False)
        if dags.returncode == 0 and any(row["dag_id"] == DAG_ID for row in parse_json(dags.stdout)):
            break
        time.sleep(2)
    else:
        raise RuntimeError("DAG was not imported. Run start-airflow.ps1 -Task imports.")
    cli("dags", "unpause", DAG_ID)
    run_id = f"manual__opsyntra_{engine}_{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%S%fZ')}"
    conf = json.dumps({"engine": engine, "prediction_input": prediction_input})
    result = cli("dags", "trigger", DAG_ID, "--run-id", run_id, "--conf", conf, "--output", "json")
    print(f"Triggered {run_id}; watching eight scheduled tasks.", flush=True)
    previous = None
    for _ in range(1440):
        process = cli("tasks", "states-for-dag-run", DAG_ID, run_id, "--output", "json", check=False)
        if process.returncode == 0:
            tasks = parse_json(process.stdout)
            states = {t["task_id"]: t["state"] for t in tasks}
            if states != previous:
                print(json.dumps(states), flush=True)
                previous = states
            if len(states) == 8 and all(state == "success" for state in states.values()):
                # Wait for scheduler to finalize the DagRun, not just the last task.
                for _ in range(30):
                    rows = parse_json(cli("dags", "list-runs", DAG_ID, "--output", "json").stdout)
                    row = next((r for r in rows if r.get("run_id", r.get("dag_run_id")) == run_id), None)
                    if row and row["state"] == "success":
                        evidence = {"dag_id": DAG_ID, "run_id": run_id, "engine": engine, "state": "success",
                                    "tasks": tasks, "dag_run": row, "airflow_version": "3.3.2"}
                        LOCAL.mkdir(exist_ok=True)
                        (LOCAL / f"last_success_{engine}.json").write_text(json.dumps(evidence, indent=2, default=str))
                        print(f"SUCCESS: complete {engine} DAG run {run_id}", flush=True)
                        return
                    time.sleep(2)
            if any(state == "failed" for state in states.values()):
                raise RuntimeError(f"DAG run failed. Inspect task logs for {run_id} at http://localhost:8080")
        time.sleep(3)
    raise TimeoutError("DAG monitoring reached its two-hour limit.")


def record_success(engine):
    report = json.loads((ROOT / "artifacts/airflow" / f"latest_success_{engine}.json").read_text())
    run_id = report["run_id"]
    tasks = parse_json(cli("tasks", "states-for-dag-run", DAG_ID, run_id, "--output", "json").stdout)
    rows = parse_json(cli("dags", "list-runs", DAG_ID, "--output", "json").stdout)
    row = next(r for r in rows if r.get("run_id", r.get("dag_run_id")) == run_id)
    if row["state"] != "success" or len(tasks) != 8 or any(t["state"] != "success" for t in tasks):
        raise RuntimeError("Airflow has not finalized all eight tasks successfully")
    evidence = {"dag_id": DAG_ID, "run_id": run_id, "engine": engine, "state": "success",
                "tasks": tasks, "dag_run": row, "airflow_version": "3.3.2"}
    (LOCAL / f"last_success_{engine}.json").write_text(json.dumps(evidence, indent=2, default=str))
    print(json.dumps(evidence, indent=2, default=str))


def run_kafka(kind, engine, duration, demo_events, events_per_sec):
    """Trigger a paused bonus DAG without enabling its daily schedule."""
    start()
    dag_id = "opsyntra_kafka_" + kind
    for _ in range(60):
        dags = cli("dags", "list", "--output", "json", check=False)
        if dags.returncode == 0 and any(row["dag_id"] == dag_id for row in parse_json(dags.stdout)):
            break
        time.sleep(2)
    else:
        raise RuntimeError("Kafka DAG missing; inspect import errors")
    # Paused DAGs do not execute manual runs. Temporarily unpause, then restore
    # the initial state. New bonus DAGs start paused to avoid unsolicited replay.
    rows = parse_json(cli("dags", "list", "--output", "json").stdout)
    was_paused = next(r for r in rows if r["dag_id"] == dag_id).get("is_paused", True)
    run_id = f"manual__opsyntra_kafka_{kind}_{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%S%fZ')}"
    conf = json.dumps({"engine": engine, "duration_seconds": duration, "demo_events": demo_events, "events_per_sec": events_per_sec})
    cli("dags", "trigger", dag_id, "--run-id", run_id, "--conf", conf, "--output", "json")
    cli("dags", "unpause", dag_id)
    print(f"Triggered {dag_id}: {run_id}; watching four scheduled tasks.", flush=True)
    previous = None
    try:
        for _ in range(1440):
            process = cli("tasks", "states-for-dag-run", dag_id, run_id, "--output", "json", check=False)
            if process.returncode == 0:
                tasks = parse_json(process.stdout)
                states = {t["task_id"]: t["state"] for t in tasks}
                if states != previous:
                    print(json.dumps(states), flush=True)
                    previous = states
                runs = parse_json(cli("dags", "list-runs", dag_id, "--output", "json").stdout)
                row = next((r for r in runs if r.get("run_id", r.get("dag_run_id")) == run_id), None)
                if row and row["state"] == "success" and len(states) == 4 and all(s == "success" for s in states.values()):
                    evidence = {"dag_id": dag_id, "run_id": run_id, "engine": engine, "state": "success", "tasks": tasks, "dag_run": row}
                    LOCAL.mkdir(exist_ok=True)
                    (LOCAL / f"last_success_kafka_{kind}.json").write_text(json.dumps(evidence, indent=2, default=str))
                    print(f"SUCCESS: complete {kind} Kafka DAG run {run_id}", flush=True)
                    return
                if row and row["state"] == "failed":
                    raise RuntimeError(f"Kafka DAG failed; inspect http://localhost:8080/dags/{dag_id}")
            time.sleep(3)
        raise TimeoutError("Kafka DAG monitoring reached its two-hour limit")
    finally:
        if was_paused:
            cli("dags", "pause", dag_id, check=False)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("task", choices=["start", "stop", "run", "status", "imports", "record", "run-kafka"])
    parser.add_argument("--engine", choices=["sklearn", "spark"], default="sklearn")
    parser.add_argument("--prediction-input", default="examples/customers.csv")
    parser.add_argument("--kafka-kind", choices=["batch", "streaming"], default="batch")
    parser.add_argument("--duration-seconds", type=int, default=60)
    parser.add_argument("--demo-events", type=int, default=0)
    parser.add_argument("--events-per-sec", type=float, default=5)
    args = parser.parse_args()
    if args.task == "start":
        start()
    elif args.task == "stop":
        stop()
    elif args.task == "run":
        run(args.engine, args.prediction_input)
    elif args.task == "run-kafka":
        run_kafka(args.kafka_kind, args.engine, args.duration_seconds, args.demo_events, args.events_per_sec)
    elif args.task == "status":
        print("healthy" if healthy() else "not running")
    elif args.task == "record":
        record_success(args.engine)
    else:
        result = cli("dags", "list-import-errors", "--output", "json", check=False)
        print(result.stdout)
        print(result.stderr)


if __name__ == "__main__":
    main()
