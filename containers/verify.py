"""Verify the real core stack in a NEW Compose project; never reset existing application volumes."""
import argparse
import hashlib
import json
import os
import secrets
import socket
import subprocess
import time
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--project", default="opsyntra-verify-" + datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S"))
    args = parser.parse_args()
    if not args.project.startswith("opsyntra-verify-"):
        raise ValueError("Verification requires its own opsyntra-verify-* project")
    private = ROOT / ".verification/containers" / args.project
    if private.exists():
        raise ValueError("Verification project already used; choose a new project name")
    private.mkdir(parents=True)
    directory = ROOT / "docs/evidence/containers" / args.project
    directory.mkdir(parents=True, exist_ok=True)
    env_file = private / ".env"
    ports = {}
    for name in ("FRONTEND_PORT", "API_PORT", "KAFKA_PORT", "POSTGRES_PORT", "MLFLOW_PORT", "AIRFLOW_PORT"):
        with socket.socket() as sock:
            sock.bind(("127.0.0.1", 0))
            ports[name] = sock.getsockname()[1]
    values = {"COMPOSE_PROJECT_NAME": args.project, **ports,
              **{k: secrets.token_hex(24) for k in ("POSTGRES_PASSWORD", "ANALYTICS_OWNER_PASSWORD", "ANALYTICS_WRITE_PASSWORD", "ANALYTICS_READ_PASSWORD")}}
    env_file.write_text("\n".join(f"{k}={v}" for k, v in values.items()) + "\n", encoding="utf-8")
    prefix = ["docker", "compose", "--env-file", str(env_file), "-p", args.project]
    environment = {k: v for k, v in os.environ.items() if k not in values}
    def compose(*command, name=None, timeout=900):
        print(f"Compose {command[0]}: {name or 'check'}", flush=True)
        result = subprocess.run([*prefix, *command], cwd=ROOT, env=environment, text=True, encoding="utf-8", errors="replace", capture_output=True, timeout=timeout)
        if name:
            output = result.stdout + result.stderr
            for key in ("POSTGRES_PASSWORD", "ANALYTICS_OWNER_PASSWORD", "ANALYTICS_WRITE_PASSWORD", "ANALYTICS_READ_PASSWORD"):
                output = output.replace(str(values[key]), "[REDACTED]")
            (directory / (name + ".txt")).write_text(output, encoding="utf-8")
        if result.returncode:
            raise RuntimeError(f"Compose {command[0]} failed ({result.returncode}); inspect {name or 'verification logs'}")
        return result.stdout
    def request(endpoint):
        with urllib.request.urlopen(f"http://127.0.0.1:{ports['FRONTEND_PORT']}/api/analytics/{endpoint}", timeout=15) as response:
            return json.load(response)
    def until(operation, predicate, timeout=90):
        deadline = time.monotonic() + timeout
        error = None
        while time.monotonic() < deadline:
            try:
                value = operation()
                if predicate(value):
                    return value
            except Exception as e:
                error = type(e).__name__
            time.sleep(1)
        raise RuntimeError(f"Acceptance condition timed out; last error type={error}")
    def snapshot(label):
        return json.loads(compose("exec", "-T", "api", "python", "container_checks.py", "snapshot", name=label))
    def ledger(label):
        return json.loads(compose("exec", "-T", "inference", "python", "container_checks.py", "ledger", name=label))
    report = {"status": "RUNNING", "project": args.project, "started_at": datetime.now(timezone.utc).isoformat(), "ports": ports, "checks": []}
    try:
        compose("config", "--quiet", name="config")
        compose("up", "--build", "-d", "--wait", "--wait-timeout", "600", name="up-build", timeout=2400)
        initial = request("summary")
        assert initial["metrics"]["prediction_volume"] == 0, "Project is not empty"
        report["checks"].append("documented core up --build and healthy empty dashboard")
        started = time.monotonic()
        compose("run", "--rm", "--no-deps", "producer", "producer", "--mode", "streaming", "--events-per-sec", "5", "--max-events", "3", "--exclude-churn", name="producer-three")
        summary = until(lambda: request("summary"), lambda x: x["metrics"]["prediction_volume"] == 3)
        report["producer_to_proxy_seconds"] = time.monotonic() - started
        (directory / "summary.json").write_text(json.dumps(summary, indent=2))
        before = until(lambda: snapshot("before"), lambda x: sum(max(0, v) for v in x["input_offsets"].values()) == 3)
        before_ledger = ledger("ledger-before")
        assert before["counts"]["raw_customer_events"] == before["counts"]["prediction_events"] == 3
        assert not before["reader"]["can_insert"]
        report["checks"].append("real producer -> saved model -> PostgreSQL -> Next.js proxy; SQL counts agree; reader cannot insert")
        browser_env = {**environment, "OPSYNTRA_CONTAINER_WEB_URL": f"http://127.0.0.1:{ports['FRONTEND_PORT']}", "OPSYNTRA_CONTAINER_EVIDENCE": str(directory)}
        browser = subprocess.run(["node", "containers/browser-check.mjs"], cwd=ROOT, env=browser_env, text=True, capture_output=True, timeout=90)
        (directory / "browser.txt").write_text(browser.stdout + browser.stderr)
        if browser.returncode:
            raise RuntimeError("Real container browser check failed")
        report["checks"].append("production browser shows actual event/model IDs and matching count")
        compose("restart", "kafka", "postgres", "inference", "analytics", "api", "frontend", name="restart")
        compose("up", "-d", "--wait", "--wait-timeout", "180", name="restart-readiness", timeout=300)
        until(lambda: request("health"), lambda x: x["broker"] == "ready" and x["consumer_state"] == "active" and x["lag"] == 0)
        until(lambda: request("summary"), lambda x: x["metrics"]["prediction_volume"] == 3)
        until(lambda: snapshot("after-restart"), lambda x: x == before)
        assert ledger("ledger-after-restart") == before_ledger
        report["checks"].append("service restart preserves database, broker offsets, model hash and SQLite outbox")
        compose("down", name="down-preserve")
        compose("up", "-d", "--wait", "--wait-timeout", "300", name="recreate", timeout=600)
        until(lambda: request("health"), lambda x: x["broker"] == "ready" and x["consumer_state"] == "active" and x["lag"] == 0)
        until(lambda: snapshot("after-recreate"), lambda x: x == before)
        assert ledger("ledger-after-recreate") == before_ledger
        report["checks"].append("down/up container recreation preserves all named-volume results and recovery state")
        replay = json.loads(compose("exec", "-T", "inference", "python", "container_checks.py", "replay", name="replay"))
        until(lambda: snapshot("replay-observed"), lambda x: x["counts"]["kafka_deliveries"] == 9)
        after = snapshot("after-replay")
        assert after["predictions"] == before["predictions"]
        assert after["counts"]["raw_customer_events"] == after["counts"]["prediction_events"] == 3
        assert ledger("ledger-after-replay") == before_ledger
        report["checks"].append("three repeated raw events add physical deliveries without logical prediction inflation")
        report.update(status="PASS", before=before, after=after, ledger=before_ledger, replay=replay)
    finally:
        compose("ps", "--all", name="services", timeout=30)
        compose("logs", "--no-color", "--tail", "120", name="services-log", timeout=45)
        compose("down", name="final-stop-preserves-volumes", timeout=150)
        report["finished_at"] = datetime.now(timezone.utc).isoformat()
        report["volumes_preserved"] = True
        sources = [ROOT / "compose.yaml", ROOT / ".dockerignore", *sorted((ROOT / "containers").glob("*"))]
        report["source_sha256"] = {str(p.relative_to(ROOT)).replace("\\", "/"): hashlib.sha256(p.read_bytes()).hexdigest() for p in sources if p.is_file()}
        if report["status"] != "PASS":
            report["status"] = "FAIL"
        (directory / "verification.json").write_text(json.dumps(report, indent=2))
        print(json.dumps({"status": report["status"], "evidence": str(directory), "checks": report["checks"]}, indent=2))


if __name__ == "__main__":
    main()
