"""Execute all Phase 5 notebooks using this Python, and retain verified exports."""
import ast
import hashlib
import importlib.metadata
import json
import os
import platform
import sys
import time
import unittest
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import unquote, urlsplit
from uuid import uuid4

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "src"))


def main():
    import nbformat
    from nbclient import NotebookClient
    from dotenv import dotenv_values, load_dotenv
    from tools.verification.runner import EvidenceResult

    load_dotenv(ROOT / ".env", override=False)
    os.environ["OPSYNTRA_RUN_ANALYTICS_TESTS"] = "1"
    identity = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ") + "-" + uuid4().hex[:8]
    evidence = ROOT.parent / "docs/evidence/notebooks" / identity
    evidence.mkdir(parents=True)
    runtime = ROOT / "artifacts/notebook_runtime" / identity
    kernel = runtime / "kernels/opsyntra-notebook-check"
    kernel.mkdir(parents=True)
    (kernel / "kernel.json").write_text(json.dumps({"argv": [sys.executable, "-m", "ipykernel_launcher", "-f", "{connection_file}"], "display_name": "Opsyntra verification", "language": "python"}))
    previous_jupyter_path = os.environ.get("JUPYTER_PATH")
    os.environ["JUPYTER_PATH"] = str(runtime) + (os.pathsep + previous_jupyter_path if previous_jupyter_path else "")
    secrets = set()
    for key, value in {**dotenv_values(ROOT / ".env"), **os.environ}.items():
        if key.endswith("_URL") and value:
            password = urlsplit(value).password
            if password: secrets.update((password, unquote(password)))

    def no_secrets(value):
        if any(secret in value for secret in secrets):
            raise ValueError("Credential detected in generated output; output was not saved")

    report = {"status": "RUNNING", "command": "python -m tools.verification.notebook_check", "started_at": datetime.now(timezone.utc).isoformat(),
              "python": platform.python_version(), "packages": {name: importlib.metadata.version(name) for name in
              ("nbformat", "nbclient", "ipykernel", "jupyter-client", "pandas", "matplotlib", "scikit-learn", "psycopg")}, "notebooks": []}
    report_path = ROOT / "reports/notebook_verification.json"
    report_path.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    try:
        suite = unittest.defaultTestLoader.loadTestsFromNames(["tests.unit.test_notebooks", "tests.integration.test_notebook_analytics"])
        with (evidence / "tests.txt").open("w", encoding="utf-8") as stream:
            result = unittest.TextTestRunner(stream=stream, resultclass=EvidenceResult, verbosity=2).run(suite)
        report["tests"] = {"run": result.testsRun, "passed": sum(row["status"] == "passed" for row in result.records), "skipped": len(result.skipped), "results": result.records}
        if not result.wasSuccessful() or result.skipped:
            raise RuntimeError("Notebook input/real PostgreSQL API-agreement checks failed; see tests.txt")
        for path in sorted((ROOT / "notebooks").glob("0[123]_*.ipynb")):
            print("Executing", path.name, flush=True)
            notebook = nbformat.read(path, as_version=4)
            nbformat.validate(notebook)
            for cell in notebook.cells:
                if cell.cell_type != "code": continue
                tree = ast.parse(cell.source)
                calls = {getattr(n.func, "attr", getattr(n.func, "id", "")) for n in ast.walk(tree) if isinstance(n, ast.Call)}
                if calls & {"fit", "fit_transform", "run_training", "select_threshold", "GridSearchCV", "train_test_split"}:
                    raise ValueError("Notebook contains a training/tuning call")
            began = time.perf_counter()
            NotebookClient(notebook, timeout=180, kernel_name="opsyntra-notebook-check", allow_errors=False,
                           resources={"metadata": {"path": str(ROOT)}}).execute()
            serialized = nbformat.writes(notebook)
            no_secrets(serialized)
            cells = [c for c in notebook.cells if c.cell_type == "code"]
            if any(c.execution_count is None or any(o.output_type == "error" for o in c.outputs) for c in cells):
                raise ValueError("Notebook has unexecuted or failed cells")
            path.write_text(serialized, encoding="utf-8")
            manifest_path = ROOT / "reports/notebooks" / path.stem / "manifest.json"
            manifest = json.loads(manifest_path.read_text())
            for filename, expected in manifest["outputs"].items():
                output = manifest_path.parent / filename
                if hashlib.sha256(output.read_bytes()).hexdigest() != expected: raise ValueError("Export hash mismatch")
                if output.suffix in (".csv", ".svg", ".json"): no_secrets(output.read_text(encoding="utf-8"))
            forbidden = [p for p in manifest.get("inputs", {}) if p.endswith("/test.csv")]
            if forbidden or manifest.get("fitted_models") != 0: raise ValueError("Notebook touched held-out features or fitted a model")
            report["notebooks"].append({"name": path.name, "status": "PASS", "executed_code_cells": len(cells), "seconds": time.perf_counter() - began,
                                        "manifest": str(manifest_path.relative_to(ROOT.parent)).replace("\\", "/"), "export_count": len(manifest["outputs"]),
                                        **({"snapshot": {k: manifest[k] for k in ("engine", "model_version", "start", "end", "snapshot_at", "logical_predictions")}} if "snapshot_at" in manifest else {})})
            print("PASS", path.name, flush=True)
        if len(report["notebooks"]) != 3: raise ValueError("Expected exactly three notebooks")
        report["status"] = "PASS"
    except Exception as error:
        report["status"] = "FAIL"
        detail = str(error)
        for secret in secrets: detail = detail.replace(secret, "[redacted]")
        (evidence / "error.txt").write_text(type(error).__name__ + ": " + detail, encoding="utf-8")
        report["error"] = {"type": type(error).__name__, "message": detail[:500]}
    finally:
        if previous_jupyter_path is None: os.environ.pop("JUPYTER_PATH", None)
        else: os.environ["JUPYTER_PATH"] = previous_jupyter_path
    report["finished_at"] = datetime.now(timezone.utc).isoformat()
    paths = [Path(__file__), ROOT / "src/opsyntra/ml/notebooks.py", ROOT / "requirements/notebooks.txt",
             ROOT / "tests/unit/test_notebooks.py", ROOT / "tests/integration/test_notebook_analytics.py",
             *sorted((ROOT / "notebooks").glob("*.ipynb")), *sorted((ROOT / "reports/notebooks").rglob("*")), *sorted(evidence.glob("*"))]
    report["files"] = [{"path": str(p.relative_to(ROOT.parent)).replace("\\", "/"), "sha256": hashlib.sha256(p.read_bytes()).hexdigest()} for p in paths if p.is_file()]
    report_path.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    (evidence / "verification.json").write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    lines = ["# Phase 5 notebook verification", "", f"Status: **{report['status']}**.", "",
             "Command from backend: `python -m tools.verification.notebook_check`. Kernels use the invoking Python environment.", "",
             "| Notebook | Executed code cells | Exports | Status |", "|---|---:|---:|---|"]
    for item in report["notebooks"]:
        lines.append(f"| {item['name']} | {item['executed_code_cells']} | {item['export_count']} | {item['status']} |")
    if "tests" in report: lines += ["", f"Input/SQL/API checks: {report['tests']['passed']}/{report['tests']['run']} passed; {report['tests']['skipped']} skipped."]
    lines += ["", "No training or tuning calls; no held-out test feature CSV is read. Existing final test metrics are displayed as recorded assessments. Validation curves are checked against saved AP/ROC-AUC. Figure/table export hashes and credential scans passed only when status is PASS.", "",
              "Streaming analysis reads the application database with the reader role in one repeatable-read transaction; real integration fixtures use the separate test database. Its scope is engine/model-specific and excludes the current partial UTC hour. Snapshot CSVs and metadata preserve the inputs; rerunning against a live database can change results.", "",
              "Synthetic streaming replay is a simulation, not evidence of real company churn trends or observed model accuracy.", "",
              "See [setup and interpretation](../../docs/NOTEBOOKS.md), [JSON results and SHA-256 hashes](notebook_verification.json), and [exported figures/tables](notebooks/).", ""]
    (ROOT / "reports/notebook_verification.md").write_text("\n".join(lines), encoding="utf-8")
    print(report["status"], report.get("error", {"notebooks": len(report["notebooks"])}), flush=True)
    return 0 if report["status"] == "PASS" else 1


if __name__ == "__main__": raise SystemExit(main())
