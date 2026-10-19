"""Run real unittest checks and retain reproducible, machine-readable evidence."""
import argparse
import importlib.metadata
import json
import os
import platform
import sys
import time
import unittest
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))


class EvidenceResult(unittest.TextTestResult):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.records = []
        self.started = {}

    def startTest(self, test):
        self.started[test.id()] = time.perf_counter()
        super().startTest(test)

    def record(self, test, status, detail=None):
        self.records.append({"test": test.id(), "status": status,
                             "seconds": time.perf_counter() - self.started.get(test.id(), time.perf_counter()), "detail": detail})

    def addSuccess(self, test):
        self.record(test, "passed")
        super().addSuccess(test)

    def addSkip(self, test, reason):
        self.record(test, "skipped", reason)
        super().addSkip(test, reason)

    def addError(self, test, err):
        self.record(test, "error", str(err[1]))
        super().addError(test, err)

    def addFailure(self, test, err):
        self.record(test, "failed", str(err[1]))
        super().addFailure(test, err)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--spark-only", action="store_true")
    args = parser.parse_args()
    if args.spark_only:
        if os.name == "nt":
            parser.error("Run --spark-only inside the configured Ubuntu Spark runtime")
        os.environ["OPSYNTRA_RUN_SPARK_TESTS"] = "1"
    os.chdir(ROOT)
    name = "test_results_spark" if args.spark_only else "test_results"
    directory = ROOT / "reports"
    directory.mkdir(exist_ok=True)
    suite = (unittest.defaultTestLoader.loadTestsFromName("tests.integration.test_spark_workflow") if args.spark_only
             else unittest.defaultTestLoader.discover(str(ROOT / "tests"), top_level_dir=str(ROOT)))
    started = time.perf_counter()
    with (directory / (name + ".log")).open("w", encoding="utf-8") as log:
        result = unittest.TextTestRunner(stream=log, resultclass=EvidenceResult, verbosity=2).run(suite)
    versions = {}
    for package in ("numpy", "pandas", "scikit-learn", "joblib", "xgboost", "mlflow", "pyspark", "confluent-kafka"):
        try:
            versions[package] = importlib.metadata.version(package)
        except importlib.metadata.PackageNotFoundError:
            pass
    report = {"status": "PASS" if result.wasSuccessful() else "FAIL", "checked_at": datetime.now(timezone.utc).isoformat(),
              "command": "python -m pipelines.verification" + (" --spark-only" if args.spark_only else ""),
              "python": platform.python_version(), "platform": platform.platform(), "dependencies": versions,
              "tests_run": result.testsRun, "passed": sum(r["status"] == "passed" for r in result.records),
              "skipped": len(result.skipped), "failures": len(result.failures), "errors": len(result.errors),
              "seconds": time.perf_counter() - started, "tests": result.records, "log": name + ".log"}
    (directory / (name + ".json")).write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    print(f"{report['status']}: {result.testsRun} tests; {report['passed']} passed, {report['skipped']} skipped; {report['seconds']:.2f}s", flush=True)
    if not result.wasSuccessful():
        print((directory / (name + ".log")).read_text(encoding="utf-8")[-12000:])
    print("Evidence:", directory / (name + ".json"))
    return 0 if result.wasSuccessful() else 1


if __name__ == "__main__":
    raise SystemExit(main())
