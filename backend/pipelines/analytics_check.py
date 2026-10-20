"""Run the Phase 2 unit/real PostgreSQL checks and retain credential-free evidence."""
import argparse
import hashlib
import importlib.metadata
import json
import os
import sys
import time
import unittest
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "src"))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--regression", action="store_true", help="Also run existing backend tests; native Spark checks still require WSL")
    args = parser.parse_args()
    from dotenv import load_dotenv
    from opsyntra.analytics.config import AnalyticsConfig
    from opsyntra.analytics.migrations import migrate
    from pipelines.verification import EvidenceResult
    load_dotenv(ROOT / ".env", override=False)
    os.environ["OPSYNTRA_RUN_ANALYTICS_TESTS"] = "1"
    # Create the opt-in suite after setting the flag (also works in discovery).
    suite = unittest.defaultTestLoader.loadTestsFromNames([
        "tests.unit.test_analytics_contracts", "tests.integration.test_analytics_storage"])
    reports = ROOT / "reports"
    started = time.perf_counter()
    with (reports / "analytics_verification.log").open("w", encoding="utf-8") as stream:
        result = unittest.TextTestRunner(stream=stream, resultclass=EvidenceResult, verbosity=2).run(suite)
    config = AnalyticsConfig.from_env()
    with config.connect("migration") as connection:
        migrations = migrate(connection)
        database = connection.info.dbname
        version = connection.execute("SELECT version() AS version").fetchone()["version"]
        tables = connection.execute("SELECT tablename FROM pg_tables WHERE schemaname='analytics' ORDER BY tablename").fetchall()
        grants = connection.execute("""SELECT grantee,table_name,privilege_type FROM information_schema.role_table_grants
            WHERE table_schema='analytics' AND grantee IN ('opsyntra_analytics_reader','opsyntra_analytics_writer')
            ORDER BY grantee,table_name,privilege_type""").fetchall()
    with config.connect("read") as connection:
        main_rows = {table: connection.execute("SELECT count(*) AS n FROM analytics." + table).fetchone()["n"]
                     for table in ("raw_customer_events", "prediction_events", "deadletter_events", "kafka_deliveries")}
    passed = result.wasSuccessful() and not result.skipped and result.testsRun > 0
    files = [*sorted((ROOT / "migrations").glob("*.sql")), *sorted((ROOT / "src/opsyntra/analytics").glob("*.py")),
             ROOT / "pipelines/analytics_check.py", ROOT / "pipelines/analytics_db.py", ROOT / "pipelines/analytics_local.py",
             ROOT / "tests/unit/test_analytics_contracts.py", ROOT / "tests/integration/test_analytics_storage.py", ROOT / "tests/analytics_helpers.py",
             ROOT / "requirements/analytics.txt", ROOT / ".env.example", ROOT / "migrations/README.md",
             ROOT.parent / "start-analytics.ps1", ROOT.parent / "docs/ANALYTICS_STORAGE.md"]
    report = {"status": "PASS" if passed else "FAIL", "checked_at": datetime.now(timezone.utc).isoformat(),
              "command": "python -m pipelines.analytics_check", "database": database, "server": version,
              "driver": importlib.metadata.version("psycopg"), "tests_run": result.testsRun,
              "passed": sum(r["status"] == "passed" for r in result.records), "skipped": len(result.skipped),
              "failures": len(result.failures), "errors": len(result.errors), "seconds": time.perf_counter() - started,
              "tests": result.records, "migrations": migrations, "log": "analytics_verification.log",
              "tables": tables, "application_role_grants": grants, "main_database_rows": main_rows,
              "scope": "Real PostgreSQL; separate test database; UUID fixtures; no Kafka analytics consumer or live dashboard in Phase 2",
              "files": [{"path": str(p.relative_to(ROOT.parent)).replace('\\', '/'), "sha256": hashlib.sha256(p.read_bytes()).hexdigest()} for p in files]}
    (reports / "analytics_verification.json").write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    lines = ["# Phase 2: analytics storage verification", "", f"Status: **{report['status']}**. {report['passed']}/{result.testsRun} tests passed; {len(result.skipped)} skipped.", "",
             f"PostgreSQL: {version}. Driver: psycopg {report['driver']}.", "",
             "Tests ran against the separately provisioned `opsyntra_analytics_test` database; the main analytics database receives migrations only.", "",
             "Verified schema reruns/checksums, duplicate and conflicting logical IDs, source provenance conflicts, transaction rollback, concurrent inserts,",
             "both arrival orders, original event profiles, model/engine-separated metrics, UTC timestamps and actual read/write database grants.", "",
             "| Test | Result |", "|---|---|"]
    lines.extend(f"| {r['test'].split('.')[-1]} | {r['status']} |" for r in result.records)
    lines += ["", "See [machine-readable results](analytics_verification.json), [test transcript](analytics_verification.log),",
              "and [schema/setup/metric definitions](../../docs/ANALYTICS_STORAGE.md).", "",
              "Phase 3 will connect Kafka to this store; Phase 4 will expose analytics APIs and the dashboard. These are not implemented by this checkpoint."]
    (reports / "analytics_verification.md").write_text("\n".join(lines) + "\n", encoding="utf-8")
    print(f"{report['status']}: {report['passed']}/{result.testsRun} analytics checks passed, {len(result.skipped)} skipped")
    if not passed:
        print((reports / "analytics_verification.log").read_text()[-16000:])
    if args.regression:
        suite = unittest.defaultTestLoader.discover(str(ROOT / "tests"), top_level_dir=str(ROOT))
        started = time.perf_counter()
        with (reports / "analytics_regression.log").open("w", encoding="utf-8") as stream:
            regression = unittest.TextTestRunner(stream=stream, resultclass=EvidenceResult, verbosity=2).run(suite)
        allowed_skips = all(test.id().startswith("tests.integration.test_spark_workflow.") for test, _ in regression.skipped)
        regression_passed = regression.wasSuccessful() and allowed_skips
        data = {"status": "PASS" if regression_passed else "FAIL", "checked_at": datetime.now(timezone.utc).isoformat(),
                "command": "python -m pipelines.analytics_check --regression", "tests_run": regression.testsRun,
                "passed": sum(r["status"] == "passed" for r in regression.records), "skipped": len(regression.skipped),
                "failures": len(regression.failures), "errors": len(regression.errors), "tests": regression.records,
                "seconds": time.perf_counter() - started, "log": "analytics_regression.log",
                "scope": "Existing backend and Phase 2 PostgreSQL checks on Windows; four native Spark checks remain in separate WSL verification"}
        (reports / "analytics_regression.json").write_text(json.dumps(data, indent=2) + "\n", encoding="utf-8")
        print(f"Regression {data['status']}: {data['passed']}/{regression.testsRun} passed, {data['skipped']} native Spark skips")
        if not regression_passed:
            print((reports / "analytics_regression.log").read_text()[-16000:])
        passed = passed and regression_passed
    return 0 if passed else 1


if __name__ == "__main__":
    raise SystemExit(main())
