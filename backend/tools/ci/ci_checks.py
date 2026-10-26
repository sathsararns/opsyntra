"""Strict CI test profiles with JSON/JUnit output and no silently skipped checks."""
import argparse
import json
import os
import platform
import sys
import time
import unittest
from datetime import datetime, timezone
from pathlib import Path
from xml.etree.ElementTree import Element, SubElement, ElementTree

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "src"))

POSTGRES = ("tests.integration.test_analytics_storage.", "tests.integration.test_analytics_ingestion.",
            "tests.integration.test_analytics_api.AnalyticsApiDatabaseTests.", "tests.integration.test_notebook_analytics.")
SPARK = "tests.integration.test_spark_workflow."


def flatten(suite):
    for item in suite:
        if isinstance(item, unittest.TestSuite): yield from flatten(item)
        else: yield item


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--profile", choices=("core", "postgres", "spark"), required=True)
    parser.add_argument("--output", type=Path, default=ROOT / "artifacts/ci/reports")
    args = parser.parse_args()
    if args.profile == "postgres":
        from dotenv import load_dotenv
        load_dotenv(ROOT / ".env", override=False)
    os.environ["OPSYNTRA_RUN_ANALYTICS_TESTS"] = "1" if args.profile == "postgres" else "0"
    os.environ["OPSYNTRA_RUN_SPARK_TESTS"] = "1" if args.profile == "spark" else "0"
    from tools.verification.runner import EvidenceResult
    os.chdir(ROOT)
    discovered = list(flatten(unittest.defaultTestLoader.discover(str(ROOT / "tests"), top_level_dir=str(ROOT))))
    def selected(test):
        identity = test.id()
        if "_FailedTest" in identity: return True
        if args.profile == "postgres": return identity.startswith(POSTGRES)
        if args.profile == "spark": return identity.startswith(SPARK)
        return not identity.startswith((*POSTGRES, SPARK))
    suite = unittest.TestSuite(t for t in discovered if selected(t))
    args.output.mkdir(parents=True, exist_ok=True)
    began = time.perf_counter()
    with (args.output / f"{args.profile}.txt").open("w", encoding="utf-8") as stream:
        result = unittest.TextTestRunner(stream=stream, verbosity=2, resultclass=EvidenceResult).run(suite)
    success = result.wasSuccessful() and result.testsRun > 0 and not result.skipped
    report = {"status": "PASS" if success else "FAIL", "profile": args.profile, "python": platform.python_version(),
              "checked_at": datetime.now(timezone.utc).isoformat(), "tests_run": result.testsRun,
              "passed": sum(t["status"] == "passed" for t in result.records), "skipped": len(result.skipped),
              "failures": len(result.failures), "errors": len(result.errors), "seconds": time.perf_counter()-began,
              "tests": result.records}
    (args.output / f"{args.profile}.json").write_text(json.dumps(report, indent=2)+"\n", encoding="utf-8")
    xml = Element("testsuite", name=args.profile, tests=str(result.testsRun), failures=str(len(result.failures)), errors=str(len(result.errors)), skipped=str(len(result.skipped)), time=str(report["seconds"]))
    for row in result.records:
        identity, _, name = row["test"].rpartition(".")
        case = SubElement(xml, "testcase", classname=identity, name=name, time=str(row["seconds"]))
        if row["status"] != "passed":
            element = SubElement(case, {"failed":"failure","error":"error","skipped":"skipped"}[row["status"]])
            element.text = row["detail"] or ""
    # Class setup/import failures are present in unittest.errors even without a started testcase.
    if result.errors and not any(r["status"] == "error" for r in result.records):
        SubElement(SubElement(xml,"testcase",name="setup_or_import"),"error").text = str(result.errors[0][1])
    ElementTree(xml).write(args.output / f"{args.profile}.xml", encoding="utf-8", xml_declaration=True)
    print(f"{report['status']}: {args.profile}: {report['passed']}/{report['tests_run']} passed; {report['skipped']} skipped", flush=True)
    if not success: print((args.output / f"{args.profile}.txt").read_text(encoding="utf-8")[-9000:])
    return 0 if success else 1


if __name__ == "__main__": raise SystemExit(main())
