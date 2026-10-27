"""Build and verify live analytics with real producer/model/Kafka/SQL/browser flow."""
import argparse
import csv
import hashlib
import json
import os
import shutil
import signal
import socket
import subprocess
import sys
import time
import unittest
from datetime import datetime,timezone
from pathlib import Path
from uuid import uuid4

ROOT=Path(__file__).resolve().parents[2]
FRONTEND=ROOT.parent/"frontend"
sys.path.insert(0,str(ROOT))
sys.path.insert(0,str(ROOT/"src"))


def free_port():
    with socket.socket() as sock:
        sock.bind(("127.0.0.1",0))
        return sock.getsockname()[1]


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--regression",action="store_true")
    parser.add_argument("--skip-build",action="store_true",help="Reuse an already built frontend when only test tooling changed")
    args=parser.parse_args()
    from dotenv import load_dotenv
    import yaml
    from confluent_kafka.admin import AdminClient,NewTopic
    from tools.verification.analytics_consumer_check import test_config
    from tools.verification.runner import EvidenceResult
    from opsyntra.analytics.migrations import migrate
    from opsyntra.kafka.config import load_kafka_config
    load_dotenv(ROOT/".env",override=False)
    os.environ["OPSYNTRA_RUN_ANALYTICS_TESTS"]="1"
    identity=datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")+"-"+uuid4().hex[:8]
    directory=ROOT/"artifacts/analytics/dashboard"/identity
    evidence=ROOT.parent/"docs/evidence/dashboard"/identity
    directory.mkdir(parents=True);evidence.mkdir(parents=True)
    screenshots=ROOT.parent/"docs/screenshots/phase4"
    screenshots.mkdir(exist_ok=True)
    report={"status":"RUNNING","started_at":datetime.now(timezone.utc).isoformat(),"command":"python -m tools.verification.analytics_dashboard_check"+(" --regression" if args.regression else "")+(" --skip-build" if args.skip_build else ""),"stages":[]}
    target=ROOT/"reports/live_dashboard_verification.json"
    target.write_text(json.dumps(report,indent=2)+"\n")
    actors=[]
    streams=[]
    def execute(name,command,cwd,env=None):
        began=time.perf_counter()
        result=subprocess.run(command,cwd=cwd,env=env,text=True,capture_output=True,timeout=240)
        stdout=evidence/(name+".stdout.txt");stderr=evidence/(name+".stderr.txt")
        stdout.write_text(result.stdout,encoding="utf-8");stderr.write_text(result.stderr,encoding="utf-8")
        report["stages"].append({"name":name,"exit_code":result.returncode,"seconds":time.perf_counter()-began,"command":list(map(str,command)),"stdout":str(stdout.relative_to(ROOT.parent)).replace('\\','/'),"stderr":str(stderr.relative_to(ROOT.parent)).replace('\\','/')})
        if result.returncode: raise RuntimeError(f"{name} failed; inspect {stdout} / {stderr}")
        return result
    try:
        config=test_config()
        with config.connect("migration") as c: report["migrations"]=migrate(c)
        names=["tests.integration.test_analytics_api"]
        suite=unittest.defaultTestLoader.discover(str(ROOT/"tests"),top_level_dir=str(ROOT)) if args.regression else unittest.defaultTestLoader.loadTestsFromNames(names)
        with (evidence/"backend-tests.txt").open("w",encoding="utf-8") as stream:
            result=unittest.TextTestRunner(stream=stream,resultclass=EvidenceResult,verbosity=2).run(suite)
        valid_skips=all(t.id().startswith("tests.integration.test_spark_workflow.") for t,_ in result.skipped)
        report["backend"]={"tests_run":result.testsRun,"passed":sum(r["status"]=="passed" for r in result.records),"skipped":len(result.skipped),"failures":len(result.failures),"errors":len(result.errors),"tests":result.records}
        if not result.wasSuccessful() or not valid_skips: raise RuntimeError("Backend tests failed")
        node=shutil.which("node")
        execute("typecheck",[node,str(FRONTEND/"node_modules/typescript/bin/tsc"),"--noEmit"],FRONTEND)
        if not args.skip_build: execute("production-build",[node,str(FRONTEND/"node_modules/next/dist/bin/next"),"build"],FRONTEND)
        settings=load_kafka_config(ROOT/"config.yaml")
        topics={name:"opsyntra.dashboard."+identity+"."+name for name in ("raw","predictions","deadletter")}
        group="opsyntra-dashboard-"+identity
        admin=AdminClient({"bootstrap.servers":settings["bootstrap_servers"]})
        for future in admin.create_topics([NewTopic(t,1,1,config={"retention.ms":"86400000"}) for t in topics.values()]).values():future.result(timeout=20)
        customer="LIVE-DEMO-"+uuid4().hex[:12]
        record=json.loads((ROOT/"examples/customer.json").read_text())
        record["customerID"]=customer
        csv_path=directory/"customer.csv"
        with csv_path.open("w",newline="",encoding="utf-8") as stream:
            writer=csv.DictWriter(stream,fieldnames=list(record));writer.writeheader();writer.writerow(record)
        full=yaml.safe_load((ROOT/"config.yaml").read_text())
        full["kafka"]["topics"]=topics
        full["kafka"]["report_dir"]=str(directory/"producer-runs")
        full["kafka"]["consumer"]["report_dir"]=str(directory/"prediction-runs")
        full["data"]["raw_path"]=str(csv_path)
        configuration=directory/"config.yaml"
        configuration.write_text(yaml.safe_dump(full),encoding="utf-8")
        manifest={"directory":str(directory),"customer_id":customer,"csv":str(csv_path),"config":str(configuration),"topics":topics,"bootstrap_servers":settings["bootstrap_servers"],"group":group}
        manifest_path=directory/"manifest.json"
        manifest_path.write_text(json.dumps(manifest,indent=2)+"\n")
        env=dict(os.environ)
        env["OPSYNTRA_ANALYTICS_WRITE_URL"]=config.write_url
        env["OPSYNTRA_ANALYTICS_READ_URL"]=config.read_url
        env["OPSYNTRA_ANALYTICS_GROUP_ID"]=group+"-analytics"
        for name,value in topics.items():env["OPSYNTRA_ANALYTICS_"+{"raw":"RAW","predictions":"PREDICTION","deadletter":"DEADLETTER"}[name]+"_TOPIC"]=value
        commands=[("analytics-consumer",[sys.executable,str(ROOT/"analytics_consumer.py"),"--config",str(configuration),"--group-id",group+"-analytics","--timeout-seconds","180"]),
                  ("prediction-consumer",[sys.executable,str(ROOT/"consumer.py"),"--mode","streaming","--engine","sklearn","--config",str(configuration),"--model",str(settings["model_config"]["sklearn_model"]),"--group-id",group+"-inference","--state-dir",str(directory/"state"),"--timeout-seconds","180"])]
        for name,command in commands:
            stream=(evidence/(name+".txt")).open("w",encoding="utf-8");streams.append(stream)
            process=subprocess.Popen(command,cwd=ROOT,env=env,stdout=stream,stderr=subprocess.STDOUT,creationflags=subprocess.CREATE_NEW_PROCESS_GROUP if os.name=="nt" else 0)
            actors.append(process)
        deadline=time.monotonic()+35
        while True:
            if any(p.poll() is not None for p in actors):raise RuntimeError("Owned test consumer exited during startup")
            futures=admin.describe_consumer_groups([group+"-analytics",group+"-inference"],request_timeout=2)
            try:
                if all(f.result(timeout=3).members for f in futures.values()):break
            except Exception:pass
            if time.monotonic()>deadline:raise RuntimeError("Test consumers did not join Kafka")
            time.sleep(.25)
        env.update(CI="1",OPSYNTRA_TEST_API_PORT=str(free_port()),OPSYNTRA_TEST_WEB_PORT=str(free_port()),OPSYNTRA_DASHBOARD_MANIFEST=str(manifest_path),OPSYNTRA_TEST_PYTHON=sys.executable,OPSYNTRA_SCREENSHOT_DIR=str(screenshots))
        browser=execute("browser-tests",[node,str(FRONTEND/"node_modules/@playwright/test/cli.js"),"test","--reporter=json"],FRONTEND,env)
        browser_report=json.loads(browser.stdout)
        report["browser"]=browser_report["stats"]
        if any(browser_report["stats"].get(k,0) for k in ("unexpected","skipped","flaky")):raise RuntimeError("Browser checks had failures/skips/flaky results")
        measurement=json.loads((directory/"visibility.json").read_text())
        (evidence/"visibility.json").write_text(json.dumps(measurement,indent=2)+"\n",encoding="utf-8")
        for name in ("producer.txt","browser-summary.json"):
            shutil.copyfile(directory/name,evidence/name)
        report.update(measurement=measurement,topics=topics,group_id=group,test_database="separate configured analytics test database",fresh_servers=True)
        report["status"]="PASS"
    except Exception as error:
        report["status"]="FAIL"
        report["error"]={"type":type(error).__name__,"message":str(error)[:1200]}
    finally:
        for actor in actors:
            if actor.poll() is None:
                try:
                    actor.send_signal(signal.CTRL_BREAK_EVENT if os.name=="nt" else signal.SIGTERM)
                    actor.wait(timeout=8)
                except (OSError,subprocess.TimeoutExpired):
                    actor.terminate();actor.wait(timeout=10)
        for stream in streams:stream.close()
    report["finished_at"]=datetime.now(timezone.utc).isoformat()
    files=[ROOT/"src/opsyntra/analytics/read_api.py",ROOT/"src/opsyntra/api/app.py",ROOT/"tools/verification/dashboard_event.py",Path(__file__),ROOT/"tests/integration/test_analytics_api.py",FRONTEND/"src/components/live-monitoring.tsx",FRONTEND/"src/lib/analytics-types.ts",FRONTEND/"src/app/api/[...path]/route.ts",FRONTEND/"src/components/workspace.tsx",FRONTEND/"src/app/globals.css",FRONTEND/"playwright.config.ts",FRONTEND/"tests/analytics.spec.ts",FRONTEND/"tests/workspace.spec.ts",*sorted(evidence.glob("*")),*sorted(screenshots.glob("*.png"))]
    report["files"]=[{"path":str(p.relative_to(ROOT.parent)).replace('\\','/'),"sha256":hashlib.sha256(p.read_bytes()).hexdigest()} for p in files if p.is_file()]
    target.write_text(json.dumps(report,indent=2)+"\n",encoding="utf-8")
    text=["# Phase 4: live dashboard verification","",f"Status: **{report['status']}**.","", "Read endpoints, proxy query forwarding, model/time filters, SQL agreement, direct inference and polling/outage browser behavior were tested.",""]
    if "measurement" in report:
        m=report["measurement"]
        text += [f"A real streaming producer event passed through the saved sklearn pipeline and PostgreSQL analytics consumer. At the default 5-second polling interval, ingestion-to-visible time was **{m['ingestion_to_visible_seconds']:.3f} seconds** (local test target: <=10 seconds). This is a measured local sample, not a general latency guarantee.","",f"Event: `{m['event_id']}`. Prediction: `{m['prediction_id']}`. Model: `{m['model_version']}`.","",f"Backend: {report['backend']['passed']}/{report['backend']['tests_run']} passed, {report['backend']['skipped']} native Spark skips. Browser: {report['browser'].get('expected',0)} passed, no failures/skips/flaky checks.",""]
    text += ["The verifier uses its own Kafka topics/groups and the separate test database. It starts fresh API/Next servers on free ports and cleans up only its own processes; the normal application groups are not reset. UI outage tests use labelled browser fixtures; the visibility/SQL test uses actual producer, prediction, Kafka, PostgreSQL and HTTP services.","", "See [JSON results and file hashes](live_dashboard_verification.json), [live monitoring guide](../../docs/LIVE_MONITORING.md), [desktop evidence](../../docs/screenshots/phase4/live-monitoring.png), and [mobile evidence](../../docs/screenshots/phase4/live-monitoring-mobile.png)."]
    (ROOT/"reports/live_dashboard_verification.md").write_text("\n".join(text)+"\n",encoding="utf-8")
    print(report["status"],report.get("error") or {"browser":report.get("browser"),"visibility_seconds":report.get("measurement",{}).get("ingestion_to_visible_seconds")})
    return 0 if report["status"]=="PASS" else 1


if __name__=="__main__":raise SystemExit(main())
