"""Verify PostgreSQL ingestion and optionally real Kafka hard-crash/replay recovery."""
import argparse
import hashlib
import json
import os
import subprocess
import sys
import time
import unittest
from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "src"))


def test_config():
    from opsyntra.analytics.config import AnalyticsConfig
    env = dict(os.environ)
    for access in ("READ", "WRITE", "MIGRATION"):
        env["OPSYNTRA_ANALYTICS_"+access+"_URL"] = env.get("OPSYNTRA_ANALYTICS_TEST_"+access+"_URL", "")
    config = AnalyticsConfig.from_env(env)
    with config.connect("migration") as test, AnalyticsConfig.from_env().connect("migration") as normal:
        if test.info.dbname == normal.info.dbname:
            raise ValueError("Live checks require a separate analytics test database")
    return config


def live_check(config, directory):
    from confluent_kafka import Consumer, Producer, TopicPartition
    from confluent_kafka.admin import AdminClient, NewTopic
    from opsyntra.kafka.config import load_kafka_config
    from tests.analytics_helpers import deadletter_event, prediction_event, raw_event
    settings = load_kafka_config(ROOT / "config.yaml")
    identity = uuid4().hex
    topics = {key: f"opsyntra.analytics.verify.{identity}.{key}" for key in ("raw", "predictions", "deadletter")}
    group = "opsyntra-analytics-verify-" + identity
    model = "verify-" + identity
    admin = AdminClient({"bootstrap.servers": settings["bootstrap_servers"]})
    for future in admin.create_topics([NewTopic(topic, 1, 1, config={"retention.ms": "86400000"}) for topic in topics.values()]).values():
        future.result(timeout=20)
    producer = Producer({"bootstrap.servers": settings["bootstrap_servers"], "enable.idempotence": True, "delivery.timeout.ms": 15000})
    events, output = [], []
    def publish(kind, value, key=None):
        if key is None:
            key = value["event_id"] if kind == "deadletter" else value.get("customerID", "invalid") if isinstance(value, dict) else "invalid"
        wire = value if isinstance(value, bytes) or value is None else json.dumps(value, allow_nan=False).encode()
        errors = []
        def ack(error, message):
            if error: errors.append(str(error))
            else: events.append({"topic": message.topic(), "partition": message.partition(), "offset": message.offset(), "kind": kind})
        producer.produce(topics[kind], partition=0, key=key.encode(), value=wire, on_delivery=ack)
        if producer.flush(20) or errors: raise RuntimeError("Verification publication not acknowledged")
    def offsets(identifier):
        from confluent_kafka import KafkaException, KafkaError
        client = Consumer({"bootstrap.servers": settings["bootstrap_servers"], "group.id": identifier, "enable.auto.commit": False})
        # A fresh CI broker can accept metadata before its group coordinator is ready.
        # Retry only transient coordinator/timeout errors, with an overall deadline.
        deadline = time.monotonic() + 20
        transient = {getattr(KafkaError, name) for name in ("NOT_COORDINATOR", "COORDINATOR_LOAD_IN_PROGRESS", "COORDINATOR_NOT_AVAILABLE", "_WAIT_COORD", "_TIMED_OUT") if hasattr(KafkaError, name)}
        try:
            while True:
                try:
                    return {p.topic: p.offset for p in client.committed([TopicPartition(t,0) for t in topics.values()],timeout=3)}
                except KafkaException as error:
                    if error.args[0].code() not in transient or time.monotonic() >= deadline:
                        raise
                    time.sleep(.2)
        finally: client.close()
    def rows():
        with config.connect("read") as connection:
            deliveries = connection.execute("SELECT count(*) AS n FROM analytics.kafka_deliveries WHERE topic=ANY(%s)", (list(topics.values()),)).fetchone()["n"]
            errors = connection.execute("SELECT count(*) AS n FROM analytics.ingestion_errors WHERE topic=ANY(%s)", (list(topics.values()),)).fetchone()["n"]
            predictions = connection.execute("SELECT count(*) AS n FROM analytics.prediction_events WHERE model_version=%s", (model,)).fetchone()["n"]
            metrics = connection.execute("SELECT engine,prediction_volume,unique_customers,matched_raw_predictions FROM analytics.prediction_metrics_by_model WHERE model_version=%s ORDER BY engine", (model,)).fetchall()
        return {"deliveries": deliveries, "errors": errors, "predictions": predictions, "metrics": metrics}
    # Only writer credentials reach consumer subprocesses; no owner/read login.
    child_env = dict(os.environ)
    for name in list(child_env):
        if name.startswith("OPSYNTRA_ANALYTICS_"): child_env.pop(name)
    child_env["OPSYNTRA_ANALYTICS_WRITE_URL"] = config.write_url
    args = [sys.executable, str(ROOT / "analytics_consumer.py"), "--bootstrap-servers", settings["bootstrap_servers"],
            "--raw-topic", topics["raw"], "--prediction-topic", topics["predictions"], "--deadletter-topic", topics["deadletter"],
            "--env-file", str(ROOT/".analytics-runtime/nonexistent-verification.env"), "--batch-size", "100", "--idle-timeout", "15"]
    def invoke(name, extra, expected=0, identifier=group, env=child_env):
        report = ROOT/"artifacts/analytics/verification"/identity/(name+".json")
        command = [*args, "--group-id", identifier, "--report", str(report), *extra]
        result = subprocess.run(command, cwd=ROOT, env=env, text=True, capture_output=True, timeout=75)
        transcript = directory / (name + ".txt")
        transcript.write_text(result.stdout+result.stderr, encoding="utf-8")
        if result.returncode != expected:
            raise RuntimeError(f"Live analytics stage {name} exited {result.returncode}, expected {expected}; inspect {transcript}")
        value = json.loads(report.read_text()) if report.exists() else None
        output.append({"stage": name, "exit_code": result.returncode, "log": str(transcript.relative_to(ROOT.parent)).replace('\\','/'), "result": value})
        return value
    a = raw_event()
    p = prediction_event(a, model)
    publish("predictions", p)
    before = offsets(group)
    invoke("crash-after-database-commit", ["--mode","batch","--max-messages","1","--test-crash-at","after_db_commit"], 86)
    after = offsets(group)
    assert after == before, "Crash must not commit Kafka input"
    assert rows()["predictions"] == 1
    with config.connect("read") as connection:
        assert not connection.execute("SELECT raw_available FROM analytics.prediction_details WHERE prediction_id=%s", (p["prediction_id"],)).fetchone()["raw_available"]
    recovery = invoke("restart-after-commit", ["--mode","batch","--snapshot"])
    assert recovery["records"] == 1 and recovery["logical_duplicates"] == 1
    assert rows()["predictions"] == 1
    b = raw_event(a["customerID"], "Two year")
    b["event_ts"] = "2026-10-19T23:59:58Z"
    q = prediction_event(b, model)
    q.update(churn_probability=.2, prediction="No")
    for value in (a,b,a,{**a,"Contract":"Two year"},b"not-json",None,b"\xff"):
        publish("raw", value)
    for value in (q, prediction_event(a, model, "spark"), {**p,"processed_ts":"2026-10-20T09:00:00Z"}, {**p,"schema_version":2}):
        publish("predictions", value)
    dead = deadletter_event()
    publish("deadletter", dead)
    known = invoke("known-event-snapshot", ["--mode","batch","--snapshot"])
    assert known["records"] == 12 and known["rejected"] == 5
    expected = rows()
    assert (expected["deliveries"],expected["errors"],expected["predictions"]) == (8,5,3), expected
    assert [r["prediction_volume"] for r in expected["metrics"]] == [2,1]
    with config.connect("read") as connection:
        details=connection.execute("SELECT contract FROM analytics.prediction_details WHERE prediction_id=ANY(%s) ORDER BY contract", ([p["prediction_id"],q["prediction_id"]],)).fetchall()
        assert [r["contract"] for r in details] == ["Month-to-month","Two year"]
        days=connection.execute("SELECT bucket_start::date AS day,prediction_volume FROM analytics.prediction_daily WHERE model_version=%s AND engine='sklearn' ORDER BY bucket_start", (model,)).fetchall()
        assert [str(r["day"]) for r in days] == ["2026-10-19","2026-10-20"]
    replay = invoke("new-group-full-replay", ["--mode","batch","--snapshot"], identifier=group+"-replay")
    assert replay["records"] == 13 and replay["quarantine_duplicates"] == 5
    assert rows() == expected, "Replay inflated logical or physical counts"
    empty = invoke("completed-group-restart", ["--mode","batch","--snapshot"])
    assert empty["records"] == 0
    c=raw_event()
    publish("raw",c)
    checkpoint=offsets(group)
    invoke("crash-before-database-commit", ["--mode","batch","--snapshot","--test-crash-at","before_db_commit"],86)
    assert offsets(group) == checkpoint
    with config.connect("read") as connection:
        assert connection.execute("SELECT count(*) AS n FROM analytics.raw_customer_events WHERE event_id=%s", (c["event_id"],)).fetchone()["n"] == 0
    recovered=invoke("restart-before-commit", ["--mode","batch","--snapshot"])
    assert recovered["records"] == 1 and recovered["raw_inserted"] == 1
    # A real connection refusal must not advance the existing committed group.
    from urllib.parse import urlsplit,urlunsplit
    bad_env=dict(child_env)
    url=urlsplit(config.write_url)
    bad_env["OPSYNTRA_ANALYTICS_WRITE_URL"]=urlunsplit((url.scheme,url.netloc.rsplit(':',1)[0]+':1',url.path,url.query,url.fragment))
    checkpoint=offsets(group)
    invoke("database-unavailable", ["--mode","batch","--snapshot","--retries","1","--retry-delay","0.01"],1,env=bad_env)
    assert offsets(group) == checkpoint
    streamed=invoke("bounded-streaming", ["--mode","streaming","--timeout-seconds","4"],identifier=group+"-stream")
    assert streamed["status"] == "complete" and streamed["records"] == 14
    final=rows()
    assert (final["deliveries"],final["errors"],final["predictions"]) == (9,5,3)
    return {"status":"PASS","cluster_id":settings.get("cluster_id") or known["cluster_id"],"topics":topics,"group_id":group,
            "model_version":model,"published":len(events),"events":events,"stages":output,"final_counts":final,
            "offsets_before_hard_exit":before,"offsets_after_hard_exit":after,
            "scope":"Real broker, separate PostgreSQL test database, isolated diagnostic topics/groups; hard exits are os._exit(86)"}


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--live",action="store_true",help="Publish isolated real Kafka events and test process crashes/replays")
    parser.add_argument("--regression",action="store_true",help="Also run all backend tests")
    args=parser.parse_args()
    from dotenv import load_dotenv
    from tools.verification.runner import EvidenceResult
    load_dotenv(ROOT/".env",override=False)
    os.environ["OPSYNTRA_RUN_ANALYTICS_TESTS"]="1"
    report={"status":"RUNNING","checked_at":datetime.now(timezone.utc).isoformat(),"command":"python -m tools.verification.analytics_consumer_check"+(" --live" if args.live else "")+(" --regression" if args.regression else "")}
    path=ROOT/"reports/analytics_consumer_verification.json"
    path.write_text(json.dumps(report,indent=2)+"\n")
    suite=unittest.defaultTestLoader.loadTestsFromNames(["tests.unit.test_analytics_contracts","tests.unit.test_analytics_consumer","tests.integration.test_analytics_storage","tests.integration.test_analytics_ingestion"])
    with (ROOT/"reports/analytics_consumer_verification.log").open("w",encoding="utf-8") as stream:
        result=unittest.TextTestRunner(stream=stream,resultclass=EvidenceResult,verbosity=2).run(suite)
    report.update(tests_run=result.testsRun,passed=sum(r["status"]=="passed" for r in result.records),skipped=len(result.skipped),failures=len(result.failures),errors=len(result.errors),tests=result.records)
    passed=result.wasSuccessful() and not result.skipped
    try:
        if not passed: raise RuntimeError("Analytics contract/database checks failed")
        if args.live:
            directory=ROOT.parent/"docs/evidence/analytics"/(datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")+"-"+uuid4().hex[:8])
            directory.mkdir(exist_ok=True,parents=True)
            report["live"]=live_check(test_config(),directory)
        if args.regression:
            suite=unittest.defaultTestLoader.discover(str(ROOT/"tests"),top_level_dir=str(ROOT))
            with (ROOT/"reports/analytics_consumer_regression.log").open("w",encoding="utf-8") as stream:
                result=unittest.TextTestRunner(stream=stream,resultclass=EvidenceResult,verbosity=2).run(suite)
            regression={"passed":sum(r["status"]=="passed" for r in result.records),"tests_run":result.testsRun,"skipped":len(result.skipped),"failures":len(result.failures),"errors":len(result.errors),"tests":result.records}
            regression["status"]="PASS" if result.wasSuccessful() and all(t.id().startswith("tests.integration.test_spark_workflow.") for t,_ in result.skipped) else "FAIL"
            report["regression"]=regression
            passed=passed and regression["status"]=="PASS"
    except Exception as error:
        passed=False
        report["error"]={"type":type(error).__name__,"message":str(error)[:700]}
    report["status"]="PASS" if passed else "FAIL"
    files=[ROOT/"analytics_consumer.py",*sorted((ROOT/"src/opsyntra/analytics").glob("*.py")),*sorted((ROOT/"migrations").glob("*.sql")),ROOT/"tools/verification/analytics_consumer_check.py",ROOT/"tests/unit/test_analytics_consumer.py",ROOT/"tests/integration/test_analytics_ingestion.py",ROOT/"requirements/analytics-consumer.txt",ROOT.parent / "scripts/windows/start-analytics.ps1",ROOT.parent/"docs/ANALYTICS_CONSUMER.md",*sorted((ROOT.parent/"docs/evidence/analytics").rglob("*.txt"))]
    report["files"]=[{"path":str(p.relative_to(ROOT.parent)).replace('\\','/'),"sha256":hashlib.sha256(p.read_bytes()).hexdigest()} for p in files]
    path.write_text(json.dumps(report,indent=2)+"\n",encoding="utf-8")
    lines=["# Phase 3: Kafka analytics verification", "", f"Status: **{report['status']}**. Contract/PostgreSQL checks: {report['passed']}/{report['tests_run']}; skipped: {report['skipped']}.", "",
           "The live check uses real Kafka and a separate PostgreSQL test database. It never loads an ML model. Diagnostic topics/groups and model IDs are recorded in the JSON report.", ""]
    if "live" in report:
        live=report["live"]
        lines += [f"Published inputs: **{live['published']}**. Final logical predictions: **{live['final_counts']['predictions']}**; physical accepted deliveries: **{live['final_counts']['deliveries']}**; durable rejected inputs: **{live['final_counts']['errors']}**.", "",
                  "The fixture contains three raw event identities, three prediction identities (two sklearn and one Spark), one incoming dead letter, duplicate deliveries, one conflicting event and four malformed inputs. Logical counts stay unchanged on replay.", "",
                  "| Real process scenario | Expected exit | Evidence |", "|---|---|---|"]
        lines += [f"| {s['stage']} | {s['exit_code']} | [transcript](../../{s['log']}) |" for s in live["stages"]]
        lines += ["", "Exit 86 is the deliberately injected hard crash. Exit 1 in the database-unavailable stage is the expected refusal with no input commit. Other stages must exit 0.", "",
                  "The after-database-commit crash stores one prediction before exiting; Kafka offsets remain unchanged. Restarting the same group replays it without another logical row. A new group replays all inputs without count inflation. The before-commit crash rolls back its new raw event; restart inserts it once. Late raw features and previous-day predictions repair the event-based joins/time buckets.", ""]
    if "regression" in report:
        r=report["regression"]
        lines += [f"Backend regression: **{r['status']}**, {r['passed']}/{r['tests_run']} passed, {r['skipped']} native Spark checks skipped on Windows.", ""]
    lines += ["## Contract/database checks", "", "| Test | Result |", "|---|---|"]
    lines += [f"| {r['test'].split('.')[-1]} | {r['status']} |" for r in report["tests"]]
    lines += ["", "See [machine-readable results](analytics_consumer_verification.json), [test log](analytics_consumer_verification.log),",
              "[full regression log](analytics_consumer_regression.log) when requested, and [operating/recovery guide](../../docs/ANALYTICS_CONSUMER.md).", "",
              "The live diagnostic tests do not reset the application/inference groups. Delivery is at-least-once with idempotent PostgreSQL facts, not an atomic exactly-once transaction across systems."]
    (ROOT/"reports/analytics_consumer_verification.md").write_text("\n".join(lines)+"\n",encoding="utf-8")
    print(f"{report['status']}: {report['passed']}/{report['tests_run']} analytics checks; live Kafka: {report.get('live',{}).get('status','not completed')}")
    if not passed: print(json.dumps(report.get("error",{})))
    return 0 if passed else 1


if __name__=="__main__": raise SystemExit(main())
