"""Real-broker, fresh-process checks for prediction, poison records and crash recovery."""
import argparse
import json
import sqlite3
import subprocess
import sys
from contextlib import closing
from pathlib import Path
from uuid import uuid4

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "src"))


def main(include_spark=False):
    from confluent_kafka import Consumer, TopicPartition
    from opsyntra.kafka_config import atomic_json, load_kafka_config
    from opsyntra.kafka_events import CustomerSource, customer_rows, make_event, utc_now
    from opsyntra.kafka_publishing import kafka_producer
    from opsyntra.kafka_models import PredictionModel
    from tools.services.kafka_topics import ensure_topics
    settings = load_kafka_config(ROOT / "config.yaml")
    metadata = ensure_topics(settings)
    cluster = metadata["cluster_id"]
    folder = ROOT / "artifacts/kafka/consumer_checks" / uuid4().hex
    folder.mkdir(parents=True)
    calls = {}
    producer = kafka_producer(settings)
    topic = settings["topics"]["raw"]
    source = CustomerSource.inspect(settings["source"])
    rows = list(customer_rows(source.path))
    first_missing = next(row for row in rows if row[1]["TotalCharges"] is None)
    selected_rows = rows[:5] + [first_missing]
    state_dir = folder / "recovery"

    def reader(group=None):
        return Consumer({"bootstrap.servers": settings["bootstrap_servers"],
            "group.id": group or "opsyntra-check-reader-" + uuid4().hex,
            "enable.auto.commit": False, "enable.auto.offset.store": False,
            "allow.auto.create.topics": False, "isolation.level": "read_committed"})

    def bounds(name):
        consumer = reader()
        try:
            info = consumer.list_topics(name, timeout=10)
            return {str(p): consumer.get_watermark_offsets(TopicPartition(name, p), timeout=10)[1]
                    for p in sorted(info.topics[name].partitions)}
        finally:
            consumer.close()

    def events():
        run_id = str(uuid4())
        result = [make_event(row, run_id=run_id, source=source, row_index=i, sequence=i,
                             mode="batch", started_at=utc_now()) for i, row in selected_rows]
        for event in result[::2]:
            event.pop("Churn", None)
        return result

    def send(name, messages):
        before = bounds(topic)
        errors, delivered = [], []
        for key, value in messages:
            def callback(error, message):
                if error: errors.append(str(error))
                else: delivered.append({"partition": message.partition(), "offset": message.offset()})
            producer.produce(topic, key=key, value=value, on_delivery=callback)
            producer.poll(0)
        remaining = producer.flush(35)
        assert not errors and not remaining and len(delivered) == len(messages)
        after = bounds(topic)
        window = {"cluster_id": cluster, "topic": topic,
                  "partitions": {p: {"start": before[p], "end": after[p]} for p in before}}
        path = folder / (name + "_window.json")
        atomic_json(path, window)
        return path, delivered

    def wire(event, key=None):
        return (key if key is not None else event["customerID"].encode(), json.dumps(event, allow_nan=False).encode())

    def execute(name, group, *, mode="batch", window=None, max_messages=None, crash=None, spark=False):
        args = [sys.executable, str(ROOT / "consumer.py")] if not spark else [
            "wsl", "-d", "Ubuntu", "--cd", subprocess.check_output(
                ["wsl", "-d", "Ubuntu", "--exec", "wslpath", "-a", str(ROOT).replace("\\", "/")], text=True).strip(),
            "--exec", ".venv-spark/bin/python", "consumer.py", "--engine", "spark"]
        args += ["--mode", mode, "--group-id", group, "--batch-size", "100"]
        if window is not None:
            path = str(window)
            if spark:
                path = subprocess.check_output(["wsl", "-d", "Ubuntu", "--exec", "wslpath", "-a", path.replace("\\", "/")], text=True).strip()
            args += ["--offset-window", path]
        if max_messages is not None:
            args += ["--max-messages", str(max_messages)]
        if not spark:
            args += ["--state-dir", str(state_dir)]
        if crash:
            args += ["--test-crash-at", crash]
        process = subprocess.run(args, cwd=ROOT, text=True, capture_output=True, timeout=240)
        (folder / (name + ".log")).write_text(process.stderr + "\n" + process.stdout, encoding="utf-8")
        if crash:
            assert process.returncode == 86, process.stderr[-3000:]
            calls[name] = {"exit_code": 86, "crash_stage": crash, "group_id": group}
            print(f"{name}: confirmed hard process exit at {crash}", flush=True)
            return calls[name]
        assert process.returncode == 0, process.stderr[-4000:]
        decoder = json.JSONDecoder()
        for i, char in enumerate(process.stdout):
            if char == "{":
                try:
                    summary, _ = decoder.raw_decode(process.stdout[i:])
                    if isinstance(summary, dict) and "invocation_id" in summary:
                        break
                except ValueError:
                    pass
        else:
            raise ValueError("Consumer did not return its JSON summary")
        assert summary["status"] == "completed" and summary["model_loads"] == 1
        calls[name] = summary
        print(f"{name}: input={summary['committed_records']}; predictions={summary['predictions_published']}; "
              f"deadletters={summary['deadletters_published']}; deduplicated={summary['deduplicated']}", flush=True)
        return summary

    def read_range(name, start, end):
        consumer = reader()
        records, positions = [], {p: start[p] for p in start}
        consumer.assign([TopicPartition(name, int(p), start[p]) for p in start])
        import time
        deadline = time.monotonic() + 45
        try:
            while any(positions[p] < end[p] for p in start):
                if time.monotonic() > deadline:
                    raise TimeoutError("Output verification could not read its fixed offset window")
                message = consumer.poll(.5)
                if message is None: continue
                if message.error(): raise RuntimeError(str(message.error()))
                p = str(message.partition())
                positions[p] = max(positions[p], message.offset() + 1)
                if message.offset() < end[p]:
                    records.append({"key": message.key().decode(), "partition": message.partition(),
                                    "offset": message.offset(), "value": json.loads(message.value())})
        finally:
            consumer.close()
        return records

    output_topic, dead_topic = settings["topics"]["predictions"], settings["topics"]["deadletter"]
    output_start, dead_start = bounds(output_topic), bounds(dead_topic)
    valid = events()
    conflict = {**valid[0], "MonthlyCharges": valid[0]["MonthlyCharges"] + 10}
    invalid = [(b"bad-json", b"not-json"), (b"missing-fields", b'{"customerID":"missing-fields"}'),
               (b"tombstone", None), wire(valid[0], key=b"mismatched-customer"),
               (b"invalid-utf8", b"\xff"), (b"not-finite", b'{"value":NaN}'), wire(conflict)]
    window, delivered = send("batch", [wire(e) for e in valid] + [wire(valid[0])] + invalid)
    group = "opsyntra-check-batch-" + uuid4().hex[:12]
    first = execute("batch_count", group, window=window, max_messages=5)
    rest = execute("batch_resume_window", group, window=window)
    completed = execute("batch_completed_window", group, window=window)
    assert first["committed_records"] + rest["committed_records"] == 14
    assert first["predictions_published"] + rest["predictions_published"] == 6
    assert first["deadletters_published"] + rest["deadletters_published"] == 7
    assert first["deduplicated"] + rest["deduplicated"] == 1
    assert completed["committed_records"] == 0
    duplicate_window, _ = send("duplicate_replay", [wire(valid[0]), wire(valid[2])])
    replay = execute("duplicate_replay", group, window=duplicate_window)
    assert replay["deduplicated"] == 2 and replay["predictions_published"] == 0

    # Start a new diagnostic group at current end so streaming waits for new events.
    streaming_group = "opsyntra-check-stream-" + uuid4().hex[:12]
    observer = reader(streaming_group)
    streaming_start = bounds(topic)
    try:
        offsets = [TopicPartition(topic, int(p), offset) for p, offset in streaming_start.items()]
        observer.assign(offsets)
        observer.commit(offsets=offsets, asynchronous=False)
    finally:
        observer.close()
    stream_events = events()[:3]
    # Fresh data is then consumed via subscribe/group offsets in streaming mode.
    send("streaming", [wire(event) for event in stream_events])
    streamed = execute("streaming", streaming_group, mode="streaming", max_messages=3)
    assert streamed["predictions_published"] == 3

    crash_events = {}
    for stage in ("after_ledger_commit", "after_output_ack"):
        event = events()[0]
        crash_events[stage] = event
        crash_window, _ = send(stage, [wire(event)])
        crash_group = "opsyntra-check-crash-" + uuid4().hex[:12]
        execute("crash_" + stage, crash_group, window=crash_window, crash=stage)
        # An abrupt process exit releases the OS lock; the same group/store resumes.
        restarted = execute("restart_" + stage, crash_group, window=crash_window)
        if stage == "after_ledger_commit":
            assert restarted["deduplicated"] == 1 and restarted["predictions_published"] == 0
        else:
            assert restarted["inference_batches"] == 0 and restarted["predictions_published"] == 1

    spark_event = None
    if include_spark:
        spark_event = events()[0]
        spark_window, _ = send("spark", [wire(spark_event)])
        spark_summary = execute("spark_batch", "opsyntra-check-spark-" + uuid4().hex[:12], window=spark_window, spark=True)
        assert spark_summary["predictions_published"] == 1

    outputs = read_range(output_topic, output_start, bounds(output_topic))
    deadletters = read_range(dead_topic, dead_start, bounds(dead_topic))
    expected_ids = {event["event_id"] for event in valid + stream_events + list(crash_events.values())}
    own = [record for record in outputs if record["value"]["event_id"] in expected_ids]
    assert len(own) == 12
    for stage, event in crash_events.items():
        matching = [record["value"] for record in own if record["value"]["event_id"] == event["event_id"]]
        if stage == "after_ledger_commit":
            assert len(matching) == 1
        else:
            assert len(matching) == 2 and matching[0] == matching[1]
    assert len({r["value"]["prediction_id"] for r in own}) == 11
    own_sources = {(item["partition"], item["offset"]) for item in delivered}
    deadletters = [r for r in deadletters if (r["value"]["source"]["partition"], r["value"]["source"]["offset"]) in own_sources]
    assert len(deadletters) == 7
    real_model = PredictionModel(settings)
    try:
        expected_events = valid + stream_events + list(crash_events.values())
        expected = {event["event_id"]: result for event, result in zip(expected_events, real_model.predict(expected_events))}
        for record in own:
            payload = record["value"]
            assert record["key"] == payload["customerID"]
            assert payload["model_version"] == real_model.version
            assert abs(payload["churn_probability"] - expected[payload["event_id"]]["churn_probability"]) <= 1e-12
            assert payload["prediction"] == expected[payload["event_id"]]["prediction"]
            from datetime import datetime
            assert datetime.fromisoformat(payload["event_ts"].replace("Z", "+00:00")).tzinfo is not None
            assert datetime.fromisoformat(payload["processed_ts"].replace("Z", "+00:00")).tzinfo is not None
    finally:
        real_model.close()
    spark_records = [r for r in outputs if spark_event and r["value"]["event_id"] == spark_event["event_id"]]
    if include_spark:
        assert len(spark_records) == 1 and spark_records[0]["value"]["engine"] == "spark"
    report = {"status": "PASS", "checked_at": utc_now(), "broker": metadata,
        "check_directory": folder.relative_to(ROOT).as_posix(), "calls": calls,
        "observed": {"normal_batch_predictions": 6, "normal_batch_deadletters": 7,
                     "duplicates_suppressed_after_restart": 2, "streaming_predictions": 3,
                     "ledger_crash_physical_outputs": 1, "ack_before_ledger_crash_physical_outputs": 2,
                     "sklearn_physical_outputs": len(own), "sklearn_unique_prediction_ids": 11,
                     "spark_predictions": len(spark_records)},
        "checks": {"count_bound_and_window_resume": True, "completed_window_noop": True,
                   "malformed_records_deadlettered_without_blocking_valid_records": True,
                   "predictions_match_direct_saved_pipeline": True, "unlabelled_events_and_null_total_charges": True,
                   "duplicate_event_ids_suppressed": True, "real_hard_exit_and_restart_after_ledger": True,
                   "real_hard_exit_and_restart_after_output_ack": True, "ack_window_replay_preserves_complete_payload": True,
                   "one_model_load_per_consumer_process": True},
        "sample_prediction": own[0], "sample_deadletter": deadletters[0],
        "sample_spark_prediction": spark_records[0] if spark_records else None,
        "guarantee": "At-least-once Kafka output with durable local deduplication. The tested ack-before-ledger crash window can produce identical duplicate predictions; prediction_id/event_id+model_version supports downstream deduplication. No end-to-end exactly-once claim."}
    atomic_json(folder / "evidence.json", report)
    atomic_json(ROOT / "reports/kafka_consumption_report.json", report)
    lines = ["# Kafka prediction and recovery checkpoint", "", "**PASS**: real broker, saved complete pipelines, fresh consumer processes, and two abrupt process crashes (exit code 86).", "",
        "| Scenario | Observed result |", "|---|---|",
        "| Count-limited batch then offset-window resume | 14 input records committed across two processes |",
        "| Normal batch outputs | 6 predictions, 7 deadletters, 1 suppressed duplicate |",
        "| Completed window rerun | 0 input records and 0 new output |",
        "| Re-published event IDs after restart | 2 duplicates suppressed, 0 new predictions |",
        "| Streaming mode | 3 new-event predictions, bounded by --max-messages |",
        "| Hard exit after durable delivery ledger, before input commit | Restart suppressed output replay; 1 physical prediction |",
        "| Hard exit after output ack, before ledger update | Restart re-sent identical prepared payload; 2 physical records, 1 prediction ID |",
        f"| Spark saved-model batch inference | {len(spark_records)} prediction(s) |", "",
        "All sklearn predictions matched direct saved-pipeline probabilities within 1e-12 and had the same labels. Tests included records without Churn and a missing TotalCharges value. Every consumer process loaded its model once. Output keys matched customerID; event/processing timestamps and model version were present.", "",
        report["guarantee"], "",
        "The raw source dataset, producer checkpoint and ordinary consumer groups were not reset. Diagnostic groups and fixed windows isolated these checks from earlier topic history.", "",
        f"[Command logs, offset windows and JSON evidence](../{folder.relative_to(ROOT).as_posix()}/)", "",
        "See [Kafka consumer commands and recovery policy](../KAFKA.md#prediction-consumer-step-7). The JSON report retains exact consumer summaries, group IDs, model hashes, observed counts and sample prediction/dead-letter records.", ""]
    (ROOT / "reports/kafka_consumption_report.md").write_text("\n".join(lines), encoding="utf-8")
    print("PASS: real prediction, bounded consumption, malformed records, deduplication and both crash/restart windows verified.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--include-spark", action="store_true")
    main(parser.parse_args().include_spark)
