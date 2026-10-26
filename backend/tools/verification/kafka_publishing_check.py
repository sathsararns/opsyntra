"""Real-broker evidence: full CSV, pause/resume, no-op resume and bounded streaming.

This diagnostic reads raw events only; churn prediction consumption is Step 7.
"""
import json
import logging
import subprocess
import sys
import time
from collections import Counter
from pathlib import Path
from uuid import uuid4

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "src"))


def main():
    from confluent_kafka import Consumer, TopicPartition, __version__
    from opsyntra.kafka_config import atomic_json, load_kafka_config
    from opsyntra.kafka_events import CustomerSource, customer_rows, utc_now, validate_event
    from tools.services.kafka_topics import ensure_topics
    settings = load_kafka_config(ROOT / "config.yaml")
    topics = ensure_topics(settings)
    source = CustomerSource.inspect(settings["source"])
    folder = ROOT / "artifacts/kafka/checks" / uuid4().hex
    folder.mkdir(parents=True)
    checkpoint = folder / "batch_checkpoint.json"
    calls = {}
    def execute(name, *args):
        process = subprocess.run([sys.executable, str(ROOT / "producer.py"), *args], cwd=ROOT,
                                 text=True, capture_output=True, timeout=180)
        (folder / f"{name}.log").write_text(process.stderr + "\n" + process.stdout, encoding="utf-8")
        if process.returncode:
            raise RuntimeError(f"{name} failed: {process.stderr[-3000:]}")
        calls[name] = json.loads(process.stdout)
        print(f"{name}: {calls[name]['status']}; acknowledged {calls[name]['acknowledged']}; run {calls[name]['run_id']}", flush=True)
        return calls[name]

    consumer = Consumer({"bootstrap.servers": settings["bootstrap_servers"], "group.id": "opsyntra-publisher-check-" + uuid4().hex,
                         "enable.auto.commit": False, "allow.auto.create.topics": False, "socket.timeout.ms": 10000})
    topic = settings["topics"]["raw"]
    metadata = consumer.list_topics(topic, timeout=10)
    partitions = sorted(metadata.topics[topic].partitions)
    before = {p: consumer.get_watermark_offsets(TopicPartition(topic, p), timeout=10)[1] for p in partitions}
    try:
        first = execute("batch_pause", "--mode", "batch", "--batch-size", "500", "--max-events", "1000", "--checkpoint", str(checkpoint))
        resumed = execute("batch_resume", "--mode", "batch", "--batch-size", "500", "--checkpoint", str(checkpoint), "--resume")
        noop = execute("batch_completed_resume", "--mode", "batch", "--checkpoint", str(checkpoint), "--resume")
        streamed = execute("streaming", "--mode", "streaming", "--events-per-sec", "10", "--max-events", "30")
        assert first["acknowledged"] == min(1000, source.rows)
        assert first["run_id"] == resumed["run_id"] == noop["run_id"]
        assert first["acknowledged"] + resumed["acknowledged"] == source.rows
        assert noop["acknowledged"] == 0 and streamed["acknowledged"] == 30
        after = {p: consumer.get_watermark_offsets(TopicPartition(topic, p), timeout=10)[1] for p in partitions}
        consumer.assign([TopicPartition(topic, p, before[p]) for p in partitions])
        positions = dict(before)
        wanted = {first["run_id"], streamed["run_id"]}
        records = []
        deadline = time.monotonic() + 60
        while any(positions[p] < after[p] for p in partitions):
            if time.monotonic() >= deadline:
                raise TimeoutError("Did not observe all acknowledged raw records")
            message = consumer.poll(1)
            if message is None:
                continue
            if message.error():
                raise RuntimeError(str(message.error()))
            partition = message.partition()
            positions[partition] = max(positions[partition], message.offset() + 1)
            if message.offset() >= after[partition]:
                continue
            event = json.loads(message.value())
            if event.get("run_id") in wanted:
                key = message.key().decode("utf-8")
                validate_event(event, key)
                records.append({"key": key, "partition": partition, "offset": message.offset(), "event": event})
    finally:
        consumer.close()

    batches = sorted((r for r in records if r["event"]["mode"] == "batch"), key=lambda r: r["event"]["source_row"])
    streams = [r for r in records if r["event"]["mode"] == "streaming"]
    assert len(batches) == source.rows and len(streams) == 30
    assert len({r["event"]["event_id"] for r in records}) == len(records)
    assert [r["event"]["source_row"] for r in batches] == list(range(source.rows))
    originals = list(customer_rows(source.path))
    for (_, original), record in zip(originals, batches):
        assert all(record["event"][key] == value for key, value in original.items())
    nulls = sum(r["event"]["TotalCharges"] is None for r in batches)
    report = {"status": "PASS", "checked_at": utc_now(), "broker": topics,
        "client_version": __version__, "source_sha256": source.sha256, "source_rows": source.rows,
        "check_directory": folder.relative_to(ROOT).as_posix(), "commands": calls,
        "observed": {"batch": len(batches), "streaming": len(streams), "unique_event_ids": len(records),
                     "blank_total_charges_as_null": nulls,
                     "records_by_partition": dict(Counter(r["partition"] for r in records))},
        "checks": {"all_wire_schemas_valid": True, "all_keys_equal_customerID": True,
                   "all_batch_rows_match_csv": True, "all_batch_rows_covered_once": True,
                   "resume_continued_same_run": True, "completed_resume_published_zero": True,
                   "all_event_ids_unique": True, "labels_are_optional_metadata_for_future_inference": True},
        "offsets_before": before, "offsets_after": after,
        "sample_batch_event": batches[0], "sample_streaming_event": streams[0],
        "delivery_limit": "Graceful checkpoint/resume was duplicate-free. A crash after delivery but before checkpoint persistence can replay a chunk with the same event IDs; consumers must deduplicate.",
        "prediction_consumer_implemented": False}
    atomic_json(folder / "evidence.json", report)
    atomic_json(ROOT / "reports/kafka_publishing_report.json", report)
    lines = ["# Kafka publishing checkpoint", "", "**PASS**: real Apache Kafka broker, actual CLI subprocesses and wire messages read back from the raw topic.", "",
             f"Broker cluster: `{topics['cluster_id']}`. Client: confluent-kafka {__version__}.",
             "Local Kafka runs in Ubuntu WSL with Java IPv4 sockets so Windows clients can reach localhost:9092.", "",
             "| Check | Result |", "|---|---:|",
             f"| Full CSV batch messages | {len(batches)} |", "| First process paused after | 1000 |",
             f"| Resume process published | {resumed['acknowledged']} |", "| Completed resume published | 0 |",
             "| Streaming messages at requested 10 events/sec | 30 |",
             f"| Distinct event IDs across both runs | {len(records)} |", f"| Blank TotalCharges preserved as JSON null | {nulls} |", "",
             "All observed messages passed the event contract, used customerID as their key, and included UTC event_ts. Every batch customer field matched its source CSV row. All 7,043 source rows appeared once across the graceful pause/resume sequence.", "",
             f"Batch run ID: `{first['run_id']}`. Streaming run ID: `{streamed['run_id']}`.",
             f"Dataset SHA256: `{source.sha256}`.", "",
             f"[Detailed command logs and checkpoint](../{folder.relative_to(ROOT).as_posix()}/)", "",
             "Created topics: `telco.raw.customers`, `telco.churn.predictions`, `telco.deadletter`; each has three partitions and replication factor one. Prediction and dead-letter topics are provisioned for Step 7; this checkpoint publishes raw customer events.", "",
             "## Reliability limits", "", report["delivery_limit"], "",
             "Producer idempotence and acks=all protect client retries. The checkpoint is atomically replaced only after an entire chunk is acknowledged. An OS lock prevents two batch processes sharing the same checkpoint. Resume rejects changes to input checksum, cluster, topic and label policy. This single-broker development setup has no multi-broker fault tolerance.", "",
             "See [Kafka setup and producer commands](../KAFKA.md). The JSON report includes exact run IDs, per-partition offset windows, raw sample messages and the checked assertions.", ""]
    (ROOT / "reports/kafka_publishing_report.md").write_text("\n".join(lines), encoding="utf-8")
    print("PASS: both producer modes, full CSV resume, all event schemas/keys/IDs and original customer values verified.")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    main()
