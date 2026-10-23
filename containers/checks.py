"""Read-only storage checks and bounded producer/replay probes for an isolated Compose project."""
import json
import os
import sqlite3
import sys
import time
from pathlib import Path

from container_runtime import ROOT, digest, prepare_config, role_url


def snapshot():
    import psycopg
    from psycopg.rows import dict_row
    _, config = prepare_config()
    with psycopg.connect(role_url("read"), row_factory=dict_row) as c:
        counts = {table: c.execute("SELECT count(*) AS n FROM analytics." + table).fetchone()["n"]
                  for table in ("raw_customer_events", "prediction_events", "kafka_deliveries", "ingestion_errors")}
        rows = c.execute("SELECT prediction_id,event_id::text,customer_id,engine,model_version FROM analytics.prediction_events ORDER BY prediction_id").fetchall()
        role = c.execute("SELECT current_user AS role, has_table_privilege(current_user,'analytics.prediction_events','INSERT') AS can_insert").fetchone()
    model = Path(config["training"]["artifact_dir"]) / "best_pipeline.joblib"
    from confluent_kafka import Consumer, TopicPartition
    client = Consumer({"bootstrap.servers": os.environ["OPSYNTRA_KAFKA_BOOTSTRAP_SERVERS"], "group.id": "opsyntra-churn-sklearn", "enable.auto.commit": False})
    try:
        offsets = client.committed([TopicPartition(config["kafka"]["topics"]["raw"], p) for p in range(3)], timeout=15)
        committed = {str(p.partition): p.offset for p in offsets}
    finally:
        client.close()
    return {"counts": counts, "predictions": rows, "model_sha256": digest(model), "reader": role, "input_offsets": committed}


def ledger():
    result = []
    for path in (ROOT / "artifacts/kafka/consumer_state").rglob("*.sqlite3"):
        with sqlite3.connect(f"file:{path}?mode=ro", uri=True) as c:
            result.append({"file": str(path.relative_to(ROOT)), "outbox_rows": c.execute("SELECT count(*) FROM outbox").fetchone()[0]})
    if not result:
        raise ValueError("Persistent inference ledger was not found")
    return result


def replay():
    from confluent_kafka import Consumer, Producer, TopicPartition
    _, config = prepare_config()
    topic = config["kafka"]["topics"]["raw"]
    bootstrap = os.environ["OPSYNTRA_KAFKA_BOOTSTRAP_SERVERS"]
    c = Consumer({"bootstrap.servers": bootstrap, "group.id": "opsyntra-container-observer", "enable.auto.commit": False})
    producer = Producer({"bootstrap.servers": bootstrap, "enable.idempotence": True})
    messages = []
    try:
        c.assign([TopicPartition(topic, p, 0) for p in range(3)])
        deadline = time.monotonic() + 30
        while len(messages) < 3 and time.monotonic() < deadline:
            message = c.poll(1)
            if message is not None and not message.error():
                messages.append(message)
        if len(messages) != 3:
            raise ValueError("Verification expects an isolated topic with three original events")
        errors = []
        for m in messages:
            producer.produce(topic, key=m.key(), value=m.value(), on_delivery=lambda error, _: errors.append(str(error)) if error else None)
        if producer.flush(20) or errors:
            raise ValueError("Replay was not acknowledged")
    finally:
        c.close()
    return {"replayed": len(messages), "event_ids": [json.loads(m.value())["event_id"] for m in messages]}


if __name__ == "__main__":
    print(json.dumps({"snapshot": snapshot, "ledger": ledger, "replay": replay}[sys.argv[1]](), default=str))
