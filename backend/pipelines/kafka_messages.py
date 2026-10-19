"""Inspect a bounded number of Kafka messages without committing application offsets."""
import argparse
import base64
import json
import sys
import time
from pathlib import Path
from uuid import uuid4

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))


def main():
    from confluent_kafka import Consumer
    from opsyntra.kafka_config import load_kafka_config
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--topic", choices=["raw", "predictions", "deadletter"], default="predictions")
    parser.add_argument("--max-messages", type=int, default=5)
    parser.add_argument("--timeout-seconds", type=float, default=10)
    parser.add_argument("--config", type=Path, default=ROOT / "config.yaml")
    args = parser.parse_args()
    if args.max_messages <= 0 or not 0 < args.timeout_seconds <= 300:
        parser.error("Use a positive message count and a finite timeout up to 300 seconds")
    settings = load_kafka_config(args.config)
    topic = settings["topics"][args.topic]
    consumer = Consumer({"bootstrap.servers": settings["bootstrap_servers"], "group.id": "opsyntra-inspect-" + uuid4().hex,
        "enable.auto.commit": False, "enable.auto.offset.store": False, "auto.offset.reset": "earliest",
        "allow.auto.create.topics": False, "isolation.level": "read_committed"})
    messages = []
    try:
        consumer.subscribe([topic])
        deadline = time.monotonic() + args.timeout_seconds
        while len(messages) < args.max_messages and time.monotonic() < deadline:
            message = consumer.poll(min(.5, max(0, deadline - time.monotonic())))
            if message is None:
                continue
            if message.error():
                raise RuntimeError(str(message.error()))
            value = message.value()
            try:
                payload = json.loads(value) if value is not None else None
            except (ValueError, UnicodeError):
                payload = {"raw_base64": base64.b64encode(value).decode()}
            messages.append({"key": message.key().decode("utf-8", errors="replace") if message.key() else None,
                             "partition": message.partition(), "offset": message.offset(), "value": payload})
    finally:
        consumer.close()
    print(json.dumps({"topic": topic, "messages": messages}, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()
