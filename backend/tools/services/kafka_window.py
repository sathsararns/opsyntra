"""Capture a fixed raw-topic offset window without consuming or committing data."""
import argparse
import json
import sys
from pathlib import Path
from uuid import uuid4

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "src"))


def capture_window(settings):
    from confluent_kafka import Consumer, TopicPartition
    from opsyntra.kafka_events import utc_now
    consumer = Consumer({"bootstrap.servers": settings["bootstrap_servers"], "group.id": "opsyntra-window-" + uuid4().hex,
                         "enable.auto.commit": False, "allow.auto.create.topics": False})
    try:
        topic = settings["topics"]["raw"]
        metadata = consumer.list_topics(timeout=10)
        if topic not in metadata.topics or metadata.topics[topic].error:
            raise ValueError("Raw topic is unavailable")
        partitions = {}
        for partition in sorted(metadata.topics[topic].partitions):
            low, high = consumer.get_watermark_offsets(TopicPartition(topic, partition), timeout=10)
            partitions[str(partition)] = {"start": low, "end": high}
        return {"cluster_id": metadata.cluster_id, "topic": topic, "partitions": partitions, "captured_at": utc_now()}
    finally:
        consumer.close()


if __name__ == "__main__":
    from opsyntra.kafka_config import atomic_json, load_kafka_config
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, default=ROOT / "config.yaml")
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    if args.output.exists():
        parser.error("Window file already exists; choose a new output to preserve the original cutoff")
    result = capture_window(load_kafka_config(args.config))
    atomic_json(args.output, result)
    print(json.dumps(result, indent=2))
