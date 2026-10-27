"""Create the required topics idempotently and verify broker metadata."""
import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "src"))


def ensure_topics(settings):
    from confluent_kafka import KafkaError, KafkaException
    from confluent_kafka.admin import AdminClient, NewTopic
    client = AdminClient({"bootstrap.servers": settings["bootstrap_servers"], "socket.timeout.ms": 10000})
    requested = [NewTopic(topic, num_partitions=settings["partitions"], replication_factor=settings["replication_factor"])
                 for topic in settings["topics"].values()]
    futures = client.create_topics(requested, request_timeout=20)
    for topic, future in futures.items():
        try:
            future.result()
        except KafkaException as error:
            if error.args[0].code() != KafkaError.TOPIC_ALREADY_EXISTS:
                raise
    metadata = client.list_topics(timeout=10)
    results = {}
    for topic in settings["topics"].values():
        info = metadata.topics.get(topic)
        if info is None or info.error or len(info.partitions) != settings["partitions"]:
            raise ValueError(f"Topic {topic} is unavailable or has an unexpected partition count")
        if any(p.error or len(p.replicas) != settings["replication_factor"] for p in info.partitions.values()):
            raise ValueError(f"Topic {topic} has an unexpected replica configuration")
        results[topic] = {"partitions": len(info.partitions), "replication_factor": settings["replication_factor"]}
    return {"bootstrap_servers": settings["bootstrap_servers"], "cluster_id": metadata.cluster_id, "topics": results}


if __name__ == "__main__":
    from opsyntra.kafka.config import load_kafka_config
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, default=ROOT / "config.yaml")
    args = parser.parse_args()
    print(json.dumps(ensure_topics(load_kafka_config(args.config)), indent=2))
