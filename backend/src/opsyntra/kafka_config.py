"""Kafka settings and atomic, durable JSON outputs."""
import json
import math
import os
import re
import uuid
from pathlib import Path
import yaml


def load_kafka_config(path):
    path = Path(path).resolve()
    full = yaml.safe_load(path.read_text(encoding="utf-8-sig"))
    settings = dict(full["kafka"])
    settings["source"] = (path.parent / full["data"]["raw_path"]).resolve()
    for name in ("checkpoint", "report_dir"):
        settings[name] = (path.parent / settings[name]).resolve()
    settings["bootstrap_servers"] = os.environ.get("OPSYNTRA_KAFKA_BOOTSTRAP_SERVERS", settings["bootstrap_servers"])
    topics = list(settings["topics"].values())
    if (set(settings["topics"]) != {"raw", "predictions", "deadletter"} or len(set(topics)) != 3
            or any(not isinstance(t, str) or not re.fullmatch(r"[A-Za-z0-9_.-]{1,249}", t) or t in (".", "..") for t in topics)):
        raise ValueError("Configure three distinct valid Kafka topic names")
    for name in ("partitions", "replication_factor", "batch_size", "delivery_timeout_ms", "flush_timeout_seconds", "queue_timeout_seconds"):
        if type(settings[name]) is not int or settings[name] <= 0:
            raise ValueError(f"kafka.{name} must be a positive integer")
    for name in ("events_per_sec", "streaming_event_jitter_seconds"):
        value = settings[name]
        if type(value) not in (int, float) or not math.isfinite(value) or value < 0 or (name == "events_per_sec" and value == 0):
            raise ValueError(f"Invalid kafka.{name}")
    if settings["flush_timeout_seconds"] * 1000 < settings["delivery_timeout_ms"]:
        raise ValueError("flush_timeout_seconds must cover delivery_timeout_ms")
    return settings


def atomic_json(path, value):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(path.name + ".tmp-" + uuid.uuid4().hex)
    with temporary.open("w", encoding="utf-8", newline="\n") as output:
        json.dump(value, output, indent=2, sort_keys=True, allow_nan=False)
        output.write("\n")
        output.flush()
        os.fsync(output.fileno())
    temporary.replace(path)
