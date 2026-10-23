"""Acknowledged publishing with bounded queues, stable replay IDs and checkpoints."""
import json
import logging
import os
import random
import threading
import time
from contextlib import contextmanager
from itertools import islice
from pathlib import Path
from uuid import uuid4

from .kafka_config import atomic_json
from .kafka_events import CustomerSource, customer_rows, encode_event, file_sha256, make_event, utc_now

LOG = logging.getLogger("opsyntra.kafka.producer")


class DeliveryError(RuntimeError):
    pass


@contextmanager
def checkpoint_lock(path):
    """OS-released lock: simultaneous publishers cannot share a batch cursor."""
    path = Path(path).with_name(Path(path).name + ".lock")
    path.parent.mkdir(parents=True, exist_ok=True)
    lock = path.open("a+b")
    locked = False
    try:
        lock.seek(0, os.SEEK_END)
        if lock.tell() == 0:
            lock.write(b"0")
            lock.flush()
        lock.seek(0)
        try:
            if os.name == "nt":
                import msvcrt
                msvcrt.locking(lock.fileno(), msvcrt.LK_NBLCK, 1)
            else:
                import fcntl
                fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            locked = True
        except OSError as error:
            raise ValueError("Another producer holds this batch checkpoint") from error
        yield
    finally:
        if locked:
            lock.seek(0)
            if os.name == "nt":
                import msvcrt
                msvcrt.locking(lock.fileno(), msvcrt.LK_UNLCK, 1)
            else:
                import fcntl
                fcntl.flock(lock, fcntl.LOCK_UN)
        lock.close()


class AcknowledgedPublisher:
    def __init__(self, producer, settings):
        self.producer, self.settings = producer, settings
        self.topic = settings["topics"]["raw"]
        self.enqueued = self.acknowledged = 0
        self.errors = []
        self.offsets = {}

    def check(self):
        if self.errors:
            raise DeliveryError("Kafka delivery failed; checkpoint retained: " + "; ".join(self.errors[:3]))

    def enqueue(self, event):
        self.check()
        def delivered(error, message):
            if error is not None:
                self.errors.append(f"event_id={event['event_id']}: {error}")
                LOG.error("Delivery failed for %s: %s", event["event_id"], error)
            else:
                self.acknowledged += 1
                partition = str(message.partition())
                self.offsets[partition] = max(self.offsets.get(partition, -1), message.offset())
                LOG.debug("ACK customer=%s partition=%s offset=%s event_id=%s", event["customerID"], partition, message.offset(), event["event_id"])
        deadline = time.monotonic() + self.settings["queue_timeout_seconds"]
        while True:
            try:
                self.producer.produce(self.topic, key=event["customerID"].encode("utf-8"),
                                      value=encode_event(event), on_delivery=delivered)
                self.enqueued += 1
                break
            except BufferError:
                self.producer.poll(.1)
                self.check()
                if time.monotonic() >= deadline:
                    raise DeliveryError("Producer queue remained full; checkpoint retained")
        self.producer.poll(0)
        self.check()

    def drain(self):
        remaining = self.producer.flush(self.settings["flush_timeout_seconds"])
        self.check()
        if remaining or self.acknowledged != self.enqueued:
            raise DeliveryError(f"Unconfirmed deliveries: {self.enqueued - self.acknowledged}; checkpoint retained")


def kafka_producer(settings):
    from confluent_kafka import Producer
    return Producer({"bootstrap.servers": settings["bootstrap_servers"], "client.id": "opsyntra-customer-producer",
        "enable.idempotence": True, "acks": "all", "retries": 2147483647,
        "max.in.flight.requests.per.connection": 5, "retry.backoff.ms": 250,
        "delivery.timeout.ms": settings["delivery_timeout_ms"], "request.timeout.ms": min(10000, settings["delivery_timeout_ms"]),
        "linger.ms": 10, "compression.type": "gzip", "allow.auto.create.topics": False,
        "queue.buffering.max.messages": 10000})


def broker_identity(producer, settings):
    metadata = producer.list_topics(timeout=10)
    topic = metadata.topics.get(settings["topics"]["raw"])
    if topic is None or topic.error or not topic.partitions:
        raise ValueError("Raw customer topic is unavailable; run scripts/windows/start-kafka.ps1 -Task topics")
    if not metadata.cluster_id:
        raise ValueError("Broker did not return its cluster identifier")
    return metadata.cluster_id


def run_producer(settings, *, mode, input_csv=None, checkpoint=None, resume=False, batch_size=None,
                 events_per_sec=None, max_events=None, seed=42, exclude_churn=False,
                 stop=None, producer=None, cluster_id=None):
    """Both CLI modes share the same delivery, schema and result handling."""
    import math
    if mode not in ("batch", "streaming"):
        raise ValueError("mode must be batch or streaming")
    batch_size = settings["batch_size"] if batch_size is None else batch_size
    rate = settings["events_per_sec"] if events_per_sec is None else events_per_sec
    if type(batch_size) is not int or batch_size <= 0 or (max_events is not None and (type(max_events) is not int or max_events <= 0)):
        raise ValueError("batch_size and max_events must be positive integers")
    if not math.isfinite(rate) or rate <= 0:
        raise ValueError("events_per_sec must be finite and positive")
    if mode == "streaming" and resume:
        raise ValueError("--resume applies only to batch mode")
    source = CustomerSource.inspect(input_csv or settings["source"])
    checkpoint = Path(checkpoint or settings["checkpoint"]).resolve()
    if checkpoint == source.path or source.path.is_relative_to(settings["report_dir"]):
        raise ValueError("Input CSV and checkpoint/result paths must be separate")
    stop = stop or threading.Event()
    invocation = str(uuid4())
    directory = Path(settings["report_dir"]) / invocation
    directory.mkdir(parents=True, exist_ok=False)
    handler = logging.FileHandler(directory / "producer.log", encoding="utf-8")
    handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(message)s"))
    LOG.addHandler(handler)
    started = time.monotonic()
    summary = {"invocation_id": invocation, "mode": mode, "source_sha256": source.sha256, "source_rows": source.rows,
               "bootstrap_servers": settings["bootstrap_servers"], "topic": settings["topics"]["raw"],
               "started_at": utc_now(), "delivery_semantics": "at-least-once across process crashes; stable event_id permits deduplication",
               "include_churn": not exclude_churn, "report_directory": str(directory)}
    publisher = None
    try:
        producer = producer if producer is not None else kafka_producer(settings)
        cluster_id = cluster_id or broker_identity(producer, settings)
        summary["cluster_id"] = cluster_id
        publisher = AcknowledgedPublisher(producer, settings)
        if mode == "batch":
            with checkpoint_lock(checkpoint):
                _batch(source, publisher, summary, checkpoint, resume, batch_size, max_events, exclude_churn, stop)
        else:
            _stream(source, publisher, summary, rate, max_events, seed, exclude_churn, stop)
        LOG.info("%s mode=%s acknowledged=%s run_id=%s", summary["status"].upper(), mode, publisher.acknowledged, summary["run_id"])
        return summary
    except BaseException as error:
        summary.update(status="failed", error=str(error))
        LOG.exception("Publishing failed; no unconfirmed chunk is checkpointed")
        raise
    finally:
        if publisher:
            summary.update(enqueued=publisher.enqueued, acknowledged=publisher.acknowledged, acknowledged_offsets=publisher.offsets)
        summary.update(finished_at=utc_now(), elapsed_seconds=time.monotonic() - started)
        atomic_json(directory / "summary.json", summary)
        LOG.removeHandler(handler)
        handler.close()


def _batch(source, publisher, summary, checkpoint, resume, batch_size, max_events, exclude_churn, stop):
    identity = {"schema_version": 1, "source_sha256": source.sha256, "total_rows": source.rows,
                "cluster_id": summary["cluster_id"], "topic": publisher.topic, "include_churn": not exclude_churn}
    if checkpoint.exists():
        if not resume:
            raise ValueError("Checkpoint already exists. Use --resume, or choose a new --checkpoint for intentional replay")
        state = json.loads(checkpoint.read_text(encoding="utf-8"))
        if any(state.get(key) != value for key, value in identity.items()):
            raise ValueError("Checkpoint does not match the input checksum, broker cluster, topic or event policy")
        from uuid import UUID
        UUID(state["run_id"])
        from datetime import datetime
        timestamp = datetime.fromisoformat(state["started_at"].replace("Z", "+00:00"))
        if timestamp.tzinfo is None:
            raise ValueError("Checkpoint needs a timezone-aware timestamp")
        if type(state["next_row"]) is not int or not 0 <= state["next_row"] <= source.rows:
            raise ValueError("Checkpoint cursor is invalid")
    else:
        if resume:
            raise ValueError("Cannot resume: checkpoint file does not exist")
        state = {**identity, "run_id": str(uuid4()), "started_at": utc_now(), "next_row": 0,
                 "status": "in_progress", "acknowledged_offsets": {}}
        atomic_json(checkpoint, state)
    initial = state["next_row"]
    summary.update(run_id=state["run_id"], checkpoint=str(checkpoint), resumed=resume, start_row=initial)
    rows = iter(islice(customer_rows(source.path), initial, None))
    limit = source.rows if max_events is None else min(source.rows, initial + max_events)
    while state["next_row"] < limit and not stop.is_set():
        chunk = list(islice(rows, min(batch_size, limit - state["next_row"])))
        if not chunk:
            raise ValueError("Source CSV ended before its validated row count")
        for index, customer in chunk:
            if exclude_churn:
                customer.pop("Churn", None)
            publisher.enqueue(make_event(customer, run_id=state["run_id"], source=source, row_index=index,
                sequence=index, mode="batch", started_at=state["started_at"]))
        publisher.drain()
        if file_sha256(source.path) != source.sha256:
            raise ValueError("Source CSV changed; checkpoint not advanced")
        state.update(next_row=chunk[-1][0] + 1, acknowledged_offsets={**state["acknowledged_offsets"], **publisher.offsets},
                     updated_at=utc_now())
        state["status"] = "completed" if state["next_row"] == source.rows else "in_progress"
        atomic_json(checkpoint, state)
        summary["next_row"] = state["next_row"]
        LOG.info("ACK batch=%s next_row=%s/%s run_id=%s checkpoint=%s", len(chunk), state["next_row"], source.rows, state["run_id"], checkpoint)
    summary.update(next_row=state["next_row"], status="completed" if state["next_row"] == source.rows else "paused")


def _stream(source, publisher, summary, rate, max_events, seed, exclude_churn, stop):
    rows = list(customer_rows(source.path))
    if file_sha256(source.path) != source.sha256:
        raise ValueError("Source CSV changed before streaming")
    rng = random.Random(seed)
    run_id, sequence = str(uuid4()), 0
    summary.update(run_id=run_id, requested_events_per_sec=rate, seed=seed,
                   jitter_seconds=publisher.settings["streaming_event_jitter_seconds"])
    while not stop.is_set() and (max_events is None or sequence < max_events):
        began = time.monotonic()
        index, customer = rng.choice(rows)
        customer = dict(customer)
        if exclude_churn:
            customer.pop("Churn", None)
        publisher.enqueue(make_event(customer, run_id=run_id, source=source, row_index=index,
            sequence=sequence, mode="streaming", started_at=summary["started_at"], rng=rng,
            jitter_seconds=publisher.settings["streaming_event_jitter_seconds"]))
        sequence += 1
        if sequence % 100 == 0:
            LOG.info("Streaming enqueued=%s acknowledged=%s run_id=%s", sequence, publisher.acknowledged, run_id)
        if max_events is None or sequence < max_events:
            stop.wait(max(0, 1 / rate - (time.monotonic() - began)))
    publisher.drain()
    summary["status"] = "stopped" if stop.is_set() else "completed"
