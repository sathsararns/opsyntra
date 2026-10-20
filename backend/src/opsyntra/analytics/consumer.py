"""Kafka -> PostgreSQL only. No model loading, scoring or output-topic writes."""
import json
import logging
import math
import re
import threading
import time
from dataclasses import dataclass
from datetime import datetime, timezone

import psycopg
from confluent_kafka import Consumer, KafkaError, KafkaException, TopicPartition

from .ingestion import process_batch

LOG = logging.getLogger("opsyntra.analytics")
TRANSIENT_DATABASE = (psycopg.OperationalError, psycopg.InterfaceError,
                      psycopg.errors.SerializationFailure, psycopg.errors.DeadlockDetected,
                      psycopg.errors.LockNotAvailable, psycopg.errors.QueryCanceled)


def log(event, **fields):
    LOG.info(json.dumps({"ts": datetime.now(timezone.utc).isoformat(), "event": event, **fields}, separators=(",", ":"), default=str))


class InterruptedRetry(Exception):
    pass


@dataclass(frozen=True)
class Options:
    group_id: str = "opsyntra-analytics-v1"
    mode: str = "streaming"
    batch_size: int = 100
    max_messages: int | None = None
    snapshot: bool = False
    timeout_seconds: float | None = None
    idle_timeout: float = 30
    retries: int = 3
    retry_delay: float = .5
    max_event_bytes: int = 262144
    max_poll_interval_ms: int = 300000

    def __post_init__(self):
        if not re.fullmatch(r"[A-Za-z0-9_.-]{1,200}", self.group_id) or self.group_id.startswith("opsyntra-churn"):
            raise ValueError("Use a distinct analytics group; never reuse the inference group")
        if self.mode not in ("streaming", "batch") or (self.snapshot and self.mode != "batch"):
            raise ValueError("Snapshot requires batch mode")
        for name, low, high in (("batch_size", 1, 1000), ("retries", 0, 5), ("max_event_bytes", 1024, 4194304),
                                ("max_poll_interval_ms", 30000, 900000)):
            if type(getattr(self, name)) is not int or not low <= getattr(self, name) <= high:
                raise ValueError(f"Invalid analytics {name}")
        if self.max_messages is not None and (type(self.max_messages) is not int or self.max_messages <= 0):
            raise ValueError("max_messages must be positive")
        for name in ("timeout_seconds", "idle_timeout", "retry_delay"):
            value = getattr(self, name)
            if value is not None and (type(value) not in (int, float) or not math.isfinite(value) or value <= 0):
                raise ValueError(f"Invalid analytics {name}")
        if self.mode == "batch" and not (self.snapshot or self.max_messages):
            raise ValueError("Batch mode needs max_messages or snapshot")
        if self.snapshot and self.max_messages is not None:
            raise ValueError("Use a full snapshot or a message count, not both")


def retry(operation, options, stop, *, name, retry_hook=lambda: None):
    for attempt in range(options.retries + 1):
        try:
            return operation()
        except TRANSIENT_DATABASE + (KafkaException,) as error:
            if isinstance(error, KafkaException) and error.args and hasattr(error.args[0], "retriable"):
                local_transient = error.args[0].code() in (KafkaError._TRANSPORT, KafkaError._ALL_BROKERS_DOWN, KafkaError._TIMED_OUT)
                if not error.args[0].retriable() and not local_transient:
                    raise
            if attempt == options.retries:
                raise
            # Do not log exception strings: connection errors may contain DSNs.
            log("retry", operation=name, attempt=attempt + 1, error_type=type(error).__name__)
            retry_hook()
            if stop.wait(min(options.retry_delay * 2**attempt, 5)):
                raise InterruptedRetry("Stopped during retry backoff") from error


def database_ready(connect):
    with connect() as connection:
        for table in ("raw_customer_events", "prediction_events", "deadletter_events", "ingestion_errors", "kafka_deliveries"):
            connection.execute("SELECT 1 FROM analytics." + table + " LIMIT 0")
        connection.execute("SELECT 1 FROM analytics.prediction_hourly LIMIT 0")
        connection.execute("SELECT 1 FROM analytics.prediction_daily LIMIT 0")


def commit_offsets(consumer, offsets):
    if not offsets:
        return
    requested = [TopicPartition(topic, partition, offset) for (topic, partition), offset in sorted(offsets.items())]
    result = consumer.commit(offsets=requested, asynchronous=False)
    if result is None or len(result) != len(requested) or any(p.error for p in result):
        raise RuntimeError("Kafka input commit was not fully acknowledged; restart safely replays database facts")


def configure_snapshot(consumer, topics, metadata):
    partitions = [TopicPartition(topic, p) for topic in topics for p in sorted(metadata.topics[topic].partitions)]
    committed = {(p.topic, p.partition): p.offset for p in consumer.committed(partitions, timeout=10)}
    assignment, ends, starts = [], {}, {}
    for part in partitions:
        low, high = consumer.get_watermark_offsets(part, timeout=10, cached=False)
        previous = committed[(part.topic, part.partition)]
        start = low if previous < 0 else previous
        if not low <= start <= high:
            raise ValueError("Committed analytics offset is outside retained Kafka data; refusing to skip lost input")
        starts[(part.topic, part.partition)] = start
        ends[(part.topic, part.partition)] = high
        assignment.append(TopicPartition(part.topic, part.partition, start))
    return starts, ends


def assign_retained(consumer, partitions):
    """Explicit first-start earliest, but fail instead of hiding retention loss."""
    committed = {(p.topic, p.partition): p.offset for p in consumer.committed(partitions, timeout=10)}
    assignment = []
    for part in partitions:
        low, high = consumer.get_watermark_offsets(part, timeout=10, cached=False)
        previous = committed[(part.topic, part.partition)]
        start = low if previous < 0 else previous
        if not low <= start <= high:
            raise ValueError("Committed analytics offset is outside retained Kafka data")
        assignment.append(TopicPartition(part.topic, part.partition, start))
    consumer.assign(assignment)
    return assignment


def run(settings, connect, *, options=Options(), stop=None, consumer_factory=Consumer, fault_hook=lambda stage: None):
    stop = stop or threading.Event()
    names = settings["topics"]
    if set(names) != {"raw", "predictions", "deadletter"} or len(set(names.values())) != 3:
        raise ValueError("Configure three distinct analytics topics")
    stats = {"records": 0, "raw_inserted": 0, "prediction_inserted": 0, "deadletter_inserted": 0,
             "logical_duplicates": 0, "deliveries_inserted": 0, "rejected": 0,
             "quarantine_inserted": 0, "quarantine_duplicates": 0, "event_conflicts": 0,
             "database_batches_committed": 0, "kafka_commits": 0, "database_retries": 0}
    started = last_record = time.monotonic()
    consumer = None
    acknowledged = {}
    def add_retry(): stats["database_retries"] += 1
    try:
        retry(lambda: database_ready(connect), options, stop, name="database_ready", retry_hook=add_retry)
        consumer = consumer_factory({"bootstrap.servers": settings["bootstrap_servers"], "group.id": options.group_id,
            "enable.auto.commit": False, "enable.auto.offset.store": False,
            "auto.offset.reset": "error", "allow.auto.create.topics": False,
            "enable.partition.eof": True, "isolation.level": "read_committed",
            "max.poll.interval.ms": options.max_poll_interval_ms, "socket.timeout.ms": 10000,
            "session.timeout.ms": 10000})
        metadata = retry(lambda: consumer.list_topics(timeout=10), options, stop, name="broker_ready")
        if not metadata.cluster_id or any(topic not in metadata.topics or metadata.topics[topic].error for topic in names.values()):
            raise ValueError("Kafka analytics topics are missing or unavailable; create them before starting")
        starts, ends = {}, {}
        snapshot_assigned = False
        if options.snapshot:
            starts, ends = configure_snapshot(consumer, list(names.values()), metadata)
            def snapshot_assignment(client, partitions):
                nonlocal snapshot_assigned, last_record
                if {(p.topic,p.partition) for p in partitions} != set(ends):
                    raise ValueError("A bounded snapshot needs exclusive ownership of its analytics group")
                assigned = assign_retained(client, partitions)
                for part in assigned:
                    if part.offset > ends[(part.topic,part.partition)]:
                        raise ValueError("Group advanced beyond the captured snapshot; use a separate snapshot group")
                    starts[(part.topic,part.partition)] = part.offset
                snapshot_assigned = True
                last_record = time.monotonic()
            consumer.subscribe(list(names.values()), on_assign=snapshot_assignment)
        else:
            consumer.subscribe(list(names.values()), on_assign=assign_retained)
        log("ready", group_id=options.group_id, cluster_id=metadata.cluster_id, topics=names, mode=options.mode)
        last_record = time.monotonic()
        status = "complete"
        while True:
            if stop.is_set():
                status = "stopped"
                break
            if options.max_messages is not None and stats["records"] >= options.max_messages:
                break
            if options.snapshot and snapshot_assigned:
                positions = {(p.topic, p.partition): p.offset for p in consumer.position([TopicPartition(t, p) for t, p in ends])}
                finished = {part: end for part, end in ends.items() if starts[part] >= end or positions.get(part, -1) >= end}
                # Last DB batch already committed before this point. Empty/gap
                # ranges and records fetched beyond a captured end are covered.
                boundary = {part: end for part, end in finished.items() if acknowledged.get(part, starts[part]) < end}
                if boundary:
                    commit_offsets(consumer, boundary)
                    acknowledged.update(boundary)
                    stats["kafka_commits"] += 1
                if len(finished) == len(ends):
                    break
                if finished:
                    consumer.pause([TopicPartition(t, p) for t, p in finished])
            elapsed = time.monotonic() - started
            if options.timeout_seconds is not None and elapsed >= options.timeout_seconds:
                status = "incomplete" if options.mode == "batch" else "complete"
                break
            if options.mode == "batch" and time.monotonic() - last_record >= options.idle_timeout:
                status = "incomplete"
                break
            count = options.batch_size if options.max_messages is None else min(options.batch_size, options.max_messages-stats["records"])
            messages = consumer.consume(num_messages=count, timeout=1)
            records = []
            for message in messages:
                if message.error():
                    if message.error().code() == KafkaError._PARTITION_EOF:
                        continue
                    raise KafkaException(message.error())
                part = (message.topic(), message.partition())
                if not options.snapshot or message.offset() < ends[part]:
                    records.append(message)
            if not records:
                continue
            def persist():
                with connect() as connection:
                    return process_batch(connection, records, names, metadata.cluster_id,
                                         max_event_bytes=options.max_event_bytes, fault_hook=fault_hook)
            counts = retry(persist, options, stop, name="database_batch", retry_hook=add_retry)
            stats["database_batches_committed"] += 1
            offsets = {}
            for message in records:
                part = (message.topic(), message.partition())
                offsets[part] = max(offsets.get(part, 0), message.offset() + 1)
            # Never derive acknowledgments from fetch positions before persisting
            # all corresponding records. Sync commits also check partition errors.
            commit_offsets(consumer, offsets)
            fault_hook("after_kafka_commit")
            acknowledged.update(offsets)
            stats["kafka_commits"] += 1
            for name, value in counts.items(): stats[name] += value
            last_record = time.monotonic()
            log("batch_committed", group_id=options.group_id, **counts)
        return {"status": status, "group_id": options.group_id, "cluster_id": metadata.cluster_id,
                "topics": names, "mode": options.mode, **stats, "elapsed_seconds": time.monotonic()-started,
                "committed_offsets": [{"topic": t, "partition": p, "offset": o} for (t, p), o in sorted(acknowledged.items())],
                "snapshot": [{"topic": t, "partition": p, "start": starts[(t,p)], "end": o} for (t,p),o in sorted(ends.items())]}
    except InterruptedRetry:
        return {"status": "stopped", "group_id": options.group_id, **stats}
    finally:
        if consumer is not None:
            consumer.close()  # auto commits are disabled, including during close
