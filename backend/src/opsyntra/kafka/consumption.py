"""Validate -> infer -> durable outbox -> acknowledge -> ledger -> input commit."""
import base64
import hashlib
import json
import logging
import math
import threading
import time
from pathlib import Path
from uuid import NAMESPACE_URL, uuid4, uuid5

from opsyntra.kafka.config import atomic_json
from opsyntra.kafka.events import utc_now, validate_event
from opsyntra.kafka.publishing import DeliveryError, checkpoint_lock, kafka_producer
from opsyntra.kafka.recovery import RecoveryStore

LOG = logging.getLogger("opsyntra.kafka.consumer")


class InvalidRecord(ValueError):
    pass


def strict_json(value):
    def pairs(items):
        result = {}
        for key, item in items:
            if key in result:
                raise ValueError(f"Duplicate JSON field: {key}")
            result[key] = item
        return result
    def constant(value):
        raise ValueError(f"Non-finite JSON literal: {value}")
    return json.loads(value, object_pairs_hook=pairs, parse_constant=constant)


def decode_record(message, maximum_bytes):
    try:
        value, key = message.value(), message.key()
        if value is None or key is None:
            raise ValueError("Customer event needs a value and a customerID key; tombstones are not prediction requests")
        if len(value) > maximum_bytes:
            raise ValueError("Customer event exceeds the configured byte limit")
        event = strict_json(value.decode("utf-8"))
        if not isinstance(event, dict):
            raise ValueError("Customer event must be a JSON object")
        validate_event(event, key.decode("utf-8"))
        return event
    except (ValueError, TypeError, AttributeError, OverflowError, RecursionError) as error:
        raise InvalidRecord(str(error)[:500]) from error


def fingerprint(event):
    return hashlib.sha256(json.dumps(event, sort_keys=True, ensure_ascii=False, allow_nan=False).encode()).hexdigest()


def deadletter_job(message, reason, settings, cluster_id, event=None):
    location = f"{cluster_id}:{message.topic()}:{message.partition()}:{message.offset()}"
    identifier = str(uuid5(NAMESPACE_URL, "opsyntra-deadletter:" + location))
    raw = message.value()
    original_key = message.key()
    payload = {"schema_version": 1, "event_id": identifier, "processed_ts": utc_now(),
        "original_event_id": event.get("event_id") if event else None,
        "customerID": event.get("customerID") if event else None,
        "error": {"type": "invalid_record", "message": str(reason)[:500]},
        "source": {"cluster_id": cluster_id, "topic": message.topic(), "partition": message.partition(), "offset": message.offset()},
        "raw_value_is_null": raw is None, "raw_key_is_null": original_key is None,
        "raw_value_base64": base64.b64encode((raw or b"")[:65536]).decode(),
        "raw_key_base64": base64.b64encode((original_key or b"")[:4096]).decode(),
        "raw_value_truncated": len(raw or b"") > 65536, "raw_key_truncated": len(original_key or b"") > 4096,
        "raw_value_sha256": hashlib.sha256(raw or b"").hexdigest()}
    digest = hashlib.sha256((b"null:" if raw is None else b"value:") + (raw or b"") + b"\x00key:" + (original_key or b"")).hexdigest()
    return {"record_id": "deadletter:" + location, "fingerprint": digest,
            "topic": settings["topics"]["deadletter"], "key": identifier, "payload": payload}


def prepare_jobs(messages, store, model, settings, cluster_id, stats):
    jobs = {}
    def add(job):
        previous = jobs.get(job["record_id"]) or store.get(job["record_id"])
        if previous:
            if previous["fingerprint"] != job["fingerprint"]:
                return False
            if job["record_id"] in jobs or previous.get("state") == "sent":
                stats["deduplicated"] += 1
                return True
            job["payload"] = json.loads(previous["payload"])
        jobs[job["record_id"]] = job
        return True
    for message in messages:
        try:
            event = decode_record(message, settings["consumer"]["max_event_bytes"])
        except InvalidRecord as error:
            LOG.warning("Invalid input %s[%s]@%s: %s", message.topic(), message.partition(), message.offset(), error)
            if not add(deadletter_job(message, error, settings, cluster_id)):
                raise ValueError("Source coordinates changed contents in the recovery store")
            continue
        job = {"record_id": "prediction:" + event["event_id"], "fingerprint": fingerprint(event),
               "topic": settings["topics"]["predictions"], "key": event["customerID"], "payload": None, "event": event}
        if not add(job):
            LOG.warning("Conflicting payload reused event_id=%s", event["event_id"])
            add(deadletter_job(message, "event_id was already used with different customer/event contents", settings, cluster_id, event))
    pending = [job for job in jobs.values() if job["payload"] is None]
    if pending:
        predictions = model.predict([job["event"] for job in pending])
        if len(predictions) != len(pending):
            raise ValueError("Model returned an unexpected prediction count")
        stats["inference_batches"] += 1
        for job, result in zip(pending, predictions):
            probability = float(result["churn_probability"])
            if (not math.isfinite(probability) or not 0 <= probability <= 1 or result["prediction"] not in ("Yes", "No")
                    or result["model_version"] != model.version):
                raise ValueError("Model produced invalid results")
            event = job["event"]
            job["payload"] = {"customerID": event["customerID"], "churn_probability": probability,
                "prediction": result["prediction"], "event_ts": event["event_ts"], "processed_ts": utc_now(),
                "event_id": event["event_id"], "model_version": model.version, "engine": model.engine,
                "schema_version": 1, "prediction_id": hashlib.sha256((model.sha256 + ":" + event["event_id"]).encode()).hexdigest()}
    store.prepare(jobs.values())
    return [store.get(record_id) for record_id in jobs]


def deliver_jobs(jobs, store, producer, settings, stats, fault_hook):
    acknowledgements, errors = [], []
    for job in jobs:
        def delivered(error, message, job=job):
            if error:
                errors.append(str(error))
            else:
                acknowledgements.append((job["record_id"], message.partition(), message.offset()))
                name = "predictions_published" if job["topic"] == settings["topics"]["predictions"] else "deadletters_published"
                stats[name] += 1
        deadline = time.monotonic() + settings["queue_timeout_seconds"]
        while True:
            try:
                producer.produce(job["topic"], key=job["message_key"].encode("utf-8"),
                                 value=job["payload"].encode("utf-8"), on_delivery=delivered)
                break
            except BufferError:
                producer.poll(.1)
                if time.monotonic() >= deadline:
                    raise DeliveryError("Prediction producer queue timeout; input offsets not committed")
        producer.poll(0)
    if jobs:
        remaining = producer.flush(settings["flush_timeout_seconds"])
        if acknowledgements and not errors and not remaining:
            fault_hook("after_output_ack")
        # Preserve individually confirmed outputs even if another output failed.
        store.mark_sent(acknowledgements)
        if errors or remaining or len(acknowledgements) != len(jobs):
            raise DeliveryError("Output delivery was not fully confirmed; input offsets not committed: " + "; ".join(errors[:3]))
        fault_hook("after_ledger_commit")


def commit_inputs(consumer, messages, stats):
    from confluent_kafka import TopicPartition
    offsets = {}
    for message in messages:
        key = (message.topic(), message.partition())
        offsets[key] = max(offsets.get(key, 0), message.offset() + 1)
    result = consumer.commit(offsets=[TopicPartition(topic, partition, offset) for (topic, partition), offset in offsets.items()], asynchronous=False)
    if result is None or any(part.error for part in result):
        raise DeliveryError("Input commit was not confirmed; restart will use the delivery ledger")
    stats["committed_records"] += len(messages)
    stats["committed_offsets"].update({str(partition): offset for (_, partition), offset in offsets.items()})


def configure_window(consumer, window, topic, cluster_id):
    from confluent_kafka import TopicPartition
    if window.get("cluster_id") != cluster_id or window.get("topic") != topic:
        raise ValueError("Offset window belongs to a different broker cluster or input topic")
    partitions = window.get("partitions")
    if not isinstance(partitions, dict) or not partitions:
        raise ValueError("Offset window requires nonempty partitions")
    assignment, ends, cursors = [], {}, {}
    committed = consumer.committed([TopicPartition(topic, int(p)) for p in partitions], timeout=10)
    committed = {p.partition: p.offset for p in committed}
    for key, bounds in partitions.items():
        p = int(key)
        if str(p) != key or set(bounds) != {"start", "end"} or any(type(bounds[k]) is not int for k in bounds):
            raise ValueError("Window partitions use integer names and integer start/end offsets")
        low, high = consumer.get_watermark_offsets(TopicPartition(topic, p), timeout=10)
        if not low <= bounds["start"] <= bounds["end"] <= high:
            raise ValueError("Window is outside available offsets; refusing to skip unavailable source data")
        cursor = min(bounds["end"], max(bounds["start"], committed[p]))
        ends[p], cursors[p] = bounds["end"], cursor
        assignment.append(TopicPartition(topic, p, cursor))
    consumer.assign(assignment)
    return {"ends": ends, "cursors": cursors, "paused": set()}


def run_consumer(settings, *, mode, engine="sklearn", group_id=None, model_path=None, state_dir=None,
                 max_messages=None, batch_size=None, idle_timeout=None, timeout_seconds=None, offset_window=None,
                 stop=None, consumer=None, producer=None, model=None, fault_hook=lambda stage: None, progress_callback=None):
    from confluent_kafka import Consumer, KafkaError, TopicPartition
    from opsyntra.kafka.models import PredictionModel
    policy = settings["consumer"]
    if mode not in ("batch", "streaming"):
        raise ValueError("mode must be batch or streaming")
    if engine not in ("sklearn", "spark"):
        raise ValueError("engine must be sklearn or spark")
    if offset_window is not None and mode != "batch":
        raise ValueError("Offset windows apply only to batch mode")
    if max_messages is None and mode == "batch" and offset_window is None:
        max_messages = policy["max_messages"]
    batch_size = policy["batch_size"] if batch_size is None else batch_size
    idle_timeout = policy["idle_timeout_seconds"] if idle_timeout is None else idle_timeout
    timeout_seconds = policy["batch_timeout_seconds"] if timeout_seconds is None else timeout_seconds
    for name, value in (("batch_size", batch_size), ("max_messages", max_messages)):
        if value is not None and (type(value) is not int or value <= 0):
            raise ValueError(f"{name} must be a positive integer")
    if any(type(v) not in (int, float) or not math.isfinite(v) or v <= 0 for v in (idle_timeout, timeout_seconds)):
        raise ValueError("Timeouts must be finite and positive")
    group_id = group_id or policy["group_prefix"] + "-" + engine
    if not group_id or len(group_id) > 200 or any(ord(c) < 32 for c in group_id):
        raise ValueError("Consumer group ID must contain 1-200 printable characters")
    state_dir = Path(state_dir or policy["state_dir"]).resolve()
    stop = stop or threading.Event()
    invocation = str(uuid4())
    directory = policy["report_dir"] / invocation
    directory.mkdir(parents=True, exist_ok=False)
    handler = logging.FileHandler(directory / "consumer.log", encoding="utf-8")
    handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(message)s"))
    LOG.addHandler(handler)
    stats = {"invocation_id": invocation, "mode": mode, "engine": engine, "group_id": group_id,
        "started_at": utc_now(), "report_directory": str(directory), "received_records": 0, "committed_records": 0,
        "predictions_published": 0, "deadletters_published": 0, "deduplicated": 0, "inference_batches": 0,
        "committed_offsets": {}, "max_messages": max_messages, "batch_size": batch_size,
        "delivery_semantics": "at-least-once with a durable outbox and ledger; output-ack/local-ledger crash window can duplicate stable prediction IDs"}
    began = time.monotonic()
    store = None
    try:
        consumer = consumer if consumer is not None else Consumer({"bootstrap.servers": settings["bootstrap_servers"],
            "group.id": group_id, "enable.auto.commit": False, "enable.auto.offset.store": False,
            "auto.offset.reset": "earliest", "allow.auto.create.topics": False, "isolation.level": "read_committed",
            "max.poll.interval.ms": policy["max_poll_interval_ms"], "session.timeout.ms": 10000, "socket.timeout.ms": 10000})
        metadata = consumer.list_topics(timeout=10)
        for topic in settings["topics"].values():
            if topic not in metadata.topics or metadata.topics[topic].error:
                raise ValueError(f"Topic unavailable: {topic}; run scripts/windows/start-kafka.ps1 -Task topics")
        stats["cluster_id"] = metadata.cluster_id
        scope = hashlib.sha256((metadata.cluster_id + ":" + group_id).encode()).hexdigest()[:24]
        ledger_path = state_dir / (scope + ".sqlite3")
        stats["recovery_database"] = str(ledger_path)
        with checkpoint_lock(ledger_path):
            model = model if model is not None else PredictionModel(settings, engine, model_path, directory)
            stats.update(model_version=model.version, model_sha256=model.sha256, model_loads=1)
            if progress_callback is not None:
                progress_callback(dict(stats))
            LOG.info("Loaded %s model once: version=%s group=%s", engine, model.version, group_id)
            identity = {"cluster_id": metadata.cluster_id, "group_id": group_id, "topics": settings["topics"],
                        "engine": engine, "model_sha256": model.sha256, "wire_version": 1}
            store = RecoveryStore(ledger_path, identity)
            producer = producer if producer is not None else kafka_producer(settings)
            topic = settings["topics"]["raw"]
            window = None
            if offset_window is not None:
                specification = json.loads(Path(offset_window).read_text()) if not isinstance(offset_window, dict) else offset_window
                window = configure_window(consumer, specification, topic, metadata.cluster_id)
                stats["offset_window"] = specification
            else:
                consumer.subscribe([topic])
            last_input = processing_started = time.monotonic()
            bounded = mode == "batch" or max_messages is not None
            last_progress = time.monotonic()
            while True:
                if progress_callback is not None and time.monotonic() - last_progress >= 5:
                    progress_callback(dict(stats))
                    last_progress = time.monotonic()
                if stop.is_set():
                    stats.update(status="stopped", reason="stop_requested")
                    break
                if max_messages is not None and stats["committed_records"] >= max_messages:
                    stats.update(status="completed", reason="message_limit")
                    break
                if window and all(window["cursors"][p] >= end for p, end in window["ends"].items()):
                    stats.update(status="completed", reason="offset_window_exhausted")
                    break
                if bounded and time.monotonic() - processing_started >= timeout_seconds:
                    stats.update(status="incomplete", reason="batch_timeout")
                    break
                amount = batch_size if max_messages is None else min(batch_size, max_messages - stats["committed_records"])
                messages = consumer.consume(num_messages=amount, timeout=.5)
                accepted = []
                for message in messages:
                    if message.error():
                        if message.error().code() == KafkaError._PARTITION_EOF:
                            continue
                        raise DeliveryError(str(message.error()))
                    if window:
                        p = message.partition()
                        window["cursors"][p] = max(window["cursors"][p], message.offset() + 1)
                        if message.offset() >= window["ends"][p]:
                            continue
                    accepted.append(message)
                if window:
                    # Positions also progress through aborted transaction/control-record gaps.
                    for position in consumer.position([TopicPartition(topic, p) for p in window["ends"]]):
                        if position.offset >= 0:
                            window["cursors"][position.partition] = max(window["cursors"][position.partition], position.offset)
                    done = [p for p in window["ends"] if window["cursors"][p] >= window["ends"][p] and p not in window["paused"]]
                    if done:
                        consumer.pause([TopicPartition(topic, p) for p in done])
                        window["paused"].update(done)
                if accepted:
                    last_input = time.monotonic()
                    stats["received_records"] += len(accepted)
                    jobs = prepare_jobs(accepted, store, model, settings, metadata.cluster_id, stats)
                    deliver_jobs(jobs, store, producer, settings, stats, fault_hook)
                    # Every output is either confirmed now or already recorded as sent.
                    commit_inputs(consumer, accepted, stats)
                    LOG.info("COMMITTED input=%s predictions=%s deadletters=%s duplicates=%s offsets=%s",
                        stats["committed_records"], stats["predictions_published"], stats["deadletters_published"],
                        stats["deduplicated"], stats["committed_offsets"])
                elif bounded and (not window or not all(window["cursors"][p] >= end for p, end in window["ends"].items())) and time.monotonic() - last_input >= idle_timeout:
                    stats.update(status="incomplete", reason="idle_timeout")
                    break
            LOG.info("%s group=%s input=%s predictions=%s deadletters=%s deduplicated=%s", stats["status"].upper(), group_id,
                     stats["committed_records"], stats["predictions_published"], stats["deadletters_published"], stats["deduplicated"])
        return stats
    except BaseException as error:
        stats.update(status="failed", error=str(error))
        LOG.exception("Consumption failed; no unacknowledged output is covered by an input commit")
        raise
    finally:
        if store is not None:
            store.close()
        if consumer is not None:
            consumer.close()  # auto-commit is disabled, including on close.
        if model is not None:
            model.close()
        stats.update(finished_at=utc_now(), elapsed_seconds=time.monotonic() - began)
        atomic_json(directory / "summary.json", stats)
        LOG.removeHandler(handler)
        handler.close()
