"""Retryable local Kafka DAG tasks, with frozen inputs and managed streaming."""
import csv
import hashlib
import json
import logging
import os
import shutil
import signal
import sqlite3
import threading
from contextlib import closing
from pathlib import Path
from uuid import uuid4

import yaml

from opsyntra.kafka.config import atomic_json, load_kafka_config
from opsyntra.kafka.events import CustomerSource, customer_rows, make_event, utc_now
from opsyntra.kafka.publishing import checkpoint_lock, run_producer

LOG = logging.getLogger(__name__)


def run_directory(backend, kind, run_id):
    if kind not in ("batch", "streaming") or not run_id:
        raise ValueError("A workflow kind and nonempty Airflow run ID are required")
    return Path(backend) / "artifacts/kafka/airflow" / kind / hashlib.sha256(run_id.encode()).hexdigest()[:20]


def read_json(path):
    return json.loads(Path(path).read_text(encoding="utf-8"))


def snapshot_file(source, destination):
    """Atomic copy; compare the source before and after copying."""
    from opsyntra.orchestration.models import model_digest
    source, destination = Path(source), Path(destination)
    expected = model_digest(source)
    if destination.exists():
        if model_digest(destination) != expected:
            raise ValueError("Existing frozen input differs; use a new DAG run")
        return expected
    temporary = destination.with_name(destination.name + ".tmp-" + uuid4().hex)
    destination.parent.mkdir(parents=True, exist_ok=True)
    if source.is_dir():
        shutil.copytree(source, temporary)
    else:
        shutil.copyfile(source, temporary)
    if model_digest(source) != expected or model_digest(temporary) != expected:
        raise ValueError("Input changed while freezing the Kafka workflow")
    temporary.replace(destination)
    return expected


class KafkaWorkflow:
    def __init__(self, backend, kind, run_id, engine="sklearn", duration=60, demo_events=0, events_per_sec=5):
        if engine not in ("sklearn", "spark"):
            raise ValueError("Unknown engine")
        if type(duration) is not int or not 5 <= duration <= 3600:
            raise ValueError("Streaming duration must be 5-3600 seconds")
        if type(demo_events) is not int or not 0 <= demo_events <= 10000:
            raise ValueError("demo_events must be 0-10000")
        if type(events_per_sec) not in (int, float) or not 0 < events_per_sec <= 1000:
            raise ValueError("events_per_sec must be 0-1000")
        if demo_events and demo_events / events_per_sec + 3 > duration:
            raise ValueError("Allow enough streaming duration for the demo events and delivery")
        self.backend = Path(backend).resolve()
        self.kind, self.run_id, self.engine = kind, run_id, engine
        self.duration, self.demo_events, self.events_per_sec = duration, demo_events, events_per_sec
        self.run = run_directory(self.backend, kind, run_id)
        self.run.mkdir(parents=True, exist_ok=True)
        self.manifest = self.run / "manifest.json"
        self.config = self.run / "config.yaml"

    @property
    def group(self):
        return f"opsyntra-airflow-{self.kind}-{self.engine}-" + self.run.name

    def health_checks(self):
        from tools.services.kafka_topics import ensure_topics
        from tools.services.kafka_window import capture_window
        # Airflow retries reuse the original source, model and lower offsets.
        with checkpoint_lock(self.manifest):
            if self.manifest.exists():
                settings, manifest = self.settings()
                if ensure_topics(settings)["cluster_id"] != manifest["cluster_id"]:
                    raise ValueError("Broker cluster changed during workflow recovery")
                return manifest
            source_config = Path(os.environ.get("OPSYNTRA_CONFIG", self.backend / "config.yaml"))
            settings = load_kafka_config(source_config)
            broker = ensure_topics(settings)
            source = CustomerSource.inspect(settings["source"])
            source_path = self.run / "customers.csv"
            snapshot_file(source.path, source_path)
            if self.engine == "sklearn":
                model = settings["model_config"]["sklearn_model"]
                saved_model = self.run / "model/best_pipeline.joblib"
            else:
                pointer = read_json(settings["model_config"]["spark_pointer"])
                model = self.backend / pointer["pipeline_path"]
                saved_model = self.run / "model/spark_pipeline"
            model_sha = snapshot_file(model, saved_model)
            full = yaml.safe_load(source_config.read_text(encoding="utf-8"))
            full["data"]["raw_path"] = str(source_path)
            full["training"]["artifact_dir"] = str(saved_model.parent)
            full["spark"]["artifact_dir"] = str(settings["model_config"]["spark_pointer"].parent)
            full["kafka"]["bootstrap_servers"] = settings["bootstrap_servers"]
            full["kafka"].update(checkpoint=str(self.run / "producer_checkpoint.json"), report_dir=str(self.run / "producer_runs"))
            full["kafka"]["consumer"].update(state_dir=str(self.run / "recovery"), report_dir=str(self.run / "consumer_runs"),
                                             batch_timeout_seconds=1200, idle_timeout_seconds=30)
            self.config.write_text(yaml.safe_dump(full), encoding="utf-8")
            from opsyntra.orchestration.models import model_digest
            manifest = {"run_id": self.run_id, "kind": self.kind, "engine": self.engine,
                        "group_id": self.group, "created_at": utc_now(), "cluster_id": broker["cluster_id"],
                        "broker": broker, "source_sha256": source.sha256, "source_rows": source.rows,
                        "model_path": str(saved_model), "model_sha256": model_sha,
                        "config_sha256": model_digest(self.config), "start_window": capture_window(settings),
                        "duration_seconds": self.duration, "demo_events": self.demo_events, "events_per_sec": self.events_per_sec}
            atomic_json(self.manifest, manifest)
            LOG.info("HEALTHY: cluster=%s group=%s rows=%s model_sha256=%s", broker["cluster_id"], self.group, source.rows, model_sha)
            return manifest

    def settings(self):
        from opsyntra.orchestration.models import model_digest
        manifest = read_json(self.manifest)
        if (manifest["engine"], manifest["kind"], manifest["run_id"]) != (self.engine, self.kind, self.run_id):
            raise ValueError("Workflow identity changed")
        if model_digest(self.config) != manifest["config_sha256"]:
            raise ValueError("Frozen workflow configuration changed")
        settings = load_kafka_config(self.config)
        # Environment overrides must not silently change a retry's broker.
        settings["bootstrap_servers"] = manifest["broker"]["bootstrap_servers"]
        if CustomerSource.inspect(settings["source"]).sha256 != manifest["source_sha256"]:
            raise ValueError("Frozen workflow data changed")
        if model_digest(manifest["model_path"]) != manifest["model_sha256"]:
            raise ValueError("Frozen workflow model changed")
        return settings, manifest

    def publish_batch(self):
        from tools.services.kafka_window import capture_window
        settings, manifest = self.settings()
        result_path = self.run / "producer_result.json"
        if result_path.exists():
            return read_json(result_path)
        checkpoint = settings["checkpoint"]
        result = run_producer(settings, mode="batch", checkpoint=checkpoint, resume=checkpoint.exists(), exclude_churn=True)
        if result["status"] != "completed":
            raise ValueError("Batch publisher did not finish the complete CSV")
        after = capture_window(settings)
        before = manifest["start_window"]
        if after["cluster_id"] != before["cluster_id"] or after["partitions"].keys() != before["partitions"].keys():
            raise ValueError("Cluster or partition layout changed during publication")
        window = {"cluster_id": after["cluster_id"], "topic": after["topic"], "partitions": {
            p: {"start": before["partitions"][p]["end"], "end": after["partitions"][p]["end"]} for p in after["partitions"]}}
        atomic_json(self.run / "window.json", window)
        atomic_json(result_path, result)
        return result

    def predict_batch(self):
        from opsyntra.kafka.consumption import run_consumer
        settings, manifest = self.settings()
        path = self.run / "consumer_result.json"
        if path.exists():
            return read_json(path)
        result = run_consumer(settings, mode="batch", engine=self.engine, group_id=self.group,
                              model_path=manifest["model_path"], offset_window=self.run / "window.json")
        if result["status"] != "completed":
            raise ValueError("Batch consumer did not exhaust its fixed window: " + result["reason"])
        atomic_json(path, result)
        return result

    def publish_samples(self):
        settings, manifest = self.settings()
        path = self.run / "samples_result.json"
        if path.exists():
            return read_json(path)
        if manifest["demo_events"]:
            result = run_producer(settings, mode="streaming", max_events=manifest["demo_events"],
                                  events_per_sec=manifest["events_per_sec"], exclude_churn=True)
        else:
            result = {"status": "skipped", "reason": "Use a separate producer for live traffic", "acknowledged": 0}
        atomic_json(path, result)
        return result

    def seed_stream_offsets(self, settings, manifest):
        """New run starts at captured high watermarks; a retry preserves commits."""
        from confluent_kafka import Consumer, TopicPartition
        client = Consumer({"bootstrap.servers": settings["bootstrap_servers"], "group.id": self.group,
                           "enable.auto.commit": False, "enable.auto.offset.store": False})
        try:
            metadata = client.list_topics(timeout=10)
            if metadata.cluster_id != manifest["cluster_id"]:
                raise ValueError("Streaming broker cluster changed")
            floors = manifest["start_window"]["partitions"]
            topic = settings["topics"]["raw"]
            committed = client.committed([TopicPartition(topic, int(p)) for p in floors], timeout=10)
            missing = [TopicPartition(topic, p.partition, floors[str(p.partition)]["end"]) for p in committed if p.offset < 0]
            if missing:
                result = client.commit(offsets=missing, asynchronous=False)
                if result is None or any(p.error for p in result):
                    raise ValueError("Could not initialize new streaming group offsets")
                LOG.info("New streaming group starts at captured high watermarks; earlier history is excluded")
        finally:
            client.close()

    def start_and_monitor_consumer(self):
        from opsyntra.kafka.consumption import run_consumer
        settings, manifest = self.settings()
        if (self.run / "consumer_result.json").exists():
            return read_json(self.run / "consumer_result.json")
        self.seed_stream_offsets(settings, manifest)
        stop = threading.Event()
        stop_path = self.run / "stop.requested"
        timer = None
        previous_handlers = {}
        def request_stop(signum=None, frame=None):
            LOG.info("Graceful streaming stop requested; finish acknowledged outputs and input commits")
            stop.set()
        def progress(stats):
            nonlocal timer
            if timer is None:
                timer = threading.Timer(manifest["duration_seconds"], request_stop)
                timer.daemon = True
                timer.start()
            atomic_json(self.run / "consumer_heartbeat.json", {**stats, "heartbeat_at": utc_now(), "stop_file": str(stop_path)})
            if stop_path.exists():
                request_stop()
            LOG.info("STREAMING heartbeat: input=%s predictions=%s duplicates=%s", stats["committed_records"], stats["predictions_published"], stats["deduplicated"])
        for signum in (signal.SIGTERM, signal.SIGINT):
            previous_handlers[signum] = signal.signal(signum, request_stop)
        try:
            result = run_consumer(settings, mode="streaming", engine=self.engine, group_id=self.group,
                                  model_path=manifest["model_path"], stop=stop, progress_callback=progress)
            atomic_json(self.run / "consumer_result.json", result)
            atomic_json(self.run / "consumer_heartbeat.json", {**result, "heartbeat_at": utc_now(), "stop_file": str(stop_path)})
            if result["status"] != "stopped":
                raise ValueError("Streaming consumer did not stop gracefully")
            return result
        finally:
            if timer is not None:
                timer.cancel()
            for signum, handler in previous_handlers.items():
                signal.signal(signum, handler)

    def summarize(self):
        settings, manifest = self.settings()
        result = read_json(self.run / "consumer_result.json")
        # Query the durable group ledger, including confirmed records from retries.
        scope = hashlib.sha256((manifest["cluster_id"] + ":" + self.group).encode()).hexdigest()[:24]
        database = settings["consumer"]["state_dir"] / (scope + ".sqlite3")
        with closing(sqlite3.connect(database.as_uri() + "?mode=ro", uri=True)) as connection:
            predictions = [json.loads(row[0]) for row in connection.execute(
                "SELECT payload FROM outbox WHERE state='sent' AND topic=? ORDER BY record_id", (settings["topics"]["predictions"],))]
            deadletters = connection.execute("SELECT count(*) FROM outbox WHERE state='sent' AND topic=?", (settings["topics"]["deadletter"],)).fetchone()[0]
        if self.kind == "batch":
            checkpoint = read_json(settings["checkpoint"])
            source = CustomerSource.inspect(settings["source"])
            expected = {make_event(row, run_id=checkpoint["run_id"], source=source, row_index=i, sequence=i,
                                   mode="batch", started_at=checkpoint["started_at"])["event_id"] for i, row in customer_rows(source.path)}
            predictions = [p for p in predictions if p["event_id"] in expected]
            if {p["event_id"] for p in predictions} != expected:
                raise ValueError("Missing confirmed predictions for this batch's customer events")
        elif manifest["demo_events"]:
            from uuid import UUID, uuid5
            producer = read_json(self.run / "samples_result.json")
            expected = {str(uuid5(UUID(producer["run_id"]), f"streaming:{i}")) for i in range(manifest["demo_events"])}
            if not expected <= {p["event_id"] for p in predictions}:
                raise ValueError("Streaming demo did not predict all published events; increase duration")
        output = self.run / "predictions.csv"
        temporary = output.with_suffix(".tmp")
        columns = ["customerID", "churn_probability", "prediction", "event_ts", "processed_ts", "event_id", "model_version", "prediction_id", "engine", "schema_version"]
        with temporary.open("w", newline="", encoding="utf-8") as stream:
            writer = csv.DictWriter(stream, fieldnames=columns)
            writer.writeheader()
            writer.writerows(predictions)
        temporary.replace(output)
        report = {"status": "PASS", "kind": self.kind, "dag_id": "opsyntra_kafka_" + self.kind,
                  "run_id": self.run_id, "group_id": self.group, "engine": self.engine,
                  "completed_at": utc_now(), "model_sha256": manifest["model_sha256"], "model_version": result["model_version"],
                  "predictions": len(predictions), "deadletters_in_window": deadletters,
                  "consumer": result, "source_sha256": manifest["source_sha256"],
                  "predictions_csv": str(output), "run_directory": str(self.run),
                  "delivery_semantics": "at-least-once; durable group ledger and stable prediction IDs; no exactly-once claim"}
        atomic_json(self.run / "summary.json", report)
        atomic_json(self.backend / "artifacts/kafka/airflow" / f"latest_success_{self.kind}.json", report)
        LOG.info("SUCCESS: %s Kafka workflow; confirmed predictions=%s CSV=%s", self.kind, len(predictions), output)
        return report
