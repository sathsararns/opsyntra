"""Bonus DAG retries, immutable inputs, fixed windows and managed shutdown."""
import json
import signal
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import joblib
import pandas as pd
import yaml

from opsyntra.data_validation import FEATURE_COLUMNS
from opsyntra.kafka_config import atomic_json
from opsyntra.kafka_consumption import run_consumer
from opsyntra.kafka_events import CustomerSource, customer_rows, make_event, utc_now
from opsyntra.kafka_workflow import KafkaWorkflow, read_json, run_directory
from opsyntra.model_building import build_model_pipeline
from tests.helpers import sample_data
from tests.integration.test_kafka_consumer import Broker, Input, Output, Record

ROOT = Path(__file__).resolve().parents[2]


class KafkaWorkflowTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        data = sample_data(20)
        data.to_csv(self.root / "customers.csv", index=False)
        model = build_model_pipeline("logistic_regression", jobs=1).fit(
            data[list(FEATURE_COLUMNS)], data.Churn.map({"Yes": 1, "No": 0}))
        model.opsyntra_metadata_ = {"model_version": "workflow-fixture"}
        (self.root / "models").mkdir()
        joblib.dump(model, self.root / "models/best_pipeline.joblib")
        full = yaml.safe_load((ROOT / "config.yaml").read_text())
        full["data"]["raw_path"] = "customers.csv"
        full["training"]["artifact_dir"] = "models"
        (self.root / "config.yaml").write_text(yaml.safe_dump(full), encoding="utf-8")
        self.workflow = KafkaWorkflow(self.root, "batch", "manual:with/unsafe/path", duration=5)
        self.window = {"cluster_id": "fixture", "topic": "telco.raw.customers", "partitions": {"0": {"start": 0, "end": 0}}}
        self.health = patch("tools.services.kafka_topics.ensure_topics", return_value={"cluster_id": "fixture", "bootstrap_servers": "127.0.0.1:9092"})
        self.capture = patch("tools.services.kafka_window.capture_window", return_value=self.window)
        self.health.start()
        self.capture.start()
        self.addCleanup(self.health.stop)
        self.addCleanup(self.capture.stop)

    def test_health_retry_reuses_snapshot_and_rejects_tampering(self):
        original = self.workflow.health_checks()
        # Changes to live data/model must not alter a retry's frozen input.
        (self.root / "customers.csv").write_text("changed")
        (self.root / "models/best_pipeline.joblib").write_bytes(b"changed")
        self.assertEqual(self.workflow.health_checks(), original)
        self.assertEqual(self.workflow.run.parent.parent, self.root / "artifacts/kafka/airflow")
        frozen = self.workflow.run / "customers.csv"
        frozen.write_text(frozen.read_text() + "\n")
        with self.assertRaisesRegex(ValueError, "data changed"):
            self.workflow.settings()

    def test_failed_publication_reuses_checkpoint_and_preserves_window_lower_bound(self):
        self.workflow.health_checks()
        checkpoint = self.workflow.run / "producer_checkpoint.json"
        calls = []
        def publisher(settings, **kwargs):
            calls.append(kwargs["resume"])
            if len(calls) == 1:
                atomic_json(checkpoint, {"next_row": 10})
                raise RuntimeError("delivery interrupted")
            return {"status": "completed", "acknowledged": 10}
        with patch("opsyntra.kafka_workflow.run_producer", side_effect=publisher):
            with self.assertRaisesRegex(RuntimeError, "interrupted"):
                self.workflow.publish_batch()
            self.window["partitions"]["0"]["end"] = 20
            self.workflow.publish_batch()
            self.workflow.publish_batch()  # completed task retry publishes nothing
        self.assertEqual(calls, [False, True])
        self.assertEqual(read_json(self.workflow.run / "window.json")["partitions"]["0"], {"start": 0, "end": 20})

    def test_batch_predictions_and_summary_are_complete_and_retry_does_not_append(self):
        self.workflow.health_checks()
        settings, manifest = self.workflow.settings()
        source = CustomerSource.inspect(settings["source"])
        from uuid import uuid4
        checkpoint = {"run_id": str(uuid4()), "started_at": utc_now()}
        atomic_json(settings["checkpoint"], checkpoint)
        events = [make_event(row, run_id=checkpoint["run_id"], source=source, row_index=i,
                             sequence=i, mode="batch", started_at=checkpoint["started_at"]) for i, row in customer_rows(source.path)]
        records = [Record(json.dumps(e).encode(), i, e["customerID"].encode()) for i, e in enumerate(events)]
        broker = Broker(records, settings)
        window = {**self.window, "partitions": {"0": {"start": 0, "end": 20}}}
        atomic_json(self.workflow.run / "window.json", window)
        def consume(configuration, **kwargs):
            return run_consumer(configuration, **kwargs, consumer=Input(broker), producer=Output(broker))
        with patch("opsyntra.kafka_consumption.run_consumer", side_effect=consume):
            self.workflow.predict_batch()
            self.workflow.predict_batch()
        report = self.workflow.summarize()
        self.assertEqual(report["predictions"], 20)
        self.assertEqual(len(broker.outputs), 20)
        before = (self.workflow.run / "predictions.csv").read_bytes()
        self.workflow.summarize()
        self.assertEqual(before, (self.workflow.run / "predictions.csv").read_bytes())
        self.assertEqual(len(pd.read_csv(self.workflow.run / "predictions.csv")), 20)
        # An incomplete ledger cannot produce a successful report.
        import sqlite3
        from contextlib import closing
        with closing(sqlite3.connect(report["consumer"]["recovery_database"])) as db:
            db.execute("DELETE FROM outbox WHERE record_id=(SELECT record_id FROM outbox LIMIT 1)")
            db.commit()
        with self.assertRaisesRegex(ValueError, "Missing confirmed"):
            self.workflow.summarize()

    def test_streaming_stop_marker_stops_owned_consumer_and_restores_handlers(self):
        workflow = KafkaWorkflow(self.root, "streaming", "stream-session", duration=5)
        workflow.health_checks()
        (workflow.run / "stop.requested").touch()
        handlers = {s: signal.getsignal(s) for s in (signal.SIGTERM, signal.SIGINT)}
        def managed(settings, **kwargs):
            self.assertEqual(kwargs["mode"], "streaming")
            kwargs["progress_callback"]({"committed_records": 0, "predictions_published": 0, "deduplicated": 0})
            self.assertTrue(kwargs["stop"].is_set())
            return {"status": "stopped"}
        with patch.object(workflow, "seed_stream_offsets"), patch("opsyntra.kafka_consumption.run_consumer", side_effect=managed) as consume:
            workflow.start_and_monitor_consumer()
            workflow.start_and_monitor_consumer()
            self.assertEqual(consume.call_count, 1)
        self.assertEqual(handlers, {s: signal.getsignal(s) for s in handlers})
        self.assertTrue((workflow.run / "consumer_heartbeat.json").is_file())

    def test_new_streaming_group_initializes_only_missing_partition_offsets(self):
        from confluent_kafka import TopicPartition
        workflow = KafkaWorkflow(self.root, "streaming", "preserve-existing-offsets", duration=5)
        manifest = workflow.health_checks()
        manifest["start_window"]["partitions"] = {"0": {"start": 0, "end": 30}, "1": {"start": 0, "end": 40}}
        settings, _ = workflow.settings()
        with patch("confluent_kafka.Consumer") as factory:
            client = factory.return_value
            client.list_topics.return_value.cluster_id = "fixture"
            client.committed.return_value = [TopicPartition("telco.raw.customers", 0, 35), TopicPartition("telco.raw.customers", 1, -1001)]
            client.commit.return_value = [TopicPartition("telco.raw.customers", 1, 40)]
            workflow.seed_stream_offsets(settings, manifest)
            offsets = client.commit.call_args.kwargs["offsets"]
            self.assertEqual([(p.partition, p.offset) for p in offsets], [(1, 40)])
            client.close.assert_called_once()

    def test_parameters_and_run_paths_cannot_escape_workflow_root(self):
        self.assertEqual(run_directory(self.root, "batch", "../../evil").parent, self.root / "artifacts/kafka/airflow/batch")
        with self.assertRaises(ValueError):
            KafkaWorkflow(self.root, "streaming", "short", duration=5, demo_events=100, events_per_sec=1)
        with self.assertRaises(ValueError):
            run_directory(self.root, "../escape", "run")
