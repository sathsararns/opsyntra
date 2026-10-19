"""Consumer failures/replays with a durable real SQLite ledger and fake Kafka I/O."""
import json
import sqlite3
import tempfile
import time
import unittest
from contextlib import closing
from pathlib import Path
from types import SimpleNamespace
from uuid import uuid4

import joblib
from confluent_kafka import TopicPartition

from opsyntra.kafka_config import load_kafka_config
from opsyntra.kafka_consumption import run_consumer
from opsyntra.kafka_events import CustomerSource, customer_rows, make_event, utc_now
from opsyntra.kafka_publishing import DeliveryError
from tests.helpers import sample_data

ROOT = Path(__file__).resolve().parents[2]


class Record:
    def __init__(self, value, offset, key=b"TEST-00000", topic="telco.raw.customers", partition=0):
        self.data, self.index, self.message_key, self.name, self.part = value, offset, key, topic, partition
    def value(self): return self.data
    def key(self): return self.message_key
    def topic(self): return self.name
    def partition(self): return self.part
    def offset(self): return self.index
    def error(self): return None


class Broker:
    def __init__(self, records, settings):
        self.records, self.settings, self.outputs, self.commits, self.trace = records, settings, [], {}, []
        self.fail_commit = False
        self.fail_output = None


class Input:
    def __init__(self, broker):
        self.broker, self.cursor, self.closed = broker, 0, False
    def list_topics(self, **kwargs):
        return SimpleNamespace(cluster_id="fixture", topics={t: SimpleNamespace(error=None) for t in self.broker.settings["topics"].values()})
    def subscribe(self, topics): self.cursor = self.broker.commits.get(0, 0)
    def consume(self, num_messages, timeout):
        result = self.broker.records[self.cursor:self.cursor + num_messages]
        self.cursor += len(result)
        if not result: time.sleep(.002)
        return result
    def commit(self, offsets, asynchronous):
        self.broker.trace.append("commit")
        self.assert_ledger_sent()
        if self.broker.fail_commit: raise RuntimeError("simulated commit failure")
        for p in offsets: self.broker.commits[p.partition] = p.offset
        return offsets
    def assert_ledger_sent(self):
        db = next(self.broker.settings["consumer"]["state_dir"].glob("*.sqlite3"))
        with closing(sqlite3.connect(db)) as connection:
            assert not connection.execute("SELECT record_id FROM outbox WHERE state!='sent'").fetchall()
    def committed(self, partitions, timeout):
        return [TopicPartition(p.topic, p.partition, self.broker.commits.get(p.partition, -1001)) for p in partitions]
    def get_watermark_offsets(self, p, timeout): return 0, len(self.broker.records)
    def assign(self, partitions): self.cursor = partitions[0].offset
    def position(self, partitions): return [TopicPartition(p.topic, p.partition, self.cursor) for p in partitions]
    def pause(self, partitions): pass
    def close(self): self.closed = True


class Output:
    def __init__(self, broker): self.broker, self.pending = broker, []
    def produce(self, topic, *, key, value, on_delivery): self.pending.append((topic, key, value, on_delivery))
    def poll(self, timeout): pass
    def flush(self, timeout):
        for topic, key, value, callback in reversed(self.pending):
            if topic == self.broker.fail_output:
                callback("simulated output delivery error", Record(None, -1))
            else:
                self.broker.outputs.append((topic, key, json.loads(value)))
                self.broker.trace.append("ack")
                callback(None, Record(None, len(self.broker.outputs) - 1))
        self.pending.clear()
        return 0


class Model:
    engine, version, sha256 = "sklearn", "fixture-model", "f" * 64
    def __init__(self): self.calls, self.closed = [], False
    def predict(self, events):
        self.calls.append(events)
        return [{"churn_probability": .7, "prediction": "Yes", "model_version": self.version} for _ in events]
    def close(self): self.closed = True


class KafkaConsumerTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        csv = self.root / "source.csv"
        sample_data(4).to_csv(csv, index=False)
        source = CustomerSource.inspect(csv)
        self.events = [make_event(row, run_id=str(uuid4()), source=source, row_index=i,
                                  sequence=i, mode="batch", started_at=utc_now()) for i, row in customer_rows(csv)]
        self.settings = load_kafka_config(ROOT / "config.yaml")
        self.settings["consumer"].update(state_dir=self.root / "state", report_dir=self.root / "runs")

    def record(self, event, offset, **kwargs):
        return Record(json.dumps(event).encode(), offset, event["customerID"].encode(), **kwargs)

    def run_case(self, broker, model=None, **overrides):
        options = dict(mode="batch", max_messages=len(broker.records), model=model or Model(),
                       consumer=Input(broker), producer=Output(broker), idle_timeout=.02, timeout_seconds=2)
        options.update(overrides)
        return run_consumer(self.settings, **options)

    def test_outputs_are_acknowledged_and_ledger_is_durable_before_input_commit(self):
        broker = Broker([self.record(e, i) for i, e in enumerate(self.events)], self.settings)
        model = Model()
        summary = self.run_case(broker, model, batch_size=2)
        self.assertEqual(summary["predictions_published"], 4)
        self.assertEqual(summary["committed_records"], 4)
        self.assertEqual(summary["model_loads"], 1)
        self.assertEqual(len(model.calls), 2)
        self.assertEqual(broker.trace, ["ack", "ack", "commit", "ack", "ack", "commit"])
        for _, key, event in broker.outputs:
            self.assertEqual(key.decode(), event["customerID"])
            self.assertTrue({"customerID", "churn_probability", "prediction", "event_ts", "processed_ts", "event_id", "model_version"} <= set(event))
            self.assertNotIn("Churn", event)

    def test_restart_after_ledger_before_input_commit_suppresses_duplicate_output(self):
        broker = Broker([self.record(self.events[0], 0)], self.settings)
        def fail(stage):
            if stage == "after_ledger_commit": raise RuntimeError("crash after ledger")
        with self.assertRaisesRegex(RuntimeError, "crash"):
            self.run_case(broker, fault_hook=fail)
        self.assertEqual(broker.commits, {})
        self.assertEqual(len(broker.outputs), 1)
        restarted = Model()
        summary = self.run_case(broker, restarted)
        self.assertEqual(summary["deduplicated"], 1)
        self.assertEqual(restarted.calls, [])
        self.assertEqual(len(broker.outputs), 1)
        self.assertEqual(broker.commits, {0: 1})

    def test_output_ack_before_ledger_crash_replays_identical_outbox_payload(self):
        broker = Broker([self.record(self.events[0], 0)], self.settings)
        def fail(stage):
            if stage == "after_output_ack": raise RuntimeError("crash after ack")
        with self.assertRaisesRegex(RuntimeError, "crash"):
            self.run_case(broker, fault_hook=fail)
        first = broker.outputs[0][2]
        restarted = Model()
        self.run_case(broker, restarted)
        self.assertEqual(len(broker.outputs), 2)
        self.assertEqual(first, broker.outputs[1][2])
        self.assertEqual(restarted.calls, [])
        self.assertEqual(len({e["prediction_id"] for _, _, e in broker.outputs}), 1)

    def test_output_and_deadletter_delivery_failure_never_commit_input(self):
        for invalid in (False, True):
            with self.subTest(invalid=invalid):
                settings = dict(self.settings)
                settings["consumer"] = {**self.settings["consumer"], "state_dir": self.root / f"state-{invalid}"}
                record = Record(b"not-json", 0) if invalid else self.record(self.events[0], 0)
                broker = Broker([record], settings)
                broker.fail_output = settings["topics"]["deadletter" if invalid else "predictions"]
                with self.assertRaisesRegex(DeliveryError, "not fully confirmed"):
                    run_consumer(settings, mode="batch", max_messages=1, model=Model(), consumer=Input(broker), producer=Output(broker))
                self.assertEqual(broker.commits, {})
                self.assertEqual(broker.outputs, [])

    def test_commit_failure_restarts_without_republishing_acknowledged_outputs(self):
        broker = Broker([self.record(self.events[0], 0)], self.settings)
        broker.fail_commit = True
        with self.assertRaisesRegex(RuntimeError, "commit failure"):
            self.run_case(broker)
        broker.fail_commit = False
        self.assertEqual(self.run_case(broker)["deduplicated"], 1)
        self.assertEqual(len(broker.outputs), 1)

    def test_duplicate_events_are_suppressed_within_chunk_and_across_restarts(self):
        broker = Broker([self.record(self.events[0], 0), self.record(self.events[0], 1)], self.settings)
        self.assertEqual(self.run_case(broker)["deduplicated"], 1)
        broker.records.append(self.record(self.events[0], 2))
        self.assertEqual(self.run_case(broker, max_messages=1)["deduplicated"], 1)
        self.assertEqual(len(broker.outputs), 1)

    def test_conflicting_event_identity_is_deadlettered(self):
        changed = {**self.events[0], "MonthlyCharges": 999.}
        broker = Broker([self.record(self.events[0], 0), self.record(changed, 1)], self.settings)
        result = self.run_case(broker)
        self.assertEqual((result["predictions_published"], result["deadletters_published"]), (1, 1))

    def test_malformed_records_do_not_block_valid_records(self):
        duplicate = b'{"customerID":"a","customerID":"b"}'
        wrong_key = self.record(self.events[0], 5)
        wrong_key.message_key = b"DIFFERENT"
        records = [Record(b"not-json", 0), Record(b"[]", 1), Record(None, 2), Record(duplicate, 3),
                   Record(b'\xff', 4), wrong_key, Record(b"x" * 300000, 6), self.record(self.events[0], 7)]
        broker = Broker(records, self.settings)
        result = self.run_case(broker)
        self.assertEqual((result["predictions_published"], result["deadletters_published"], result["committed_records"]), (1, 7, 8))
        self.assertTrue(any(payload.get("raw_value_truncated") for _, _, payload in broker.outputs))
        for topic, _, payload in broker.outputs:
            if topic.endswith("deadletter"):
                self.assertIn("raw_value_base64", payload)
                self.assertIn("offset", payload["source"])

    def test_same_group_cannot_silently_switch_models_and_empty_count_is_bounded(self):
        broker = Broker([self.record(self.events[0], 0)], self.settings)
        self.run_case(broker)
        model = Model()
        model.sha256 = "b" * 64
        with self.assertRaisesRegex(ValueError, "different model"):
            self.run_case(broker, model)
        empty = self.run_case(broker, max_messages=1)
        self.assertEqual((empty["status"], empty["reason"]), ("incomplete", "idle_timeout"))

    def test_offset_window_uses_saved_commits_and_completes_without_waiting(self):
        broker = Broker([self.record(e, i) for i, e in enumerate(self.events)], self.settings)
        window = {"cluster_id": "fixture", "topic": self.settings["topics"]["raw"], "partitions": {"0": {"start": 1, "end": 3}}}
        result = self.run_case(broker, max_messages=None, offset_window=window)
        self.assertEqual((result["committed_records"], result["reason"]), (2, "offset_window_exhausted"))
        self.assertEqual(self.run_case(broker, max_messages=None, offset_window=window)["committed_records"], 0)

    def test_actual_pipeline_loads_once_and_matches_direct_unlabelled_predictions(self):
        from unittest.mock import patch
        from opsyntra.data_validation import FEATURE_COLUMNS
        from opsyntra.model_building import build_model_pipeline
        from opsyntra.model_inference import load_pipeline, predict_records
        raw = sample_data(100)
        pipeline = build_model_pipeline("logistic_regression", seed=42, jobs=1)
        pipeline.fit(raw[list(FEATURE_COLUMNS)], raw.Churn.map({"No": 0, "Yes": 1}))
        pipeline.opsyntra_metadata_ = {"model_version": "real-fixture"}
        path = self.root / "model.joblib"
        joblib.dump(pipeline, path)
        self.settings["model_config"]["sklearn_model"] = path
        event = {k: v for k, v in self.events[0].items() if k != "Churn"}
        broker = Broker([self.record(event, i) for i in range(2)], self.settings)
        expected = predict_records(pipeline, [{name: event[name] for name in FEATURE_COLUMNS}]).iloc[0]
        with patch("opsyntra.model_inference.load_pipeline", wraps=load_pipeline) as loader:
            run_consumer(self.settings, mode="batch", max_messages=2, batch_size=1, consumer=Input(broker), producer=Output(broker))
            self.assertEqual(loader.call_count, 1)
        output = broker.outputs[0][2]
        self.assertAlmostEqual(output["churn_probability"], expected.churn_probability, places=12)
        self.assertEqual(output["prediction"], expected.prediction)
