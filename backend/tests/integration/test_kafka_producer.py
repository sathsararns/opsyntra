"""Failure/restart publishing checks; broker-independent delivery simulation."""
import json
import tempfile
import threading
import unittest
from pathlib import Path

from opsyntra.kafka_config import load_kafka_config
from opsyntra.kafka_events import CustomerSource, validate_event
from opsyntra.kafka_publishing import DeliveryError, checkpoint_lock, run_producer
from tests.helpers import sample_data

ROOT = Path(__file__).resolve().parents[2]


class Message:
    def __init__(self, offset): self.index = offset
    def partition(self): return 0
    def offset(self): return self.index


class DeliveryBroker:
    """Callbacks may arrive out of order, fail, or remain unconfirmed."""
    def __init__(self, fail_row=None, unconfirmed=False, full_once=False):
        self.pending, self.records = [], []
        self.fail_row, self.unconfirmed, self.full_once = fail_row, unconfirmed, full_once
        self.polls = 0

    def produce(self, topic, *, key, value, on_delivery):
        if self.full_once:
            self.full_once = False
            raise BufferError("Local queue full")
        self.pending.append((topic, key, json.loads(value), on_delivery))

    def poll(self, timeout): self.polls += 1

    def flush(self, timeout):
        if self.unconfirmed:
            return len(self.pending)
        for topic, key, event, callback in reversed(self.pending):
            if event["source_row"] == self.fail_row:
                callback("simulated delivery failure", Message(-1))
            else:
                self.records.append((key.decode(), event))
                callback(None, Message(len(self.records) - 1))
        self.pending.clear()
        return 0


class KafkaProducerTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.source = self.root / "customers.csv"
        sample_data(12).to_csv(self.source, index=False)
        self.settings = load_kafka_config(ROOT / "config.yaml")
        self.settings.update(source=self.source, checkpoint=self.root / "checkpoint.json", report_dir=self.root / "runs")

    def run_batch(self, broker, **kwargs):
        return run_producer(self.settings, mode="batch", batch_size=3, producer=broker, cluster_id="fixture-cluster", **kwargs)

    def test_chunk_resume_and_completed_rerun_publish_each_row_once(self):
        first, second, third = DeliveryBroker(), DeliveryBroker(), DeliveryBroker()
        paused = self.run_batch(first, max_events=7)
        self.assertEqual(paused["status"], "paused")
        self.assertEqual(paused["next_row"], 7)
        finished = self.run_batch(second, resume=True)
        self.assertEqual(finished["status"], "completed")
        self.assertEqual(finished["run_id"], paused["run_id"])
        self.assertEqual(finished["acknowledged"], 5)
        self.assertEqual(self.run_batch(third, resume=True)["acknowledged"], 0)
        events = first.records + second.records
        self.assertEqual(len({e["event_id"] for _, e in events}), 12)
        self.assertEqual({e["source_row"] for _, e in events}, set(range(12)))
        for key, event in events:
            validate_event(event, key)
        self.assertIsNone(next(e for _, e in events if e["source_row"] == 0)["TotalCharges"])

    def test_partial_failed_chunk_does_not_advance_cursor_and_replays_stable_events(self):
        failed = DeliveryBroker(fail_row=4)
        with self.assertRaisesRegex(DeliveryError, "delivery failed"):
            self.run_batch(failed)
        checkpoint = json.loads(self.settings["checkpoint"].read_text())
        self.assertEqual(checkpoint["next_row"], 3)
        resumed = DeliveryBroker()
        self.run_batch(resumed, resume=True)
        replayed = {event["source_row"]: event for _, event in resumed.records}
        for _, event in failed.records:
            if event["source_row"] >= 3:
                self.assertEqual(replayed[event["source_row"]], event)
        combined = {e["source_row"] for _, e in failed.records + resumed.records}
        self.assertEqual(combined, set(range(12)))

    def test_unconfirmed_deliveries_never_advance_checkpoint(self):
        with self.assertRaisesRegex(DeliveryError, "Unconfirmed"):
            self.run_batch(DeliveryBroker(unconfirmed=True))
        self.assertEqual(json.loads(self.settings["checkpoint"].read_text())["next_row"], 0)

    def test_queue_backpressure_is_serviced_and_all_events_confirmed(self):
        broker = DeliveryBroker(full_once=True)
        result = self.run_batch(broker)
        self.assertEqual(result["acknowledged"], 12)
        self.assertGreater(broker.polls, 12)

    def test_resume_rejects_changed_data_cluster_and_label_policy(self):
        self.run_batch(DeliveryBroker(), max_events=3)
        original = self.settings["checkpoint"].read_bytes()
        for overrides in ({"cluster_id": "different-cluster"}, {"exclude_churn": True}):
            broker = DeliveryBroker()
            arguments = dict(mode="batch", producer=broker, resume=True, cluster_id="fixture-cluster")
            arguments.update(overrides)
            with self.assertRaisesRegex(ValueError, "does not match"):
                run_producer(self.settings, **arguments)
            self.assertEqual(broker.records, [])
        self.source.write_text(self.source.read_text().replace("29.85", "30.85"))
        with self.assertRaisesRegex(ValueError, "does not match"):
            self.run_batch(DeliveryBroker(), resume=True)
        self.assertEqual(self.settings["checkpoint"].read_bytes(), original)

    def test_invalid_late_csv_row_is_rejected_before_any_publishing(self):
        frame = sample_data(12)
        frame.loc[11, "MonthlyCharges"] = "Infinity"
        frame.to_csv(self.source, index=False)
        broker = DeliveryBroker()
        with self.assertRaisesRegex(ValueError, "finite"):
            self.run_batch(broker)
        self.assertEqual(broker.records, [])
        self.assertFalse(self.settings["checkpoint"].exists())

    def test_streaming_repeated_customer_has_distinct_events_and_optional_labels(self):
        sample_data(1).drop(columns="Churn").to_csv(self.source, index=False)
        broker = DeliveryBroker()
        result = run_producer(self.settings, mode="streaming", events_per_sec=10000,
            max_events=5, exclude_churn=True, producer=broker, cluster_id="fixture-cluster")
        self.assertEqual(result["acknowledged"], 5)
        self.assertEqual(len({e["event_id"] for _, e in broker.records}), 5)
        for key, event in broker.records:
            self.assertNotIn("Churn", event)
            validate_event(event, key)
        self.assertFalse(self.settings["checkpoint"].exists())

    def test_active_checkpoint_is_exclusive_and_released_after_exit(self):
        with checkpoint_lock(self.settings["checkpoint"]):
            with self.assertRaisesRegex(ValueError, "Another producer"):
                with checkpoint_lock(self.settings["checkpoint"]):
                    self.fail("Acquired a checkpoint twice")
        with checkpoint_lock(self.settings["checkpoint"]):
            pass

    def test_stop_keeps_resumable_cursor_and_existing_checkpoint_needs_resume(self):
        stop = threading.Event()
        stop.set()
        result = self.run_batch(DeliveryBroker(), stop=stop)
        self.assertEqual((result["status"], result["next_row"]), ("paused", 0))
        with self.assertRaisesRegex(ValueError, "already exists"):
            self.run_batch(DeliveryBroker())
        self.assertEqual(self.run_batch(DeliveryBroker(), resume=True)["next_row"], 12)

    def test_duplicate_ids_and_invalid_rates_fail_before_publishing(self):
        frame = sample_data(12)
        frame.loc[11, "customerID"] = frame.loc[0, "customerID"]
        frame.to_csv(self.source, index=False)
        with self.assertRaisesRegex(ValueError, "Duplicate"):
            CustomerSource.inspect(self.source)
        for rate in (0, -1, float("nan"), float("inf")):
            with self.assertRaisesRegex(ValueError, "events_per_sec"):
                run_producer(self.settings, mode="streaming", events_per_sec=rate)
