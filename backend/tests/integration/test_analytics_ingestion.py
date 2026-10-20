"""Real PostgreSQL transaction, quarantine and late-event aggregation checks."""
import json
import os
import unittest
import importlib.util
from types import SimpleNamespace
from uuid import uuid4

AVAILABLE = importlib.util.find_spec("psycopg") is not None and importlib.util.find_spec("confluent_kafka") is not None
if AVAILABLE:
    import psycopg
    from opsyntra.analytics.consumer import Options, run
    from opsyntra.analytics.ingestion import process_batch

from opsyntra.analytics.contracts import ProvenanceConflict
from tests.analytics_helpers import prediction_event, raw_event
from tests.integration import test_analytics_storage as storage_tests

TOPICS = {"raw": "analytics-test.raw", "predictions": "analytics-test.predictions", "deadletter": "analytics-test.deadletter"}


class Message:
    def __init__(self, value, kind="raw", offset=0, key=None):
        self.payload = value if isinstance(value, bytes) or value is None else json.dumps(value).encode()
        self.message_key = key if isinstance(key, bytes) else (key or (value.get("customerID") if isinstance(value, dict) else "invalid")).encode()
        self.kind, self.index = kind, offset
    def topic(self): return TOPICS[self.kind]
    def partition(self): return 0
    def offset(self): return self.index
    def key(self): return self.message_key
    def value(self): return self.payload
    def error(self): return None


@unittest.skipUnless(AVAILABLE and os.environ.get("OPSYNTRA_RUN_ANALYTICS_TESTS") == "1", "Requires analytics-consumer dependencies and separate PostgreSQL test database")
class AnalyticsIngestionTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls): storage_tests.AnalyticsStorageTests.setUpClass.__func__(cls)
    def setUp(self): storage_tests.AnalyticsStorageTests.setUp(self)

    def process(self, messages, **kwargs): return process_batch(self.write, messages, TOPICS, self.cluster, **kwargs)
    def count_raw(self, raw):
        return self.read.execute("SELECT count(*) AS n FROM analytics.raw_customer_events WHERE event_id=%s", (raw["event_id"],)).fetchone()["n"]
    def errors(self):
        return self.read.execute("SELECT * FROM analytics.ingestion_errors WHERE cluster_id=%s", (self.cluster,)).fetchall()

    def test_malformed_tombstones_oversized_and_conflicts_are_durable_rejections(self):
        raw = raw_event()
        messages = [Message(raw, offset=0), Message({**raw, "Contract": "Two year"}, offset=1),
                    Message(b"not-json", offset=2), Message(b"\xff", offset=3), Message(None, offset=4),
                    Message(b'{"schema_version":1,"schema_version":1}', offset=5),
                    Message(b"x" * 70000, offset=6)]
        result = self.process(messages, max_event_bytes=1024)
        self.assertEqual((result["raw_inserted"], result["rejected"], result["event_conflicts"]), (1, 6, 1))
        self.assertEqual(len(self.errors()), 6)
        self.assertTrue(any(row["raw_value_truncated"] for row in self.errors()))
        replay = self.process(messages, max_event_bytes=1024)
        self.assertEqual((replay["raw_inserted"], replay["quarantine_duplicates"]), (0, 6))

    def test_before_commit_failure_rolls_back_facts_and_quarantine(self):
        raw = raw_event()
        def fail(stage):
            if stage == "before_db_commit": raise RuntimeError("crash")
        with self.assertRaises(RuntimeError): self.process([Message(raw), Message(b"invalid", offset=1)], fault_hook=fail)
        self.assertEqual(self.count_raw(raw), 0)
        self.assertFalse(self.errors())

    def test_changed_quarantined_or_accepted_coordinates_stop_without_new_facts(self):
        self.process([Message(b"invalid")])
        with self.assertRaises(ProvenanceConflict): self.process([Message(raw_event())])
        raw = raw_event()
        self.process([Message(raw, offset=2)])
        with self.assertRaises(ProvenanceConflict): self.process([Message(b"invalid", offset=2)])
        self.assertEqual(len(self.errors()), 1)

    def test_late_raw_and_prediction_recompute_old_time_buckets(self):
        model = "late-" + uuid4().hex
        a = raw_event()
        p = prediction_event(a, model)
        self.process([Message(p, "predictions")])
        def metric():
            return self.read.execute("SELECT * FROM analytics.prediction_hourly WHERE model_version=%s AND engine='sklearn'", (model,)).fetchone()
        self.assertEqual((metric()["prediction_volume"], metric()["matched_raw_predictions"]), (1, 0))
        b = raw_event(a["customerID"], "Two year")
        q = prediction_event(b, model)
        q.update(churn_probability=.1, prediction="No")
        self.process([Message(a, offset=1), Message(b, offset=2), Message(q, "predictions", offset=1),
                      Message(prediction_event(a, model, "spark"), "predictions", offset=2)])
        self.assertEqual((metric()["prediction_volume"], metric()["matched_raw_predictions"], metric()["unique_customers"]), (2, 2, 1))
        self.assertAlmostEqual(metric()["predicted_churn_share"], .5)
        self.process([Message(q, "predictions", offset=3)])
        self.assertEqual(metric()["prediction_volume"], 2)
        daily = self.read.execute("SELECT prediction_volume FROM analytics.prediction_daily WHERE model_version=%s AND engine='sklearn'", (model,)).fetchone()
        self.assertEqual(daily["prediction_volume"], 2)
        segments = self.read.execute("SELECT contract,prediction_volume FROM analytics.prediction_hourly_segments WHERE model_version=%s AND engine='sklearn' ORDER BY contract", (model,)).fetchall()
        self.assertEqual([row["contract"] for row in segments], ["Month-to-month", "Two year"])

    def client(self, messages, on_commit=lambda: None):
        owner = self
        class Client:
            def __init__(self, config): self.config, self.sent, self.commits, self.closed = config, False, [], False
            def list_topics(self, **kwargs):
                return SimpleNamespace(cluster_id=owner.cluster, topics={t: SimpleNamespace(error=None) for t in TOPICS.values()})
            def subscribe(self, topics, **kwargs): pass
            def consume(self, **kwargs):
                if self.sent: return []
                self.sent = True
                return messages
            def commit(self, offsets, asynchronous):
                on_commit()
                self.commits.extend(offsets)
                return [SimpleNamespace(error=None) for p in offsets]
            def close(self): self.closed = True
        return Client

    def run_client(self, messages, **kwargs):
        factory = self.client(messages, kwargs.pop("on_commit", lambda: None))
        clients = []
        def make(config):
            client = factory(config)
            clients.append(client)
            return client
        result = run({"bootstrap_servers": "fixture", "topics": TOPICS}, lambda: self.config.connect("write"),
                     options=Options(mode="batch", max_messages=len(messages), retries=1, retry_delay=.001), consumer_factory=make, **kwargs)
        return result, clients[0]

    def test_input_commit_observes_durable_database_rows_and_auto_commit_disabled(self):
        raw = raw_event()
        def assert_durable(): self.assertEqual(self.count_raw(raw), 1)
        result, client = self.run_client([Message(raw)], on_commit=assert_durable)
        self.assertEqual(result["status"], "complete")
        self.assertFalse(client.config["enable.auto.commit"])
        self.assertFalse(client.config["enable.auto.offset.store"])
        self.assertTrue(client.closed)

    def test_database_failure_never_commits_inputs(self):
        raw = raw_event()
        commits = []
        def fail(stage):
            if stage == "before_db_commit": raise psycopg.OperationalError("database lost")
        with self.assertRaises(psycopg.OperationalError):
            self.run_client([Message(raw)], fault_hook=fail, on_commit=lambda: commits.append(1))
        self.assertFalse(commits)
        self.assertEqual(self.count_raw(raw), 0)

    def test_ambiguous_database_commit_retries_idempotently_before_input_commit(self):
        raw = raw_event()
        calls = []
        def once(stage):
            if stage == "after_db_commit" and not calls:
                calls.append(1)
                raise psycopg.OperationalError("lost commit response")
        result, _ = self.run_client([Message(raw)], fault_hook=once)
        self.assertEqual(result["database_retries"], 1)
        self.assertEqual(self.count_raw(raw), 1)
        self.assertEqual(result["logical_duplicates"], 1)

    def test_graceful_stop_finishes_current_database_and_input_commit(self):
        import threading
        stop = threading.Event()
        raw = raw_event()
        def halt(stage):
            if stage == "before_db_commit": stop.set()
        result, client = self.run_client([Message(raw)], stop=stop, fault_hook=halt)
        self.assertEqual(result["status"], "stopped")
        self.assertEqual(len(client.commits), 1)
