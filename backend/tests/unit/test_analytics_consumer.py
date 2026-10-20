"""Offset discipline without a broker: protocol fakes never stand in for SQL tests."""
import threading
import unittest
import importlib.util
from types import SimpleNamespace

AVAILABLE = importlib.util.find_spec("psycopg") is not None and importlib.util.find_spec("confluent_kafka") is not None
if AVAILABLE:
    from confluent_kafka import TopicPartition
    import psycopg
    from opsyntra.analytics.consumer import Options, assign_retained, commit_offsets, retry, run

from opsyntra.analytics.config import AnalyticsConfig


@unittest.skipUnless(AVAILABLE, "Install requirements/analytics-consumer.txt for analytics client checks")
class AnalyticsConsumerTests(unittest.TestCase):
    def test_runtime_needs_only_write_credentials(self):
        config = AnalyticsConfig.from_env({"OPSYNTRA_ANALYTICS_WRITE_URL": "postgresql://writer:secret@localhost/test"}, required_roles=("write",))
        self.assertEqual(config.read_url, "")
        with self.assertRaises(ValueError): config.connect("migration")

    def test_options_reject_unbounded_batch_bad_limits_and_inference_group(self):
        for values in ({"mode": "batch"}, {"batch_size": 0}, {"batch_size": 1001}, {"retries": 6},
                       {"timeout_seconds": float("nan")}, {"retry_delay": -1}, {"group_id": "opsyntra-churn-sklearn"},
                       {"mode": "batch", "snapshot": True, "max_messages": 5}):
            with self.subTest(values=values), self.assertRaises(ValueError): Options(**values)

    def test_retries_are_bounded_and_stop_interrupts_backoff(self):
        calls = []
        def operation():
            calls.append(1)
            raise psycopg.OperationalError("unavailable")
        options = Options(retries=2, retry_delay=.001)
        with self.assertRaises(psycopg.OperationalError): retry(operation, options, threading.Event(), name="test")
        self.assertEqual(len(calls), 3)
        stop = threading.Event()
        stop.set()
        from opsyntra.analytics.consumer import InterruptedRetry
        with self.assertRaises(InterruptedRetry): retry(operation, options, stop, name="test")

    def test_readiness_failure_does_not_create_kafka_client(self):
        created = []
        def connect(): raise psycopg.OperationalError("unavailable")
        with self.assertRaises(psycopg.OperationalError):
            run({"bootstrap_servers": "unused", "topics": {"raw": "r", "predictions": "p", "deadletter": "d"}},
                connect, options=Options(retries=0), consumer_factory=lambda config: created.append(config))
        self.assertFalse(created)

    def test_commit_is_synchronous_and_checks_partition_errors(self):
        class Client:
            def commit(self, **kwargs):
                self.kwargs = kwargs
                return [SimpleNamespace(error="failed")]
        client = Client()
        with self.assertRaises(RuntimeError): commit_offsets(client, {("topic", 0): 4})
        self.assertFalse(client.kwargs["asynchronous"])
        self.assertEqual(client.kwargs["offsets"][0].offset, 4)

    def test_retention_loss_is_not_reset_to_earliest(self):
        class Client:
            def committed(self, partitions, timeout): return [TopicPartition("topic", 0, 1)]
            def get_watermark_offsets(self, part, **kwargs): return 5, 10
            def assign(self, parts): raise AssertionError("must reject unavailable data")
        with self.assertRaises(ValueError): assign_retained(Client(), [TopicPartition("topic", 0)])
