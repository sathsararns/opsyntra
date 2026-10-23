"""Opt-in real PostgreSQL checks; fixtures use UUIDs and never truncate live data."""
import os
import tempfile
import unittest
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from threading import Barrier
from uuid import uuid4

from opsyntra.analytics.config import AnalyticsConfig
from opsyntra.analytics.contracts import EventConflict, Source
from tests.analytics_helpers import deadletter_event, prediction_event, raw_event


@unittest.skipUnless(os.environ.get("OPSYNTRA_RUN_ANALYTICS_TESTS") == "1", "Run start-analytics.ps1 -Task test against the separate PostgreSQL test database")
class AnalyticsStorageTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        from opsyntra.analytics.migrations import migrate
        env = dict(os.environ)
        for name in ("READ", "WRITE", "MIGRATION"):
            env["OPSYNTRA_ANALYTICS_" + name + "_URL"] = env.get("OPSYNTRA_ANALYTICS_TEST_" + name + "_URL", "")
        cls.config = AnalyticsConfig.from_env(env)
        with cls.config.connect("migration") as connection:
            # A naming convention alone is insufficient: require a different DB
            # from the configured application's normal migration connection.
            test_name = connection.info.dbname
            normal = AnalyticsConfig.from_env(os.environ)
            with normal.connect("migration") as other:
                if other.info.dbname == test_name:
                    raise ValueError("Analytics integration tests require a separate database")
            cls.migrations = migrate(connection)

    def setUp(self):
        from opsyntra.analytics.storage import AnalyticsStore
        self.write = self.config.connect("write")
        self.read = self.config.connect("read")
        self.owner = self.config.connect("migration")
        self.addCleanup(self.owner.close)
        self.addCleanup(self.read.close)
        self.addCleanup(self.write.close)
        self.store = AnalyticsStore(self.write)
        self.cluster = "test-" + uuid4().hex
        self.offset = 0

    def insert(self, kind, value, source=None):
        self.offset += 1
        return self.store.ingest(kind, value,
            source=source or Source(self.cluster, "telco." + kind, 0, self.offset),
            key=value["event_id"] if kind == "deadletter" else value["customerID"])

    def details(self, prediction):
        return self.read.execute("SELECT * FROM analytics.prediction_details WHERE prediction_id=%s", (prediction["prediction_id"],)).fetchone()

    def test_migrations_rerun_and_detect_checksum_changes(self):
        from opsyntra.analytics.migrations import DIRECTORY, migrate
        self.assertTrue(all(row["status"] == "already_applied" for row in migrate(self.owner)))
        with tempfile.TemporaryDirectory() as directory:
            for path in DIRECTORY.glob("*.sql"):
                (Path(directory) / path.name).write_bytes(path.read_bytes().replace(b"\n", b"\r\n"))
            self.assertTrue(all(row["status"] == "already_applied" for row in migrate(self.owner, directory)))
        with tempfile.TemporaryDirectory() as directory:
            for path in DIRECTORY.glob("*.sql"):
                (Path(directory) / path.name).write_bytes(path.read_bytes() + b"\n-- mutation\n")
            with self.assertRaisesRegex(ValueError, "changed"):
                migrate(self.owner, directory)
        self.assertTrue(all(row["status"] == "already_applied" for row in migrate(self.owner)))

    def test_failed_migration_rolls_back_ddl_and_version(self):
        import psycopg
        from opsyntra.analytics.migrations import DIRECTORY, migrate
        with tempfile.TemporaryDirectory() as directory:
            for path in DIRECTORY.glob("*.sql"):
                (Path(directory) / path.name).write_bytes(path.read_bytes())
            version = f"{max(int(p.name[:4]) for p in DIRECTORY.glob('*.sql')) + 1:04d}"
            (Path(directory) / (version + "_failure.sql")).write_text("CREATE TABLE analytics.should_rollback(x int); SELECT 1/0;")
            with self.assertRaises(psycopg.errors.DivisionByZero): migrate(self.owner, directory)
        self.assertIsNone(self.owner.execute("SELECT to_regclass('analytics.should_rollback') AS name").fetchone()["name"])
        self.assertIsNone(self.owner.execute("SELECT * FROM analytics.schema_migrations WHERE version=%s", (version,)).fetchone())

    def test_prediction_first_then_raw_joins_by_original_event(self):
        raw = raw_event()
        predicted = prediction_event(raw)
        self.insert("prediction", predicted)
        self.assertFalse(self.details(predicted)["raw_available"])
        self.insert("raw", raw)
        detail = self.details(predicted)
        self.assertEqual(detail["contract"], raw["Contract"])
        self.assertTrue(detail["raw_available"])
        self.assertIsNone(detail["features"]["TotalCharges"])
        self.assertEqual(detail["processing_latency_seconds"], 2)

    def test_repeated_customer_uses_event_profile_not_latest_profile(self):
        first = raw_event()
        second = raw_event(first["customerID"], "Two year")
        p1, p2 = prediction_event(first), prediction_event(second)
        self.insert("raw", second)
        self.insert("prediction", p1)
        self.insert("raw", first)
        self.insert("prediction", p2)
        self.assertEqual(self.details(p1)["contract"], "Month-to-month")
        self.assertEqual(self.details(p2)["contract"], "Two year")

    def test_logical_duplicates_keep_first_processing_time_and_count_deliveries(self):
        raw = raw_event()
        value = prediction_event(raw)
        source = Source(self.cluster, "predictions", 0, 1)
        self.assertTrue(self.insert("prediction", value, source)["logical_inserted"])
        self.assertEqual(self.insert("prediction", value, source)["delivery_inserted"], False)
        replay = {**value, "processed_ts": "2026-10-20T09:00:00Z"}
        self.assertFalse(self.insert("prediction", replay)["logical_inserted"])
        self.assertEqual(self.details(value)["processed_ts"].hour, 8)
        count = self.read.execute("SELECT count(*) AS n FROM analytics.kafka_deliveries WHERE prediction_id=%s", (value["prediction_id"],)).fetchone()["n"]
        self.assertEqual(count, 2)

    def test_raw_duplicates_and_conflicting_features(self):
        raw = raw_event()
        self.assertTrue(self.insert("raw", raw)["logical_inserted"])
        self.assertFalse(self.insert("raw", raw)["logical_inserted"])
        with self.assertRaises(EventConflict): self.insert("raw", {**raw, "Contract": "Two year"})
        row = self.read.execute("SELECT contract FROM analytics.raw_customer_events WHERE event_id=%s", (raw["event_id"],)).fetchone()
        self.assertEqual(row["contract"], "Month-to-month")

    def test_conflicting_prediction_and_model_alias_are_rejected(self):
        raw = raw_event()
        p = prediction_event(raw)
        self.insert("prediction", p)
        with self.assertRaises(EventConflict): self.insert("prediction", {**p, "churn_probability": .8})
        with self.assertRaises(EventConflict): self.insert("prediction", {**p, "prediction_id": "b"*64})
        self.assertEqual(self.details(p)["churn_probability"], .7)

    def test_raw_and_prediction_customer_or_time_conflicts_in_either_order(self):
        for first in ("raw", "prediction"):
            raw = raw_event()
            p = prediction_event(raw)
            self.insert(first, raw if first == "raw" else p)
            if first == "raw":
                with self.assertRaises(EventConflict): self.insert("prediction", {**p, "customerID": "OTHER"})
                with self.assertRaises(EventConflict): self.insert("prediction", {**p, "event_ts": "2026-10-20T10:00:00Z"})
            else:
                with self.assertRaises(EventConflict): self.insert("raw", {**raw, "customerID": "OTHER"})

    def test_coordinate_conflict_rolls_back_entire_new_logical_event(self):
        a, b = raw_event(), raw_event()
        coordinates = Source(self.cluster, "raw", 0, 9)
        self.insert("raw", a, coordinates)
        with self.assertRaises(EventConflict): self.insert("raw", b, coordinates)
        self.assertIsNone(self.read.execute("SELECT event_id FROM analytics.customer_events WHERE event_id=%s", (b["event_id"],)).fetchone())

    def test_deadletter_replay_and_conflicting_error_are_separate_from_predictions(self):
        value = deadletter_event()
        self.assertTrue(self.insert("deadletter", value)["logical_inserted"])
        self.assertFalse(self.insert("deadletter", {**value, "processed_ts": "2026-10-20T12:00:00Z"})["logical_inserted"])
        with self.assertRaises(EventConflict):
            self.insert("deadletter", {**value, "error": {"type": "invalid_record", "message": "Different error"}})

    def test_distinct_models_engines_and_event_counts_are_not_customer_counts(self):
        model = "metrics-" + uuid4().hex
        a = raw_event()
        b = raw_event(a["customerID"])
        first, second = prediction_event(a, model), prediction_event(b, model)
        second.update(churn_probability=.2, prediction="No")
        self.insert("prediction", first)
        self.insert("prediction", second)
        self.insert("prediction", first)  # Logical duplicate must not affect views.
        self.insert("prediction", prediction_event(a, model, "spark"))
        metrics = self.read.execute("SELECT * FROM analytics.prediction_metrics_by_model WHERE engine='sklearn' AND model_version=%s", (model,)).fetchone()
        self.assertEqual((metrics["prediction_volume"], metrics["unique_customers"], metrics["unique_events"]), (2, 1, 2))
        self.assertAlmostEqual(metrics["predicted_churn_share"], .5)
        self.assertAlmostEqual(metrics["mean_risk"], .45)

    def test_database_enforces_probability_foreign_keys_and_labels(self):
        import psycopg
        raw = raw_event()
        p = prediction_event(raw)
        self.insert("prediction", p)
        for statement in ["churn_probability=1.1", "churn_probability='NaN'::float8", "prediction='Maybe'"]:
            with self.assertRaises(psycopg.errors.CheckViolation), self.owner.transaction():
                self.owner.execute("UPDATE analytics.prediction_events SET " + statement + " WHERE prediction_id=%s", (p["prediction_id"],))
        with self.assertRaises(psycopg.errors.ForeignKeyViolation), self.owner.transaction():
            self.owner.execute("UPDATE analytics.prediction_events SET customer_id='OTHER' WHERE prediction_id=%s", (p["prediction_id"],))
        with self.assertRaises(psycopg.errors.UniqueViolation), self.owner.transaction():
            self.owner.execute("INSERT INTO analytics.prediction_events SELECT * FROM analytics.prediction_events WHERE prediction_id=%s", (p["prediction_id"],))

    def test_reader_cannot_write_and_writer_cannot_update_delete_or_migrate(self):
        import psycopg
        owner_role = psycopg.sql.Identifier(self.owner.info.user).as_string(self.owner)
        # Turn off client-side read-only mode: real GRANTs must still protect it.
        self.read.execute("SET default_transaction_read_only=off")
        for connection, statements in [(self.read, ["DELETE FROM analytics.customer_events", "INSERT INTO analytics.customer_events(event_id,customer_id,event_ts) VALUES (gen_random_uuid(),'x',now())"]),
            (self.write, ["UPDATE analytics.customer_events SET customer_id='x'", "DELETE FROM analytics.kafka_deliveries", "TRUNCATE analytics.kafka_deliveries", "CREATE SCHEMA forbidden", "CREATE TABLE analytics.forbidden(x int)", "SELECT * FROM analytics.schema_migrations", "SET ROLE " + owner_role])]:
            for statement in statements:
                with self.subTest(statement=statement), self.assertRaises(psycopg.errors.InsufficientPrivilege): connection.execute(statement)

    def test_transaction_rollback_removes_fact_identity_and_provenance(self):
        raw = raw_event()
        with self.assertRaises(RuntimeError):
            with self.write.transaction():
                self.insert("raw", raw)
                raise RuntimeError("Simulated crash before database commit")
        self.assertIsNone(self.read.execute("SELECT * FROM analytics.raw_customer_events WHERE event_id=%s", (raw["event_id"],)).fetchone())
        self.assertTrue(self.insert("raw", raw)["logical_inserted"])

    def test_concurrent_duplicate_inserts_have_one_logical_winner(self):
        from opsyntra.analytics.storage import AnalyticsStore
        for kind in ("raw", "prediction", "deadletter"):
            for attempt in range(4):
                raw = raw_event()
                value = raw if kind == "raw" else prediction_event(raw) if kind == "prediction" else deadletter_event()
                key = value["event_id"] if kind == "deadletter" else value["customerID"]
                barrier = Barrier(4)
                def insert(index):
                    with self.config.connect("write") as connection:
                        barrier.wait(timeout=10)
                        return AnalyticsStore(connection).ingest(kind, value,
                            source=Source(self.cluster, f"parallel-{kind}-{attempt}", 0, index), key=key)
                with self.subTest(kind=kind, attempt=attempt), ThreadPoolExecutor(max_workers=4) as workers:
                    results = list(workers.map(insert, range(4)))
                    self.assertEqual(sum(r["logical_inserted"] for r in results), 1)
                    self.assertEqual(sum(r["delivery_inserted"] for r in results), 4)

    def test_utc_storage_keeps_timezone_display_out_of_the_fact(self):
        from datetime import timedelta
        raw = raw_event()
        p = prediction_event(raw)
        p["processed_ts"] = "2026-10-20T13:30:02+05:30"
        self.insert("prediction", p)
        self.assertEqual(self.details(p)["processed_ts"].utcoffset(), timedelta(0))
        self.assertEqual(self.details(p)["processing_latency_seconds"], 2)
