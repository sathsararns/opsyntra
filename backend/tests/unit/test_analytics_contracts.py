import unittest

from opsyntra.analytics.config import AnalyticsConfig
from opsyntra.analytics.contracts import Source, logical_hash, normalize
from tests.analytics_helpers import deadletter_event, prediction_event, raw_event


class AnalyticsContractTests(unittest.TestCase):
    def test_missing_total_charges_preserved_and_source_validated(self):
        event = raw_event()
        self.assertIsNone(normalize("raw", event, event["customerID"])["TotalCharges"])
        for source in [("", "topic", 0, 0), ("cluster", "topic", -1, 0), ("cluster", "topic", 0, True)]:
            with self.assertRaises(ValueError): Source(*source)

    def test_wire_validation_rejects_bad_keys_versions_probabilities_and_times(self):
        raw = raw_event()
        value = prediction_event(raw)
        for kind, record, key in [("raw", raw, None), ("prediction", value, "wrong")]:
            with self.assertRaises(ValueError): normalize(kind, record, key)
        for changes in ({"schema_version": True}, {"schema_version": 2}, {"churn_probability": float("nan")},
                        {"churn_probability": 1.1}, {"churn_probability": True}, {"processed_ts": "2026-10-20T08:00:00"},
                        {"engine": "other"}, {"extra": 1}, {"prediction_id": "bad"}):
            with self.subTest(changes=changes), self.assertRaises(ValueError):
                normalize("prediction", {**value, **changes}, raw["customerID"])

    def test_logical_hash_excludes_retry_processing_time_but_not_probability(self):
        raw = raw_event()
        a = normalize("prediction", prediction_event(raw), raw["customerID"])
        b = normalize("prediction", {**a, "processed_ts": "2026-10-20T14:00:00+05:30"}, raw["customerID"])
        self.assertEqual(b["processed_ts"], "2026-10-20T08:30:00.000000Z")
        self.assertEqual(logical_hash("prediction", a), logical_hash("prediction", b))
        self.assertNotEqual(logical_hash("prediction", a), logical_hash("prediction", {**b, "churn_probability": .8}))

    def test_deadletter_matches_existing_inference_contract(self):
        event = deadletter_event()
        self.assertEqual(normalize("deadletter", event, event["event_id"])["source"], event["source"])
        with self.assertRaises(ValueError): normalize("deadletter", {**event, "schema_version": 2}, event["event_id"])

    def test_deadletter_corrupt_bytes_and_nonobject_errors_are_rejected(self):
        value = deadletter_event()
        for changes in ({"raw_value_sha256": "a" * 64}, {"error": "bad"}, {"raw_value_is_null": True},
                        {"source": {**value["source"], "extra": 1}}, {"raw_key_base64": 12}):
            with self.subTest(changes=changes), self.assertRaises(ValueError):
                normalize("deadletter", {**value, **changes}, value["event_id"])

    def test_configuration_requires_credentials_and_hides_them(self):
        with self.assertRaises(ValueError): AnalyticsConfig.from_env({})
        env = {"OPSYNTRA_ANALYTICS_" + name + "_URL": "postgresql://user:secret@localhost/test" for name in ("READ", "WRITE", "MIGRATION")}
        config = AnalyticsConfig.from_env(env)
        self.assertNotIn("secret", repr(config))
        self.assertEqual(config.display_timezone, "Asia/Colombo")
        with self.assertRaises(ValueError): AnalyticsConfig.from_env({**env, "OPSYNTRA_ANALYTICS_CONNECT_TIMEOUT": "0"})
