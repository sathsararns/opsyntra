"""Real SQL views and dashboard API agree on the notebook's fixed scope."""
import os
import unittest
from unittest.mock import patch
from uuid import uuid4

from fastapi.testclient import TestClient
from opsyntra.notebook_support import analytics_snapshot
from opsyntra.web_api import app
from tests.analytics_helpers import prediction_event, raw_event
from tests.integration import test_analytics_storage as storage


@unittest.skipUnless(os.getenv("OPSYNTRA_RUN_ANALYTICS_TESTS") == "1", "Requires separate PostgreSQL test database")
class NotebookAnalyticsTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls): storage.AnalyticsStorageTests.setUpClass.__func__(cls)

    def setUp(self):
        storage.AnalyticsStorageTests.setUp(self)
        self.version = "notebook-" + uuid4().hex

    insert = storage.AnalyticsStorageTests.insert

    def test_out_of_order_and_duplicate_events_match_dashboard_and_views(self):
        raw = raw_event()
        prediction = prediction_event(raw, self.version)
        self.insert("prediction", prediction)
        self.insert("prediction", prediction)  # Another physical offset, same logical prediction.
        self.insert("raw", raw)
        self.insert("prediction", prediction_event(raw, self.version, "spark"))
        tables, scope = analytics_snapshot(model_version=self.version, as_of="2026-10-20T10:47:00Z", config=self.config)
        self.assertEqual(scope["end"], "2026-10-20T10:00:00+00:00")
        with patch("opsyntra.analytics.read_api.reader_config", return_value=self.config), TestClient(app) as client:
            params = {k: scope[k] for k in ("engine", "model_version", "start", "end")}
            response = client.get("/api/analytics/summary", params=params)
        self.assertEqual(response.status_code, 200, response.text)
        for field, value in tables["summary"].iloc[0].items():
            self.assertAlmostEqual(value, response.json()["metrics"][field])
        self.assertEqual(int(tables["summary"].iloc[0].prediction_volume), 1)
        self.assertEqual(int(tables["summary"].iloc[0].matched_raw_predictions), 1)
        self.assertEqual(int(tables["deliveries"].iloc[0].physical_deliveries), 2)
        self.assertEqual(int(tables["deliveries"].iloc[0].repeated_deliveries), 1)
        self.assertEqual(int(tables["hourly"].prediction_volume.sum()), 1)

    def test_empty_snapshot_has_real_zero_counts_and_null_risk(self):
        tables, _ = analytics_snapshot(model_version=self.version, config=self.config)
        self.assertEqual(int(tables["summary"].iloc[0].prediction_volume), 0)
        self.assertIsNone(tables["summary"].iloc[0].mean_risk)
        self.assertTrue(tables["hourly"].empty)
        self.assertTrue(tables["recent"].empty)
