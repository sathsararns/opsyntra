import unittest
from unittest.mock import patch

import numpy as np
import httpx

from opsyntra.model_building import build_model_pipeline
from opsyntra.model_inference import predict_records
from opsyntra.web_api import app
from tests.helpers import sample_data


class WebApiTests(unittest.IsolatedAsyncioTestCase):
    @classmethod
    def setUpClass(cls):
        data = sample_data()
        cls.rows = data.drop(columns="Churn")
        cls.model = build_model_pipeline("logistic_regression", jobs=1)
        cls.model.fit(cls.rows, data["Churn"].map({"No": 0, "Yes": 1}).astype(int))
        cls.model.opsyntra_metadata_ = {"model_version": "api-test-model"}

    async def asyncSetUp(self):
        self.enterContext(patch("opsyntra.web_api.get_model", return_value=self.model))
        self.client = await self.enterAsyncContext(httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://testserver",
        ))

    async def test_single_prediction_matches_existing_pipeline_without_churn(self):
        row = self.rows.iloc[0].to_dict()
        response = await self.client.post("/api/predict", json={"record": row})
        self.assertEqual(response.status_code, 200, response.text)
        result = response.json()
        expected = predict_records(self.model, row).iloc[0]
        self.assertAlmostEqual(result["predictions"][0]["churn_probability"], expected["churn_probability"], places=14)
        self.assertEqual(result["model_version"], "api-test-model")

    async def test_batch_predictions_and_summary_match_the_saved_pipeline(self):
        rows = self.rows.iloc[:6]
        response = await self.client.post("/api/predict/batch", json={"csv_text": rows.to_csv(index=False)})
        self.assertEqual(response.status_code, 200, response.text)
        result = response.json()
        expected = predict_records(self.model, rows)
        self.assertEqual(result["summary"]["total"], 6)
        self.assertEqual(result["summary"]["predicted_churn"], int(expected["prediction"].eq("Yes").sum()))
        np.testing.assert_allclose([row["churn_probability"] for row in result["predictions"]], expected["churn_probability"])

    async def test_invalid_customer_input_returns_a_useful_validation_error(self):
        row = self.rows.iloc[0].to_dict()
        row["tenure"] = "not-a-number"
        response = await self.client.post("/api/predict", json={"record": row})
        self.assertEqual(response.status_code, 422)
        self.assertIn("tenure", response.json()["detail"])

    async def test_empty_and_oversized_batches_are_rejected(self):
        for csv in (" ", "customerID\n", "customerID\n" + "customer\n" * 10_001):
            response = await self.client.post("/api/predict/batch", json={"csv_text": csv})
            self.assertEqual(response.status_code, 422, response.text)

    async def test_health_reports_readiness_and_unavailable_model(self):
        self.assertEqual((await self.client.get("/api/health")).json()["status"], "ready")
        with patch("opsyntra.web_api.get_model", side_effect=FileNotFoundError):
            with self.assertLogs("opsyntra.web_api", level="WARNING"):
                self.assertEqual((await self.client.get("/api/health")).status_code, 503)
            response = await self.client.post("/api/predict", json={"record": self.rows.iloc[0].to_dict()})
            self.assertEqual(response.status_code, 503)

    async def test_missing_project_reports_return_unavailable_instead_of_fake_data(self):
        with patch("opsyntra.web_api.read_overview", side_effect=FileNotFoundError):
            with self.assertLogs("opsyntra.web_api", level="WARNING"):
                response = await self.client.get("/api/overview")
            self.assertEqual(response.status_code, 503)
