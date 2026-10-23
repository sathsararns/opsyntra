"""HTTP/SQL agreement, validated filters, latest-customer risk and outages."""
import importlib.util
import os
import unittest
from datetime import datetime, timedelta, timezone
from unittest.mock import patch
from uuid import uuid4

import httpx
from opsyntra.web_api import app
from tests.analytics_helpers import raw_event,prediction_event
from tests.integration import test_analytics_storage as storage_tests


class AnalyticsApiValidationTests(unittest.IsolatedAsyncioTestCase):
    async def test_invalid_filters_fail_before_database_access(self):
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app),base_url="http://test") as client:
            cases=["engine=other","limit=101","offset=-1","min_probability=NaN","min_probability=.8&max_probability=.2",
                   "start=2026-10-20T00:00:00","start=2026-10-20T00:00:00Z&end=2026-10-19T00:00:00Z","lookback_hours=745","extra=bad"]
            with patch("opsyntra.analytics.read_api.reader_config",side_effect=AssertionError("must not connect")):
                for query in cases:
                    response=await client.get("/api/analytics/predictions?"+query)
                    self.assertEqual(response.status_code,422,response.text)
                self.assertEqual((await client.get("/api/analytics/timeseries?interval=week")).status_code,422)

    async def test_unavailable_database_is_not_reported_as_empty_or_model_failure(self):
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app),base_url="http://test") as client:
            with patch("opsyntra.analytics.read_api.reader_config",side_effect=ValueError("private-credential")),patch("opsyntra.web_api.get_model",side_effect=AssertionError("analytics must not load ML")):
                for endpoint in ("health","summary","predictions","timeseries","high-risk"):
                    response=await client.get("/api/analytics/"+endpoint)
                    self.assertEqual(response.status_code,503)
                    self.assertNotIn("private-credential",response.text)
                    self.assertNotIn('"prediction_volume":0',response.text)


@unittest.skipUnless(importlib.util.find_spec("psycopg") is not None and os.getenv("OPSYNTRA_RUN_ANALYTICS_TESTS")=="1","Requires separate analytics PostgreSQL test database")
class AnalyticsApiDatabaseTests(unittest.IsolatedAsyncioTestCase):
    @classmethod
    def setUpClass(cls): storage_tests.AnalyticsStorageTests.setUpClass.__func__(cls)

    async def asyncSetUp(self):
        storage_tests.AnalyticsStorageTests.setUp(self)
        self.enterContext(patch("opsyntra.analytics.read_api.reader_config",return_value=self.config))
        self.enterContext(patch("opsyntra.analytics.read_api.broker_health",return_value={"broker":"ready","consumer_state":"inactive","lag":0,"group_id":"test","members":0}))
        self.client=await self.enterAsyncContext(httpx.AsyncClient(transport=httpx.ASGITransport(app),base_url="http://test"))
        self.version="api-"+uuid4().hex
        self.now=datetime.now(timezone.utc)
        self.index=0
        self.customer="API-"+uuid4().hex

    def seed(self,probability=.8,customer=None,engine="sklearn",seconds=0):
        raw=raw_event(customer or self.customer)
        raw["event_ts"]=(self.now-timedelta(minutes=5)+timedelta(seconds=seconds)).isoformat()
        predicted=prediction_event(raw,self.version,engine)
        predicted.update(churn_probability=probability,prediction="Yes" if probability>.5 else "No",processed_ts=self.now.isoformat())
        from opsyntra.analytics.contracts import Source
        for kind,value in (("raw",raw),("prediction",predicted)):
            self.index+=1
            self.store.ingest(kind,value,source=Source(self.cluster,"api-"+kind,0,self.index),key=raw["customerID"])
        return predicted

    async def get(self,path,**params):
        response=await self.client.get("/api/analytics/"+path,params={"model_version":self.version,**params})
        self.assertEqual(response.status_code,200,response.text)
        self.assertEqual(response.headers["cache-control"],"no-store")
        return response.json()

    async def test_summary_counts_and_segments_equal_sql(self):
        self.seed(.8)
        self.seed(.2,seconds=30)
        self.seed(.9,engine="spark")
        result=await self.get("summary")
        expected=self.read.execute("SELECT count(*) AS n,avg(churn_probability) AS risk FROM analytics.prediction_events WHERE engine='sklearn' AND model_version=%s",(self.version,)).fetchone()
        self.assertEqual(result["metrics"]["prediction_volume"],expected["n"])
        self.assertAlmostEqual(result["metrics"]["mean_risk"],expected["risk"])
        self.assertEqual(result["metrics"]["unique_customers"],1)
        self.assertEqual(result["metrics"]["predicted_churn_share"],.5)
        self.assertEqual(result["segments"]["contract"][0]["predictions"],2)
        self.assertEqual((await self.get("summary",engine="spark"))["metrics"]["prediction_volume"],1)

    async def test_pagination_identity_and_literal_sql_input(self):
        self.seed(.8)
        self.seed(.2,seconds=10)
        a,b=await self.get("predictions",limit=1),await self.get("predictions",limit=1,offset=1)
        self.assertEqual(a["total"],2)
        self.assertNotEqual(a["items"][0]["prediction_id"],b["items"][0]["prediction_id"])
        self.assertEqual(a["items"][0]["model_version"],self.version)
        self.assertIn("event_id",a["items"][0])
        result=await self.get("predictions",customer_id="x'; DROP TABLE analytics.prediction_events;--")
        self.assertEqual(result["total"],0)
        self.assertEqual((await self.get("predictions"))["total"],2)

    async def test_high_risk_uses_latest_customer_event_before_threshold(self):
        self.seed(.99)
        self.seed(.1,seconds=20)
        other=self.seed(.8,customer="OTHER-"+uuid4().hex)
        risk=await self.get("high-risk")
        self.assertEqual(risk["total"],1)
        self.assertEqual(risk["items"][0]["prediction_id"],other["prediction_id"])

    async def test_timeseries_sums_and_successful_empty_response(self):
        self.seed(.8)
        self.seed(.4,seconds=10)
        series=await self.get("timeseries")
        self.assertEqual(sum(row["predictions"] for row in series["items"]),2)
        self.assertTrue(any(row["predictions"]==0 for row in series["items"]))
        empty=await self.get("summary",customer_id="does-not-exist")
        self.assertEqual(empty["metrics"]["prediction_volume"],0)
        self.assertIsNone(empty["metrics"]["mean_risk"])
        status=(await self.client.get("/api/analytics/health")).json()
        self.assertEqual(status["database"],"ready")
        self.assertEqual(status["consumer_state"],"inactive")
