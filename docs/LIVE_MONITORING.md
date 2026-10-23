# Live monitoring

The Next.js **Live monitoring** screen reads stored Kafka predictions through
five FastAPI endpoints. It polls every five seconds by default. Single-customer
and CSV predictions remain available in their original screens; those direct
requests do not publish Kafka events or contribute to the stream dashboard.

## Start locally

Run these commands from the repository root. Install the optional analytics API
dependencies once in the Python environment used by the frontend launcher:

```powershell
python -m pip install -r backend/requirements/analytics-web.txt
.\scripts\windows\start-kafka.ps1 -Task start
.\scripts\windows\start-analytics.ps1 -Task start
```

On a new machine, follow [PostgreSQL setup](ANALYTICS_STORAGE.md) and
[Kafka setup](../backend/KAFKA.md) first. The dataset and selected saved model
must already be prepared following the root README.

Keep each of the following running in its own terminal:

```powershell
# Terminal 1: model prediction consumer
python backend/consumer.py --mode streaming --engine sklearn

# Terminal 2: store raw, predicted and rejected events in PostgreSQL
python backend/analytics_consumer.py --mode streaming

# Terminal 3: Python API and Next.js frontend
.\scripts\windows\start-frontend.ps1

# Terminal 4: sample customer events; Ctrl+C stops publishing
python backend/producer.py --mode streaming --events-per-sec 5
```

Open http://127.0.0.1:3000 and choose **Live monitoring**. Expand **Trace event**
in a recent prediction to see its full event ID, prediction ID, model version,
original contract/payment profile and timestamps. A model consuming a backlog
can take longer to reach newly published events; consumer lag is shown where
Kafka supplies committed offsets. Stop the consumers with Ctrl+C when finished.

From `backend/`, launchers need `..\`, while Python script paths omit `backend/`.
The launcher checks an already-running API for the analytics routes. If it reports
older code, stop that API process and restart the launcher.

## Filters and metric definitions

The initial selection is **sklearn**, the most recently ingested model version
for that engine, and the last 24 hours of **event time**. Choose an explicit model
version to keep it fixed while another model starts producing events. Engine and
model versions are never added together in the selected prediction metrics.
The UI supports engine, model, time window, exact customer ID, minimum risk,
contract and payment method; apply changes with **Apply filters**.

- **Prediction events:** stored logical `prediction_id` count in the selection.
  Kafka redeliveries do not increase this count.
- **Unique customers:** distinct customer IDs in those predictions. Legitimate
  repeated events remain separate predictions.
- **Predicted churn share:** fraction labelled `Yes` by the saved model's decision
  threshold. It is not an observed churn rate or a model accuracy measurement.
- **Mean risk:** average churn probability, not confidence in model accuracy.
- **Raw events:** logical raw events in the time window across all engines/models;
  this contextual count intentionally does not use prediction/profile filters.
- **Matched profiles:** predictions whose original raw event is available. Profile
  segments join by `event_id`; a later profile for the same customer is not used.
  A prediction arriving before its raw event appears immediately and gains its
  profile when the matching raw event arrives. Missing profiles form an Unknown
  segment and do not vanish from prediction totals.
- **Highest-risk customers:** latest event per customer within the selected
  engine/model/time window, then profile/risk filters and a 70% risk threshold.
  Older high-risk events cannot override that customer's newer low-risk event.

The timeseries counts prediction events in UTC hourly buckets; the API also
supports UTC daily buckets. Empty buckets have zero volume and null risk/share.
The display timezone comes from `OPSYNTRA_ANALYTICS_TIMEZONE` (Asia/Colombo by
default). Event time, model processing time and database ingestion time are
separate fields. Late data is reflected by the next query, without incrementing
delivery-based counters. A timestamped SQL snapshot backs each endpoint; one
poll uses the same model and time bounds for its summary, chart and tables.
Events committed during the parallel requests may appear on the following poll.

## Health and refresh behavior

The screen distinguishes these conditions:

| State | Meaning |
|---|---|
| Empty stream | Database reachable and no stored predictions |
| No activity in this selection | Database has predictions, but the chosen filters match none |
| Connected, no new activity | Consumer is active with no recent deliveries; zero activity can be successful |
| Consumer inactive | The configured Kafka analytics group has no active members |
| Data stale | Stored activity is old while there is known outstanding consumer lag |
| Broker disconnected | Stored SQL history is readable, but Kafka metadata is unavailable |
| Disconnected | The API or database cannot be read; retained values are labelled as the last successful result |
| Polling paused | Automatic updates are paused by the user |

The age of stored data is shown separately; old timestamps alone do not prove a
consumer failure. Lag is the summed unprocessed offset distance for the three
analytics topics, not the model prediction group's lag. Missing initial offsets
on nonempty partitions produce **Unavailable**, not a fabricated zero. Group
membership is a readiness observation, not proof that every dependency is
healthy. The observer never joins the group or commits application offsets.
Kafka health observations are cached for five seconds.

Change refresh intervals in the screen (1/5/10/30 seconds), pause/resume, or click
**Refresh now**. Requests run serially between polls, cancel when filters change,
and have a deadline; polling resumes automatically after outages. Set
`NEXT_PUBLIC_ANALYTICS_POLL_MS=5000` in `frontend/.env.local` to change the initial
interval (bounded to 1–60 seconds). Restart/rebuild Next.js after changing it.
There is no SSE subscription.

## Read endpoints and server configuration

| GET endpoint | Result |
|---|---|
| `/api/analytics/summary` | Scoped logical metrics, contract/payment segments, model choices |
| `/api/analytics/timeseries` | Gap-filled UTC hour/day volume and risk/share |
| `/api/analytics/predictions` | Recent predictions and traceability fields with total/offset/limit |
| `/api/analytics/high-risk` | Paginated latest-per-customer risk ranking |
| `/api/analytics/health` | Database, broker, analytics group membership/lag, activity age |

The same paths work through the Next.js server proxy (port 3000) and FastAPI
(port 8000). The proxy forwards query strings only to explicitly allowed routes.
All responses use `Cache-Control: no-store`; analytics requests do not load the
ML pipeline. Database outages return 503, invalid filters return 422, and a
broker outage still permits reading SQL history with degraded health.

Supported query parameters: `engine=sklearn|spark`, `model_version`,
`lookback_hours=1..744`, timezone-aware ISO `start`/`end` with increasing bounds
of at most 31 days, exact `customer_id`, `min_probability`/`max_probability`
between 0 and 1, `prediction=Yes|No`, `contract`, and `payment_method`.
Tables accept `limit=1..100` and `offset=0..100000`; high-risk also accepts
`threshold=0..1`; timeseries accepts `interval=hour|day`.
Omit `model_version` to select the latest ingested version for that engine.
Use URL encoding for timestamps, spaces and payment-method punctuation.

```powershell
Invoke-RestMethod 'http://127.0.0.1:8000/api/analytics/summary?engine=sklearn&lookback_hours=24'
Invoke-RestMethod 'http://127.0.0.1:8000/api/analytics/predictions?limit=10&offset=0'
```

SQL uses bound parameters and a read-only, repeatable-read transaction with a
three-second statement timeout. API connections use only the reader login from
`OPSYNTRA_ANALYTICS_READ_URL` in `backend/.env` or the process environment.
Credentials stay on the Python server; never use `NEXT_PUBLIC_` for them. Health
can override `OPSYNTRA_KAFKA_BOOTSTRAP_SERVERS`,
`OPSYNTRA_ANALYTICS_GROUP_ID`, `OPSYNTRA_ANALYTICS_RAW_TOPIC`,
`OPSYNTRA_ANALYTICS_PREDICTION_TOPIC`, and `OPSYNTRA_ANALYTICS_DEADLETTER_TOPIC`.
These should match the running analytics consumer. Local setup uses the normal
three Telco topics. Prediction APIs continue working when analytics dependencies
or credentials are absent, while analytics reports its unavailable state.

## Verification and retained evidence

From the repository root, with PostgreSQL and Kafka started:

```powershell
cd backend
python -m pipelines.analytics_dashboard_check --regression
```

This runs backend checks, TypeScript checks, a production build and browser
tests. It creates diagnostic Kafka topics/groups, uses the separate configured
test database, and starts fresh API/Next.js servers on free ports. A real
streaming producer publishes a customer, the saved model generates its
prediction, and the analytics consumer stores it. The browser waits for the row
without a reload, verifies event/model IDs, measures ingestion-to-visible time
at the default five-second interval, and compares API metrics with an independent
SQL query. The local target is at most ten seconds for that observed sample;
it is not a latency guarantee for other hosts or a prediction backlog.

Polling updates, database/API outages, broker outages, inactive consumers,
recovery and mobile layout also have labelled browser fixtures. Existing direct
single-customer, CSV and overview browser tests run alongside these checks.
The verifier stops only its own processes; it does not reset application groups.

See [verification results and limits](../backend/reports/live_dashboard_verification.md),
[machine-readable results and SHA-256 hashes](../backend/reports/live_dashboard_verification.json),
[desktop screenshot](screenshots/phase4/live-monitoring.png) and
[mobile screenshot](screenshots/phase4/live-monitoring-mobile.png).
