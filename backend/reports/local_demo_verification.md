# Final local Docker demo verification

**PASS - 2026-10-25 (Asia/Colombo).** The agreed delivery target is the local
Docker Compose application. AWS/QuickSight are deferred future work. Hosted CI
verification remains pending; this report does not claim cloud deployment or a
new hosted GitHub Actions pass.

| Check | Actual result |
|---|---|
| Startup | Updated launcher started Docker Desktop, waited for Linux engine readiness and brought up the six core services healthy; setup jobs exited successfully |
| Overview/model | 7,043 customers; model version `34e84e9fa5d1de06` loaded through the frontend proxy |
| Raw single/CSV inference | Identical probability and label for the same unlabelled customer with blank TotalCharges |
| Bounded streaming demo | 20 events acknowledged; exactly 20 new raw rows and 20 new logical predictions |
| SQL/API metrics | Prediction volume, unique events/customers, mean risk and predicted churn share match for the same time window/engine/model |
| Browser flows | 6 passed, 0 skipped: overview/export, customer/history, CSV/search/download, model/data views, mobile and API-unavailable UI |
| Live desktop/mobile | Real counts and trace IDs shown; model matches; mobile has no page overflow; screenshots retained |
| Stop/start recreation | SQL rows, selected-model checksum, Kafka committed offsets and SQLite outbox unchanged; API ready afterward |
| Final shutdown | Core containers removed; named data/model/recovery volumes retained |
| Evidence scan | Configured database password values absent from saved evidence |

The default last-24-hours view contained 20 predictions from the new demo. Its
predicted churn share was 30%
and mean predicted risk 0.2389. These are
synthetic replay model outputs, not observed company churn or accuracy.
The producer-start-to-proxy check took 6.74
seconds, including producer startup, ingestion and a SQL snapshot check. Browser
assertions were separate; this is not an ingestion-to-browser latency SLA.

## Evidence

- [Machine-readable report](local_demo_verification.json), [run manifest](../../docs/evidence/local-demo/20261025T065632Z/report.json).
- [Browser results](../../docs/evidence/local-demo/20261025T065632Z/browser-tests.json), [browser log](../../docs/evidence/local-demo/20261025T065632Z/browser-tests.txt).
- [Overview](../../docs/evidence/local-demo/20261025T065632Z/frontend-desktop.png), [customer prediction](../../docs/evidence/local-demo/20261025T065632Z/frontend-prediction.png), [mobile](../../docs/evidence/local-demo/20261025T065632Z/frontend-mobile.png).
- [Live monitoring](../../docs/evidence/local-demo/20261025T065632Z/live-monitoring.png), [live mobile](../../docs/evidence/local-demo/20261025T065632Z/live-monitoring-mobile.png), [event/model IDs](../../docs/evidence/local-demo/20261025T065632Z/live-browser.json).
- [Producer receipt](../../docs/evidence/local-demo/20261025T065632Z/producer.json), [SQL agreement](../../docs/evidence/local-demo/20261025T065632Z/sql-agreement.json), [analytics health](../../docs/evidence/local-demo/20261025T065632Z/analytics-health.json).
- [Before recreation](../../docs/evidence/local-demo/20261025T065632Z/before-recreate.txt), [after recreation](../../docs/evidence/local-demo/20261025T065632Z/after-recreate.txt), [final stop](../../docs/evidence/local-demo/20261025T065632Z/final-stop.txt).

## Scope and reproduction

Follow [the local demo guide](../../docs/DEMO_GUIDE.md). This check used the
existing workspace and preserved application volumes; it is not a replacement
for the earlier [clean-clone evidence](clean_clone_verification.md). Source and
evidence hashes are retained in the JSON report. This run did not retrain/tune
models, run paid resources, push commits, or repeat the complete Spark/Airflow
scheduler checks. Those dated results remain linked in the submission evidence.
For a new isolated full core restart/replay acceptance run, follow
[container verification](../../docs/CONTAINERS.md#reproduce-acceptance-checks).
