# Phase 4: live dashboard verification

Status: **PASS**.

Read endpoints, proxy query forwarding, model/time filters, SQL agreement, direct inference and polling/outage browser behavior were tested.

A real streaming producer event passed through the saved sklearn pipeline and PostgreSQL analytics consumer. At the default 5-second polling interval, ingestion-to-visible time was **2.207 seconds** (local test target: <=10 seconds). This is a measured local sample, not a general latency guarantee.

Event: `d9bc6548-6158-51d2-ac5d-b8934dfc3dbd`. Prediction: `8d3bf472af9a959b7c385dd09ad0214b703e98d9bff7461724633717b9258de8`. Model: `d9a13b4cf6839ed4`.

Backend: 116/120 passed, 4 native Spark skips. Browser: 11 passed, no failures/skips/flaky checks.

The verifier uses its own Kafka topics/groups and the separate test database. It starts fresh API/Next servers on free ports and cleans up only its own processes; the normal application groups are not reset. UI outage tests use labelled browser fixtures; the visibility/SQL test uses actual producer, prediction, Kafka, PostgreSQL and HTTP services.

See [JSON results and file hashes](live_dashboard_verification.json), [live monitoring guide](../../docs/LIVE_MONITORING.md), [desktop evidence](../../docs/screenshots/phase4/live-monitoring.png), and [mobile evidence](../../docs/screenshots/phase4/live-monitoring-mobile.png).
