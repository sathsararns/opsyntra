# Opsyntra: submission, live analytics and deployment roadmap

Created: 2026-10-20 (Asia/Colombo). Status: **Phases 1–2 complete; Phases 3–10 planned**.

The assessed ML/Kafka workflows and bonus DAGs are already implemented. This
roadmap closes submission checks and adds the analytics/deployment features
reviewed in the lecture project. See [the original roadmap](OPSYNTRA_ROADMAP.md)
and [verification guide](docs/VERIFICATION.md) for the completed foundation.

## Intended result

```text
                         existing inference consumer
                                  |
CSV -> producer -> telco.raw.customers -> saved model -> telco.churn.predictions
                         |                                    |
                         +------------+-----------------------+
                                      v
                            new analytics consumer
                                      |
                                  PostgreSQL
                                  /        \
                       FastAPI read API    SQL views / QuickSight
                              |
                       Next.js live dashboard

Invalid records -> telco.deadletter -> analytics error counts
MLflow -> models and experiment history
Airflow -> existing ML and Kafka workflows
GitHub Actions -> tests -> container build -> controlled deployment
```

PostgreSQL holds durable analytics. The existing inference SQLite outbox/ledger
continues to serve recovery until a separately tested cloud storage migration.
The browser reads FastAPI; Kafka and database credentials stay on the server.

## Order and dependencies

| Phase | Deliverable | Depends on | Completion checkpoint |
|---|---|---|---|
| 1 (complete) | Submission refresh, clean-clone proof and demo | Existing system | [Verified fresh checkout and retained logs](backend/reports/clean_clone_verification.md) |
| 2 (complete) | Analytics schema and migrations | 1 | [Real PostgreSQL verification](backend/reports/analytics_verification.md) |
| 3 | Kafka-to-database analytics consumer | 2 | Replay/restart does not double-count logical predictions |
| 4 | Analytics API and live frontend | 3 | Published events appear on the dashboard automatically |
| 5 | EDA and analytics notebooks | 2–4 | Notebooks run from top to bottom and export their figures |
| 6 | GitHub Actions CI | 1; expand after 3–5 | Pull requests run meaningful checks automatically |
| 7 | Docker/Compose | 3, 4, 6 | A documented container startup runs the local application |
| 8 | S3 and RDS migration | 7 | Cloud data/model loading and analytics persistence work |
| 9 | ECR/ECS deployment | 6–8 | Public application, private dependencies and tested rollback |
| 10 | QuickSight and final cloud evidence | 8, 9 | BI dashboards agree with database queries and have refresh evidence |

Phase 6 can begin immediately after Phase 1 with the current checks. Expand its
integration jobs as later phases land. Complete each local checkpoint before
introducing the corresponding cloud service.

## Phase 1 — Finish submission and establish a reproducible baseline

Completed on 2026-10-20: latest-run evidence refreshed with older provenance
archived; both engines rebuilt in a separate Git clone with fresh ML
environments; single/CSV inference, Kafka recovery, six frontend browser flows
and both eight-task ML DAGs passed. The original Airflow service was restored.
The verification report records its baseline commit, one applied Spark logging
portability patch, package caches and host prerequisites reused. Current host
checks: 74 passed plus four native Spark checks passed (78 unique tests).
Windows long-path logging/restore fixes, isolated worker requirements and native
Linux environment setup are included. See [the report](backend/reports/clean_clone_verification.md),
[demo guide](docs/DEMO_GUIDE.md) and [evidence index](backend/reports/submission_evidence.md).

- Refresh Kafka bonus screenshots for the latest successful batch/streaming
  runs, then regenerate the evidence matrix. Preserve previous run provenance.
- Run frontend typecheck, production build and browser tests against real
  predictions. Retain the test result and relevant screenshots.
- Verify a clean Git clone in a different directory and fresh Python environments.
  Install dependencies, obtain the supplied CSV/archive, validate its checksum,
  prepare memberships, train and run single/CSV inference. Verify both ML engines
  and the documented Kafka/Airflow setup; record anything reused from the host.
- Do not copy ignored artifacts into the clone as evidence of a clean rebuild.
  Use a separately labelled, checksum-verified artifact bundle when demonstrating
  the faster saved-model setup path.
- Document dataset acquisition, compatible runtime versions, model rebuilding,
  optional artifact-bundle import and setup failures a new user could encounter.
- Prepare a short demo: data quality -> model comparison -> customer prediction
  -> CSV upload -> Airflow run -> Kafka input/output -> restart/recovery proof.

Planned outputs: `backend/reports/clean_clone_verification.json`,
`docs/DEMO_GUIDE.md`, refreshed screenshot provenance and submission evidence.

**Done when:** the new checkout predicts raw records without labels; model/data
checksums and test results are recorded; the frontend demo works.

## Phase 2 — Design the analytics contract and PostgreSQL storage

Completed on 2026-10-20: versioned PostgreSQL migrations, separate owner/reader/writer
roles, immutable logical storage and physical-delivery provenance, event-based
out-of-order joins, reference metric views, local WSL setup and contract/database
tests. See [storage setup and definitions](docs/ANALYTICS_STORAGE.md) and
[actual verification](backend/reports/analytics_verification.md). The main database
is ready; test fixtures are stored separately. Phase 3 Kafka ingestion and Phase 4
live APIs/dashboard remain planned.

- Add database configuration through environment variables and a placeholder
  `.env.example`; use versioned migrations and separate read/write roles.
- Create `raw_customer_events`, keyed by `event_id`, with customer fields,
  event timestamp and source provenance.
- Create `prediction_events`, keyed by `prediction_id`, retaining `event_id`,
  `customerID`, engine, model version, probability, label, event/processing times
  and ingestion time. Add indexes for time, customer, model and risk queries.
- Create dead-letter/provenance storage with stable event/source identifiers.
  Optionally retain physical Kafka deliveries by cluster/topic/partition/offset
  while counting logical predictions by `prediction_id`.
- Preserve feature values at the original event: contract/payment analytics
  must join raw and predicted events by **event_id**, rather than the customer's
  most recently seen profile. Either topic may arrive first.
- Keep repeated legitimate customer events. Detect conflicting reuse of an
  event ID instead of silently overwriting different contents.
- Define dashboard terms: prediction volume, unique customers, predicted churn
  share, mean risk and latency. These are not observed churn/accuracy measures
  without suitable ground-truth labels.
- Store UTC timestamps; present a chosen user timezone. Distinguish event time,
  processing time and database ingestion time.

Planned code: `backend/src/opsyntra/analytics/`, `backend/migrations/`,
`backend/requirements/analytics.txt`, schema/config documentation.

**Done when:** migrations and duplicate/conflict constraints pass tests, and
out-of-order raw/prediction inserts can be joined correctly.

## Phase 3 — Implement the Kafka analytics consumer

- Add `backend/analytics_consumer.py` to consume raw, prediction and dead-letter
  topics with its own group. It does not run the ML model again.
- Validate each supported schema, then insert/upsert idempotently in PostgreSQL.
  Commit the database transaction **before** committing covered Kafka offsets.
- Test the crash between database commit and Kafka commit: replay must leave
  logical prediction counts unchanged through primary/unique constraints.
- Add retries, bounded batch size, graceful shutdown, structured logs and
  database/broker readiness checks. A database failure must prevent input commits.
- Handle raw/prediction arrival order, duplicate deliveries, conflicting event
  contents and invalid analytics messages with an explicit error policy.
- Compute hourly/daily views from stored logical events. Recompute affected
  event-time windows when late data arrives instead of incrementing counters
  blindly on every physical delivery.
- Filter/segment by engine and model version so comparing both engines does not
  look like twice as many distinct customer requests.

**Done when:** publish a known event set, observe matching database rows, restart
the consumer, replay it, and confirm no count inflation or lost valid events.

## Phase 4 — Add analytics APIs and the live Next.js dashboard

Planned read endpoints:

- `GET /api/analytics/summary`
- `GET /api/analytics/timeseries`
- `GET /api/analytics/predictions` with pagination and filters
- `GET /api/analytics/high-risk`
- `GET /api/analytics/health`

Add the endpoints to the existing Next.js server proxy allowlist and TypeScript
types. Validate filter ranges, paginate queries and use parameterized SQL.

Add a **Live monitoring** screen containing event volume, predicted churn share,
recent predictions, highest-risk customers, contract/payment segments, model
version, last update and consumer lag/health where available. Distinguish an
empty stream, stale data, disconnected services and successful zero activity.

Start with configurable polling (for example, five seconds). Add SSE only if
polling does not meet an agreed freshness goal. Keep direct customer/CSV
prediction flows available alongside stream monitoring. Preserve the supplied
logo and the existing frontend design.

**Done when:** a producer event is automatically visible with its event/model ID;
the dashboard reports the same counts as SQL. A reasonable initial demo target
is visibility within ten seconds of ingestion under a healthy local setup,
measured rather than claimed in advance. Browser tests cover updates and outages.

## Phase 5 — Add reproducible notebooks

- `backend/notebooks/01_telco_eda.ipynb`: training-partition distributions,
  missing TotalCharges policy, class balance and contract/tenure relationships.
- `backend/notebooks/02_model_comparison.ipynb`: load the existing reports for
  validation selection, thresholds, PR/ROC curves and honest engine timing.
- `backend/notebooks/03_streaming_analytics.ipynb`: read database views for
  hourly trends, risk segments, recent predictions and late/duplicate events.
- Fit nothing on held-out test data. Use final test metrics only as the recorded
  assessment; do not repeatedly tune against them.
- Load connection settings from the environment and export figures/tables to
  `backend/reports/`. Do not embed credentials in saved notebook output.

**Done when:** notebooks execute from top to bottom using documented inputs,
and their charts agree with the reports/dashboard. Explain that synthetic
streaming replay is a simulation, not evidence of real company churn trends.

## Phase 6 — Add GitHub Actions CI

- Run backend unit/integration checks on pull requests and pushes, with locked
  dependencies and recorded test artifacts.
- Make the fast suite self-contained: synthetic customer fixtures and small
  temporary fitted models must not depend on ignored production artifacts.
  Refactor tests that currently assume the full local CSV/model accordingly.
- Run frontend typecheck/build and deterministic browser flows. Keep a separate
  full-data job supplied with a documented, checksum-verified dataset/model setup;
  do not hide missing prerequisites by skipping essential checks.
- Add PostgreSQL/Kafka integration jobs once the sink is implemented. Keep
  heavier native Spark checks in a clearly identified scheduled/manual job if
  they are too expensive for every commit.
- Upload reports/screenshots on failures and preserve relevant successful results.
- Add controlled container build/deployment jobs later. For AWS, prefer OIDC
  roles over stored long-lived keys; do not deploy untrusted pull-request code.

Planned files: `.github/workflows/ci.yml`, then integration/deployment workflows.

**Done when:** a deliberately broken change fails CI, a valid change passes,
and the repository README links to actual CI results.

## Phase 7 — Containerize the local system

- Add images for the API/ML worker, frontend and analytics consumer. Keep the
  Airflow environment isolated; put heavy Spark/MLflow/Airflow components behind
  Compose profiles where useful.
- Add Kafka and PostgreSQL services, health checks and named persistent volumes.
  Use internal service names inside containers instead of Windows localhost.
- Configure runtime Python/artifact paths instead of assuming the current WSL
  `.venv-spark` layout. Mount the shared paths required by existing DAG tasks.
- Persist broker data, analytics data, approved models and consumer recovery
  state. Explicitly test restart/recreate behavior.
- Keep inference at one replica while its SQLite recovery store and group lock
  remain local. Scaling requires a separately designed shared recovery backend.
- Provide `.env.example`, `.dockerignore`, startup, stop and explicit reset
  instructions; the normal stop path must preserve data.

**Done when:** a documented `docker compose up --build` path brings up the core
application, events reach the dashboard, and service restarts preserve results.

## Phase 8 — Introduce S3 and RDS

- Choose AWS region, deployment lifetime and budget before provisioning. Record
  a current service estimate; do not assume every service is free-tier eligible.
- Store versioned model bundles, manifests and reports in private S3 locations.
  Verify checksums and model metadata after download and use atomic local caches.
- Apply the local PostgreSQL migrations to RDS. Keep it private and test access
  from the application network; configure backups and recovery for retained data.
- Use task/instance IAM roles and Secrets Manager or SSM for credentials.
  Configure application URLs and database settings separately from local defaults.
- Test one bounded event set against RDS and verify idempotent replay again.

**Done when:** an authorized runtime loads an exact S3 model version and writes
deduplicated analytics to RDS, with matching local/cloud query results.

## Phase 9 — Deploy application services to ECR/ECS

- Build tagged images and push them to ECR. Deploy frontend, API and analytics
  worker on ECS with health checks, logs, HTTPS and a tested rollback path.
- Expose the application through a load balancer; keep databases, brokers and
  operational admin UIs private. Replace local-only admin assumptions before
  hosting any Airflow/MLflow interfaces.
- Select Kafka hosting after the budget/runtime decision: a managed MSK setup
  or an explicitly limited persistent EC2 demo. Do not place broker state on
  disposable container storage.
- **Resolve inference recovery before moving that consumer to Fargate:** either
  implement a tested PostgreSQL recovery-store adapter/group ownership mechanism,
  or keep the single consumer on a persistent EC2/EBS runtime. A task replacement
  must not silently lose its SQLite outbox and deduplication history.
- Keep the working local Spark/Airflow runtime for the first cloud milestone.
  Moving training/orchestration to cloud is a separate checkpoint: task paths,
  shared artifacts, credentials and scheduling must be redesigned/tested there.
- Add alerts for unavailable services, growing consumer lag, repeated failures
  and stale ingestion. Test deployment failure, worker restart and rollback.

**Done when:** the hosted UI receives real Kafka-derived data, dependencies are
private, secrets are externalized, and restart/rollback evidence is saved.

## Phase 10 — Build QuickSight dashboards and close the cloud demo

- Connect QuickSight to approved RDS SQL views through the appropriate network
  connection and a read-only database identity.
- Create prediction-volume, predicted-risk, contract/payment segment, hourly
  trend and high-risk customer visuals. Keep observed outcomes separate.
- Define timezone, filtering and refresh behavior. If using an imported/SPICE
  dataset, demonstrate its refresh delay rather than calling it instantaneous.
- Compare every KPI against SQL for the same time window/model filter.
- Record the deployment architecture, costs, demo procedure, rollback and
  teardown instructions. Export screenshots and cloud test evidence.

**Done when:** hosted application and BI views can be demonstrated from known
events, their numbers agree, and the environment has a documented cleanup path.

## Milestones

1. **Submission ready:** Phase 1.
2. **Local live analytics platform:** Phases 2–5.
3. **Repeatable engineering workflow:** Phases 6–7.
4. **Cloud demonstration:** Phases 8–10.

Phase 1 is complete. Start Phase 2 next: the first major new application feature is the PostgreSQL
analytics contract and idempotent Kafka sink, followed by the live frontend.
Each phase should end with its checkpoint and a reviewable commit/PR.

## Official references checked for this plan

- [ECS IAM roles and image/secret access](https://docs.aws.amazon.com/AmazonECS/latest/developerguide/ecs-iam-role-overview.html)
- [ECS Secrets Manager integration](https://docs.aws.amazon.com/AmazonECS/latest/developerguide/specifying-sensitive-data.html)
- [QuickSight private VPC connections](https://docs.aws.amazon.com/quicksight/latest/user/vpc-creating-a-connection-in-quicksight.html)
- [MSK Serverless overview](https://docs.aws.amazon.com/en_en/msk/latest/developerguide/serverless.html)
- [MSK pricing](https://aws.amazon.com/msk/pricing/)

Consult current regional service availability/pricing when implementation reaches
the cloud phases. The plan does not provision resources or publish the application.
