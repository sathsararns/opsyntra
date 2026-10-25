# Opsyntra project roadmap

Consolidated: 2026-10-23 (Asia/Colombo). Demo scope confirmed: 2026-10-25.

This is the single roadmap for the assignment foundation, submission checks,
live analytics, engineering workflow and optional cloud deployment. The statuses
below refer to the linked verification evidence; consolidating this document
has not rerun those checks.

| Area | Recorded status |
|---|---|
| Foundation steps 1-9: ML, Kafka, tests and bonus DAGs | Implemented; evidence retained |
| Platform Phases 1-5: submission and local live analytics | Complete |
| Platform Phase 6: GitHub Actions CI | Implemented and locally verified; hosted verification pending |
| Platform Phase 7: Docker/Compose | Core packaging and acceptance checks complete |
| Platform Phases 8-10: AWS and QuickSight | Deferred future work; outside the final local demo |

**Final delivery scope:** present the working application with local Docker
Compose. Follow the [demo guide](docs/DEMO_GUIDE.md) for startup, direct/CSV
predictions, Kafka live monitoring, evidence and data-preserving shutdown.
Keep the existing ML/Spark/Airflow evidence for the assessed workflows.
See [the final local demo verification](backend/reports/local_demo_verification.md)
for fresh browser, Kafka/SQL and stop/start results recorded on 2026-10-25.

**Budget constraint:** no budget is available for paid deployment. AWS Phases
8-10 are deferred future work, subject to a separate budget/credits decision
before provisioning. They are not required to close this local demo. Hosted
GitHub Actions verification remains pending separately; no hosted pass or cloud
deployment is claimed. TensorFlow/Terraform additions are outside this scope.

Foundation work uses **Step 1-9** below. Platform work keeps the existing
**Phase 1-10** numbering used in the guides and evidence reports.

- [Assignment requirements](#assignment-requirements)
- [Foundation steps](#foundation-steps)
- [Platform phases and dependencies](#platform-phases-and-dependencies)
- [Milestones](#milestones)

## Assignment requirements

| Assignment | Requirement | Marks | Planned evidence |
|---|---|---:|---|
| Mini Project 1 | Reproducible scikit-learn preprocessing, training, persistence, inference, tests | 25 | Saved complete pipeline, training report, inference tests |
| Mini Project 1 | MLflow parameters, metrics, artifacts, models, run comparison | 25 | Multiple runs and UI screenshots |
| Mini Project 1 | PySpark preprocessing, MLlib models, inference, comparison with sklearn | 30 | Saved Spark PipelineModel, comparable metrics and timings |
| Mini Project 1 | Airflow preprocessing, tuned training, evaluation, inference and storage | 20 | DAG source and successful run screenshot |
| Mini Project 1 | Final deliverables bonus | 5 | Organised repository and complete documentation/evidence |
| Mini Project 2 | Batch and streaming producer | 20 | CLI scripts, delivery logs, restart demonstration |
| Mini Project 2 | Batch and streaming consumer with predictions | 40 | Output messages and batch prediction results |
| Mini Project 2 | Integration and reliability | 20 | Checkpoint, offsets, retry and failure recovery evidence |
| Mini Project 2 | Testing and observability | 10 | Automated tests, logs and flow screenshots |
| Mini Project 2 | Documentation and repository hygiene | 10 | README, config examples and clean repository |
| Mini Project 2 | Kafka Airflow orchestration bonus | 10 | Streaming health/consumer DAG and scheduled batch DAG |

Airflow and PySpark are core requirements in Mini Project 1. Kafka-specific Airflow DAGs are bonus work in Mini Project 2. Neither PDF imposes a specific numerical model score threshold. Select and justify a validation objective rather than copying the lecture's conflicting gates.

AWS S3, ECS, RDS, QuickSight, a web dashboard, a REST API, and cloud CI/CD are optional extensions. Complete the assessed local workflows before adding these.

## Core ML architecture

```text
Telco CSV -> schema validation -> fixed train/validation/test membership
                                      |
                   +------------------+------------------+
                   v                                     v
         sklearn preprocessing + model          Spark preprocessing + model
                   |                                     |
                   +---------- MLflow tracking -----------+
                   |                                     |
          complete joblib pipeline              saved Spark PipelineModel
                   +------------------+------------------+
                                      v
                          common inference interface
                           /                     \
                   batch file inference       Kafka consumer
                                                 ^
CSV replay -> producer.py -> telco.raw.customers --+
                                                 |
                                                 v
                                    telco.churn.predictions
                                                 |
                                     results and summaries

Invalid events -> telco.deadletter (optional in PDF; recommended)
Airflow -> orchestration, retries, evaluation and evidence
```

The implemented local services run natively in Ubuntu WSL, with a pinned Linux Spark runtime and an isolated Airflow environment. Kafka uses persistent native Linux storage and Windows localhost forwarding. Docker is optional and is not required by the launchers. See docs/VERIFICATION.md for the actual compatible runtimes and setup commands.

## Repository layout

The repository separates the Next.js frontend from the Python API and ML workflows.
Python source names and paths in the later build phases are relative to `backend/`.
MLflow, Spark, required Airflow orchestration, Kafka publishing/prediction/recovery and both bonus Kafka DAGs are implemented.

```text
Opsyntra/
  frontend/                   # Next.js + Tailwind; includes browser tests
    src/
    public/
    tests/
    package.json
  backend/                    # Python API and ML workflows
    src/opsyntra/
    pipelines/
    dags/
    notebooks/
    tests/
    data/
      raw/
      processed/
      splits/
    artifacts/
    reports/
    examples/
    requirements/
    logs/
    config.yaml
    pyproject.toml
    README.md
  containers/                 # Docker images and runtime helpers
  docs/
    screenshots/
  compose.yaml                # Local container services and volumes
  .gitignore
  README.md
  OPSYNTRA_ROADMAP.md
  scripts/windows/            # Windows and WSL service launchers
```

The implemented producer.py, consumer.py, Spark modules and Airflow DAGs live inside
`backend/`. Shared deployment configuration can live at the repository root.

Use the lecture's separation of reusable components, pipelines and services. Build Telco-specific schemas and transformations instead of copying its bank features or hard-coded feature ordering.

## Foundation steps

These steps describe the implemented assignment foundation and its acceptance criteria.

### Step 1: Environment and data contract

- Create the repository skeleton, configuration, logging and reproducible dependency files.
- Confirm local MLflow, Spark and Airflow smoke tests; bring up Kafka when its phase begins.
- Preserve the CSV and record its checksum and schema.
- Verify the inspected dataset facts: 7,043 rows; 21 columns; 1,869 churners (26.54%); 11 blank TotalCharges entries; unique customerID values.
- Define prediction inputs separately from labels and event metadata. customerID is an identifier; Churn is the target; event_ts is metadata. None is a model feature.
- Convert blank TotalCharges to missing numeric values. Keep the imputation policy explicit; learn a median from training data initially. Retain meaningful service categories such as 'No internet service'.
- Persist a stratified 60/20/20 train/validation/test membership manifest using customerID. Reuse those exact records in both engines.

Done when: validation reports row counts, target balance and missing values, and both engines can read the same split memberships.

### Step 2: Scikit-learn training and inference

- Implement a ColumnTransformer inside one sklearn Pipeline.
- Numeric processing: impute; scale for models such as logistic regression.
- Categorical processing: explicit missing-value handling and OneHotEncoder(handle_unknown='ignore').
- Fit every learned transformation only on training data; cross-validation must include the preprocessing pipeline.
- Train a DummyClassifier baseline, LogisticRegression and RandomForestClassifier. Add XGBoost after these work.
- Tune hyperparameters on training folds. Choose a decision threshold on validation data if needed; reserve test data for final assessment.
- Report positive-class precision, recall and F1, ROC-AUC, average precision/PR-AUC, accuracy and a confusion matrix. Document which PR metric is used.
- Save the entire fitted preprocessing/model pipeline with joblib, together with threshold, feature schema, dataset checksum and dependency versions.
- Implement file-based batch inference and single-record inference using that same artifact. Churn must not be required in prediction requests.

Done when: a fresh process loads the artifact and produces the same probabilities for the same inputs, including supported unseen categories and missing numeric values.

### Step 3: MLflow tracking

Implemented: local SQLite tracking, four comparable candidate runs per batch,
complete sklearn/Joblib artifacts, validation AP model selection and validation
F1 thresholds. See `backend/reports/model_selection.md`, `model_training_report.json`,
and `docs/screenshots/mlflow-*.png`. Start the UI with `.\scripts\windows\start-mlflow.ps1`.

- Add tracking as soon as the first baseline runs; do not wait until every model is finished.
- Log model parameters, seed, preprocessing configuration, split identifier, data checksum, metrics and timing.
- Log ROC/PR plots, confusion matrices, evaluation JSON and the complete fitted model artifact.
- Compare several meaningful runs: baseline, logistic regression, random forest and tuned candidates.
- Capture readable screenshots of the experiment table, an individual run, metrics/artifacts and model details.

Done when: a reviewer can identify the selected run and reproduce why it was selected.

### Step 4: PySpark reconstruction and comparison

Implemented in Ubuntu WSL with Java 21, Python 3.12 and PySpark 4.0.2: native
Logistic Regression, Random Forest and SparkXGBClassifier pipelines. Training
reuses saved memberships and fits preprocessing within training-only folds.
Complete PipelineModels, validation thresholds, raw-record/CSV inference and
MLflow artifacts are saved. Read `backend/reports/engine_comparison.md` for the
same-customer test metrics and measured local timings. Use `scripts/windows/start-spark.ps1`.

- Implement schema conversion and equivalent cleaning with Spark DataFrame APIs.
- Construct Spark ML pipelines with Imputer, StringIndexer/OneHotEncoder, VectorAssembler, optional scaling and estimators as appropriate.
- Handle unknown categories explicitly and use the same raw feature contract and split membership as sklearn.
- Train Spark LogisticRegression and RandomForestClassifier first. Include XGBoost through its Spark integration to cover the algorithms listed in the brief; it is not a built-in MLlib estimator.
- Save/reload the fitted PipelineModel and demonstrate inference on new records.
- Compare both engines on the same held-out customers using the same metric definitions. Spark's weighted F1 is not the same as churn-class F1; calculate comparable positive-class metrics.
- Report preprocessing, fit, inference and total wall-clock times separately. State whether Spark startup is included; trigger actual execution when timing lazy Spark transformations.
- Describe local Spark honestly. This small dataset may show startup overhead rather than a speed advantage; a local run does not demonstrate multi-node performance.

Done when: both engines have persisted artifacts, working inference and a fair comparison report.

### Step 5: Required Airflow ML DAG

Implemented with Airflow 3.3.2 in an isolated Ubuntu WSL environment. The actual
scheduler runs all eight tasks for sklearn or Spark, reusing saved memberships
and keeping fitted preprocessing within training. Each run freezes its inputs,
checks candidate validation/integrity, saves its selected complete pipeline,
loads it for batch predictions, and writes results with per-attempt logs.
See `backend/dags/README.md`, `backend/reports/airflow_workflow_report.md` and
`docs/screenshots/airflow-*.png`. Use `scripts/windows/start-airflow.ps1`.

```text
validate_data -> prepare_splits -> tune_and_train
                -> evaluate -> validate_candidate
                -> save_selected_model -> predict -> save_results
```

- Keep deterministic cleaning and split preparation distinct from fitted preprocessing. Fit imputers/encoders inside training folds.
- Support the engine through DAG configuration or separate tasks.
- Add meaningful retries, timeouts, task logs and run-specific output paths.
- Pass paths and small metadata between tasks rather than whole datasets through XCom.
- Persist the candidate model in its training task so later tasks can evaluate it. Update the selected-model reference only after the candidate passes the documented checks.
- Include actual hyperparameter tuning, as requested by the PDF.

Done when: a complete DAG run succeeds, creates prediction outputs and has a saved Graph/Grid screenshot.

### Step 6: Kafka producer

Implemented: Apache Kafka 4.3.1 in Ubuntu WSL, pinned Windows Python client,
three required topics, and one producer.py with continuous sampling/rate control
and acknowledged chunk/checkpoint/resume. Events have customerID keys, typed
customer fields, UTC timestamps and stable batch event IDs. The real broker check
verified 7,043 batch records across two processes plus 30 streaming records.
See `backend/KAFKA.md` and `backend/reports/kafka_publishing_report.md`.
Crash-window delivery is documented as at-least-once with stable IDs for future
consumer deduplication, which is now implemented in Step 7.

Create these exact topics:

- telco.raw.customers
- telco.churn.predictions
- telco.deadletter, if implementing invalid-event routing

Implement one producer.py with:

- --mode streaming: sample dataset rows continuously and support --events-per-sec.
- --mode batch: read the full CSV in chunks and support --batch-size plus checkpoint/resume.
- customerID as the Kafka key; JSON values matching the PDF's fields and event_ts.
- Optionally retain Churn for simulated ground truth, while keeping it outside model inputs. It is not known in real prediction traffic.
- Add a stable event_id and batch run identifier as documented schema extensions. New streaming events for the same customer need distinct IDs; replayed batch events retain IDs.
- Delivery callbacks, producer retries and acknowledgement-aware checkpoints.

Done when: both modes publish valid records, an interrupted batch resumes, and logs show acknowledged counts and checkpoint progress.

### Step 7: Kafka consumer and recovery

Implemented: one consumer.py for batch/count, fixed offset windows and continuous
streaming; sklearn and native Spark models are loaded once per process. Valid
records produce predictions and malformed/conflicting records go to deadletter.
A durable SQLite outbox/ledger precedes manual input commits. Stable prediction
IDs support replay handling. Real hard exits demonstrated both durable-ledger
deduplication and the remaining ack-before-ledger duplicate window, with no
end-to-end exactly-once claim. See `backend/KAFKA.md` and
`backend/reports/kafka_consumption_report.md` for commands and evidence.

Implement one consumer.py with:

- --mode streaming: validate inputs, load a selected model once, predict continuously and publish outputs.
- --mode batch: consume a defined count or fixed per-partition offset/time window, predict in batches and stop predictably.
- For count-limited runs, include an idle timeout so an empty topic cannot hang forever. Use isolated batch consumer groups/run identifiers.
- Output customerID, churn_probability, prediction, event_ts and processed_ts as required. Add event_id and model version for traceability.
- Exclude Churn, identifiers and timestamps from model features.
- Commit offsets only after prediction output delivery is acknowledged, or after invalid input is acknowledged in the dead-letter topic.
- Use persistent event deduplication where needed, structured error logs and graceful shutdown.
- Produce optional contract-type summaries and top-risk customer lists.

Checkpointing and producer idempotence alone do not guarantee duplicate-free processing across every crash window. Start with documented at-least-once delivery and persistent deduplication; use Kafka transactions if claiming atomic input-offset/output publication. Test the failure window between output delivery and input offset commit.

Done when: both modes publish correct results; malformed input does not stop valid traffic; restart tests demonstrate the documented recovery behavior.

### Step 8: Automated tests and evidence

Implemented: the current host suite has 74 passing tests and four Spark-runtime skips;
those exact four checks passed in fresh WSL Spark. All 78 unique tests pass
across both runtimes. Additional checks explicitly compare the same model/input
through single, CSV, batch Kafka and streaming Kafka inference. Actual MLflow and
Airflow screenshots, labelled Kafka CLI/recovery transcripts and a SHA-256
evidence index are retained. See `docs/VERIFICATION.md` and
`backend/reports/submission_evidence.md`.

Platform Phase 1 is complete: independent clean-clone training/inference,
fresh Airflow metadata/runs, six production-browser flows, updated evidence,
portable setup fixes and a demo guide. See Platform Phase 1 below and
`backend/reports/clean_clone_verification.md` for the exact tested candidate,
source patch and reused host prerequisites.

Test the behavior that matters:

- TotalCharges blanks, schema/type errors and supported unknown categories.
- No label or customer identifier in model features.
- Same probabilities before and after artifact reload.
- Matching single-record, batch-file and Kafka inference for the same artifact/input.
- Valid producer keys, schemas, timestamps and both operating modes.
- Batch checkpoint/resume, invalid events and consumer restart after partial processing.
- Valid-record, dead-letter and deduplicated-output counts for an isolated test run.
- Bounded Kafka batch completion and output probabilities in [0, 1].

Capture screenshots during implementation: MLflow comparisons, successful Airflow runs, raw Kafka messages, prediction messages, and batch/streaming logs. Keep evidence labelled with the command, configuration and model version that produced it.

Done when: a clean environment can run the documented workflow and the evidence maps to every rubric row.

### Step 9: Kafka Airflow bonus

Kafka bonus implemented: `opsyntra_kafka_batch` has a daily schedule (initially
paused), frozen inputs, acknowledged publishing/checkpoint resume, fixed-window
consumption and a summary CSV. `opsyntra_kafka_streaming` owns/monitors a live
consumer with heartbeat logs, a bounded session/stop marker, graceful cleanup and
optional sample publishing. Both completed real four-task LocalExecutor runs.
Submission runs saved 7,043 batch predictions and 20 streaming predictions.
Use `scripts/windows/start-airflow.ps1 -Task run-kafka -KafkaKind batch|streaming`.

- Batch DAG: health checks -> batch producer -> bounded batch consumer -> summary.
- Streaming DAG: health checks -> launch/monitor the long-running consumer. Prevent overlapping copies and define stop/cleanup behavior. Capture the consumer running successfully; a permanently running consumer does not have a completed success state until stopped gracefully.
- Continue with the platform phases below for PostgreSQL analytics, the live dashboard, CI, containers and optional cloud deployment.

Done when: batch and bounded streaming Kafka DAG runs succeed, persist their prediction results and have saved scheduler/log evidence.

### Original foundation planning sequence

This is the original planning estimate for the completed foundation, retained for context; it is not an assignment deadline or remaining-work schedule.

| Week | Focus | Checkpoint |
|---|---|---|
| 1 | Environment, data contract, sklearn pipeline and MLflow | Persisted model predicts raw unseen records; tracked runs exist |
| 2 | Spark equivalents, engine comparison and required Airflow DAG | Mini Project 1 workflows and evidence complete |
| 3 | Kafka producer and consumer in both modes | Dataset -> Kafka -> predictions works for batch and streaming |
| 4 | Restart recovery, tests, screenshots, documentation and bonus DAGs | Both submissions reproducible from README |

Build one working path before broadening it: sklearn CSV -> persisted pipeline -> raw-record prediction -> MLflow -> Spark -> Airflow -> Kafka -> recovery -> evidence.

## Live analytics and deployment architecture

```text
                         existing inference consumer
                                  |
CSV -> producer -> telco.raw.customers -> saved model -> telco.churn.predictions
                         |                                    |
                         +------------+-----------------------+
                                      v
                            analytics consumer
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

## Platform phases and dependencies

| Phase | Deliverable | Depends on | Completion checkpoint |
|---|---|---|---|
| 1 (complete) | Submission refresh, clean-clone proof and demo | Existing system | [Verified fresh checkout and retained logs](backend/reports/clean_clone_verification.md) |
| 2 (complete) | Analytics schema and migrations | 1 | [Real PostgreSQL verification](backend/reports/analytics_verification.md) |
| 3 (complete) | Kafka-to-database analytics consumer | 2 | [Real crash/replay verification](backend/reports/analytics_consumer_verification.md) |
| 4 (complete) | Analytics API and live frontend | 3 | [Real producer-to-browser visibility and SQL agreement](backend/reports/live_dashboard_verification.md) |
| 5 (complete) | EDA and analytics notebooks | 2–4 | [Executed notebooks, exported figures and SQL/API checks](backend/reports/notebook_verification.md) |
| 6 (hosted verification pending) | GitHub Actions CI | 1; expand after 3–5 | [Local checks pass; first hosted runs require commit/push](backend/reports/ci_verification.md) |
| 7 (core complete) | Docker/Compose | 3, 4; CI integration from 6 | [Real container startup, restart/recreate and replay checks](backend/reports/container_verification.md) |
| 8 (deferred) | S3 and RDS migration | 7; approved cloud budget | Future cloud data/model loading and analytics persistence |
| 9 (deferred) | ECR/ECS deployment | 6–8; approved cloud budget | Future public application, private dependencies and tested rollback |
| 10 (deferred) | QuickSight and final cloud evidence | 8, 9; approved cloud budget | Future BI/SQL agreement and refresh evidence |

Phase 6 can begin immediately after Phase 1 with the current checks. Expand its
integration jobs as later phases land. Complete each local checkpoint before
introducing the corresponding cloud service.

### Phase 1 — Finish submission and establish a reproducible baseline

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

### Phase 2 — Design the analytics contract and PostgreSQL storage

Completed on 2026-10-20: versioned PostgreSQL migrations, separate owner/reader/writer
roles, immutable logical storage and physical-delivery provenance, event-based
out-of-order joins, reference metric views, local WSL setup and contract/database
tests. See [storage setup and definitions](docs/ANALYTICS_STORAGE.md) and
[actual verification](backend/reports/analytics_verification.md). The main database
is ready; test fixtures are stored separately. Kafka ingestion and the Phase 4 live APIs/dashboard are also implemented.

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

### Phase 3 — Implement the Kafka analytics consumer

Completed on 2026-10-20: dedicated Kafka analytics group, database-before-offset
commits, durable rejection quarantine, bounded retry/streaming/snapshot modes,
graceful shutdown and UTC hourly/daily/feature-segment views. Real Kafka checks
hard-exited processes before/after database commit, restarted/replayed input,
and verified unchanged logical counts and repaired late-event buckets. See
[operating guide](docs/ANALYTICS_CONSUMER.md) and
[verification report](backend/reports/analytics_consumer_verification.md).
The consumer does not load an ML model. Live HTTP/dashboard work is implemented in Phase 4.

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

### Phase 4 — Add analytics APIs and the live Next.js dashboard

Completed on 2026-10-20: five read-only FastAPI endpoints, validated bounded
filters and pagination, server-side proxy/types, and a live monitoring screen
with five-second polling, original-event segments, traceable IDs, model selection
and explicit outage/inactive/empty states. Existing single/CSV flows and the supplied
logo are preserved. Real Kafka, saved-model inference, PostgreSQL and fresh browser
servers verified automatic visibility and independent SQL agreement; browser tests
cover updates, outages and mobile layout. See [operating guide](docs/LIVE_MONITORING.md)
and [measured verification](backend/reports/live_dashboard_verification.md).

Implemented read endpoints:

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

### Phase 5 — Add reproducible notebooks

Completed on 2026-10-20: all three notebooks execute in fresh kernels and export
CSV tables plus PNG/SVG figures and provenance manifests. Training-only EDA,
saved validation/threshold/curve comparisons and recorded benchmark timings are
kept separate from final test assessment. A read-only SQL snapshot verifies
hourly/segment/risk counts; real PostgreSQL tests check dashboard API agreement,
out-of-order profiles and physical duplicates. No fitting or held-out test
feature exploration occurs. See [notebook guide](docs/NOTEBOOKS.md) and
[execution evidence](backend/reports/notebook_verification.md).

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

### Phase 6 — Add GitHub Actions CI

Implemented on 2026-10-21: push/PR backend, frontend and PostgreSQL/Kafka jobs;
scheduled/manual native Spark; explicit checksum-gated full-data workflow;
Linux Python locks with hashes, immutable official action revisions, fresh
synthetic browser models and retained JSON/JUnit/browser artifacts. Local clean
checkout checks passed with no selected checks skipped; deliberate feature
leakage failed the strict runner. Native service provisioning and crash/replay
were verified separately from the application's existing services.
Hosted runs are still pending commit/push, so this phase's full completion
checkpoint remains open. See [CI guide](docs/CI.md) and
[local evidence and limitations](backend/reports/ci_verification.md).

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

### Phase 7 — Containerize the local system

**Implemented:** root `compose.yaml`, backend/frontend/Airflow Dockerfiles,
isolated optional Spark/MLflow/Airflow profiles and persistent named volumes.
See [startup, stop/reset and recovery guide](docs/CONTAINERS.md) and
[measured container acceptance checks](backend/reports/container_verification.md).
The core acceptance run passed real producer/model/SQL/browser checks, service
restart, container recreation and replay without logical count inflation.

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

### Phase 8 — Introduce S3 and RDS

**Deferred:** retained as future design work, outside the final local demo.

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

### Phase 9 — Deploy application services to ECR/ECS

**Deferred:** retained as future design work, outside the final local demo.

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

### Phase 10 — Build QuickSight dashboards and close the cloud demo

**Deferred:** retained as future design work, outside the final local demo.

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
4. **Final presentation:** local Docker demo using the completed core and retained ML/Spark/Airflow evidence.
5. **Future cloud demonstration (deferred):** Phases 8–10, after a separate budget decision.

Phases 1–5 and Phase 7 core packaging are complete. Phase 6 workflows and local
verification are ready; hosted pass/fail evidence still requires commit/push.
Each phase should end with its checkpoint and a reviewable commit/PR.

## Foundation technical references

- [Scikit-learn: avoiding inconsistent preprocessing and data leakage](https://scikit-learn.org/stable/common_pitfalls.html)
- [Spark: ML pipelines and persistence](https://spark.apache.org/docs/latest/ml-pipeline.html)
- [XGBoost: distributed training with PySpark](https://xgboost.readthedocs.io/en/stable/tutorials/spark_estimator.html)
- [Kafka: delivery semantics and transactional processing](https://kafka.apache.org/41/design/design/)

Assignment sources: loKNBWKtfI.pdf, pages 4-9; HWLHplNEHU (1).pdf, pages 3-8. Uploaded dataset facts were calculated directly from archive.zip.

## Cloud reference links

- [ECS IAM roles and image/secret access](https://docs.aws.amazon.com/AmazonECS/latest/developerguide/ecs-iam-role-overview.html)
- [ECS Secrets Manager integration](https://docs.aws.amazon.com/AmazonECS/latest/developerguide/specifying-sensitive-data.html)
- [QuickSight private VPC connections](https://docs.aws.amazon.com/quicksight/latest/user/vpc-creating-a-connection-in-quicksight.html)
- [MSK Serverless overview](https://docs.aws.amazon.com/en_en/msk/latest/developerguide/serverless.html)
- [MSK pricing](https://aws.amazon.com/msk/pricing/)

Consult current regional service availability/pricing when implementation reaches
the cloud phases. The plan does not provision resources or publish the application.
