# Opsyntra

Project plan: [ML, Kafka, submission, live analytics, CI/Docker and optional AWS roadmap](OPSYNTRA_ROADMAP.md).

A local Telco customer churn workspace with a Next.js + Tailwind frontend and a
Python API backed by reproducible ML pipelines.

The final presentation target is the **local Docker demo**. AWS/QuickSight
deployment is deferred future work because there is no cloud budget. The
[demo guide](docs/DEMO_GUIDE.md) covers single-customer and CSV inference, live
Kafka monitoring, evidence and shutdown.
The [final local verification](backend/reports/local_demo_verification.md)
records six passing browser flows, 20 real Kafka predictions, SQL agreement,
screenshots and successful data-preserving stop/start.

From the repository root on Windows:

```powershell
.\scripts\windows\start-containers.ps1 -Task start
# Open http://127.0.0.1:3000, then select Live monitoring.
.\scripts\windows\start-containers.ps1 -Task demo -DemoEvents 20
# After presenting; named volumes retain data, models and recovery state.
.\scripts\windows\start-containers.ps1 -Task stop
```

Prerequisites: Docker Desktop with Linux containers, Python 3 for the launcher
setup helper, and the original CSV at
`backend/data/raw/WA_Fn-UseC_-Telco-Customer-Churn.csv`. The launcher starts the
local Desktop engine when needed and preserves an existing root `.env`.
See [container setup](docs/CONTAINERS.md) for ports and first-build requirements.

Steps 1–8 and the Kafka Airflow bonus workflows are implemented. See the
[verification guide](docs/VERIFICATION.md) and
[submission evidence matrix](backend/reports/submission_evidence.md) for actual
test results, screenshots, recovery logs and successful scheduler runs.

Phase 1 handoff: [clean-clone verification](backend/reports/clean_clone_verification.md),
[frontend checks](backend/reports/frontend_verification.json) and
[presentation/demo sequence](docs/DEMO_GUIDE.md). Rebuild models from the supplied
dataset or use the separately labelled [saved-model bundle path](docs/VERIFICATION.md#optional-saved-model-setup).

Phase 2 analytics storage is implemented: [PostgreSQL setup, schema and metric
definitions](docs/ANALYTICS_STORAGE.md), with [real database verification](backend/reports/analytics_verification.md).
Run `.\scripts\windows\start-analytics.ps1 -Task setup` then `.\scripts\windows\start-analytics.ps1 -Task test`
from this root. Phase 3 Kafka-to-database ingestion is also implemented:
[consumer/recovery guide](docs/ANALYTICS_CONSUMER.md) and
[real Kafka verification](backend/reports/analytics_consumer_verification.md).
The [application backlog check](backend/reports/analytics_application_verification.md)
also records ingestion of the existing topics and a zero-record completed restart.
Start the analytics sink from root with `python backend/analytics_consumer.py --mode streaming`.
It uses its own Kafka group and the writer login; it saves existing predictions
without running the model again.

Phase 4 adds five read-only analytics APIs and a **Live monitoring** screen with
automatic refresh, engine/model filters, event traceability, SQL-backed metrics,
segments and consumer health. See [startup and metric definitions](docs/LIVE_MONITORING.md)
and [measured browser/SQL verification](backend/reports/live_dashboard_verification.md).
Install `backend/requirements/analytics-web.txt` for the complete prediction and
analytics API. Run both Kafka consumers and the producer alongside
`.\scripts\windows\start-frontend.ps1` to see new events automatically.

Phase 5 adds [three executed notebooks](backend/notebooks/README.md) for
training-only EDA, saved model comparison and a read-only streaming analytics
snapshot. See [notebook setup and metric scope](docs/NOTEBOOKS.md) and
[execution/export verification](backend/reports/notebook_verification.md).

Phase 6 CI workflows are implemented and verified locally: [job scopes and setup](docs/CI.md),
[local verification/failure proof](backend/reports/ci_verification.md), and
[repository Actions runs](https://github.com/sathsararns/opsyntra/actions).
The first hosted run is pending commit/push; the local report does not claim a
GitHub-hosted pass.

Phase 7 provides a [local Docker/Compose setup](docs/CONTAINERS.md): Next.js,
Python API, Kafka inference, PostgreSQL analytics and persistent recovery state.
Run `python containers/configure.py` then `docker compose up --build` from root.
Use `docker compose run --rm --no-deps producer producer --mode streaming --events-per-sec 5 --exclude-churn`
to populate Live monitoring. Spark, MLflow and isolated Airflow are optional profiles.
Normal `docker compose down` preserves data; see the guide for explicit reset and
[actual container verification](backend/reports/container_verification.md).

## Repository structure

```text
Opsyntra/
  frontend/                Next.js, Tailwind, browser tests and public assets
  backend/
    src/opsyntra/          Reusable Python code, model inference and FastAPI
    pipelines/             Eight core data/ML/orchestration workflow entrypoints
    tools/                 Supporting commands grouped by purpose
      ci/                  CI fixtures and test jobs
      verification/        Runtime, recovery, notebook and clean-clone checks
      evidence/            Tracking, reports, screenshots and model bundles
      services/            Native services, setup and inspection helpers
    dags/                  Airflow workflows
    notebooks/             Exploration and engine comparisons
    tests/                 Python unit and integration tests
    data/                  Raw, processed and partition data
    artifacts/             Saved models and generated predictions
    reports/               Data validation and model evaluation reports
    examples/              Label-free single-customer and CSV examples
    requirements/          Pinned Python dependencies
    migrations/            Versioned PostgreSQL analytics schema and views
    logs/                  Local API logs
    config.yaml            Backend paths and model configuration
    pyproject.toml         Python package configuration
    README.md              Detailed backend instructions
  docs/screenshots/        Shared interface screenshots
  .gitignore
  OPSYNTRA_ROADMAP.md
  README.md
  scripts/windows/         Windows and WSL launchers
    start-frontend.ps1     Starts the local frontend and API
    start-containers.ps1   Starts the Docker Compose core stack
    start-*.ps1            Airflow, Kafka, analytics, MLflow and Spark helpers
```

The Git repository remains at this root. Temporary planning tools and old
verification environments have been removed. Verification helpers recreate
their ignored working directories when needed; retained evidence is under
`backend/reports/` and `docs/evidence/`.

All Windows/WSL launchers are grouped under `scripts/windows/`; see the
[launcher guide](scripts/README.md). Commands below use their new locations.
Backend service/verification/CI utilities are grouped under
[tools/](backend/tools/README.md); [pipelines/](backend/pipelines/README.md)
contains the eight core workflow entrypoints. Historical evidence retains the
module paths used when its checks ran.
See [refactor verification](backend/reports/structure_refactor_verification.md)
for the backend/browser and real container regression results.

## Native Windows/WSL alternative

Install dependencies from the repository root:

```powershell
python -m pip install -r backend/requirements/web.txt
python -m pip install -r backend/requirements/kafka.txt
npm --prefix frontend ci
```

The API expects the data reports and model artifacts described below. Once those
are available, start both services with:

```powershell
.\scripts\windows\start-frontend.ps1
```

Open http://127.0.0.1:3000. The local API listens on http://127.0.0.1:8000.
The launcher refreshes the real-data frontend snapshot, starts or reuses the API,
and stops an API it created when the frontend exits. Its logs live in
`backend/logs/`.

For separate terminals, run the API from the repository root:

```powershell
python -m uvicorn opsyntra.web_api:app --app-dir backend/src --host 127.0.0.1 --port 8000
```

Run the frontend in the second terminal:

```powershell
cd frontend
npm run dev
```

## Data, training and CLI inference

Run Python workflow commands from `backend/`. Paths in `backend/config.yaml`
resolve relative to that configuration file.

```powershell
cd backend

# First import, when the raw CSV is not present.
python -m pipelines.data_pipeline --archive "C:\Users\SATHSARA\Downloads\archive.zip"

# Revalidate the existing dataset and reproduce saved splits.
python -m pipelines.data_pipeline

# Train four candidate pipelines and save the selected model.
python -m pipelines.training_pipeline

# Predict raw customer records without requiring Churn.
python -m pipelines.inference_pipeline --record examples/customer.json
python -m pipelines.inference_pipeline --input examples/customers.csv --output artifacts/predictions/example_batch.csv

# Refresh the dashboard's labelled offline snapshot.
python -m tools.services.frontend_snapshot
```

The API and CLI use `backend/artifacts/models/best_pipeline.joblib`. This is one
complete sklearn pipeline, including input conversion, learned preprocessing and
classification. Model selection uses validation data after training-only CV;
the chosen model is assessed on held-out test data.
Each training invocation records four comparable MLflow runs, tunes the
decision threshold on validation F1, and logs complete models and evaluation
plots. Start the tracking UI from this root with `.\scripts\windows\start-mlflow.ps1` and visit
http://127.0.0.1:5000. From `backend/`, use `python -m tools.services.mlflow_server`.
See [model selection evidence](backend/reports/model_selection.md) and
[MLflow screenshots](docs/README.md). Restart the API after retraining.

The authoritative customer partitions are stored in
`backend/data/splits/memberships.csv`. Both sklearn and Spark
use those saved memberships. Raw data, generated partition CSVs, model binaries,
dependency folders and runtime logs are excluded from Git.

## Spark workflow

Spark workflows use a separate Linux environment in the existing Ubuntu WSL
distribution. From this repository root:

```powershell
.\scripts\windows\start-spark.ps1 -Task setup
.\scripts\windows\start-spark.ps1 -Task train
.\scripts\windows\start-spark.ps1 -Task predict -Record examples/customer.json
.\scripts\windows\start-spark.ps1 -Task predict -InputCsv examples/customers.csv -Output artifacts/predictions/spark_batch.csv
.\scripts\windows\start-spark.ps1 -Task test
```

Use `..\scripts\windows\start-spark.ps1` when your terminal is already inside `backend/`.
Prediction paths above resolve relative to `backend/`. Spark outputs are kept
under `backend/artifacts/spark/`; both engines keep using the authoritative
saved memberships. Read [Spark setup and timing details](backend/README.md#spark-training-and-engine-comparison).
The comparison is saved as `backend/reports/engine_comparison.md` and `.json`.
The local frontend continues using the saved sklearn pipeline.

## Verification

From the repository root:

```powershell
python -m unittest discover -s backend/tests -t backend -v
npm --prefix frontend run typecheck
npm --prefix frontend run build
```

Browser tests run from `frontend/` so their configuration resolves correctly:

```powershell
cd frontend
npm run test:e2e
```

The browser checks use installed Microsoft Edge on Windows. They start local
services as needed, exercise live predictions and batch uploads, and write
screenshots to `docs/screenshots/`.

## Documentation

- [Backend setup and ML workflow details](backend/README.md)
- [Frontend setup and API integration](frontend/README.md)
- [Project roadmap and assignment requirements](OPSYNTRA_ROADMAP.md)
- [Shared documentation and screenshots](docs/README.md)
- [Latest dataset overview](backend/reports/data_overview.md)

Data preparation, sklearn training/inference, and the local interface are
implemented, including MLflow tracking and validation threshold selection.
PySpark training, persistence, inference and engine comparison are implemented.
The required Airflow ML DAG is implemented for both engines. Kafka publishing in
batch and streaming modes is implemented, with model prediction consumers and durable recovery.

## Airflow ML workflow

From this repository root, start the local Airflow UI and run either engine:

```powershell
.\scripts\windows\start-airflow.ps1 -Task start
.\scripts\windows\start-airflow.ps1 -Task run -Engine sklearn
.\scripts\windows\start-airflow.ps1 -Task run -Engine spark
```

From `backend/`, use `..\scripts\windows\start-airflow.ps1`. Open http://localhost:8080 and
select **opsyntra_ml_workflow**. The launcher waits for all eight scheduled tasks
and the DagRun to succeed, then records the workflow and candidates in MLflow.
Each run has its own data snapshot, approved saved pipeline, logs and prediction
CSV under `backend/artifacts/airflow/runs/`.

For initial setup, runtime separation, retries/timeouts, validation gates and
result paths, see [Airflow operations](backend/dags/README.md). See the
[completed-run report](backend/reports/airflow_workflow_report.md) and
[screenshots](docs/README.md) for the checkpoint evidence.

## Kafka publishing

From this repository root:

```powershell
# Already installed on this machine; use setup once on a new environment.
.\scripts\windows\start-kafka.ps1 -Task start
python backend/producer.py --mode batch --batch-size 500
python backend/producer.py --mode batch --batch-size 500 --resume
python backend/producer.py --mode streaming --events-per-sec 5
```

Stop continuous streaming with Ctrl+C. From `backend/`, use
`..\scripts\windows\start-kafka.ps1` and `python producer.py`. The WSL broker is reachable at
`127.0.0.1:9092`. Topics are `telco.raw.customers`, `telco.churn.predictions` and
`telco.deadletter`. Publisher messages use customerID keys, typed customer fields,
UTC timestamps and stable batch event IDs. Checkpoints advance after confirmed
delivery.

See [Kafka setup, event schema and resume guarantees](backend/KAFKA.md) and
[real publishing evidence](backend/reports/kafka_publishing_report.md). The check
published all 7,043 customers across a pause/resume plus 30 streaming events,
then validated the actual messages read back from Kafka.

## Kafka prediction and recovery

From the repository root:

```powershell
.\scripts\windows\start-kafka.ps1 -Task start
python backend/consumer.py --mode batch --max-messages 1000
python backend/consumer.py --mode streaming
```

Use Ctrl+C to stop streaming. The default sklearn group resumes committed Kafka
offsets and reuses its durable outbox/deduplication ledger. Predictions include
customerID, probability, label, event/processing timestamps, event ID and model
version. Invalid inputs go to `telco.deadletter`. Output delivery is confirmed
and recorded before input offsets are committed. The documented ack-before-ledger
crash window can still produce identical duplicate prediction IDs.

View actual output messages:

```powershell
python backend/tools/services/kafka_messages.py --topic predictions --max-messages 5
```

Native Spark is supported through
`.\scripts\windows\start-consumer.ps1 -Engine spark -Mode batch -MaxMessages 30`.
See [consumer commands and recovery guarantees](backend/KAFKA.md#prediction-consumer-step-7)
and [real crash/restart evidence](backend/reports/kafka_consumption_report.md).
