# Opsyntra

A local Telco customer churn workspace with a Next.js + Tailwind frontend and a
Python API backed by reproducible ML pipelines.

## Repository structure

```text
Opsyntra/
  frontend/                Next.js, Tailwind, browser tests and public assets
  backend/
    src/opsyntra/          Reusable Python code, model inference and FastAPI
    pipelines/             Data preparation, training, inference and UI snapshot
    dags/                  Airflow workflows
    notebooks/             Exploration and engine comparisons
    tests/                 Python unit and integration tests
    data/                  Raw, processed and partition data
    artifacts/             Saved models and generated predictions
    reports/               Data validation and model evaluation reports
    examples/              Label-free single-customer and CSV examples
    requirements/          Pinned Python dependencies
    logs/                  Local API logs
    config.yaml            Backend paths and model configuration
    pyproject.toml         Python package configuration
    README.md              Detailed backend instructions
  docs/screenshots/        Shared interface screenshots
  .gitignore
  OPSYNTRA_ROADMAP.md
  README.md
  start-frontend.ps1        Starts both local services
  start-mlflow.ps1          Opens the local experiment tracking UI
  start-spark.ps1           Runs Spark setup, training, inference and checks in Ubuntu WSL
  start-airflow.ps1         Starts Airflow and runs the complete ML DAG in Ubuntu WSL
  start-kafka.ps1           Starts the local Kafka broker, creates topics and verifies publishing
```

The Git repository remains at this root. `.review-tools/` is an ignored local
PDF-reading utility; it is separate from the application.

## Start the application

Install dependencies from the repository root:

```powershell
python -m pip install -r backend/requirements/web.txt
npm --prefix frontend ci
```

The API expects the data reports and model artifacts described below. Once those
are available, start both services with:

```powershell
.\start-frontend.ps1
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
python -m pipelines.frontend_snapshot
```

The API and CLI use `backend/artifacts/models/best_pipeline.joblib`. This is one
complete sklearn pipeline, including input conversion, learned preprocessing and
classification. Model selection uses validation data after training-only CV;
the chosen model is assessed on held-out test data.
Each training invocation records four comparable MLflow runs, tunes the
decision threshold on validation F1, and logs complete models and evaluation
plots. Start the tracking UI from this root with `.\start-mlflow.ps1` and visit
http://127.0.0.1:5000. From `backend/`, use `python -m pipelines.mlflow_server`.
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
.\start-spark.ps1 -Task setup
.\start-spark.ps1 -Task train
.\start-spark.ps1 -Task predict -Record examples/customer.json
.\start-spark.ps1 -Task predict -InputCsv examples/customers.csv -Output artifacts/predictions/spark_batch.csv
.\start-spark.ps1 -Task test
```

Use `..\start-spark.ps1` when your terminal is already inside `backend/`.
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
batch and streaming modes is implemented; prediction consumption is next.

## Airflow ML workflow

From this repository root, start the local Airflow UI and run either engine:

```powershell
.\start-airflow.ps1 -Task start
.\start-airflow.ps1 -Task run -Engine sklearn
.\start-airflow.ps1 -Task run -Engine spark
```

From `backend/`, use `..\start-airflow.ps1`. Open http://localhost:8080 and
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
.\start-kafka.ps1 -Task start
python backend/producer.py --mode batch --batch-size 500
python backend/producer.py --mode batch --batch-size 500 --resume
python backend/producer.py --mode streaming --events-per-sec 5
```

Stop continuous streaming with Ctrl+C. From `backend/`, use
`..\start-kafka.ps1` and `python producer.py`. The WSL broker is reachable at
`127.0.0.1:9092`. Topics are `telco.raw.customers`, `telco.churn.predictions` and
`telco.deadletter`. Publisher messages use customerID keys, typed customer fields,
UTC timestamps and stable batch event IDs. Checkpoints advance after confirmed
delivery.

See [Kafka setup, event schema and resume guarantees](backend/KAFKA.md) and
[real publishing evidence](backend/reports/kafka_publishing_report.md). The check
published all 7,043 customers across a pause/resume plus 30 streaming events,
then validated the actual messages read back from Kafka.
