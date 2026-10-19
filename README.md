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
`backend/data/splits/memberships.csv`. Both sklearn and the future Spark workflow
use those saved memberships. Raw data, generated partition CSVs, model binaries,
dependency folders and runtime logs are excluded from Git.

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
PySpark, Airflow and Kafka are the next roadmap phases.
