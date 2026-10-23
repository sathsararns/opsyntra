# Opsyntra frontend

A local Next.js App Router + React + TypeScript + Tailwind CSS workspace for the
existing Telco churn project. The Python API uses the same saved Joblib pipeline
as CLI inference. No model is reimplemented in JavaScript.

## Run

Install Python dependencies from the repository root:

```powershell
python -m pip install -r backend/requirements/web.txt
```

Install the locked frontend dependencies:

```powershell
cd frontend
npm ci
cd ..
```

Then start both services from the repository root:

```powershell
.\scripts\windows\start-frontend.ps1
```

Open http://127.0.0.1:3000. The API binds to 127.0.0.1:8000. The launch script
refreshes the snapshot, checks API readiness, reuses a healthy API, and stops only
the API process it created when the frontend exits. Logs are written to `backend/logs/`.

Alternatively, run two terminals:

```powershell
# Terminal 1, repository root
python -m uvicorn opsyntra.web_api:app --app-dir backend/src --host 127.0.0.1 --port 8000

# Terminal 2
cd frontend
npm run dev
```

If PowerShell restricts script execution, use the two-terminal commands.

## Features

- Overview with real dataset totals, tenure/contract charts and saved model metrics.
- Customer form covering the 19 predictors, optional customer ID, numeric missing
  values and example input. `Churn` is never required.
- Batch CSV upload with drag/drop, validation, bounded batch sizes, searchable and
  paginated results, and downloadable prediction CSVs.
- Model comparison and selected-model confusion matrix.
- Data quality checks and reproducible partition summaries.
- Browser-local recent predictions with a clear-history control.
- Responsive sidebar navigation and a workspace setup guide.
- Live monitoring with configurable polling, original-event segments, recent and
  highest-risk predictions, engine/model filters, full event/prediction IDs and
  separate database, broker, inactive-consumer and no-activity states.

For the live stream screen install `backend/requirements/analytics-web.txt` and
follow [Live monitoring](../docs/LIVE_MONITORING.md) to start Kafka, PostgreSQL
and both consumers. Direct form/CSV predictions remain independent of streaming.

The initial dashboard snapshot is generated from the actual dataset and reports.
When the API is offline, it is explicitly labelled as a saved snapshot; prediction
forms cannot return fabricated results. The overview refreshes from the API on
load/reconnect. API failures and invalid inputs are displayed to the user.

After retraining, restart the Python API to reload the model. Refresh the saved
offline snapshot from the backend directory:

```powershell
cd backend
python -m pipelines.frontend_snapshot
```

## Connection and architecture

The browser calls same-origin `/api/...` Next.js route handlers. Those proxy a
fixed allowlist to the Python API, using the server-only `ML_API_URL` environment
variable (default `http://127.0.0.1:8000`). To override it, copy `.env.example` to
`.env.local` in this folder. No CORS wildcard or browser credential is needed.

Endpoints: GET `/api/health`, GET `/api/overview`, POST `/api/predict` with
`{"record": {...}}`, POST `/api/predict/batch` with `{"csv_text": "..."}`.
Analytics adds GET `/api/analytics/summary`, `/timeseries`, `/predictions`,
`/high-risk`, and `/health` under the same `/api/analytics` prefix; query strings
are forwarded by the server proxy. PostgreSQL credentials remain in Python.
Batches are limited to 2 MB and 10,000 rows. The API runs tree inference with one
worker for small local requests; it does not change or retrain the artifact.

This is a local development application. Authentication and external hosting are
outside its current scope. The service is bound to loopback by default.

## Checks

```powershell
cd frontend
npm run typecheck
npm run build

# Headless Edge tests on Windows; builds must be current.
npm run test:e2e

# From the repository root
cd ..
python -m unittest discover -s backend/tests -t backend -v
```

No remote font or image download is needed to render or build the interface.
Browser tests use installed Microsoft Edge on Windows. They start and stop their
own local API/frontend servers, cover six primary flows, and save desktop/mobile
screenshots under the repository's `docs/screenshots/` directory.
