# Reproducible notebooks

Phase 5 adds three executed notebooks and their exported CSV/PNG/SVG artifacts.
The notebooks do not fit models or tune on held-out test data. Their exported
manifests record input/output SHA-256 hashes; the SQL notebook also records the
database snapshot time and exact engine/model/event-time scope.

## Setup and run

From the repository root, use the Python environment containing the existing
sklearn models and API dependencies:

```powershell
python -m pip install -r backend/requirements/notebooks.txt
.\scripts\windows\start-analytics.ps1 -Task start
cd backend
python -m pipelines.notebook_check
```

The verifier executes every code cell in a fresh kernel using the **same Python
executable** that invokes it, then saves the executed notebooks in
`backend/notebooks/`. Kernel specifications are temporary files under ignored
`backend/artifacts/notebook_runtime/`; no global kernel installation is needed.
It also runs five input/real PostgreSQL/dashboard-agreement checks, verifies
export hashes and scans generated output for configured database passwords.
Tests require the separately configured analytics test database; they never
truncate or seed the application database. Existing PostgreSQL roles/migrations
must already be provisioned; see [storage setup](ANALYTICS_STORAGE.md).

To explore manually, open the `.ipynb` files in your notebook editor, select the
Python environment containing `requirements/notebooks.txt`, and run all cells.
The path bootstrap supports a working directory at the repository, backend or
notebooks folder. Manual notebook execution only needs the analytics **reader**
login; owner/writer credentials are used only by the separate database tests.

The notebooks require these existing project inputs:

| Notebook | Inputs |
|---|---|
| `01_telco_eda.ipynb` | `config.yaml`, source CSV for checksum only, saved memberships/manifest, `data/splits/train.csv` |
| `02_model_comparison.ipynb` | Saved sklearn/Spark/comparison JSON reports, validation CSV/memberships, checksum-matching sklearn candidate Joblib files, native Spark `*_validation_scores.csv` files beside its pipeline directories |
| `03_streaming_analytics.ipynb` | PostgreSQL reader credentials and existing analytics views/data; no Kafka broker, running consumer, API server, model or Spark JVM is needed to read the stored snapshot |

Raw data and model artifacts are excluded from Git. On a new checkout, obtain
the supplied dataset and follow the [main setup](../README.md) to prepare splits
and sklearn artifacts, then rebuild native Spark and the comparison report with
`..\scripts\windows\start-spark.ps1 -Task train` from `backend/`. This prepares the Spark saved
validation scores as well. The model notebook validates dataset/split hashes,
champion versions and thresholds across reports; if training has changed,
regenerate the matching comparison rather than mixing old and new runs.
Source/report filenames follow this project's `backend/reports/` conventions.

To populate SQL on a new setup, use [the live monitoring guide](LIVE_MONITORING.md)
to run producer, prediction consumer and analytics consumer. Notebooks do not
publish Kafka messages or create synthetic database fixtures themselves.

## 01: training-only EDA

The notebook reads only the persisted training feature rows (4,225 in the current
dataset). The full raw file is hashed for source provenance, not loaded for EDA.
Training IDs are checked against `memberships.csv`; changed inputs fail checksums.
Neither `customerID` nor `Churn` is treated as a predictor.

Outputs cover class balance, numeric distributions/descriptive statistics,
missing values, and observed contract/tenure associations with group sizes.
The original source contains 11 blank `TotalCharges` values; **8 are in training**.
Keep these customers and preserve their nulls. Model pipelines learn median
imputation inside training/CV folds. The notebook displays descriptive training
medians without fitting preprocessing or filling any rows. Associations are not
claims of causal effects.

## 02: saved model comparison

The notebook reads the existing reports for all four sklearn candidates and
three Spark candidates. It shows validation precision/recall/F1/ROC-AUC/AP,
saved decision thresholds and the recorded validation-F1 threshold curves.
Champions remain those selected by validation average precision.

Validation ROC/PR curves use inference from verified sklearn artifacts and
previously saved Spark validation score CSVs; the customer IDs/labels and
recomputed AP/ROC-AUC must match their saved reports. No model is refitted and
no Spark session starts. PR curves use step plotting; average precision is the
recorded sklearn metric definition, not trapezoidal PR area.

The only final test metrics shown are the already recorded assessments for each
engine's champion. The notebooks do not read test feature CSVs or use final test
results to change candidates/thresholds. Timing tables reuse the existing local
benchmark, separating a single final fit from CV, startup/load, first inference
and warm inference. They do not rerun the benchmark or claim distributed scaling.

## 03: PostgreSQL snapshot and dashboard agreement

Credentials are loaded through `AnalyticsConfig` from the process environment
or ignored `backend/.env`. Only `OPSYNTRA_ANALYTICS_READ_URL` is required for
notebook reads. Do not place connection strings in cells or notebook output.
Connection failures are sanitized before appearing in saved notebook errors.

Optional scope variables, set before launching the notebook/kernel:

```powershell
$env:OPSYNTRA_NOTEBOOK_ENGINE = 'sklearn'
$env:OPSYNTRA_NOTEBOOK_MODEL_VERSION = 'd9a13b4cf6839ed4' # choose an available version
$env:OPSYNTRA_NOTEBOOK_AS_OF = '2026-10-20T15:00:00Z'
$env:OPSYNTRA_NOTEBOOK_HOURS = '24'
python -m pipelines.notebook_check
```

Without overrides, it selects the latest ingested model for sklearn and the last
24 **complete UTC hours**. `AS_OF` rounds down to a UTC hour; `HOURS` must be 1–744.
The current partial hour is excluded so existing hourly views exactly match the
selected event-time range. An empty selection produces genuine zero volumes,
null mean risk/share and clearly empty tables.

One read-only repeatable-read transaction queries `prediction_details`,
`prediction_hourly`, `prediction_hourly_segments`, physical-delivery provenance
and rejection quarantine. The notebook exports summary, hourly time buckets,
original-event contract/payment segments, risk bands, recent
traceable predictions, late events and duplicate-delivery statistics. The chart
keeps missing risk/share gaps rather than inventing a zero value.

Hourly, segment and risk totals are checked against a direct logical prediction
count. The real PostgreSQL integration test also compares the snapshot with the
actual dashboard API handler using identical engine/model/start/end bounds,
including out-of-order raw arrival, two deliveries of one prediction, a second
engine and an empty model selection.

The exported manifest contains `dashboard_query`; append that path to
`http://127.0.0.1:3000` or `http://127.0.0.1:8000` while the application is running
to query the same scope. The frontend's default rolling 24-hour window includes
the partial current hour and can have different counts. Late commits can change
an older event-time range after the notebook's snapshot; compare snapshot times
as well as query bounds before interpreting a difference.

Terms and limits:

- Prediction volume counts logical `prediction_id` values. Repeated legitimate
  events for a customer remain separate; summed hourly distinct-customer counts
  are not a window's distinct-customer count.
- Profiles join by `event_id`, so delayed raw arrival can repair the matching
  historical segment. Missing profiles remain included in totals.
- A late event here means ingestion over one hour after event time. That is an
  explicit operational threshold; negative latency is separately reported as
  clock skew. Query-time views recompute affected buckets without incrementing
  a counter for every physical delivery.
- Repeated deliveries are extra retained Kafka offsets for a logical prediction.
  This cannot observe duplicates suppressed upstream by the inference ledger.
- Quarantine counts cover all engines/models by quarantine ingestion time;
  malformed records cannot safely be attributed to the selected model.
- **Synthetic streaming replay is a simulation, not evidence of real company
  churn trends.** Predicted churn share is not observed churn; mean risk is not
  accuracy. Volume bursts can reflect replay/backlog processing.

## Outputs and reproducing a captured snapshot

Exports are under `backend/reports/notebooks/01_telco_eda/`,
`02_model_comparison/` and `03_streaming_analytics/`. Each notebook writes CSV
tables, PNG/SVG figures and `manifest.json`. Executed notebook outputs are kept
alongside their source cells. The runner records package versions, executed cell
counts, input/output checks, SQL test evidence and artifact hashes in
[the verification report](../backend/reports/notebook_verification.md).

Training/model analyses reproduce from identical checksum-matching inputs. Live
database contents can grow, including late inserts into old event-time windows;
an AS_OF alone does not restore an older database snapshot. For the precise
captured charts, read the exported CSVs rather than querying a changed database:

```python
from pathlib import Path
import pandas as pd
captured = Path('reports/notebooks/03_streaming_analytics')  # cwd: backend/
hourly = pd.read_csv(captured / 'hourly_gap_filled.csv', parse_dates=['bucket_start'])
hourly.plot(x='bucket_start', y='prediction_volume', kind='bar')
```

Copy the exported directory before rerunning if you need to retain that exact
capture. The verifier refreshes current outputs; earlier input/output hashes
alone cannot recreate a mutable database's old contents.
