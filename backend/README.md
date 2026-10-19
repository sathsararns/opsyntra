# Opsyntra backend

Telco customer churn prediction and streaming ML project.

## Implemented: Steps 1–7

- Original dataset import from the supplied ZIP without changing its contents.
- Schema, customer ID, label, category, numeric and missing-value validation.
- Deterministic numeric conversion and whitespace cleanup.
- Fixed, stratified 60/20/20 train/validation/test customer memberships.
- JSON validation report, source/output checksums and runtime metadata.
- Automated data contract, reproducibility and failure-path tests.
- Complete sklearn pipelines for a dummy baseline, Logistic Regression, Random Forest and XGBoost.
- Training-only stratified cross-validation, validation-based selection and selected-model test evaluation.
- Joblib persistence of preprocessing and classifier together, plus single-JSON and batch-CSV inference.
- MLflow tracking, validation F1 thresholds, evaluation plots and model selection evidence.
- Native Spark pipelines for Logistic Regression, Random Forest and SparkXGBClassifier.
- Shared-customer engine comparison, measured execution times and saved Spark inference.

## Run locally

Run backend Python commands from `Opsyntra/backend/`. If your terminal is
already there, do not run `cd backend` again. From the repository root,
enter `backend/` once with `cd backend`. Spark PowerShell examples below
explicitly state their starting directory.

Use an environment with the dependencies in `requirements/web.txt` (which includes
the model and data dependencies). These checkpoints were verified with Python 3.14.6 and
the recorded dependency versions.
Spark uses the separately tested Ubuntu WSL environment described below.
Airflow uses its own constrained Python environment in Ubuntu WSL.

```powershell
python -m pip install -r requirements/web.txt
python -m pip install -r requirements/kafka.txt

# First run: copy only the configured CSV from your ZIP.
python -m pipelines.data_pipeline --archive "C:\Users\SATHSARA\Downloads\archive.zip"

# Subsequent runs use the preserved raw CSV.
python -m pipelines.data_pipeline

# Train all four models and save the selected complete pipeline.
python -m pipelines.training_pipeline

# Predict one raw customer. The JSON example has no Churn field.
python -m pipelines.inference_pipeline --record examples/customer.json

# Predict multiple raw customers.
python -m pipelines.inference_pipeline --input examples/customers.csv --output artifacts/predictions/example_batch.csv

# Standard-library test runner; pytest is not needed.
python -m unittest discover -s tests -t . -v
```

Paths in `config.yaml` resolve relative to that configuration file. You can use
`--config path/to/config.yaml`. The pipeline also runs from outside the repository
with `python path/to/Opsyntra/backend/pipelines/data_pipeline.py`.

## Outputs

| File | Purpose |
|---|---|
| `data/raw/WA_Fn-UseC_-Telco-Customer-Churn.csv` | Unchanged source CSV |
| `data/processed/telco_customers.csv` | Sorted, normalized data with numeric charges |
| `data/splits/memberships.csv` | Authoritative `customerID,split` membership map |
| `data/splits/train.csv` | Training customers |
| `data/splits/validation.csv` | Validation customers |
| `data/splits/test.csv` | Reserved final test customers |
| `data/splits/manifest.json` | Seed, ratios, class counts, SHA-256 hashes and versions |
| `reports/data_validation_report.json` | Data quality, feature contract and split checks |
| `artifacts/models/best_pipeline.joblib` | Selected fitted preprocessing + classifier pipeline |
| `artifacts/models/candidates/*.joblib` | Fitted pipeline for each compared model |
| `artifacts/models/model_metadata.json` | Provenance, metrics, version and reload verification |
| `reports/model_training_report.json` | CV/validation comparison and selected-model test metrics |
| `reports/model_comparison.csv` | Tabular validation metrics for all candidates |

Raw, normalized and partition CSVs are excluded from Git. The authoritative
membership CSV, JSON manifest, generated reports and source code can be versioned. The raw CSV can be
reimported from the user's original ZIP; it is not fetched from the internet.

## Data and missing-value policy

The supplied file has 7,043 customers and 21 columns. `customerID` identifies a
customer and `Churn` is the label (`No` -> 0; `Yes` -> 1 at training time). Neither
is a predictor; the feature contract contains 19 columns.

Blank `TotalCharges` strings become numeric missing values. All customers are
retained; no imputation or scaling is fitted in Step 1. The model training
pipeline fits median imputation using training data only, including inside
each cross-validation fold. The Step 1 warning records the expected missing values instead
of describing the data as fully imputed. Invalid numeric text, infinity, negative
charges, duplicate IDs, unknown training categories and missing required values
fail validation.

An empty numeric field in the generated CSV represents a missing number. Pandas
should load it as NaN; Spark must read it with an explicit numeric schema and
interpret empty numeric fields as null. Service categories such as
`No internet service` are preserved.

## Sharing partitions between engines

The pipeline sorts customers by ID and applies a seeded, two-stage stratified
split. Small rounding differences from 60/20/20 are expected because rows are
indivisible. Every customer belongs to exactly one partition.

Scikit-learn and Spark must read `memberships.csv` and join on `customerID`, then
filter `split` to `train`, `validation` or `test`. They must not call separate
random split functions. `apply_memberships` implements and validates the pandas
join; Spark will use the same map with its DataFrame join API.

The saved membership file is authoritative even if dependency versions change.
Hashes and versions in `manifest.json` allow drift to be identified. For the same
data, configuration and runtime, reruns reproduce identical artifact bytes. The
report omits wall-clock timestamps to make this check straightforward.

## Training, persistence and inference

`config.yaml` configures the four models, seed, three CV folds, small parameter
grids and paths. Training verifies the raw, normalized and membership checksums
from Step 1, then joins the saved memberships without making new random splits.

Each artifact is a single `sklearn.pipeline.Pipeline`:

```text
Raw named customer fields
  -> deterministic normalization and numeric conversion
  -> ColumnTransformer
       numeric: median imputer -> StandardScaler for Logistic Regression
       categorical: constant missing-value imputer -> one-hot encoder
  -> classifier
```

Trees and the dummy baseline omit scaling. All learned transformations are inside
the pipeline passed to GridSearchCV, so each fold learns its own statistics.
Unknown categories use the encoder's ignore policy; missing numeric values use
the fitted medians. An entirely missing numeric column at fit time keeps its
position using SimpleImputer's zero fallback. A missing predictor column is an
input error; blank values within an existing column can be imputed.

The input transformer selects exactly 19 predictors, independent of column order,
and ignores identifiers, labels and extra metadata. The example JSON and CSV have
no Churn column. Customer IDs are optional for prediction and preserved in output
when supplied. Invalid numeric strings, negative numbers and infinity are rejected.

Each model is tuned using training-only CV. The fitted best candidate for each
model is scored on validation data; highest validation average precision wins.
Only the selected model is assessed on test data. Average precision is a
non-interpolated precision-recall summary, not trapezoidal PR-AUC. Also reported:
churn-class precision/recall/F1, ROC-AUC, accuracy, and a confusion matrix with
rows/columns ordered No, Yes.

Each non-dummy classifier uses a threshold chosen to maximize validation F1.
Ties prefer the threshold closest to 0.5, then the higher threshold. The dummy
retains its native most-frequent baseline. A `FixedThresholdClassifier` wraps a
`FrozenEstimator`, so threshold installation never refits the learned model.
Both `pipeline.predict` and all inference interfaces use this saved rule.
No preprocessing or classifier is fitted on validation/test records. The artifact embeds
model/version, feature schema, selected parameters, pipeline description, runtime
and dataset/membership hashes. Joblib reload checks require probabilities within
1e-12 absolute tolerance and identical classifications.

The integration tests launch fresh Python processes for both inference commands
and compare their results with in-memory predictions. `--model path/to/model.joblib`
overrides the default artifact. For one record, `--output result.json` optionally
saves JSON; batch CSV input requires an output CSV. Output-path checks protect
input, configured data artifacts and the loaded model from accidental overwrites.

The CLI supports a source checkout without an editable install. To import
`opsyntra` directly from your own programs/notebooks, install with
`python -m pip install -e .` or add this repository's `src` to PYTHONPATH.

Reference: [Scikit-learn guidance on preprocessing and data leakage](https://scikit-learn.org/stable/common_pitfalls.html).

## Layout and next checkpoint

The project also includes a local **Next.js + Tailwind CSS frontend** with live
customer and CSV batch predictions, dataset charts, model comparisons and data
quality views. See [frontend/README.md](../frontend/README.md) for setup details.

After installing `requirements/web.txt` and running `npm ci` inside the sibling
`frontend/` folder, launch both services from this backend directory with:

```powershell
..\start-frontend.ps1
```

Open http://127.0.0.1:3000. The frontend connects to the saved Python pipeline
through a local FastAPI service; prediction responses are produced by the actual
model. Refresh the labelled offline snapshot after retraining with
`python -m pipelines.frontend_snapshot`.

Reusable code is in `src/opsyntra/`; `pipelines/` contains runnable workflows.
`notebooks/` is reserved for exploration; `dags/` contains the working eight-task ML DAG.
`tests/` verifies the implemented data workflow and `reports/` stores evidence.

Airflow, Kafka publishing, prediction consumption and durable recovery are implemented. The complete project plan
is in [OPSYNTRA_ROADMAP.md](../OPSYNTRA_ROADMAP.md). Shared frontend screenshots
are in [docs/screenshots/](../docs/screenshots/).

## MLflow tracking and model selection

Install `requirements/ml.txt` (or `requirements/web.txt`) and run
`python -m pipelines.training_pipeline`. Each invocation produces four runs in
the `Opsyntra-Telco-sklearn` experiment with a shared `batch_id` tag. Run logging
uses `backend/mlflow.db` and `backend/mlartifacts/`; both are local and ignored
by Git. Configured paths resolve relative to `config.yaml`, including in tests.
Training does not require the MLflow server to be running.

From `backend/`, open the UI with:

```powershell
python -m pipelines.mlflow_server
```

From the repository root, use `.\start-mlflow.ps1` instead. Visit
http://127.0.0.1:5000, open the experiment, select the four runs from a batch,
and click **Compare**. Use `validation_average_precision` for model ranking;
`validation_f1`, `validation_recall` and `decision_threshold` explain the
operating point. `validation_default_*` metrics preserve the original rule
for comparison. Only the winner has `test_*` metrics.

Each run stores classifier parameters, CV search results, preprocessing settings
and learned medians/categories, dataset and split SHA256 identifiers, config,
training/evaluation timing, raw-record input signature/example, a complete
MLflow sklearn model and the Joblib pipeline. Artifacts include ROC,
precision–recall, confusion-matrix and threshold/F1 plots. The selected run
also includes held-out test plots. Models use cloudpickle to preserve the custom
input transformer; load only artifacts created by a trusted source.
Random Forest uses the configured threads during CV fitting and one prediction
thread afterward, keeping probability sums stable at the chosen threshold.
Plot folders are grouped by batch so evidence from earlier selections stays separate.

Read [reports/model_selection.md](reports/model_selection.md) for the choice,
threshold tradeoff and final test metrics. `reports/model_comparison.csv` and
`reports/model_training_report.json` provide numeric evidence and run IDs.
Validation F1 tuning is a starting objective; use business retention costs to
choose a different policy when those costs are available. Validation metrics
can be optimistic after selection, so report held-out test performance.

After retraining, restart the API to load the new artifact and run
`python -m pipelines.frontend_snapshot` to refresh the offline dashboard evidence.

References: [MLflow local database tracking](https://www.mlflow.org/docs/latest/ml/tracking/tutorials/local-database/),
[MLflow sklearn model logging](https://mlflow.org/docs/latest/api_reference/python_api/mlflow.sklearn.html),
[scikit-learn threshold tuning](https://scikit-learn.org/stable/auto_examples/model_selection/plot_tuned_decision_threshold.html).

## Spark training and engine comparison

The Windows launcher uses your Ubuntu WSL installation with Java 21 and an
isolated Linux Python environment, `backend/.venv-spark`. XGBoost's Spark
integration is tested on Linux. The pinned Spark runtime is PySpark 4.0.2;
`requirements/spark.txt` installs the CPU XGBoost build on Linux.

From the repository root:

```powershell
# One-time dependency setup in an existing Ubuntu WSL distribution.
.\start-spark.ps1 -Task setup

# Train Logistic Regression, Random Forest and SparkXGBClassifier.
# This also generates the engine comparison and records Spark runs in MLflow.
.\start-spark.ps1 -Task train

# Load the saved Spark pipeline in a fresh process; Churn is not required.
.\start-spark.ps1 -Task predict -Record examples/customer.json
.\start-spark.ps1 -Task predict -InputCsv examples/customers.csv -Output artifacts/predictions/spark_batch.csv

# Execute the Spark integration checks in the same isolated runtime.
.\start-spark.ps1 -Task test
```

When your terminal is already inside `backend/`, use `..\start-spark.ps1`.
Record/input/output paths supplied to the launcher are relative to `backend/`.
The setup task installs Ubuntu Java and Python venv support, then Python
dependencies. It requires the existing Ubuntu WSL distribution and internet
access. The application frontend continues using its sklearn model.

The Spark pipeline performs deterministic trimming and numeric validation,
training-fitted median imputation, alphabetical StringIndexer encoding,
one-hot encoding, feature assembly, numeric scaling for Logistic Regression,
classification, and the saved validation-selected threshold. The custom
normalization and decision stages persist through Spark's native PipelineModel
writer; they execute Spark SQL expressions rather than row-wise Python UDFs.
Unknown categories map to an all-zero one-hot block, matching the sklearn policy.
Identifiers and Churn are excluded from assembled predictors.

The workflow verifies the existing data hashes, reads `memberships.csv` in Spark,
joins by customerID, and checks the exact customer IDs in each partition.
Stratified training-only CV learns preprocessing within every fold. The model
with the highest validation average precision wins; validation F1 selects its
threshold. Only that champion receives held-out test metrics.

Outputs:

- `artifacts/spark/runs/<batch_id>/<model>/`: complete native Spark pipelines.
- `artifacts/spark/selected_model.json`: current selected pipeline and version.
- `reports/spark_training_report.json`: CV, parameters, preprocessing, metrics,
  reload checks and timing.
- `reports/engine_comparison.md` and `.json`: both engine champions on exactly
  the same test customers, with comparable metric definitions and timing notes.
- `reports/engine_validation_comparison.csv`: validation results for all models.
- `reports/engine_test_predictions.csv`: paired, ID-aligned held-out predictions.

Both engines use churn-class precision, recall and F1, accuracy, ROC-AUC and
non-interpolated average precision. Spark weighted F1 and interpolated areaUnderPR
are not substituted. Spark and sklearn have different solvers, regularization
conventions, tree implementations and tuning grids; the report states these
limits, plus median and scaler differences.

Timing separates Spark JVM startup, data loading/cache materialization, CV,
final preprocessing, classifier fitting, persistence/loading and inference.
Inference collects actual probabilities and predictions, forcing lazy Spark
execution. A fresh single final sklearn fit is timed in the same Linux process;
its earlier CV duration is not compared against a single Spark fit. The first
inference action is separate from repeated warm measurements. These local[2]
measurements do not demonstrate multi-node scaling.

After WSL training, the Windows launcher logs the Spark runs and native pipeline
directories into `Opsyntra-Telco-Spark` using the existing Windows MLflow store.
This keeps artifact paths accessible through http://127.0.0.1:5000. If training
finished but logging was interrupted, run `python -m pipelines.spark_tracking`
from `backend/`; an already-recorded batch is not duplicated.
Each run also contains the custom stage modules under `code/opsyntra` and pinned
requirements under `environment/`. A downloaded native pipeline needs that code
package available on Python's import path, along with its Spark/XGBoost runtime.

In a native Linux environment, use the same requirements and install
`mlflow==3.17.1 matplotlib==3.11.2` for tracking, then run
`python -m pipelines.spark_training_pipeline` from `backend/`. For training in
WSL with Windows-owned MLflow storage, use the PowerShell launcher or pass
`--skip-tracking` and run the host logging command afterward.

References: [Spark ML pipelines](https://spark.apache.org/docs/4.0.2/ml-pipeline.html),
[Spark installation](https://spark.apache.org/docs/4.0.2/api/python/getting_started/install.html),
[XGBoost Spark integration and persistence](https://xgboost.readthedocs.io/en/stable/tutorials/spark_estimator.html).

## Airflow orchestration

The eight-task DAG supports sklearn and Spark through its engine parameter,
with immutable input snapshots, saved membership reuse, training-only tuning,
validation gates, saved-model inference and run-specific results. It includes
retries, timeouts and per-attempt logs.

From this backend directory:

```powershell
..\start-airflow.ps1 -Task start
..\start-airflow.ps1 -Task run -Engine sklearn
..\start-airflow.ps1 -Task run -Engine spark
```

Open http://localhost:8080. On a new environment first run
`..\start-airflow.ps1 -Task setup` after setting up the Spark ML runtime.
Read [DAG operations and outputs](dags/README.md) and
[successful execution evidence](reports/airflow_workflow_report.md).
The launcher records successful workflow/candidate runs in **Opsyntra-Airflow**
in the existing Windows MLflow store.

## Kafka batch and streaming publisher

One `producer.py` supports both publishing modes. Start the local WSL broker and
its three required topics from this backend directory:

```powershell
..\start-kafka.ps1 -Task start
python producer.py --mode batch --batch-size 500
python producer.py --mode batch --batch-size 500 --resume
python producer.py --mode streaming --events-per-sec 5
```

Ctrl+C finishes queued deliveries. The publisher validates customer records,
uses customerID keys and UTC event timestamps, retains blank TotalCharges as
null, and checkpoints only acknowledged chunks. Stable batch event IDs permit
consumer deduplication across the delivery/checkpoint crash window.

See [Kafka runtime, CLI examples and delivery semantics](KAFKA.md) and
[verified live-broker evidence](reports/kafka_publishing_report.md).

## Kafka prediction consumer

One `consumer.py` supports batch/count or offset-window consumption and continuous
streaming. Each process loads the selected complete pipeline once. From this
backend directory:

```powershell
..\start-kafka.ps1 -Task start
python consumer.py --mode batch --max-messages 1000
python consumer.py --mode streaming
python -m pipelines.kafka_messages --topic predictions --max-messages 5
```

Restart with the same group ID and state directory to resume. Predictions and
deadletters are acknowledged and recorded in a durable local ledger before input
offsets are synchronously committed. An outbox preserves pending payloads.
The remaining output-ack/local-ledger crash window is documented and tested;
downstream duplicate handling uses stable prediction IDs.

For native Spark use
`..\start-consumer.ps1 -Engine spark -Mode batch -MaxMessages 30`.
See [consumer setup, batch windows and recovery limits](KAFKA.md#prediction-consumer-step-7)
and [the completed live-broker checkpoint](reports/kafka_consumption_report.md).

## Final verification and Kafka Airflow bonus

From this backend directory:

```powershell
python -m pipelines.verification
..\start-airflow.ps1 -Task run-kafka -KafkaKind batch -Engine sklearn
..\start-airflow.ps1 -Task run-kafka -KafkaKind streaming -Engine sklearn -DurationSeconds 30 -DemoEvents 20
```

The bonus batch DAG has a daily schedule and acknowledged checkpoint/resume;
the streaming DAG owns and monitors a live consumer until graceful shutdown.
Each freezes its source/model, uses isolated groups and keeps run-specific logs,
recovery state and a prediction CSV. Both DAGs start paused to keep scheduling
explicit. See [the reproduction guide](../docs/VERIFICATION.md) for the native
Spark tests, stop controls and screenshot commands, and the
[evidence matrix](reports/submission_evidence.md) for results and rubric mapping.
