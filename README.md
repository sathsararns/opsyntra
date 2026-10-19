# Opsyntra

Telco customer churn prediction and streaming ML project.

## Implemented: Steps 1 and 2

- Original dataset import from the supplied ZIP without changing its contents.
- Schema, customer ID, label, category, numeric and missing-value validation.
- Deterministic numeric conversion and whitespace cleanup.
- Fixed, stratified 60/20/20 train/validation/test customer memberships.
- JSON validation report, source/output checksums and runtime metadata.
- Automated data contract, reproducibility and failure-path tests.
- Complete sklearn pipelines for a dummy baseline, Logistic Regression, Random Forest and XGBoost.
- Training-only stratified cross-validation, validation-based selection and selected-model test evaluation.
- Joblib persistence of preprocessing and classifier together, plus single-JSON and batch-CSV inference.

## Run locally

Use an environment with the dependencies in `requirements/ml.txt` (which includes
the data dependencies). These checkpoints were verified with Python 3.14.6 and
the recorded dependency versions.
Spark and Airflow compatibility will be established in their separate environments
when those phases are implemented.

```powershell
python -m pip install -r requirements/ml.txt

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
with `python path/to/Opsyntra/pipelines/data_pipeline.py`.

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

Classification uses the fitted classifier's native predict rule. This checkpoint
does not tune a threshold or refit on validation/test records. The artifact embeds
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

Reusable code is in `src/opsyntra/`; `pipelines/` contains runnable workflows.
`notebooks/` and `dags/` contain placeholders for upcoming EDA and orchestration.
`tests/` verifies the implemented data workflow and `reports/` stores evidence.

Next: integrate MLflow experiment tracking with the working training and inference
pipelines. The complete project plan
is in [OPSYNTRA_ROADMAP.md](OPSYNTRA_ROADMAP.md).
