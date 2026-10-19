# Opsyntra: Telco Churn ML and Kafka Project Roadmap

This plan is based on both supplied assignment PDFs, the supplied CSV, and the lecture project reviewed earlier. Steps 1–6 are implemented: data validation, fixed splits, sklearn and Spark pipelines, training-only CV, validation model/threshold selection, saved-model inference, MLflow tracking and a measured engine comparison. See README.md and the reports in backend/reports/. The required eight-task Airflow DAG is implemented for both engines; Kafka publishing is implemented; the prediction consumer and later phases remain planned.

## 1. Requirements and priorities

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

## 2. Architecture

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

Start with Docker Compose for local services and a pinned Linux runtime for Spark and Airflow. On Windows, use Docker Desktop with WSL2. Choose and smoke-test a compatible Python/Java/Spark/Airflow combination before implementing model code. Keep Airflow dependencies separate from ML dependencies when necessary.

## 3. Repository layout

The repository separates the Next.js frontend from the Python API and ML workflows.
Python source names and paths in the later build phases are relative to `backend/`.
MLflow tracking and local Spark workflows are implemented. Airflow orchestration and Kafka publishing are implemented; consumer and bonus Kafka DAG work remain planned.

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
  docs/
    screenshots/
  .gitignore
  README.md
  OPSYNTRA_ROADMAP.md
  start-frontend.ps1           # Starts both local services
```

Future producer.py, consumer.py, Spark modules and Airflow DAGs belong inside
`backend/`. Shared deployment configuration can live at the repository root.

Use the lecture's separation of reusable components, pipelines and services. Build Telco-specific schemas and transformations instead of copying its bank features or hard-coded feature ordering.

## 4. Ordered build phases

### Phase 1: Environment and data contract

- Create the repository skeleton, configuration, logging and reproducible dependency files.
- Confirm local MLflow, Spark and Airflow smoke tests; bring up Kafka when its phase begins.
- Preserve the CSV and record its checksum and schema.
- Verify the inspected dataset facts: 7,043 rows; 21 columns; 1,869 churners (26.54%); 11 blank TotalCharges entries; unique customerID values.
- Define prediction inputs separately from labels and event metadata. customerID is an identifier; Churn is the target; event_ts is metadata. None is a model feature.
- Convert blank TotalCharges to missing numeric values. Keep the imputation policy explicit; learn a median from training data initially. Retain meaningful service categories such as 'No internet service'.
- Persist a stratified 60/20/20 train/validation/test membership manifest using customerID. Reuse those exact records in both engines.

Done when: validation reports row counts, target balance and missing values, and both engines can read the same split memberships.

### Phase 2: Scikit-learn training and inference

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

### Phase 3: MLflow tracking

Implemented: local SQLite tracking, four comparable candidate runs per batch,
complete sklearn/Joblib artifacts, validation AP model selection and validation
F1 thresholds. See `backend/reports/model_selection.md`, `model_training_report.json`,
and `docs/screenshots/mlflow-*.png`. Start the UI with `.\start-mlflow.ps1`.

- Add tracking as soon as the first baseline runs; do not wait until every model is finished.
- Log model parameters, seed, preprocessing configuration, split identifier, data checksum, metrics and timing.
- Log ROC/PR plots, confusion matrices, evaluation JSON and the complete fitted model artifact.
- Compare several meaningful runs: baseline, logistic regression, random forest and tuned candidates.
- Capture readable screenshots of the experiment table, an individual run, metrics/artifacts and model details.

Done when: a reviewer can identify the selected run and reproduce why it was selected.

### Phase 4: PySpark reconstruction and comparison

Implemented in Ubuntu WSL with Java 21, Python 3.12 and PySpark 4.0.2: native
Logistic Regression, Random Forest and SparkXGBClassifier pipelines. Training
reuses saved memberships and fits preprocessing within training-only folds.
Complete PipelineModels, validation thresholds, raw-record/CSV inference and
MLflow artifacts are saved. Read `backend/reports/engine_comparison.md` for the
same-customer test metrics and measured local timings. Use `start-spark.ps1`.

- Implement schema conversion and equivalent cleaning with Spark DataFrame APIs.
- Construct Spark ML pipelines with Imputer, StringIndexer/OneHotEncoder, VectorAssembler, optional scaling and estimators as appropriate.
- Handle unknown categories explicitly and use the same raw feature contract and split membership as sklearn.
- Train Spark LogisticRegression and RandomForestClassifier first. Include XGBoost through its Spark integration to cover the algorithms listed in the brief; it is not a built-in MLlib estimator.
- Save/reload the fitted PipelineModel and demonstrate inference on new records.
- Compare both engines on the same held-out customers using the same metric definitions. Spark's weighted F1 is not the same as churn-class F1; calculate comparable positive-class metrics.
- Report preprocessing, fit, inference and total wall-clock times separately. State whether Spark startup is included; trigger actual execution when timing lazy Spark transformations.
- Describe local Spark honestly. This small dataset may show startup overhead rather than a speed advantage; a local run does not demonstrate multi-node performance.

Done when: both engines have persisted artifacts, working inference and a fair comparison report.

### Phase 5: Required Airflow ML DAG

Implemented with Airflow 3.3.2 in an isolated Ubuntu WSL environment. The actual
scheduler runs all eight tasks for sklearn or Spark, reusing saved memberships
and keeping fitted preprocessing within training. Each run freezes its inputs,
checks candidate validation/integrity, saves its selected complete pipeline,
loads it for batch predictions, and writes results with per-attempt logs.
See `backend/dags/README.md`, `backend/reports/airflow_workflow_report.md` and
`docs/screenshots/airflow-*.png`. Use `start-airflow.ps1`.

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

### Phase 6: Kafka producer

Implemented: Apache Kafka 4.3.1 in Ubuntu WSL, pinned Windows Python client,
three required topics, and one producer.py with continuous sampling/rate control
and acknowledged chunk/checkpoint/resume. Events have customerID keys, typed
customer fields, UTC timestamps and stable batch event IDs. The real broker check
verified 7,043 batch records across two processes plus 30 streaming records.
See `backend/KAFKA.md` and `backend/reports/kafka_publishing_report.md`.
Crash-window delivery is documented as at-least-once with stable IDs for future
consumer deduplication.

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

### Phase 7: Kafka consumer and recovery

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

### Phase 8: Automated tests and evidence

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

### Phase 9: Kafka Airflow bonus and optional lecture extensions

- Batch DAG: health checks -> batch producer -> bounded batch consumer -> summary.
- Streaming DAG: health checks -> launch/monitor the long-running consumer. Prevent overlapping copies and define stop/cleanup behavior. Capture the consumer running successfully; a permanently running consumer does not have a completed success state until stopped gracefully.
- Then, if time permits: CI test automation, PostgreSQL analytics, a dashboard, S3 artifact storage and ECS deployment.

## 5. Suggested four-week sequence

This is an adjustable planning estimate, not an assignment deadline.

| Week | Focus | Checkpoint |
|---|---|---|
| 1 | Environment, data contract, sklearn pipeline and MLflow | Persisted model predicts raw unseen records; tracked runs exist |
| 2 | Spark equivalents, engine comparison and required Airflow DAG | Mini Project 1 workflows and evidence complete |
| 3 | Kafka producer and consumer in both modes | Dataset -> Kafka -> predictions works for batch and streaming |
| 4 | Restart recovery, tests, screenshots, documentation and bonus DAGs | Both submissions reproducible from README |

Build one working path before broadening it: sklearn CSV -> persisted pipeline -> raw-record prediction -> MLflow -> Spark -> Airflow -> Kafka -> recovery -> evidence.

## 6. Technical references checked

- [Scikit-learn: avoiding inconsistent preprocessing and data leakage](https://scikit-learn.org/stable/common_pitfalls.html)
- [Spark: ML pipelines and persistence](https://spark.apache.org/docs/latest/ml-pipeline.html)
- [XGBoost: distributed training with PySpark](https://xgboost.readthedocs.io/en/stable/tutorials/spark_estimator.html)
- [Kafka: delivery semantics and transactional processing](https://kafka.apache.org/41/design/design/)

Assignment sources: loKNBWKtfI.pdf, pages 4-9; HWLHplNEHU (1).pdf, pages 3-8. Uploaded dataset facts were calculated directly from archive.zip.
