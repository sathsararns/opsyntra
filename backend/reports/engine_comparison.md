# Opsyntra: scikit-learn and PySpark comparison

Both champions predict the **same 1,409 held-out customers**. Customer IDs and labels are aligned before metrics are computed.

Each engine selects its champion by validation AP after training-only stratified CV. Thresholds maximize validation churn-class F1. Test never selects an engine or candidate.

| Engine | Selected model | Threshold | Accuracy | Precision | Recall | F1 | ROC-AUC | PR average precision |
|---|---|---:|---:|---:|---:|---:|---:|---:|
| sklearn | random_forest | 0.276342 | 0.7417 | 0.5085 | 0.7968 | 0.6208 | 0.8439 | 0.6580 |
| spark | random_forest | 0.259173 | 0.7445 | 0.5120 | 0.7968 | 0.6234 | 0.8441 | 0.6539 |

Confusion matrices: rows = actual No/Yes; columns = predicted No/Yes.

- sklearn: `[[747, 288], [76, 298]]`
- Spark: `[[751, 284], [76, 298]]`

## Measured local time

All values are seconds. Training compares a single final fit, with CV searches reported separately.

| Engine | Preprocessing | Classifier fit | Final fit total | Warm inference median |
|---|---:|---:|---:|---:|
| sklearn | 0.1232 | 0.3924 | 0.5156 | 0.0487 |
| spark | 1.7521 | 8.0547 | 9.8068 | 1.0779 |

Spark startup: 8.3751s; data load/cache: 7.2217s.

## Validation comparison

| Engine | Candidate | Validation AP | Validation F1 | Threshold |
|---|---|---:|---:|---:|
| sklearn | dummy | 0.2654 | 0.0000 | 0.500000 |
| sklearn | logistic_regression | 0.6505 | 0.6300 | 0.330058 |
| sklearn | random_forest | 0.6626 | 0.6353 | 0.276342 |
| sklearn | xgboost | 0.6522 | 0.6313 | 0.375186 |
| spark | logistic_regression | 0.6477 | 0.6253 | 0.334226 |
| spark | random_forest | 0.6618 | 0.6310 | 0.259173 |
| spark | xgboost | 0.6522 | 0.6313 | 0.375186 |

## Interpretation and limitations

- Identical saved train/validation/test customers, raw predictors, target mapping and sklearn metric definitions.
- Spark and sklearn use different solvers, regularization conventions, tree construction and tuning grids; this is a pipeline comparison, not numerical algorithm equivalence.
- Spark's exact median quantile uses an order statistic; sklearn averages middle values for even-sized samples. Spark StandardScaler uses sample standard deviation; sklearn uses population standard deviation.
- Models fit training rows only, including every CV fold's imputation and category vocabularies. Unknown categorical values map to all-zero one-hot blocks in both engines.
- Fresh sklearn final-fit timing excludes grid search. Spark final preprocessing/fit excludes its CV search, separately reported. No CV-versus-single-fit speed ratio.
- Inference uses the complete saved pipelines, raw records cached/in memory, actual probability calculation, and collection into ID-sorted pandas tables. First action and subsequent warm timings are separate.
- Spark JVM startup and data loading/cache materialization are separate. These are local single-machine measurements; they do not demonstrate distributed scaling.
- Report metrics use the existing saved sklearn artifact. The Linux timing refit is checked against its validation probabilities; it does not replace the deployed sklearn model.

Benchmark environment: `Linux-6.6.87.2-microsoft-standard-WSL2-x86_64-with-glibc2.39`.
Dataset SHA256: `88be4b93fbe0cc83421af1c503794c97c342eca914c1576db7c276e61d61358a`.
Membership SHA256: `b51f7b090312e8c727b11753dd740acdadad3e47d01c2261fe76fb5421b4e82c`.
Test customer SHA256: `6f229e2f314c3a94a30a7c7b835f8056461771292aa692eeae878f2c510f0225`.
Spark batch: `74dc8fabe77d42c2b3b18f11ed83f8b3`. Spark pipeline: `artifacts/spark/runs/74dc8fabe77d42c2b3b18f11ed83f8b3/random_forest`.

Full timing samples and metrics: `engine_comparison.json`. Paired held-out predictions: `engine_test_predictions.csv`.
