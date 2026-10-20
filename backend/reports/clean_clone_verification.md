# Clean-clone verification

Baseline application commit: `8a027da66ba0772eec2a397bb75ba1a541595ed4`.

Status: **PASS**. This is a separate local Git clone with newly created Windows and Linux ML environments. No model artifacts, virtual environments, node_modules or raw data were copied from the original workspace. The supplied archive was imported as the external dataset input. Earlier failed attempts are retained below; verification completed after the recorded portability fix and successful retry.

Archive SHA-256: `cf7e6dcd8a238ecaa841a7d133142525453992d8d5e3ef6d1e5f0d359e7bf444`.
Dataset SHA-256: `88be4b93fbe0cc83421af1c503794c97c342eca914c1576db7c276e61d61358a`.
Membership SHA-256: `b51f7b090312e8c727b11753dd740acdadad3e47d01c2261fe76fb5421b4e82c`.

Fresh clone tests: 67 host checks passed; 4 Spark checks skipped there; 4 native Spark checks passed. Browser: 6 passed, zero failed/skipped.

## Commands and results

| Stage | Result | Command output |
|---|---|---|
| clone | PASS | [log](../../docs/evidence/clean-clone/clone.txt) |
| clean_checkout | PASS | Initial Git/ignored-artifact assertions |
| windows_environment | PASS | [log](../../docs/evidence/clean-clone/windows_environment.txt) |
| windows_dependencies | PASS | [log](../../docs/evidence/clean-clone/windows_dependencies.txt) |
| data_import | PASS | [log](../../docs/evidence/clean-clone/data_import.txt) |
| sklearn_training | PASS | [log](../../docs/evidence/clean-clone/sklearn_training.txt) |
| sklearn_single | PASS | [log](../../docs/evidence/clean-clone/sklearn_single.txt) |
| sklearn_batch | PASS | [log](../../docs/evidence/clean-clone/sklearn_batch.txt) |
| spark_environment | PASS | [log](../../docs/evidence/clean-clone/spark_environment.txt) |
| spark_dependencies | PASS | [log](../../docs/evidence/clean-clone/spark_dependencies.txt) |
| spark_training | PASS | [log](../../docs/evidence/clean-clone/spark_training.txt) |
| spark_tracking | FAIL | [log](../../docs/evidence/clean-clone/spark_tracking.txt) |
| spark_tracking | PASS | [log](../../docs/evidence/clean-clone/spark_tracking-attempt-2.txt) |
| spark_single | PASS | [log](../../docs/evidence/clean-clone/spark_single-attempt-1.txt) |
| spark_batch | PASS | [log](../../docs/evidence/clean-clone/spark_batch-attempt-1.txt) |
| backend_tests | PASS | [log](../../docs/evidence/clean-clone/backend_tests-attempt-1.txt) |
| spark_tests | PASS | [log](../../docs/evidence/clean-clone/spark_tests-attempt-1.txt) |
| npm_install | PASS | [log](../../docs/evidence/clean-clone/npm_install-attempt-1.txt) |
| frontend_typecheck | PASS | [log](../../docs/evidence/clean-clone/frontend_typecheck-attempt-1.txt) |
| frontend_build | PASS | [log](../../docs/evidence/clean-clone/frontend_build-attempt-1.txt) |
| frontend_browser | PASS | [log](../../docs/evidence/clean-clone/frontend_browser-attempt-1.txt) |
| kafka_publishing | PASS | [log](../../docs/evidence/clean-clone/kafka_publishing-attempt-1.txt) |
| kafka_consumption | PASS | [log](../../docs/evidence/clean-clone/kafka_consumption-attempt-1.txt) |
| main_airflow_stop | PASS | [log](../../docs/evidence/clean-clone/main_airflow_stop-attempt-1.txt) |
| airflow_sklearn | PASS | [log](../../docs/evidence/clean-clone/airflow_sklearn-attempt-1.txt) |
| airflow_spark | PASS | [log](../../docs/evidence/clean-clone/airflow_spark-attempt-1.txt) |
| clone_airflow_stop | PASS | [log](../../docs/evidence/clean-clone/clone_airflow_stop-attempt-1.txt) |
| main_airflow_restore | PASS | [log](../../docs/evidence/clean-clone/main_airflow_restore-attempt-1.txt) |

## Fresh scheduler runs

- sklearn: `manual__opsyntra_sklearn_20261020T081904081091Z`; eight tasks successful in fresh clone Airflow metadata.
- spark: `manual__opsyntra_spark_20261020T082059520644Z`; eight tasks successful in fresh clone Airflow metadata.

## Verified portability fixes applied after cloning

- `backend/src/opsyntra/spark_tracking.py`: Native Spark MLflow directory copies fail with long Windows paths; log a complete checksum-verified archive. [Exact applied patch](../../docs/evidence/clean-clone/spark-tracking-portability.patch); patched-file SHA-256 `7b54c87cc2f5b88168774e1004ba6f4c6d390e6981b0750ea736a9c82be4173b`.

## Reused prerequisites and limits

- Installed Windows Python interpreter (fresh isolated venv)
- Installed Node/npm and Microsoft Edge
- Ubuntu WSL, python3 and Java 21 (fresh isolated Spark venv)
- Existing local Kafka broker/data and downloaded Kafka installation
- Existing Airflow 3.3.2 tool installation; clone metadata/state newly created when --include-airflow is used
- Package download caches; no existing Python environments/node_modules copied

The Git transport was local and its baseline commit is recorded above. Only the explicitly listed portability fixes were applied to the cloned application source; the patch and final source hashes identify the tested candidate without committing changes to the user's repository. Pending documentation/verification tools were not overlaid on the clone. This verifies a fresh application checkout on this computer, not provisioning a blank operating system. Kafka used the existing broker with isolated diagnostic groups/windows; Airflow used its existing tool installation with a new clone-specific metadata directory. The original project's Airflow service was restored afterwards.

Generated models/reports in the clone have their own experiment/run IDs. They do not replace the original project's deployed models or MLflow database. See [the JSON evidence](clean_clone_verification.json), [frontend results](frontend_verification.json) and [demo guide](../../docs/DEMO_GUIDE.md).

## Separate saved-model setup

A second checkout imported the matching dataset and restored a checksummed model bundle. Both engines predicted single/CSV inputs without training there. This faster setup explicitly reused installed runtimes and original deployed models; it is separate from the clean retraining proof above. The guarded restore supports long Windows native Spark paths and rejects mismatched data or conflicting models. See [bundle evidence](model_bundle_verification.json).
