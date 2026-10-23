# Opsyntra submission demo

Run commands from the repository root, `Opsyntra/`. Use `..\start-*.ps1` if
your terminal is already inside `backend/`. This demonstration uses the local
application and the supplied Telco dataset, rather than a hosted deployment.

## Before the presentation

1. Follow [verification/setup](VERIFICATION.md) to provide the CSV, build saved
   sklearn/Spark models and install the separate Airflow/ML worker environments.
2. Run these in separate terminals as appropriate:

   ```powershell
   .\scripts\windows\start-kafka.ps1 -Task start
   .\scripts\windows\start-airflow.ps1 -Task start
   .\scripts\windows\start-mlflow.ps1
   .\scripts\windows\start-frontend.ps1
   ```

3. Open the application at http://127.0.0.1:3000, MLflow at
   http://127.0.0.1:5000, and Airflow at http://localhost:8080.
4. Check the model version in the UI/health response and the saved model report.
   Keep the [evidence matrix](../backend/reports/submission_evidence.md) ready as
   dated fallback evidence if a local service is unavailable during the demo.

## Suggested presentation sequence

| Part | Show | Explain |
|---|---|---|
| Data | Overview and Data quality screens | 7,043 customers, 11 blank TotalCharges, fixed 60/20/20 customer memberships |
| Model | Model performance and MLflow comparison | Validation AP chooses the model; validation F1 chooses the threshold; test does not select either |
| Single prediction | Customer prediction → Load example → Predict churn risk | A complete saved Python pipeline transforms raw features; Churn is not required |
| Batch prediction | Upload sample CSV, search/filter rows, download results | The same persisted artifact handles CSV inference |
| Spark | Engine comparison report and CLI prediction | Both engines use the same held-out customers; local Spark startup costs are reported |
| ML orchestration | Successful `opsyntra_ml_workflow` Grid/Graph | Eight tasks, training-only preprocessing, retries, candidate validation and saved results |
| Kafka | Raw/prediction messages and producer/consumer logs | customerID key, stable event ID, timestamps and model version |
| Bonus | Kafka batch/streaming DAGs | Bounded batch completion, monitored streaming session and graceful stop |
| Reliability | Recovery screenshots and logs | Confirm output before input commit; durable deduplication; remaining at-least-once replay window |

The existing frontend's prediction forms call the Python API directly. Its
overview uses dataset/model reports. A live Kafka analytics dashboard is a future
extension; do not describe the current frontend as a Kafka monitoring interface.

## CLI demonstrations

```powershell
# Raw unlabelled input, then file batch output
Push-Location backend
python -m pipelines.inference_pipeline --record examples/customer.json
python -m pipelines.inference_pipeline --input examples/customers.csv --output artifacts/predictions/demo_batch.csv
Pop-Location

# Native Spark saved pipeline
.\scripts\windows\start-spark.ps1 -Task predict -Record examples/customer.json

# Read-only historical samples; does not change application offsets
python backend/pipelines/kafka_messages.py --topic raw --max-messages 2
python backend/pipelines/kafka_messages.py --topic predictions --max-messages 3
python backend/pipelines/kafka_messages.py --topic deadletter --max-messages 3
```

Use the following only when demonstrating fresh runs, allowing time for training:

```powershell
.\scripts\windows\start-airflow.ps1 -Task run -Engine sklearn
.\scripts\windows\start-airflow.ps1 -Task run-kafka -KafkaKind batch -Engine sklearn
.\scripts\windows\start-airflow.ps1 -Task run-kafka -KafkaKind streaming -Engine sklearn -DurationSeconds 30 -DemoEvents 20
```

Each command waits for an actual successful DagRun. The streaming command ends
normally after its configured session, and `stopped` is the expected consumer
status. The batch summary contains Yes and No predictions for every source row;
its prediction count is not the count of churners.

For a continuous feed, use two terminals:

```powershell
# Terminal A
python backend/producer.py --mode streaming --events-per-sec 5
# Terminal B
python backend/consumer.py --mode streaming
```

Stop each with Ctrl+C, allowing delivery/commit confirmation. Reuse the
same consumer group and state directory to demonstrate restart.

## Recovery explanation

- The consumer loads the selected model once and validates every record.
- Prediction/dead-letter delivery is acknowledged, then written to the durable
  local ledger, before covered input offsets are committed.
- A restart after ledger persistence suppresses repeated output.
- A hard exit after output acknowledgement but before ledger persistence can
  repeat the identical prepared output. Its stable `prediction_id` supports
  downstream deduplication. This is not an exactly-once guarantee.
- Intentional crash-check exit code 86 and malformed-record logs are diagnostic
  evidence, not failures of an otherwise successful demonstration.

To repeat those controlled live checks, run
`.\scripts\windows\start-kafka.ps1 -Task verify-consumer`. They publish diagnostic invalid events
under isolated groups/windows; normal consumers can also see those source events.

## Evidence refresh after new runs

```powershell
node frontend/scripts/capture-mlflow.mjs
node docs/scripts/capture-airflow.mjs
node docs/scripts/capture-kafka.mjs
Push-Location backend
python -m pipelines.submission_evidence
Pop-Location
```

Capture scripts use installed Microsoft Edge. Kafka message/recovery screenshots
are labelled recorded CLI output; Airflow/MLflow screenshots show their real UIs.
Prior Kafka-run evidence is retained under `docs/evidence/history/`.

## Submission checklist

- [ ] Repository URL and the tested commit/version are recorded.
- [ ] Dataset acquisition/checksum and fresh environment instructions are included.
- [ ] Clean-clone and frontend verification reports show PASS and state reused prerequisites.
- [ ] Current evidence index, screenshots, metrics and model/run IDs agree.
- [ ] Saved models are reproducible from scripts; any optional supplied bundle has checksums.
- [ ] Model/threshold justification, Spark timing limits and Kafka replay behavior can be explained.
- [ ] Demo files contain no required Churn input; no credential/environment files are committed.

The checkboxes are the presenter's final handoff checklist; they do not substitute
for the machine-readable verification reports.
