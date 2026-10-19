# Kafka prediction and recovery checkpoint

**PASS**: real broker, saved complete pipelines, fresh consumer processes, and two abrupt process crashes (exit code 86).

| Scenario | Observed result |
|---|---|
| Count-limited batch then offset-window resume | 14 input records committed across two processes |
| Normal batch outputs | 6 predictions, 7 deadletters, 1 suppressed duplicate |
| Completed window rerun | 0 input records and 0 new output |
| Re-published event IDs after restart | 2 duplicates suppressed, 0 new predictions |
| Streaming mode | 3 new-event predictions, bounded by --max-messages |
| Hard exit after durable delivery ledger, before input commit | Restart suppressed output replay; 1 physical prediction |
| Hard exit after output ack, before ledger update | Restart re-sent identical prepared payload; 2 physical records, 1 prediction ID |
| Spark saved-model batch inference | 1 prediction(s) |

All sklearn predictions matched direct saved-pipeline probabilities within 1e-12 and had the same labels. Tests included records without Churn and a missing TotalCharges value. Every consumer process loaded its model once. Output keys matched customerID; event/processing timestamps and model version were present.

At-least-once Kafka output with durable local deduplication. The tested ack-before-ledger crash window can produce identical duplicate predictions; prediction_id/event_id+model_version supports downstream deduplication. No end-to-end exactly-once claim.

The raw source dataset, producer checkpoint and ordinary consumer groups were not reset. Diagnostic groups and fixed windows isolated these checks from earlier topic history.

[Command logs, offset windows and JSON evidence](../artifacts/kafka/consumer_checks/25d9eb60faf14bd591954e9e581c39da/)

See [Kafka consumer commands and recovery policy](../KAFKA.md#prediction-consumer-step-7). The JSON report retains exact consumer summaries, group IDs, model hashes, observed counts and sample prediction/dead-letter records.

## Additional launcher and regression checks

The PowerShell consumer launcher also completed a native Spark streaming run
bounded to two input records, confirming both predictions before input commits.
Group: `opsyntra-step7-spark-stream-check`. Invocation:
`79732f60-5854-4309-9fe4-b6270bbd6da0`. Model version: `6172478f6bdcb515`.
[Saved streaming summary](../artifacts/kafka/consumer_runs/79732f60-5854-4309-9fe4-b6270bbd6da0/summary.json).

The complete backend suite ran 63 tests: 59 passed; four separate Spark workflow
tests were skipped on Windows. Native Spark Kafka batch and bounded streaming
checks ran separately through WSL. The read-only prediction viewer and offset
window capture helper were also exercised against the running broker.
