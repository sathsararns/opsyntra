# Kafka publishing: Step 6

One `producer.py` supports both continuous streaming and checkpointed CSV batches.
Reusable code lives in `src/opsyntra/kafka_events.py`, `kafka_config.py` and
`kafka_publishing.py`; topic provisioning and local broker management are in
`pipelines/`.

## Local broker

The launcher uses the existing Ubuntu WSL distribution and Java 17+ from the Spark
setup. Apache Kafka 4.3.1 runs as a persistent single-node KRaft broker with a
combined controller/broker. The Python client runs in the existing Windows backend
environment. Docker Desktop does not need to be running.

From **Opsyntra/**:

```powershell
# Once on a new environment: pinned Python client and SHA512-verified Apache download.
.\start-kafka.ps1 -Task setup

# Start/reuse the broker and create/verify all three required topics.
.\start-kafka.ps1 -Task start
.\start-kafka.ps1 -Task status
.\start-kafka.ps1 -Task topics
```

From **Opsyntra/backend/** use `..\start-kafka.ps1` instead. Relative paths in
`config.yaml` resolve against `backend/`; CLI `--input` and `--checkpoint` paths
resolve against your current terminal directory.

Topics:

| Topic | Purpose | Partitions | Replicas |
|---|---|---:|---:|
| telco.raw.customers | Raw customer events from either publishing mode | 3 | 1 |
| telco.churn.predictions | Validated churn prediction outputs | 3 | 1 |
| telco.deadletter | Invalid records and conflicting event IDs | 3 | 1 |

The service binds to loopback. Java uses `-Djava.net.preferIPv4Stack=true` so WSL
localhost forwarding works for the Windows client. Broker data/config/logs live in
the Ubuntu account's native filesystem under `~/.local/share/opsyntra/`;
`backend/.kafka-runtime/service.json` records the exact directory and process.
Broker stop/start retains topic data. Automatic topic creation is disabled.

## Publish a full CSV batch

From **Opsyntra/**:

```powershell
python backend/producer.py --mode batch --batch-size 500
```

From **backend/** use `python producer.py` instead. The configured raw Telco CSV is
the default input. The entire file is validated before any messages are sent;
publishing then reads normalized records in chunks without fitting preprocessing.

The default checkpoint is `backend/artifacts/kafka/checkpoints/batch.json`.
After stopping with **Ctrl+C**, resume with:

```powershell
python backend/producer.py --mode batch --batch-size 500 --resume
```

A completed resume publishes zero records. Starting again without `--resume`
rejects an existing checkpoint so intentional replay requires a new checkpoint.
To demonstrate a pause after the first 1,000 customers:

```powershell
python backend/producer.py --mode batch --batch-size 500 --max-events 1000 --checkpoint backend/artifacts/kafka/checkpoints/demo.json
python backend/producer.py --mode batch --batch-size 500 --checkpoint backend/artifacts/kafka/checkpoints/demo.json --resume
```

`--max-events` limits newly sent events in that invocation; it does not change the
stored source membership or dataset. You may change batch size when resuming.
Input SHA256, source row count, Kafka cluster, topic and label policy must match.
The original run ID and timestamp base remain fixed, preserving event IDs and
payloads if an uncheckpointed chunk must be replayed.

## Publish a continuous stream

```powershell
python backend/producer.py --mode streaming --events-per-sec 5
```

This samples validated source customers with replacement until **Ctrl+C**. Each
sample is a new event, even for the same customer. Rate control spaces submissions;
broker delays and backpressure can lower the achieved rate. Stop requests finish
queued deliveries before the summary is saved. On Linux, SIGTERM also requests a
graceful stop; abrupt process termination uses the documented replay semantics.

A bounded demonstration:

```powershell
python backend/producer.py --mode streaming --events-per-sec 10 --max-events 30
```

`--seed` defaults to 42 for reproducible sampling. Streaming timestamps use current
UTC minus a random delay of up to 60 seconds, simulating recently emitted events.
The jitter window is configurable; timestamps represent simulated event time.
Batch timestamps use the checkpoint's UTC start time plus source-row microseconds.

## Event contract

The key is UTF-8 `customerID`. The value is a flat UTF-8 JSON object containing the
19 raw model predictors, customerID, optional source Churn, UTC event_ts and
explicit metadata extensions:

```json
{
  "customerID": "7590-VHVEG",
  "SeniorCitizen": 0,
  "tenure": 1,
  "MonthlyCharges": 29.85,
  "TotalCharges": 29.85,
  "event_ts": "2026-10-19T12:00:00.000000Z",
  "schema_version": 1,
  "event_id": "d3d68f8c-ec17-55e4-b7b1-4de6d9b9ca20",
  "run_id": "2ff0b71c-9261-460c-a245-cc0a2bc0668c",
  "mode": "batch",
  "source_row": 0,
  "source_sha256": "<full CSV SHA256>"
}
```

This abbreviated illustration omits categorical fields. Complete actual wire
examples are saved in `reports/kafka_publishing_report.json`. `source_row` counts
CSV data records from zero, excluding the header. Metadata additions are documented
extensions to the PDF schema, not predictors.

Integers/floats are JSON numbers; blank TotalCharges is JSON `null`. NaN, infinity,
negative charges, invalid categories, missing required fields and duplicate source
customer IDs are rejected. Imputation stays inside the saved model used later by
the consumer. Neither streaming nor batch publishing fits preprocessing.

When the input CSV contains Churn, it is retained as simulated ground truth by
default. Use `--exclude-churn` to remove it. A CSV without Churn also works. Prediction excludes Churn, customerID and all event metadata from predictors;
the saved model and consumer both select the explicit 19-field feature list.

## Delivery and checkpoint guarantees

The producer enables Kafka idempotence, acks=all, client retries, compression and
bounded queue waiting. Delivery callbacks are serviced by poll/flush. A batch
checkpoint advances only after every message in that chunk is acknowledged.
Failures or flush timeouts keep the previous cursor. Atomic JSON replacement and
file fsync protect the checkpoint file; an OS-held lock prevents concurrent
publishers sharing it. Locks are released when the process exits, including crashes.

Graceful pause/resume avoids resending completed chunks. A crash after Kafka
acknowledges messages but before the local checkpoint is saved can replay some
messages. Batch replays retain the same event IDs and payloads, so Step 7 consumers
deduplicate them using the persistent delivery ledger below. This is at-least-once delivery across process crashes, not an
end-to-end exactly-once claim. One broker/one replica also provides no multi-broker
fault tolerance.

Each invocation writes `producer.log` and `summary.json` beneath
`backend/artifacts/kafka/runs/<invocation-id>/`. Summaries include source/run IDs,
submitted/acknowledged counts, partition offsets, timing and result status.
`--verbose` adds individual delivery callback details. Broker logs are separate
from application delivery logs; their exact path is in service.json.

To use an existing broker, set `OPSYNTRA_KAFKA_BOOTSTRAP_SERVERS` or pass
`--bootstrap-servers` to producer.py. The supplied launcher manages only its own
local WSL process. Topic settings, defaults, timeouts and output directories are
under `kafka:` in `backend/config.yaml`.

## Verification and evidence

```powershell
# From Opsyntra/: real broker; publishes a NEW full dataset replay and 30 samples.
.\start-kafka.ps1 -Task verify

# From backend/: isolated functional failure/restart tests, no broker required.
python -m unittest tests.integration.test_kafka_producer -v
```

The live check launches producer.py in separate processes, pauses after 1,000
records, resumes the remaining 6,043, confirms a completed resume sends zero,
then streams 30 events. A diagnostic raw-topic reader inspects a fixed partition
offset window, validates schemas and keys, compares every batch field against the
CSV, checks unique IDs and confirms all 11 missing charges are null. It never runs
churn predictions or commits offsets for the future application consumer.

See [the checkpoint report](reports/kafka_publishing_report.md) and its
[JSON evidence](reports/kafka_publishing_report.json) for exact run IDs, samples,
commands, observed offsets and counts. Unit/integration checks additionally cover
partial/out-of-order failed delivery, unconfirmed messages, queue backpressure,
invalid input, checkpoint locking and changed-data resume rejection.

Stop the broker when finished:

```powershell
.\start-kafka.ps1 -Task stop
```

References: [Apache Kafka quickstart](https://kafka.apache.org/quickstart/),
[Confluent Python producer delivery callbacks and flush](https://docs.confluent.io/kafka-clients/python/current/overview.html),
[librdkafka idempotent producer configuration](https://docs.confluent.io/platform/current/clients/librdkafka/html/md_INTRODUCTION.html).
Assignment schema: supplied `HWLHplNEHU (1).pdf`, Mini Project 2.

## Prediction consumer: Step 7

`consumer.py` is the single CLI for streaming and batch prediction, with an engine
option for sklearn or the saved native Spark pipeline. It loads the selected
complete pipeline once per process. Raw customer fields pass through its fitted
imputation/encoding/scaling; no transformations are fitted during consumption.
Only the 19 predictors are supplied to inference. Churn is optional and never a
predictor; customerID and all event metadata also stay outside the feature frame.

### Start from Opsyntra/

```powershell
.\start-kafka.ps1 -Task start

# Consume at most 1,000 INPUT records and exit.
python backend/consumer.py --mode batch --max-messages 1000

# Keep processing existing backlog and new arrivals until Ctrl+C.
python backend/consumer.py --mode streaming

# In a second terminal, publish new events if needed.
python backend/producer.py --mode streaming --events-per-sec 5
```

From `backend/`, use `python consumer.py`, `python producer.py`, and
`..\start-kafka.ps1`. The application group defaults to
`opsyntra-churn-sklearn` or `opsyntra-churn-spark`. Restart with the same group and
state directory to resume Kafka offsets and reuse durable deduplication. The CLI
has no extra resume flag; Kafka's committed offsets provide the resume position.
A new group deliberately reprocesses available history.

`--max-messages` counts input records, including malformed and duplicate records;
its value is not a promise of that many new predictions. `--batch-size` defaults
to 100 records per inference/output/commit operation. Batch defaults to a 1,000
record limit. A count-limited streaming demonstration also works:

```powershell
python backend/consumer.py --mode streaming --max-messages 30
```

Unbounded streaming keeps polling when there are no messages. Bounded runs stop
with an incomplete result if their idle timeout (default 10 seconds) or processing
deadline (default 120 seconds) is reached before the target. Exit codes are 0 for
completed/stopped, 2 for an unmet bounded target, and 1 for an error. Deadlines are
checked between processing groups; model loading is outside the processing timer
and the current inference/delivery/commit operation finishes first. Each Kafka
operation also has its own timeout. Ctrl+C requests a graceful stop after the
current operation.

### Bounded offset windows

Capture current per-partition start/end offsets without consuming or committing:

```powershell
# From Opsyntra/; choose a new output filename for each desired cutoff.
python backend/pipelines/kafka_window.py --output backend/artifacts/kafka/windows/my_batch.json
python backend/consumer.py --mode batch --offset-window backend/artifacts/kafka/windows/my_batch.json
```

The start is inclusive and end exclusive. The window freezes the available
backlog, so later producer arrivals do not extend the job. For each partition,
the consumer starts at the later of the requested start and its existing group
commit, capped at end. Restarting the same completed window is a zero-record
success. You can add `--max-messages` as an earlier stop, then rerun the same
window to finish it. Window files bind to a broker cluster and raw topic; unavailable
or expired offset ranges fail instead of silently skipping data.

### Spark inference

The wrapper accepts backend-relative file/directory paths independent of the
terminal's current folder. From Opsyntra/:

```powershell
# Once on a fresh Spark runtime; already installed for this checkpoint.
.\start-consumer.ps1 -SetupSparkClient

.\start-consumer.ps1 -Engine spark -Mode batch -MaxMessages 30
.\start-consumer.ps1 -Engine spark -Mode streaming

# Optional window; wrapper paths here are relative to backend/.
.\start-consumer.ps1 -Engine spark -Mode batch -OffsetWindow artifacts/kafka/windows/my_batch.json
```

From `backend/` use `..\start-consumer.ps1`. Spark uses the existing Ubuntu
`.venv-spark`, loads `artifacts/spark/selected_model.json` once and keeps one Spark
session alive. The default sklearn model is
`artifacts/models/best_pipeline.joblib`. `--model` overrides either artifact; the
wrapper's corresponding option is `-Model`. Direct Python CLI paths resolve
against the terminal directory, while wrapper paths are backend-relative.

For native Linux, install the matching ML runtime and requirements/kafka.txt and
run `python consumer.py --engine spark ...` directly. The local Spark runtime has
substantial JVM/model-loading overhead; batch inference amortizes it.

### Output records and inspection

The prediction topic uses the original customerID key. Each JSON output includes:

```json
{
  "customerID": "7590-VHVEG",
  "churn_probability": 0.6527880460520074,
  "prediction": "Yes",
  "event_ts": "2026-10-19T11:52:34.375500Z",
  "processed_ts": "2026-10-19T13:23:46.268687Z",
  "event_id": "6f57e370-c517-5dd1-b7ee-505b1d7d9683",
  "model_version": "d9a13b4cf6839ed4",
  "prediction_id": "18c72bcae9c116bfd3f286a16e24a40e9d5e4bd677ce01423bfccd308979f0d6",
  "engine": "sklearn",
  "schema_version": 1
}
```

This example is an actual message read back during verification. `prediction_id`
is derived from the full model artifact hash and event ID. It stays fixed on
replay, including the prepared output's processed_ts. The customerID may occur in
many legitimate events; deduplication uses event identity, not customer identity.

Inspect existing predictions or deadletters without changing application offsets:

```powershell
python backend/pipelines/kafka_messages.py --topic predictions --max-messages 5
python backend/pipelines/kafka_messages.py --topic deadletter --max-messages 5
```

The viewer uses a fresh inspection group, starts at available history and exits
after the count or timeout. Kafka has ordering within each partition; these
messages are not a global chronological listing. The viewer does not commit.

### Validation and invalid-event routing

Input follows the producer's version-1 flat event contract. The consumer rejects
invalid UTF-8/JSON, duplicate JSON keys, non-object JSON, missing/tombstone keys or
values, key/customerID mismatches, malformed metadata, invalid categories/types,
NaN/infinity and oversize events. TotalCharges null and omitted Churn are supported.
A reused event ID with changed payload is routed to deadletter as a conflict.

The dead-letter record contains a deterministic ID from the original cluster,
topic, partition and offset, an error reason, processing timestamp and base64
copies of raw key/value. Oversize raw values are truncated to 64 KiB with a full
value checksum and truncation indicator, keeping the deadletter publishable.
The input offset advances only after this deadletter is acknowledged and recorded.
Valid records in the same processing group continue through inference. Operational
model failures are fatal rather than classified as bad customer data.

### Recovery sequence and guarantees

For each processing group:

1. Validate records, detect repeated/conflicting event IDs and infer new valid events.
2. Persist immutable prepared outputs to a SQLite outbox (WAL and synchronous FULL).
3. Publish pending predictions/deadletters; service delivery callbacks and flush.
4. Record each acknowledged output as sent in the durable ledger.
5. Synchronously commit only the covered input offsets, using last offset plus one.

Both Kafka automatic offset commit and automatic offset store are disabled.
Commit errors are surfaced. Restart suppresses already-recorded deliveries and
then retries the corresponding input commit. Prepared outputs are retried with
identical contents. Already sent duplicates are not re-inferred or re-published.
The producer uses idempotence and acks=all for client retries.

| Failure point | Restart behavior |
|---|---|
| Before outbox preparation | Input is re-read and inference can run again |
| Prepared, before output acknowledgement | Retry the same prepared output |
| Output acknowledged, before local ledger update | An identical physical output can be published again |
| Sent ledger durable, before input commit | Suppress output replay and commit input when re-read |
| Input commit confirmed | Continue from Kafka's committed position |

**This is at-least-once processing with durable local deduplication, not end-to-end
exactly-once processing.** Kafka output delivery, the SQLite write and the Kafka
input offset commit are separate operations. The hard-crash checks demonstrate
the remaining ack-before-ledger window. Downstream sinks should deduplicate by
prediction_id (or the documented event/model identity). An atomic Kafka
consume/produce transaction is a separate design; it is not claimed here.

State lives in `artifacts/kafka/consumer_state/<cluster-and-group-hash>.sqlite3`.
Keep this state through restarts and for the required replay horizon. Deleting it
removes duplicate history. An OS lock prevents two local processes sharing a
store. The ledger binds the group, source/output topics, broker cluster and full
model hash. Switching models requires a new group ID, which intentionally starts
another processing history. The default setup supports one consumer per group on
one runtime with its durable local store; it does not claim distributed shared
deduplication across machines or multiple independent state directories.

Per-invocation consumer.log and summary.json are saved under
`artifacts/kafka/consumer_runs/<invocation-id>/`. Summaries report input commits,
confirmed predictions/deadletters, deduplication, inference batches, offsets,
model version/hash, one model load and termination reason. The broker must retain
source messages long enough for the chosen replay policy.

### Recovery checkpoint evidence

```powershell
# From Opsyntra/: isolated diagnostic groups; publishes a small labelled test set.
.\start-kafka.ps1 -Task verify-consumer

# From backend/: additionally exercise the saved Spark model.
python -m pipelines.kafka_consumption_check --include-spark

# Local failure-path tests, using real SQLite and simulated Kafka callbacks.
python -m unittest tests.integration.test_kafka_consumer -v
```

The real-broker check tests a count limit, bounded offset-window restart, completed
window rerun, repeated event IDs, malformed messages and streaming. It intentionally
hard-exits its own diagnostic subprocesses at two recovery boundaries (exit 86),
then restarts the same groups/stores. It verifies suppressed output after a durable
ledger, and identical replay after output ack before the ledger update. Every
sklearn probability/label is compared with direct saved-model inference.
Diagnostic groups do not reset ordinary application offsets or the producer's
CSV checkpoint. The check adds test events, including poison records, to the raw
topic; ordinary consumers will route those poison records to deadletter too.

See [the completed recovery report](reports/kafka_consumption_report.md) and
[JSON evidence](reports/kafka_consumption_report.json). These include run/group IDs,
model hashes, command logs and actual prediction/dead-letter examples. Native Spark
batch and bounded streaming were also exercised on the real broker. The backend
suite ran 63 tests: 59 passed and four separate Spark-workflow tests were skipped
on Windows; native Spark Kafka checks ran through WSL.

References: [Confluent manual consumer commits and delivery semantics](https://docs.confluent.io/platform/current/clients/consumer.html),
[Kafka delivery semantics and transactional processing](https://kafka.apache.org/43/design/design/#semantics).
