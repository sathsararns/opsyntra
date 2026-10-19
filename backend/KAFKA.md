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
| telco.churn.predictions | Reserved for the Step 7 prediction consumer | 3 | 1 |
| telco.deadletter | Reserved for invalid-event routing in Step 7 | 3 | 1 |

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
default. Use `--exclude-churn` to remove it. A CSV without Churn also works. Future
prediction must exclude Churn, customerID and all event metadata from predictors;
the existing model pipeline already selects its explicit 19-field feature list.

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
can deduplicate them. This is at-least-once delivery across process crashes, not an
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
