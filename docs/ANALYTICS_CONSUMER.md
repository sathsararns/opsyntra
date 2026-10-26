# Phase 3: Kafka analytics consumer

`backend/analytics_consumer.py` reads existing customer, prediction and dead-letter
events and saves them to PostgreSQL. It does not load a model, score customers,
publish predictions or modify the inference consumer's recovery ledger.

## Start it

From **Opsyntra repository root** in PowerShell:

```powershell
python -m pip install -r backend/requirements/analytics-consumer.txt
.\scripts\windows\start-kafka.ps1 -Task start
.\scripts\windows\start-analytics.ps1 -Task start
.\scripts\windows\start-analytics.ps1 -Task migrate
python backend/analytics_consumer.py --mode streaming
```

Leave that terminal running. Continue using the existing producer and prediction
consumer in other terminals; this analytics consumer stores their raw events,
prediction outputs and dead letters automatically. Ctrl+C requests a graceful
stop after the current database transaction and input commit. SIGTERM, and
SIGBREAK on Windows, use the same shutdown path. Start with the **same group ID**
to resume. The default group is `opsyntra-analytics-v1`; configure
`OPSYNTRA_ANALYTICS_GROUP_ID` in `.env` or use `--group-id`. Do not reuse an
inference group such as `opsyntra-churn-sklearn`.

The first start reads the earliest retained records. Later starts use committed
offsets. If retention has deleted unprocessed data, the consumer stops instead
of resetting an existing checkpoint and hiding data loss. A new group explicitly
replays retained history; database identities prevent count inflation.

The writer URL is the only database credential required by the runtime.
Migration/read URLs are needed by setup/verifiers, not consumer processes.
Credentials remain in server-side environment variables or ignored `.env`.

## Bounded runs

```powershell
# Consume exactly 100 input records, including durable rejections, or report incomplete.
python backend/analytics_consumer.py --mode batch --max-messages 100

# Capture end offsets of all three topics, then finish that finite backlog.
python backend/analytics_consumer.py --mode batch --snapshot --timeout-seconds 180

# Run continuous polling for a bounded demonstration duration.
python backend/analytics_consumer.py --mode streaming --timeout-seconds 30
```

Snapshot end offsets are captured at startup. New arrivals after those ends are
left for the next run; the snapshot still joins the actual Kafka group and waits
for ownership after a prior process crash. A snapshot needs one active member
in its group; use a separate group when running it beside a continuous consumer.
An already completed snapshot can exit with zero records on restart.

Batch mode defaults to 1,000 records when neither `--max-messages` nor `--snapshot`
is given. Batch idle timeout defaults to 30 seconds and the completion deadline
to 120 seconds. Reaching either before the chosen boundary returns status
`incomplete` and exit code 2. Exit code 0 means complete or gracefully stopped;
exit code 1 means failure. Remaining records can be resumed with the same group.
Streaming mode has no idle exit and runs until stopped or its optional duration.

Use `--batch-size` (1–1,000) to bound each transaction. Other controls include
`--retries`, `--retry-delay`, `--max-event-bytes`, `--idle-timeout`,
`--bootstrap-servers`, and `--report <path>` for a JSON run summary. Topic
overrides are available for diagnostics; ordinary use reads `config.yaml`:

- `telco.raw.customers`
- `telco.churn.predictions`
- `telco.deadletter`

## Transaction and recovery sequence

```text
Poll bounded Kafka records
  -> PostgreSQL outer transaction
       -> validate and insert logical facts + physical provenance
       -> or save the rejected record in analytics.ingestion_errors
  -> commit PostgreSQL transaction
  -> synchronously commit covered Kafka offsets
```

Kafka auto commit and automatic offset storage are both disabled. Closing the
consumer does not commit unprocessed inputs. The consumer acknowledges only the
highest processed offset plus one in each partition after the **entire database
batch** succeeds. Snapshot EOF/gap boundaries are acknowledged only after all
preceding delivered records have been persisted. No fetch position is used to
acknowledge an unpersisted batch.

| Failure | Result on restart |
|---|---|
| Before PostgreSQL commit | Entire batch, including quarantine/provenance, rolls back; input replays |
| Database unavailable or failed transaction | Bounded retry; no Kafka input commit |
| Database commit response lost | Retry whole batch on a new connection; primary keys/content checks make it idempotent |
| After PostgreSQL commit, before Kafka commit | Input replays; logical facts and physical provenance do not duplicate |
| Kafka commit partially fails or response is lost | Stop and replay from confirmed group offsets; already saved rows remain safe |
| After both commits | Same group resumes after the saved inputs |

Transient connection, serialization, deadlock and lock/statement-timeout errors
retry the full database batch with bounded exponential backoff (default three
retries). Broker metadata readiness also retries transient failures. Nonrecoverable
configuration/schema/permission failures stop. Kafka poll/commit failures stop
for safe group-based replay rather than acknowledging uncertain work. Retries
never advance input offsets before durability. A rebalance during a long batch
may cause commit failure and safe replay; choose batch sizes that fit Kafka's
poll interval. Readiness checks verify broker topics, required tables and views
before normal ingestion. Run migrations separately with the owner login.

This is at-least-once Kafka delivery with idempotent PostgreSQL logical results;
it does not claim an atomic exactly-once transaction across Kafka and PostgreSQL.

## Invalid-record policy

Valid incoming inference dead letters are stored in `deadletter_events`.
Analytics rejections are stored separately in `analytics.ingestion_errors`:

- Malformed JSON/UTF-8, duplicate JSON fields, unsupported schemas, invalid
  feature/result types, key mismatches, oversize messages and tombstones are
  quarantined as `invalid_record`.
- Reusing a logical event/prediction ID with different content is quarantined as
  `event_conflict`; the accepted facts are preserved.
- Quarantine and any valid records in the same batch commit together, **before**
  acknowledging those rejected inputs. If quarantine cannot be saved, no input
  in that batch is acknowledged.
- Quarantine is keyed by cluster/topic/partition/offset. It retains full value/key
  SHA-256 fingerprints and null flags; raw bytes are base64 encoded, with value
  previews capped at 64 KiB and keys at 4 KiB. Truncation is explicit.
- Replays of rejected inputs reuse the saved rejection. Changing payload bytes,
  kind or identity at an already accepted/quarantined coordinate is treated as
  provenance corruption and stops ingestion without offset commits.
- No rejection is published back to a topic this consumer reads, preventing
  dead-letter recursion. Correct a rejected message by publishing a new valid
  event with a new identity/delivery; automated quarantine redrive is not part
  of this phase.

Per-record savepoints keep failed inserts from leaving partial identities or
facts. Transaction advisory locks serialize decisions for the same physical
coordinate; unique logical keys handle concurrent duplicate events. Structured
JSON logs expose counts/coordinates and error types, not credentials or customer
payloads. Database audit tables retain protected diagnostic details.

## Hourly/daily metrics and late events

Migration `0004` adds ordinary SQL views:

| View | Grouping |
|---|---|
| `prediction_hourly` | UTC hour, engine, model version |
| `prediction_daily` | UTC day, engine, model version |
| `prediction_hourly_segments` | UTC hour, engine/model, raw availability, original contract/payment |

These views query stored logical predictions on every read. A late prediction
appears in its original event-time bucket, and late raw data repairs the original
event's feature segmentation through its `event_id` join. No increment-only
counters or materialized refresh job can double-count replayed deliveries or
leave old buckets stale. This is suitable for the current dataset; materializing
large datasets would require a separately tested refresh strategy.

The views intentionally keep engine/model groups separate. Never sum per-bucket
unique-customer counts to calculate a whole-period distinct count. Use
`COUNT(DISTINCT customer_id)` over the filtered logical predictions instead.
UTC bucket boundaries are fixed; convert display timestamps to the configured
timezone. Local-calendar buckets need an explicit timezone query, not just a
formatted UTC-day label. Predicted churn share and mean risk are not observed
churn or accuracy. See [metric definitions](ANALYTICS_STORAGE.md).

## Checks and evidence

With Kafka and PostgreSQL running, from root:

```powershell
.\scripts\windows\start-analytics.ps1 -Task test-consumer
```

Or from `backend/`:

```powershell
python -m tools.verification.analytics_consumer_check --live --regression
```

The verifier uses the separate test database and uniquely named diagnostic
topics/groups. It publishes a known event set, performs actual `os._exit(86)`
process crashes before/after database commit, restarts the same group, replays
from a new group, verifies late feature joins and model-separated time buckets,
checks malformed/conflicting inputs, simulates a real refused database connection,
and runs bounded streaming. Diagnostic topics have one-day retention; ordinary
application offsets/data are not reset or cleared. Synthetic test data is labelled
by its diagnostic topics/model version and is kept out of the main database.

See [verification report](../backend/reports/analytics_consumer_verification.md),
[JSON results](../backend/reports/analytics_consumer_verification.json) and
[command transcripts](evidence/analytics/).

References: Confluent [Python consumer/commit API](https://docs.confluent.io/platform/current/clients/confluent-kafka-python/html/index.html),
Psycopg [transaction/savepoint behavior](https://www.psycopg.org/psycopg3/docs/basic/transactions.html),
and PostgreSQL [event-time bucketing](https://www.postgresql.org/docs/16/functions-datetime.html).
