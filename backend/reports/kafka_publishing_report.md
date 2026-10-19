# Kafka publishing checkpoint

**PASS**: real Apache Kafka broker, actual CLI subprocesses and wire messages read back from the raw topic.

Broker cluster: `zBxppyQ0QqKUJdYFCCY2ig`. Client: confluent-kafka 2.16.0.
Local Kafka runs in Ubuntu WSL with Java IPv4 sockets so Windows clients can reach localhost:9092.

| Check | Result |
|---|---:|
| Full CSV batch messages | 7043 |
| First process paused after | 1000 |
| Resume process published | 6043 |
| Completed resume published | 0 |
| Streaming messages at requested 10 events/sec | 30 |
| Distinct event IDs across both runs | 7073 |
| Blank TotalCharges preserved as JSON null | 11 |

All observed messages passed the event contract, used customerID as their key, and included UTC event_ts. Every batch customer field matched its source CSV row. All 7,043 source rows appeared once across the graceful pause/resume sequence.

Batch run ID: `98b48d79-4616-4e5e-90aa-ed9ee3a79b98`. Streaming run ID: `4142bacf-1d2e-4b2c-9fa7-e18d8cc29839`.
Dataset SHA256: `88be4b93fbe0cc83421af1c503794c97c342eca914c1576db7c276e61d61358a`.

[Detailed command logs and checkpoint](../artifacts/kafka/checks/81273f6906a3426195855b4d85d446dc/)

Created topics: `telco.raw.customers`, `telco.churn.predictions`, `telco.deadletter`; each has three partitions and replication factor one. Prediction and dead-letter topics are provisioned for Step 7; this checkpoint publishes raw customer events.

## Reliability limits

Graceful checkpoint/resume was duplicate-free. A crash after delivery but before checkpoint persistence can replay a chunk with the same event IDs; consumers must deduplicate.

Producer idempotence and acks=all protect client retries. The checkpoint is atomically replaced only after an entire chunk is acknowledged. An OS lock prevents two batch processes sharing the same checkpoint. Resume rejects changes to input checksum, cluster, topic and label policy. This single-broker development setup has no multi-broker fault tolerance.

See [Kafka setup and producer commands](../KAFKA.md). The JSON report includes exact run IDs, per-partition offset windows, raw sample messages and the checked assertions.
