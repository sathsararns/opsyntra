# Application Kafka backlog verification

Status: **PASS**. Processed **115,871** existing Kafka records into the main analytics database.

The first bounded snapshot reached its 300-second deadline after 90,000 records and exited incomplete (code 2). The same group resumed the remaining 25,871 records and completed. Restarting that completed group processed zero records; all counts remained unchanged.

Group: `opsyntra-analytics-v1`. Cluster: `zBxppyQ0QqKUJdYFCCY2ig`. Database timezone: `UTC`.

| Stored logical/provenance table | Rows |
|---|---|
| customer_events | 61,441 |
| raw_customer_events | 61,441 |
| prediction_events | 54,371 |
| deadletter_events | 14 |
| kafka_deliveries | 115,857 |
| ingestion_errors | 14 |

Hourly and daily sums both equal the logical prediction count: **54,371**. These are event/model result counts, not unique customers or observed churn.

| Run | Result | Evidence |
|---|---|
| analytics_initial_ingestion | incomplete; 90,000 inputs | [JSON](analytics_initial_ingestion.json) |
| analytics_backlog_resume | complete; 25,871 inputs | [JSON](analytics_backlog_resume.json) |
| analytics_application_restart | complete; 0 inputs | [JSON](analytics_application_restart.json) |

Source history contains legitimate repeated customer events and earlier project diagnostic records. Fourteen rejected inputs were durably quarantined (including two conflicting events); accepted records were retained. The existing prediction consumer groups were not reset or cleared.

See [known-fixture crash/replay verification](analytics_consumer_verification.md), [JSON and evidence hashes](analytics_application_verification.json), and [operating guide](../../docs/ANALYTICS_CONSUMER.md).
