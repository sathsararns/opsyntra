-- Analytics rejection quarantine is separate from incoming inference dead letters.
-- Persist a rejected input before acknowledging it; never publish an error back
-- into a topic the analytics consumer itself reads.
CREATE TABLE analytics.ingestion_errors (
    cluster_id text NOT NULL,
    topic text NOT NULL,
    partition integer NOT NULL CHECK (partition >= 0),
    "offset" bigint NOT NULL CHECK ("offset" >= 0),
    event_kind text NOT NULL CHECK (event_kind IN ('raw', 'prediction', 'deadletter')),
    error_type text NOT NULL CHECK (error_type IN ('invalid_record', 'event_conflict')),
    error_message text NOT NULL,
    record_sha256 text NOT NULL CHECK (record_sha256 ~ '^[0-9a-f]{64}$'),
    raw_value_sha256 text NOT NULL CHECK (raw_value_sha256 ~ '^[0-9a-f]{64}$'),
    raw_key_sha256 text NOT NULL CHECK (raw_key_sha256 ~ '^[0-9a-f]{64}$'),
    raw_value_base64 text NOT NULL,
    raw_key_base64 text NOT NULL,
    raw_value_is_null boolean NOT NULL,
    raw_key_is_null boolean NOT NULL,
    raw_value_truncated boolean NOT NULL,
    raw_key_truncated boolean NOT NULL,
    ingested_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (cluster_id, topic, partition, "offset")
);
CREATE INDEX ingestion_errors_time_idx ON analytics.ingestion_errors(ingested_at);
CREATE INDEX ingestion_errors_type_idx ON analytics.ingestion_errors(error_type, ingested_at);
GRANT SELECT, INSERT ON analytics.ingestion_errors TO opsyntra_analytics_writer;
GRANT SELECT ON analytics.ingestion_errors TO opsyntra_analytics_reader;
