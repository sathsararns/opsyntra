-- Groups are provisioned by the database administrator, not the application.
REVOKE ALL ON SCHEMA analytics FROM PUBLIC;
GRANT USAGE ON SCHEMA analytics TO opsyntra_analytics_reader, opsyntra_analytics_writer;

-- Shared identities allow either topic to arrive first and enforce the same
-- customer/event timestamp across raw and prediction facts using composite FKs.
CREATE TABLE analytics.customer_events (
    event_id uuid PRIMARY KEY,
    customer_id text NOT NULL CHECK (length(btrim(customer_id)) > 0),
    event_ts timestamptz NOT NULL CHECK (isfinite(event_ts)),
    ingested_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    UNIQUE (event_id, customer_id, event_ts)
);

CREATE TABLE analytics.raw_customer_events (
    event_id uuid PRIMARY KEY,
    customer_id text NOT NULL,
    event_ts timestamptz NOT NULL,
    schema_version integer NOT NULL CHECK (schema_version = 1),
    features jsonb NOT NULL CHECK (jsonb_typeof(features) = 'object'),
    contract text GENERATED ALWAYS AS (features ->> 'Contract') STORED,
    payment_method text GENERATED ALWAYS AS (features ->> 'PaymentMethod') STORED,
    observed_churn text CHECK (observed_churn IN ('Yes', 'No')),
    run_id uuid NOT NULL,
    mode text NOT NULL CHECK (mode IN ('batch', 'streaming')),
    source_row bigint NOT NULL CHECK (source_row >= 0),
    source_sha256 text NOT NULL CHECK (source_sha256 ~ '^[0-9a-f]{64}$'),
    content_sha256 text NOT NULL CHECK (content_sha256 ~ '^[0-9a-f]{64}$'),
    payload jsonb NOT NULL CHECK (jsonb_typeof(payload) = 'object'),
    ingested_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    FOREIGN KEY (event_id, customer_id, event_ts)
        REFERENCES analytics.customer_events(event_id, customer_id, event_ts)
);

CREATE TABLE analytics.prediction_events (
    prediction_id text PRIMARY KEY CHECK (prediction_id ~ '^[0-9a-f]{64}$'),
    event_id uuid NOT NULL,
    customer_id text NOT NULL,
    engine text NOT NULL CHECK (engine IN ('sklearn', 'spark')),
    model_version text NOT NULL CHECK (length(btrim(model_version)) > 0),
    schema_version integer NOT NULL CHECK (schema_version = 1),
    churn_probability double precision NOT NULL CHECK (churn_probability >= 0 AND churn_probability <= 1),
    prediction text NOT NULL CHECK (prediction IN ('Yes', 'No')),
    event_ts timestamptz NOT NULL,
    processed_ts timestamptz NOT NULL CHECK (isfinite(processed_ts)),
    ingested_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    content_sha256 text NOT NULL CHECK (content_sha256 ~ '^[0-9a-f]{64}$'),
    payload jsonb NOT NULL CHECK (jsonb_typeof(payload) = 'object'),
    UNIQUE (event_id, engine, model_version),
    FOREIGN KEY (event_id, customer_id, event_ts)
        REFERENCES analytics.customer_events(event_id, customer_id, event_ts)
);

CREATE TABLE analytics.deadletter_events (
    event_id uuid PRIMARY KEY,
    original_event_id uuid,
    customer_id text,
    schema_version integer NOT NULL CHECK (schema_version = 1),
    error_type text NOT NULL CHECK (length(error_type) > 0),
    error_message text NOT NULL,
    source_cluster_id text NOT NULL,
    source_topic text NOT NULL,
    source_partition integer NOT NULL CHECK (source_partition >= 0),
    source_offset bigint NOT NULL CHECK (source_offset >= 0),
    raw_value_sha256 text NOT NULL CHECK (raw_value_sha256 ~ '^[0-9a-f]{64}$'),
    processed_ts timestamptz NOT NULL CHECK (isfinite(processed_ts)),
    ingested_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    content_sha256 text NOT NULL CHECK (content_sha256 ~ '^[0-9a-f]{64}$'),
    payload jsonb NOT NULL CHECK (jsonb_typeof(payload) = 'object'),
    UNIQUE (source_cluster_id, source_topic, source_partition, source_offset)
);

CREATE TABLE analytics.kafka_deliveries (
    cluster_id text NOT NULL CHECK (length(cluster_id) > 0),
    topic text NOT NULL CHECK (length(topic) > 0),
    partition integer NOT NULL CHECK (partition >= 0),
    "offset" bigint NOT NULL CHECK ("offset" >= 0),
    raw_event_id uuid REFERENCES analytics.raw_customer_events(event_id),
    prediction_id text REFERENCES analytics.prediction_events(prediction_id),
    deadletter_event_id uuid REFERENCES analytics.deadletter_events(event_id),
    message_key text NOT NULL,
    payload_sha256 text NOT NULL CHECK (payload_sha256 ~ '^[0-9a-f]{64}$'),
    ingested_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (cluster_id, topic, partition, "offset"),
    CHECK (num_nonnulls(raw_event_id, prediction_id, deadletter_event_id) = 1)
);

CREATE INDEX raw_events_time_idx ON analytics.raw_customer_events(event_ts);
CREATE INDEX raw_events_customer_idx ON analytics.raw_customer_events(customer_id, event_ts);
CREATE INDEX raw_events_contract_idx ON analytics.raw_customer_events(contract, payment_method);
CREATE INDEX predictions_time_idx ON analytics.prediction_events(event_ts);
CREATE INDEX predictions_customer_idx ON analytics.prediction_events(customer_id, event_ts);
CREATE INDEX predictions_model_time_idx ON analytics.prediction_events(engine, model_version, event_ts);
CREATE INDEX predictions_risk_idx ON analytics.prediction_events(engine, model_version, churn_probability DESC, event_ts);
CREATE INDEX deadletter_time_idx ON analytics.deadletter_events(processed_ts);
CREATE INDEX deliveries_raw_idx ON analytics.kafka_deliveries(raw_event_id) WHERE raw_event_id IS NOT NULL;
CREATE INDEX deliveries_prediction_idx ON analytics.kafka_deliveries(prediction_id) WHERE prediction_id IS NOT NULL;
CREATE INDEX deliveries_deadletter_idx ON analytics.kafka_deliveries(deadletter_event_id) WHERE deadletter_event_id IS NOT NULL;

GRANT SELECT, INSERT ON analytics.customer_events, analytics.raw_customer_events,
    analytics.prediction_events, analytics.deadletter_events, analytics.kafka_deliveries
    TO opsyntra_analytics_writer;
GRANT SELECT ON analytics.customer_events, analytics.raw_customer_events,
    analytics.prediction_events, analytics.deadletter_events, analytics.kafka_deliveries
    TO opsyntra_analytics_reader;
-- No UPDATE, DELETE, TRUNCATE, schema CREATE or migration-table privileges.
