-- Ordinary views recompute from immutable logical facts on every query. Late
-- predictions/raw profiles therefore repair old buckets without counter deltas
-- or a materialized-view refresh race. Buckets are explicitly UTC.
CREATE VIEW analytics.prediction_hourly AS
SELECT date_trunc('hour', event_ts, 'UTC') AS bucket_start,
       engine, model_version, COUNT(*) AS prediction_volume,
       COUNT(DISTINCT event_id) AS unique_events,
       COUNT(DISTINCT customer_id) AS unique_customers,
       AVG((prediction='Yes')::integer)::double precision AS predicted_churn_share,
       AVG(churn_probability) AS mean_risk,
       AVG(processing_latency_seconds) AS mean_processing_latency_seconds,
       AVG(ingestion_latency_seconds) AS mean_ingestion_latency_seconds,
       COUNT(*) FILTER (WHERE raw_available) AS matched_raw_predictions
FROM analytics.prediction_details
GROUP BY bucket_start, engine, model_version;

CREATE VIEW analytics.prediction_daily AS
SELECT date_trunc('day', event_ts, 'UTC') AS bucket_start,
       engine, model_version, COUNT(*) AS prediction_volume,
       COUNT(DISTINCT event_id) AS unique_events,
       COUNT(DISTINCT customer_id) AS unique_customers,
       AVG((prediction='Yes')::integer)::double precision AS predicted_churn_share,
       AVG(churn_probability) AS mean_risk,
       AVG(processing_latency_seconds) AS mean_processing_latency_seconds,
       AVG(ingestion_latency_seconds) AS mean_ingestion_latency_seconds,
       COUNT(*) FILTER (WHERE raw_available) AS matched_raw_predictions
FROM analytics.prediction_details
GROUP BY bucket_start, engine, model_version;

CREATE VIEW analytics.prediction_hourly_segments AS
SELECT date_trunc('hour', event_ts, 'UTC') AS bucket_start,
       engine, model_version, raw_available, contract, payment_method,
       COUNT(*) AS prediction_volume, COUNT(DISTINCT customer_id) AS unique_customers,
       AVG((prediction='Yes')::integer)::double precision AS predicted_churn_share,
       AVG(churn_probability) AS mean_risk
FROM analytics.prediction_details
GROUP BY bucket_start, engine, model_version, raw_available, contract, payment_method;

GRANT SELECT ON analytics.prediction_hourly, analytics.prediction_daily,
    analytics.prediction_hourly_segments TO opsyntra_analytics_reader, opsyntra_analytics_writer;
