CREATE VIEW analytics.prediction_details AS
SELECT p.prediction_id, p.event_id, p.customer_id, p.engine, p.model_version,
       p.churn_probability, p.prediction, p.event_ts, p.processed_ts, p.ingested_at,
       EXTRACT(EPOCH FROM (p.processed_ts - p.event_ts))::double precision AS processing_latency_seconds,
       EXTRACT(EPOCH FROM (p.ingested_at - p.processed_ts))::double precision AS ingestion_latency_seconds,
       r.event_id IS NOT NULL AS raw_available,
       r.contract, r.payment_method, r.features
FROM analytics.prediction_events p
LEFT JOIN analytics.raw_customer_events r ON r.event_id = p.event_id;

-- Logical predictions, never physical deliveries. Keep models/engines separate.
CREATE VIEW analytics.prediction_metrics_by_model AS
SELECT engine, model_version,
       COUNT(*) AS prediction_volume,
       COUNT(DISTINCT event_id) AS unique_events,
       COUNT(DISTINCT customer_id) AS unique_customers,
       AVG((prediction = 'Yes')::integer)::double precision AS predicted_churn_share,
       AVG(churn_probability) AS mean_risk,
       AVG(processing_latency_seconds) AS mean_processing_latency_seconds,
       AVG(ingestion_latency_seconds) AS mean_ingestion_latency_seconds,
       COUNT(*) FILTER (WHERE processing_latency_seconds < 0 OR ingestion_latency_seconds < 0) AS clock_skew_count,
       COUNT(*) FILTER (WHERE raw_available) AS matched_raw_predictions
FROM analytics.prediction_details
GROUP BY engine, model_version;

GRANT SELECT ON analytics.prediction_details, analytics.prediction_metrics_by_model
    TO opsyntra_analytics_reader, opsyntra_analytics_writer;
