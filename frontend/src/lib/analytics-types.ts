export type AnalyticsScope = { start: string; end: string; engine: "sklearn" | "spark"; model_version: string | null };
export type Envelope = { scope: AnalyticsScope; generated_at: string; timezone: string };
export type AnalyticsSummary = Envelope & {
  metrics: { prediction_volume: number; unique_events: number; unique_customers: number; predicted_churn_share: number | null; mean_risk: number | null; matched_raw_predictions: number; mean_processing_latency_seconds: number | null; last_ingested_at: string | null };
  available_models: { engine: string; model_version: string; last_ingested_at: string }[];
  segments: Record<"contract" | "payment_method", { label: string | null; predictions: number; mean_risk: number }[]>;
  raw_event_volume: number;
};
export type AnalyticsPrediction = { prediction_id: string; event_id: string; customer_id: string; engine: string; model_version: string; churn_probability: number; prediction: "Yes" | "No"; event_ts: string; processed_ts: string; ingested_at: string; raw_available: boolean; contract: string | null; payment_method: string | null };
export type AnalyticsPage = Envelope & { items: AnalyticsPrediction[]; total: number; limit: number; offset: number; threshold?: number };
export type AnalyticsSeries = Envelope & { interval: "hour" | "day"; bucket_timezone: string; items: { bucket_start: string; predictions: number; mean_risk: number | null; predicted_churn_share: number | null }[] };
export type AnalyticsHealth = { database: "ready" | "unavailable"; broker: "ready" | "unavailable" | "unknown"; consumer_state: "active" | "inactive" | "unknown"; group_id?: string; lag: number | null; total_predictions: number; last_ingested_at: string | null; recent_deliveries: number; stale_after_seconds: number; detail?: string; timezone?: string };
