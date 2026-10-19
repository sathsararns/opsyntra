export type View = "overview" | "predict" | "batch" | "models" | "quality" | "guide";
export type Prediction = { customerID?: string; churn_probability: number; prediction: "Yes" | "No"; model_version: string };
export type PredictionResponse = { predictions: Prediction[]; model_version: string; summary: { total: number; predicted_churn: number; average_probability: number } };
export type Segment = { label: string; customers: number; churn_rate: number; retention_rate: number; monthly_charges: number };
export type Metrics = { accuracy: number; precision: number; recall: number; f1: number; roc_auc: number; average_precision: number; confusion_matrix: number[][] };
export type Overview = {
  dataset: { customers: number; features: number; churned: number; churn_rate: number; monthly_charges: number };
  quality: { status: string; columns: number; rows: number; duplicate_customer_ids: number; checks: Record<string, boolean>; missing_values: Record<string, number>; warnings: string[]; split_checks: Record<string, boolean> };
  model: { name: string; version: string; metrics: Metrics; decision_threshold?: number; partition_rows: Record<string, number>; selection_metric: string; candidates: { name: string; selected: boolean; cv_mean_score: number; validation: Metrics; best_parameters: Record<string, unknown> }[] };
  tenure_segments: Segment[];
  contract_segments: Segment[];
  source: string;
  prediction_ready?: boolean;
  loaded_model_version?: string;
};
