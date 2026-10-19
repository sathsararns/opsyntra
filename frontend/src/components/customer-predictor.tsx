"use client";

import { useState } from "react";
import { Activity, ArrowRight, Check, ChevronDown, CircleHelp, FlaskConical, LoaderCircle, RefreshCw, RotateCcw, ShieldCheck, Sparkles, UserRound } from "lucide-react";
import type { Prediction, PredictionResponse } from "@/lib/types";
import { api } from "@/lib/api";

type Field = { key: string; label: string; options?: string[]; numeric?: boolean; optional?: boolean; hint?: string };
const yesNo = ["Yes", "No"];
const internetOptions = ["No", "Yes", "No internet service"];
const sections: { title: string; subtitle: string; fields: Field[] }[] = [
  { title: "Customer profile", subtitle: "Start with the person behind the account.", fields: [
    { key: "customerID", label: "Customer ID", optional: true, hint: "Optional identifier" },
    { key: "gender", label: "Gender", options: ["Female", "Male"] },
    { key: "SeniorCitizen", label: "Senior citizen", options: ["0", "1"], hint: "0 = No · 1 = Yes" },
    { key: "Partner", label: "Has a partner", options: yesNo },
    { key: "Dependents", label: "Has dependents", options: yesNo },
  ] },
  { title: "Account & billing", subtitle: "The relationship, in numbers.", fields: [
    { key: "tenure", label: "Tenure (months)", numeric: true },
    { key: "Contract", label: "Contract", options: ["Month-to-month", "One year", "Two year"] },
    { key: "MonthlyCharges", label: "Monthly charges ($)", numeric: true },
    { key: "TotalCharges", label: "Total charges ($)", numeric: true, optional: true, hint: "Leave blank to use trained imputation" },
    { key: "PaperlessBilling", label: "Paperless billing", options: yesNo },
    { key: "PaymentMethod", label: "Payment method", options: ["Electronic check", "Mailed check", "Bank transfer (automatic)", "Credit card (automatic)"] },
  ] },
  { title: "Services", subtitle: "What keeps this customer connected.", fields: [
    { key: "PhoneService", label: "Phone service", options: yesNo },
    { key: "MultipleLines", label: "Multiple lines", options: ["No", "Yes", "No phone service"] },
    { key: "InternetService", label: "Internet service", options: ["DSL", "Fiber optic", "No"] },
    { key: "OnlineSecurity", label: "Online security", options: internetOptions },
    { key: "OnlineBackup", label: "Online backup", options: internetOptions },
    { key: "DeviceProtection", label: "Device protection", options: internetOptions },
    { key: "TechSupport", label: "Tech support", options: internetOptions },
    { key: "StreamingTV", label: "Streaming TV", options: internetOptions },
    { key: "StreamingMovies", label: "Streaming movies", options: internetOptions },
  ] },
];
const initial: Record<string, string> = {
  customerID: "", gender: "Female", SeniorCitizen: "0", Partner: "Yes", Dependents: "No",
  tenure: "12", Contract: "Month-to-month", MonthlyCharges: "59.85", TotalCharges: "",
  PaperlessBilling: "Yes", PaymentMethod: "Electronic check", PhoneService: "Yes",
  MultipleLines: "No", InternetService: "DSL", OnlineSecurity: "No", OnlineBackup: "Yes",
  DeviceProtection: "No", TechSupport: "No", StreamingTV: "No", StreamingMovies: "No",
};
const example = { ...initial, customerID: "DEMO-NEW-001", tenure: "0", MonthlyCharges: "29.85", PhoneService: "No", MultipleLines: "No phone service" };

export function CustomerPredictor({ connected, onPredicted, onReconnect }: { connected: boolean; onPredicted: (predictions: Prediction[]) => void; onReconnect: () => void }) {
  const [form, setForm] = useState(initial);
  const [result, setResult] = useState<Prediction | null>(null);
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState("");

  const reset = (values: Record<string, string>) => { setForm({ ...values }); setResult(null); setError(""); };
  async function submit(event: React.FormEvent<HTMLFormElement>) {
    event.preventDefault();
    setBusy(true); setError(""); setResult(null);
    try {
      const record: Record<string, string | number | null> = { ...form };
      for (const key of ["SeniorCitizen", "tenure", "MonthlyCharges", "TotalCharges"]) record[key] = form[key].trim() ? Number(form[key]) : null;
      if (!form.customerID.trim()) delete record.customerID;
      const response = await api<PredictionResponse>("predict", { record });
      setResult(response.predictions[0]);
      onPredicted(response.predictions);
    } catch (cause) { setError(cause instanceof Error ? cause.message : "Prediction failed. Please try again."); }
    finally { setBusy(false); }
  }

  return <>
    <div className="page-heading"><div><p className="eyebrow">PREDICTION STUDIO</p><h1>See the customer. See the risk<span>.</span></h1><p className="page-description">A customer profile in. A clearer retention decision out.</p></div><button className="button button-secondary" disabled={busy} onClick={() => reset(example)}><Sparkles size={15} />Load example</button></div>
    {!connected && <div className="snapshot-notice"><Activity size={15} /><span>The prediction service is offline. Start the local API to continue.</span><button onClick={onReconnect}><RefreshCw size={13} />Reconnect</button></div>}
    <div className="prediction-layout">
      <form className="panel customer-form" onSubmit={submit}>
        <div className="panel-heading form-panel-heading"><div><h2>Customer details</h2><p>No churn label needed. Your model handles the rest.</p></div><UserRound size={20} className="text-emerald-800" /></div>
        <fieldset disabled={busy} className="min-w-0">
          {sections.map((section, sectionIndex) => <section className="form-section" key={section.title}><div className="form-section-heading"><span>{String(sectionIndex + 1).padStart(2, "0")}</span><div><h3>{section.title}</h3><p>{section.subtitle}</p></div></div><div className="form-grid">{section.fields.map((field) => <label className="field" key={field.key} htmlFor={field.key}><span>{field.label}{field.optional && <small>optional</small>}</span>{field.options ? <div className="select-wrapper"><select id={field.key} value={form[field.key]} onChange={(event) => { setForm({ ...form, [field.key]: event.target.value }); setResult(null); }}>{field.options.map((option) => <option key={option}>{option}</option>)}</select><ChevronDown size={13} /></div> : <input id={field.key} value={form[field.key]} type={field.numeric ? "number" : "text"} min={field.numeric ? 0 : undefined} step={field.key === "tenure" ? 1 : field.numeric ? "any" : undefined} required={!field.optional} maxLength={field.numeric ? undefined : 200} placeholder={field.key === "customerID" ? "e.g. CUSTOMER-001" : field.optional ? "Leave blank" : undefined} onChange={(event) => { setForm({ ...form, [field.key]: event.target.value }); setResult(null); }} />}{field.hint && <small className="field-hint">{field.hint}</small>}</label>)}</div></section>)}
          <div className="form-actions"><button type="button" className="button button-plain" onClick={() => reset(initial)}><RotateCcw size={14} />Reset form</button><button className="button button-primary" disabled={!connected || busy}>{busy ? <LoaderCircle size={16} className="animate-spin" /> : <Sparkles size={15} />}{busy ? "Making a prediction…" : "Predict churn risk"}{!busy && <ArrowRight size={15} />}</button></div>
        </fieldset>
        {error && <div className="error-message mx-6 mb-5" role="alert">{error}</div>}
      </form>
      <aside className="prediction-result-column" aria-live="polite" aria-busy={busy}>
        <div className={`panel result-card ${result ? "result-card-ready" : ""}`}>
          <div className="panel-heading"><h2>Your prediction</h2><span className="result-step">{result ? "COMPLETE" : "READY WHEN YOU ARE"}</span></div>
          {result ? <><span className={`prediction-status ${result.prediction === "Yes" ? "prediction-status-churn" : ""}`}>{result.prediction === "Yes" ? <Activity size={14} /> : <Check size={14} />}{result.prediction === "Yes" ? "Predicted to churn" : "Predicted to stay"}</span><div className="risk-gauge"><svg viewBox="0 0 220 140" aria-hidden="true"><path d="M30 110 A80 80 0 0 1 190 110" fill="none" stroke="#edf0e9" strokeWidth="13" strokeLinecap="round" /><path d="M30 110 A80 80 0 0 1 190 110" fill="none" stroke={result.prediction === "Yes" ? "#bc8a42" : "#407f65"} strokeWidth="13" strokeLinecap="round" strokeDasharray={`${result.churn_probability * Math.PI * 80} ${Math.PI * 80}`} /></svg><div><strong>{(result.churn_probability * 100).toFixed(1)}<small>%</small></strong><span>estimated churn probability</span></div></div><p className="result-explanation">{result.prediction === "Yes" ? "This profile may benefit from a closer look and a retention conversation." : "The model estimates this customer is more likely to stay. Keep an eye on changes in their profile."}</p><div className="result-meta"><span>Customer</span><strong>{result.customerID || "Not specified"}</strong><span>Model version</span><code>{result.model_version.slice(0, 8)}</code></div></> : <div className="result-empty"><span className="result-empty-icon">{busy ? <LoaderCircle size={29} className="animate-spin" /> : <FlaskConical size={29} strokeWidth={1.4} />}</span><h3>{busy ? "Connecting the dots…" : "A little context goes a long way."}</h3><p>{busy ? "Your saved pipeline is processing this customer profile." : "Complete the customer profile and run a prediction to see the result here."}</p></div>}
        </div>
        <div className="prediction-note"><ShieldCheck size={19} /><div><h3>Same model. Same process.</h3><p>Every profile passes through your saved preprocessing and classification pipeline.</p></div></div>
        <div className="prediction-note"><CircleHelp size={18} /><div><h3>Blank total charges?</h3><p>Leave the field empty. The model uses the median learned from training customers.</p></div></div>
      </aside>
    </div>
  </>;
}
