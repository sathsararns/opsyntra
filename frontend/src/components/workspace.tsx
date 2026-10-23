"use client";

import { useCallback, useEffect, useState } from "react";
import Image from "next/image";
import {
  Activity, ArrowDownToLine, ArrowRight, ArrowUpRight, BookOpen, ChartNoAxesCombined,
  Check, CheckCheck, ChevronDown, ChevronRight, CircleHelp, Database, FlaskConical,
  LayoutDashboard, Menu, RefreshCw, ShieldCheck, Sparkles, Upload, UserRound,
  UsersRound, X, Zap,
} from "lucide-react";
import type { LucideIcon } from "lucide-react";
import type { Overview, Prediction, Segment, View } from "@/lib/types";
import { api, downloadFile } from "@/lib/api";
import { CustomerPredictor } from "./customer-predictor";
import { BatchPredictor } from "./batch-predictor";
import { LiveMonitoring } from "./live-monitoring";

const navigation: { id: View; label: string; icon: LucideIcon }[] = [
  { id: "overview", label: "Overview", icon: LayoutDashboard },
  { id: "live", label: "Live monitoring", icon: Activity },
  { id: "predict", label: "Customer prediction", icon: UserRound },
  { id: "batch", label: "Batch predictions", icon: Upload },
  { id: "models", label: "Model performance", icon: ChartNoAxesCombined },
  { id: "quality", label: "Data quality", icon: ShieldCheck },
  { id: "guide", label: "Workspace guide", icon: BookOpen },
];

export const modelName = (name: string) => name === "xgboost" ? "XGBoost" : name.split("_").map((word) => word.charAt(0).toUpperCase() + word.slice(1)).join(" ");
const number = (value: number) => value.toLocaleString("en-US");
const percent = (value: number) => `${(value * 100).toFixed(1)}%`;

export function Workspace({ initialData }: { initialData: Overview }) {
  const [view, setView] = useState<View>("overview");
  const [data, setData] = useState(initialData);
  const [connected, setConnected] = useState(false);
  const [connecting, setConnecting] = useState(true);
  const [mobileMenu, setMobileMenu] = useState(false);
  const [recent, setRecent] = useState<Prediction[]>([]);
  const [toast, setToast] = useState("");

  const reconnect = useCallback(async () => {
    setConnecting(true);
    try {
      const overview = await api<Overview>("overview");
      setData(overview);
      setConnected(Boolean(overview.prediction_ready));
    } catch { setConnected(false); }
    finally { setConnecting(false); }
  }, []);

  useEffect(() => { void reconnect(); }, [reconnect]);
  useEffect(() => {
    try {
      const saved: unknown = JSON.parse(localStorage.getItem("opsyntra-predictions") || "[]");
      if (Array.isArray(saved)) setRecent(saved.filter((r): r is Prediction => Boolean(r && typeof r === "object" && typeof r.churn_probability === "number" && (r.prediction === "Yes" || r.prediction === "No"))).slice(0, 50));
    } catch { /* A restricted browser can still use predictions without history. */ }
  }, []);
  useEffect(() => {
    if (!toast) return;
    const timeout = setTimeout(() => setToast(""), 4000);
    return () => clearTimeout(timeout);
  }, [toast]);

  const navigate = (next: View) => { setView(next); setMobileMenu(false); window.scrollTo({ top: 0, behavior: "smooth" }); };
  const recordPredictions = (predictions: Prediction[]) => {
    setRecent((previous) => {
      const next = [...predictions, ...previous].slice(0, 50);
      try { localStorage.setItem("opsyntra-predictions", JSON.stringify(next)); } catch { /* History is optional. */ }
      return next;
    });
  };
  const clearHistory = () => {
    setRecent([]);
    try { localStorage.removeItem("opsyntra-predictions"); } catch { /* History is optional. */ }
    setToast("Prediction history cleared from this browser.");
  };

  return <div className="workspace">
    <a className="skip-link" href="#main-content">Skip to content</a>
    {mobileMenu && <button className="sidebar-scrim" aria-label="Close navigation" onClick={() => setMobileMenu(false)} />}
    <aside className={`sidebar ${mobileMenu ? "sidebar-open" : ""}`}>
      <button className="brand" onClick={() => navigate("overview")} aria-label="Opsyntra home">
        <Image
          src="/opsyntra-logo.jpg"
          alt="Opsyntra"
          width={1534}
          height={688}
          sizes="(max-width: 760px) 201px, 191px"
          className="brand-image"
          loading="eager"
        />
      </button>
      <div className="workspace-label"><span className="workspace-avatar"><Database size={15} /></span><div>Telco intelligence<small>Personal workspace</small></div><ChevronDown size={14} /></div>
      <p className="nav-label">WORKSPACE</p>
      <nav aria-label="Main navigation">
        {navigation.map(({ id, label, icon: Icon }, index) => <div key={id}>
          {id === "quality" && <p className="nav-label nav-label-second">RESOURCES</p>}
          <button className={`nav-item ${view === id ? "nav-item-active" : ""}`} aria-current={view === id ? "page" : undefined} onClick={() => navigate(id)}><Icon size={18} strokeWidth={1.7} /><span>{label}</span>{view === id && <span className="nav-active-dot" />}</button>
        </div>)}
      </nav>
      <div className="sidebar-bottom">
        <div className="connection-card"><div className="flex items-center gap-2"><span className={`status-dot ${connected ? "status-ready" : "status-muted"}`} /><span>{connecting ? "Connecting…" : connected ? "Model connected" : "Saved report snapshot"}</span><button onClick={() => void reconnect()} disabled={connecting} aria-label="Reconnect prediction service" className="ml-auto icon-button"><RefreshCw size={13} className={connecting ? "animate-spin" : ""} /></button></div><p>{connected ? "Your local prediction service is ready." : "Connect the local API for predictions."}</p></div>
        <button className="sidebar-help" onClick={() => navigate("guide")}><CircleHelp size={17} />Your workspace, explained<ArrowUpRight size={14} /></button>
        <div className="sidebar-footer"><span className="user-avatar">O</span><div>Opsyntra workspace<small>Local development</small></div><span className="text-xs text-white/35">v0.1</span></div>
      </div>
    </aside>

    <div className="main-shell">
      <header className="topbar">
        <div className="flex items-center gap-3"><button className="mobile-menu icon-button" aria-label="Open navigation" aria-expanded={mobileMenu} onClick={() => setMobileMenu(true)}><Menu size={20} /></button><span className="breadcrumb">Workspace</span><ChevronRight size={13} className="text-stone-300" /><span className="breadcrumb-current">{navigation.find((item) => item.id === view)?.label}</span></div>
        <div className="flex items-center gap-4"><span className="local-badge"><span className="status-dot status-ready" />Local workspace</span><span className="topbar-divider" /><span className="topbar-avatar" aria-label="Your workspace">O</span></div>
      </header>
      <main id="main-content" className="main-content">
        {view === "overview" && <>
          <div className="page-heading"><div><p className="eyebrow">CUSTOMER INTELLIGENCE</p><h1>Retention, in focus<span>.</span></h1><p className="page-description">Understand your customers. See the risk. Make your next move.</p></div><button className="button button-secondary" onClick={() => { downloadFile(JSON.stringify(data, null, 2), "opsyntra-workspace-report.json"); setToast("Workspace report downloaded."); }}><ArrowDownToLine size={15} />Export report</button></div>
          {!connected && !connecting && <div className="snapshot-notice"><Database size={15} /><span>Showing your saved dataset and model reports. Start the prediction service to run live predictions.</span><button onClick={() => navigate("guide")}>Setup guide <ArrowRight size={13} /></button></div>}
          <div className="metric-grid">
            <Metric label="Total customers" value={number(data.dataset.customers)} icon={UsersRound} note="Customers in your dataset" accent="mint" />
            <Metric label="Observed churn" value={`${data.dataset.churn_rate.toFixed(2)}%`} icon={Activity} note={`${number(data.dataset.churned)} customers left`} accent="sand" />
            <Metric label="Model accuracy" value={percent(data.model.metrics.accuracy)} icon={ShieldCheck} note="Measured on held-out test data" accent="sage" />
            <Metric label="ROC-AUC score" value={data.model.metrics.roc_auc.toFixed(3)} icon={ChartNoAxesCombined} note="Selected model · test evaluation" accent="lavender" />
          </div>
          <div className="overview-grid">
            <div className="overview-primary"><RetentionChart data={data} /><div className="lower-grid"><CustomerMix data={data} /><SelectedModel data={data} onView={() => navigate("models")} /></div></div>
            <div className="overview-secondary"><PredictionCallout onPredict={() => navigate("predict")} /><DataReadiness data={data} onView={() => navigate("quality")} /></div>
          </div>
          <div className="panel segments-panel"><div className="panel-heading"><div><h2>Your customer segments</h2><p>A closer look at contract types in your dataset.</p></div><span className="subtle-tag">Dataset overview</span></div><div className="table-scroll"><table><thead><tr><th>Contract type</th><th>Customers</th><th>Observed churn</th><th>Avg. monthly charges</th><th>Retention</th></tr></thead><tbody>{data.contract_segments.map((segment) => <tr key={segment.label}><td><span className={`contract-icon ${segment.label === "Month-to-month" ? "contract-icon-monthly" : ""}`}><Database size={14} /></span><strong>{segment.label}</strong></td><td>{number(segment.customers)}</td><td><span className={`rate-pill ${segment.churn_rate > 30 ? "rate-pill-amber" : ""}`}>{segment.churn_rate.toFixed(1)}%</span></td><td>${segment.monthly_charges.toFixed(2)}</td><td><span className="retention-cell"><span className="mini-track"><span style={{ width: `${segment.retention_rate}%` }} /></span>{segment.retention_rate.toFixed(1)}%</span></td></tr>)}</tbody></table></div></div>
          {recent.length > 0 && <RecentPredictions predictions={recent.slice(0, 5)} onClear={clearHistory} />}
        </>}
        {view === "predict" && <CustomerPredictor connected={connected} onPredicted={recordPredictions} onReconnect={reconnect} />}
        {view === "live" && <LiveMonitoring />}
        {view === "batch" && <BatchPredictor connected={connected} onPredicted={recordPredictions} onReconnect={reconnect} />}
        {view === "models" && <ModelPerformance data={data} />}
        {view === "quality" && <DataQuality data={data} />}
        {view === "guide" && <WorkspaceGuide connected={connected} onReconnect={reconnect} />}
        <footer className="content-footer"><span><span className="tiny-brand-mark">✳</span> Built for a clearer customer picture.</span><span>Telco Customer Churn<span className="footer-dot">·</span>{number(data.dataset.customers)} records</span></footer>
      </main>
    </div>
    {toast && <div className="toast" role="status"><CheckCheck size={17} />{toast}<button onClick={() => setToast("")} aria-label="Dismiss notification"><X size={14} /></button></div>}
  </div>;
}

function Metric({ label, value, icon: Icon, note, accent }: { label: string; value: string; icon: LucideIcon; note: string; accent: string }) {
  return <div className="metric-card"><div className="metric-top"><span>{label}</span><span className={`metric-icon ${accent}`}><Icon size={17} strokeWidth={1.65} /></span></div><strong className="metric-value">{value}</strong><p><span className="metric-note-dot" />{note}</p></div>;
}

function RetentionChart({ data }: { data: Overview }) {
  const [group, setGroup] = useState("tenure");
  const segments = group === "tenure" ? data.tenure_segments : data.contract_segments;
  const width = 620, height = 215, left = 36, right = 18, top = 20, bottom = 34;
  const x = (index: number) => left + index * (width - left - right) / Math.max(segments.length - 1, 1);
  const y = (value: number) => height - bottom - value / 100 * (height - top - bottom);
  const line = (key: "retention_rate" | "churn_rate") => segments.map((segment, i) => `${i ? "L" : "M"}${x(i)},${y(segment[key])}`).join(" ");
  const area = `${line("retention_rate")} L${x(segments.length - 1)},${height - bottom} L${left},${height - bottom} Z`;
  return <section className="panel chart-panel"><div className="panel-heading"><div><h2>Tenure tells a story</h2><p>Retention and churn across customer cohorts.</p></div><label className="chart-select"><span className="sr-only">Group customer cohorts by</span><select value={group} onChange={(event) => setGroup(event.target.value)}><option value="tenure">By tenure</option><option value="contract">By contract</option></select><ChevronDown size={12} /></label></div><div className="chart-legend"><span><i className="legend-dot green" />Stayed</span><span><i className="legend-dot amber" />Churned</span><span className="ml-auto text-stone-400">{group === "tenure" ? "Tenure in months" : "Contract type"}</span></div><div className="retention-chart"><svg viewBox={`0 0 ${width} ${height}`} role="img" aria-label={`Retention and churn percentages grouped by ${group}`}><defs><linearGradient id="chart-fill" x1="0" y1="0" x2="0" y2="1"><stop offset="0%" stopColor="#78b6a0" stopOpacity=".2" /><stop offset="100%" stopColor="#78b6a0" stopOpacity="0" /></linearGradient></defs>{[0, 25, 50, 75, 100].map((value) => <g key={value}><line x1={left} x2={width - right} y1={y(value)} y2={y(value)} stroke="#edf0ec" strokeDasharray="3 5" /><text x={left - 10} y={y(value) + 4} textAnchor="end" fill="#949d95" fontSize="10">{value}%</text></g>)}<path d={area} fill="url(#chart-fill)" /><path d={line("retention_rate")} fill="none" stroke="#367964" strokeWidth="2.5" strokeLinejoin="round" /><path d={line("churn_rate")} fill="none" stroke="#d5ae6a" strokeWidth="2" strokeDasharray="5 4" strokeLinejoin="round" />{segments.map((segment, i) => <g key={segment.label}><circle cx={x(i)} cy={y(segment.retention_rate)} r="4" fill="#367964" stroke="white" strokeWidth="2"><title>{`${segment.label}: ${segment.retention_rate}% stayed, ${segment.churn_rate}% churned (${segment.customers} customers)`}</title></circle><text x={x(i)} y={height - 8} textAnchor="middle" fill="#8a948d" fontSize="10">{segment.label}</text></g>)}</svg></div><div className="chart-footnote"><span className="status-dot status-ready" /> Historical dataset outcomes, grouped by cohort.</div></section>;
}

function CustomerMix({ data }: { data: Overview }) {
  const retained = 100 - data.dataset.churn_rate;
  return <section className="panel mix-panel"><div className="panel-heading"><div><h2>The customer picture</h2><p>Observed outcomes</p></div><UsersRound size={17} className="text-stone-400" /></div><div className="mix-body"><div className="donut" role="img" aria-label={`${retained.toFixed(1)} percent stayed, ${data.dataset.churn_rate} percent churned`} style={{ background: `conic-gradient(#34745e 0 ${retained}%, #dec899 ${retained}% 100%)` }}><div><strong>{retained.toFixed(1)}<small>%</small></strong><span>stayed</span></div></div><div className="mix-legend"><div><span><i className="legend-dot green" />Stayed</span><strong>{number(data.dataset.customers - data.dataset.churned)}</strong><small>{retained.toFixed(1)}% of customers</small></div><div><span><i className="legend-dot amber" />Churned</span><strong>{number(data.dataset.churned)}</strong><small>{data.dataset.churn_rate.toFixed(1)}% of customers</small></div></div></div></section>;
}

function SelectedModel({ data, onView }: { data: Overview; onView: () => void }) {
  return <section className="panel selected-panel"><div className="panel-heading"><h2>Your selected model</h2><span className="selected-badge"><Check size={10} />Selected</span></div><div className="selected-model-name"><span className="model-glyph"><FlaskConical size={20} strokeWidth={1.5} /></span><div><h3>{modelName(data.model.name)}</h3><p>Chosen by validation performance</p></div></div><div className="model-mini-metrics"><div><span>Precision</span><strong>{percent(data.model.metrics.precision)}</strong></div><div><span>Recall</span><strong>{percent(data.model.metrics.recall)}</strong></div><div><span>F1 score</span><strong>{percent(data.model.metrics.f1)}</strong></div></div><button className="text-link" onClick={onView}>Explore model performance<ArrowUpRight size={14} /></button></section>;
}

function PredictionCallout({ onPredict }: { onPredict: () => void }) {
  return <section className="prediction-callout"><div className="callout-art" aria-hidden="true"><svg viewBox="0 0 240 120"><ellipse cx="120" cy="65" rx="84" ry="37" fill="none" stroke="#6f9b80" strokeOpacity=".23" transform="rotate(-18 120 65)" /><ellipse cx="120" cy="65" rx="63" ry="26" fill="none" stroke="#a0c68c" strokeOpacity=".32" transform="rotate(-18 120 65)" /><rect x="80" y="27" width="72" height="66" rx="18" fill="#315a46" stroke="#74a077" transform="rotate(-12 116 60)" /><path d="M96 62l13 10 25-30" fill="none" stroke="#c5e4a3" strokeWidth="4" strokeLinecap="round" strokeLinejoin="round" /><circle cx="185" cy="47" r="5" fill="#bcda98" /><circle cx="53" cy="86" r="3" fill="#86b18b" /><path d="M181 89v12m-6-6h12M61 24v10m-5-5h10" stroke="#a4c68c" strokeWidth="1.5" /></svg></div><span className="callout-eyebrow"><Sparkles size={12} /> FROM INSIGHT TO ACTION</span><h2>One customer.<br /> A clearer next step.</h2><p>Turn a customer profile into a churn risk estimate with your trained model.</p><button className="button button-callout" onClick={onPredict}>Predict a customer<ArrowUpRight size={16} /></button><span className="callout-footnote"><ShieldCheck size={12} /> Your data stays in your local workspace</span></section>;
}

function DataReadiness({ data, onView }: { data: Overview; onView: () => void }) {
  const missing = Object.values(data.quality.missing_values).reduce((sum, value) => sum + value, 0);
  return <section className="panel readiness-panel"><div className="panel-heading"><h2>A solid foundation</h2><span className="readiness-icon"><ShieldCheck size={17} /></span></div><p className="readiness-description">Your data, checked and ready.</p><div className="readiness-row"><span>Unique customer IDs</span><Check size={14} /></div><div className="readiness-row"><span>Reproducible splits</span><Check size={14} /></div><div className="readiness-row"><span>Missing charges</span><span className="subtle-tag">{missing} handled by model</span></div><button className="text-link" onClick={onView}>View data quality<ArrowRight size={14} /></button></section>;
}

function RecentPredictions({ predictions, onClear }: { predictions: Prediction[]; onClear: () => void }) {
  return <section className="panel mt-5"><div className="panel-heading"><div><h2>Recent predictions</h2><p>Saved in this browser · latest five</p></div><button className="text-link" onClick={onClear}>Clear history</button></div><div className="table-scroll"><table><thead><tr><th>Customer</th><th>Churn probability</th><th>Prediction</th><th>Model version</th></tr></thead><tbody>{predictions.map((prediction, index) => <tr key={`${index}-${prediction.customerID}`}><td>{prediction.customerID || "Unnamed customer"}</td><td>{percent(prediction.churn_probability)}</td><td><span className={`rate-pill ${prediction.prediction === "Yes" ? "rate-pill-amber" : ""}`}>{prediction.prediction === "Yes" ? "Churn" : "Stay"}</span></td><td className="font-mono text-xs">{prediction.model_version.slice(0, 8)}</td></tr>)}</tbody></table></div></section>;
}

function ModelPerformance({ data }: { data: Overview }) {
  const matrix = data.model.metrics.confusion_matrix;
  return <><div className="page-heading"><div><p className="eyebrow">MODEL WORKSPACE</p><h1>Evidence over guesswork<span>.</span></h1><p className="page-description">Compare candidates. Understand the model behind every prediction.</p></div><span className="subtle-tag">{data.model.candidates.length} trained candidates</span></div><div className="metric-grid">{(["accuracy", "precision", "recall", "f1"] as const).map((metric) => <Metric key={metric} label={metric === "f1" ? "F1 score" : metric.charAt(0).toUpperCase() + metric.slice(1)} value={percent(data.model.metrics[metric])} icon={metric === "recall" ? UsersRound : ChartNoAxesCombined} note="Selected model · held-out test set" accent="mint" />)}</div><section className="panel"><div className="panel-heading"><div><h2>The candidate comparison</h2><p>Selection uses validation average precision. These are validation scores.</p></div></div><div className="table-scroll"><table><thead><tr><th>Model</th><th>CV avg. precision</th><th>Validation avg. precision</th><th>ROC-AUC</th><th>Recall</th><th>Status</th></tr></thead><tbody>{data.model.candidates.map((candidate) => <tr key={candidate.name}><td><strong>{modelName(candidate.name)}</strong></td><td>{candidate.cv_mean_score.toFixed(4)}</td><td>{candidate.validation.average_precision.toFixed(4)}</td><td>{candidate.validation.roc_auc.toFixed(4)}</td><td>{percent(candidate.validation.recall)}</td><td>{candidate.selected ? <span className="selected-badge"><Check size={11} />Selected</span> : <span className="text-stone-400 text-xs">Compared</span>}</td></tr>)}</tbody></table></div></section><div className="detail-grid mt-5"><section className="panel"><div className="panel-heading"><div><h2>Where predictions land</h2><p>Confusion matrix · {number(data.model.partition_rows.test)} test customers</p></div></div><div className="confusion-matrix"><div /><span>Predicted stay</span><span>Predicted churn</span><span>Actual stay</span><div className="matrix-correct"><strong>{number(matrix[0][0])}</strong><small>True negatives</small></div><div className="matrix-error"><strong>{number(matrix[0][1])}</strong><small>False positives</small></div><span>Actual churn</span><div className="matrix-error"><strong>{number(matrix[1][0])}</strong><small>False negatives</small></div><div className="matrix-correct"><strong>{number(matrix[1][1])}</strong><small>True positives</small></div></div></section><section className="panel model-details"><div className="panel-heading"><h2>Training, with boundaries</h2><FlaskConical size={19} className="text-emerald-800" /></div><p>Preprocessing is fitted inside each training fold. The validation partition selects the candidate and tunes the threshold for F1; the test partition measures the final result.</p><div className="split-bars">{Object.entries(data.model.partition_rows).map(([name, count]) => <div key={name}><span>{name}</span><div><i style={{ width: `${count / data.dataset.customers * 100}%` }} /></div><strong>{number(count)}</strong></div>)}</div><p>Decision threshold: <strong>{(data.model.decision_threshold ?? 0.5).toFixed(4)}</strong>. Probabilities at or above this value predict churn.</p><div className="model-version">MODEL VERSION<code>{data.model.version}</code></div></section></div></>;
}

function DataQuality({ data }: { data: Overview }) {
  const missing = Object.entries(data.quality.missing_values).filter(([, count]) => count > 0);
  return <><div className="page-heading"><div><p className="eyebrow">DATA FOUNDATION</p><h1>Good decisions start here<span>.</span></h1><p className="page-description">The checks, missing values, and safeguards behind your dataset.</p></div><span className="selected-badge"><Check size={12} />Validated with expected warnings</span></div><div className="metric-grid"><Metric label="Customer records" value={number(data.quality.rows)} icon={UsersRound} note="Every source customer retained" accent="mint" /><Metric label="Dataset columns" value={String(data.quality.columns)} icon={Database} note={`${data.dataset.features} model input features`} accent="lavender" /><Metric label="Duplicate IDs" value={String(data.quality.duplicate_customer_ids)} icon={CheckCheck} note="One unique ID per customer" accent="sage" /><Metric label="Missing charges" value={String(missing.reduce((sum, [, value]) => sum + value, 0))} icon={Activity} note="Imputed by the fitted model" accent="sand" /></div><div className="detail-grid"><section className="panel"><div className="panel-heading"><h2>Quality checks</h2><ShieldCheck size={19} className="text-emerald-800" /></div><div className="quality-checks">{Object.entries({ ...data.quality.checks, ...data.quality.split_checks }).map(([key, passed]) => <div key={key}><span>{key.replaceAll("_", " ")}</span><span className={`rate-pill ${!passed ? "rate-pill-amber" : ""}`}>{passed ? "Passed" : "Review"}</span></div>)}</div></section><section className="panel policy-card"><span className="metric-icon sand"><Database size={19} /></span><h2>Missing does not mean lost.</h2><p>The {missing.reduce((sum, [, value]) => sum + value, 0)} blank TotalCharges values belong to new customers with zero tenure. Those customers stay in the dataset.</p><p>Your fitted model fills missing numeric values using medians learned from training data. Validation and test data never contribute to those statistics.</p><div className="policy-note"><ShieldCheck size={18} /><span>Customer IDs, target labels, and event metadata are excluded from model features.</span></div></section></div></>;
}

function WorkspaceGuide({ connected, onReconnect }: { connected: boolean; onReconnect: () => void }) {
  return <><div className="page-heading"><div><p className="eyebrow">GETTING STARTED</p><h1>Your local intelligence hub<span>.</span></h1><p className="page-description">A small guide to everything you can do in Opsyntra.</p></div></div><div className="guide-grid">{[
    { icon: UserRound, title: "One customer, one prediction", text: "Enter a customer profile or load the example. Your saved pipeline returns an estimated churn probability and a Yes/No prediction." },
    { icon: Upload, title: "A whole batch, together", text: "Upload a customer CSV, review the prediction table, and download the results. The Churn label is never required." },
    { icon: ChartNoAxesCombined, title: "Understand the evidence", text: "Compare all four trained candidates and inspect the selected model's held-out metrics and confusion matrix." },
    { icon: ShieldCheck, title: "Keep your data local", text: "Predictions run in your local Python service. Recent prediction history is stored only in this browser and can be cleared." },
  ].map(({ icon: Icon, title, text }) => <section className="panel guide-card" key={title}><span className="metric-icon mint"><Icon size={21} /></span><h2>{title}</h2><p>{text}</p></section>)}</div><section className="panel setup-panel"><div className="panel-heading"><div><h2>Connect the prediction service</h2><p>From the Opsyntra project folder, start the Python API in a separate terminal.</p></div><span className={`rate-pill ${!connected ? "rate-pill-amber" : ""}`}>{connected ? "Connected" : "Offline"}</span></div><pre>python -m uvicorn opsyntra.web_api:app --app-dir backend/src --host 127.0.0.1 --port 8000</pre><div className="flex flex-wrap items-center justify-between gap-4"><p>After retraining, restart the API to load the newly saved model.</p><button className="button button-primary" onClick={onReconnect}><RefreshCw size={14} />Check connection</button></div></section></>;
}
