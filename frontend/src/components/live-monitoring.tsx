"use client";

import { useEffect, useState } from "react";
import { Activity, Clock3, Database, Radio, RefreshCw, UsersRound } from "lucide-react";
import type { AnalyticsHealth, AnalyticsPage, AnalyticsPrediction, AnalyticsSeries, AnalyticsSummary } from "@/lib/analytics-types";

const initialPoll = Math.max(1000, Math.min(60000, Number(process.env.NEXT_PUBLIC_ANALYTICS_POLL_MS) || 5000));
const initialFilters = { engine: "sklearn", model_version: "", lookback_hours: "24", customer_id: "", min_probability: "0", max_probability: "1", contract: "", payment_method: "", prediction: "" };
type Snapshot = { key: string; summary: AnalyticsSummary; series: AnalyticsSeries; recent: AnalyticsPage; risk: AnalyticsPage; receivedAt: string };
const count = (n: number) => n.toLocaleString("en-US");
const percent = (n: number | null) => n === null ? "—" : `${(n * 100).toFixed(1)}%`;
const when = (value: string | null, zone = "Asia/Colombo") => value ? new Intl.DateTimeFormat("en-GB", { timeZone: zone, month: "short", day: "2-digit", hour: "2-digit", minute: "2-digit", second: "2-digit" }).format(new Date(value)) : "No ingestion yet";

async function fetchLive<T>(path: string, signal: AbortSignal, health = false): Promise<T> {
  const response = await fetch(`/api/analytics/${path}`, { cache: "no-store", signal: AbortSignal.any([signal, AbortSignal.timeout(9000)]) });
  const body = await response.json();
  if (health && (body.database === "ready" || body.database === "unavailable")) return body as T;
  if (!response.ok) throw new Error(typeof body.detail === "string" ? body.detail : body.error || "Analytics request failed.");
  return body as T;
}

export function LiveMonitoring() {
  const [draft, setDraft] = useState(initialFilters);
  const [filters, setFilters] = useState(initialFilters);
  const [offset, setOffset] = useState(0);
  const [poll, setPoll] = useState(initialPoll);
  const [paused, setPaused] = useState(false);
  const [revision, setRevision] = useState(0);
  const [snapshot, setSnapshot] = useState<Snapshot | null>(null);
  const [health, setHealth] = useState<AnalyticsHealth | null>(null);
  const [error, setError] = useState("");
  const [loading, setLoading] = useState(true);
  const key = JSON.stringify({ ...filters, offset });
  const data = snapshot?.key === key ? snapshot : null;

  useEffect(() => {
    const controller = new AbortController();
    let timer: ReturnType<typeof setTimeout>;
    let active = true;
    async function refresh() {
      setLoading(true);
      try {
        const status = await fetchLive<AnalyticsHealth>("health", controller.signal, true);
        if (!active) return;
        setHealth(status);
        if (status.database !== "ready") throw new Error(status.detail || "Analytics database unavailable.");
        const query = new URLSearchParams(Object.entries(filters).filter(([,value]) => value !== ""));
        const summary = await fetchLive<AnalyticsSummary>(`summary?${query}`, controller.signal);
        // Share the returned engine/model/window across panels in this refresh.
        query.set("start", summary.scope.start); query.set("end", summary.scope.end);
        if (summary.scope.model_version) query.set("model_version", summary.scope.model_version);
        const recentQuery = new URLSearchParams(query); recentQuery.set("limit", "10"); recentQuery.set("offset", String(offset));
        const riskQuery = new URLSearchParams(query); riskQuery.set("limit", "5"); riskQuery.set("threshold", "0.7");
        const [series, recent, risk] = await Promise.all([
          fetchLive<AnalyticsSeries>(`timeseries?${query}&interval=${Number(filters.lookback_hours) > 48 ? "day" : "hour"}`, controller.signal),
          fetchLive<AnalyticsPage>(`predictions?${recentQuery}`, controller.signal),
          fetchLive<AnalyticsPage>(`high-risk?${riskQuery}`, controller.signal),
        ]);
        if (active) { setSnapshot({ key, summary, series, recent, risk, receivedAt: new Date().toISOString() }); setError(""); }
      } catch (cause) {
        if (active && !controller.signal.aborted) setError(cause instanceof Error ? cause.message : "Analytics connection failed.");
      } finally {
        if (active) { setLoading(false); if (!paused) timer = setTimeout(() => void refresh(), poll); }
      }
    }
    void refresh();
    return () => { active = false; controller.abort(); clearTimeout(timer); };
  }, [key, filters, offset, poll, paused, revision]);

  const age = health?.last_ingested_at ? (Date.now() - Date.parse(health.last_ingested_at)) / 1000 : null;
  const stale = age !== null && age > (health?.stale_after_seconds || 60);
  const state = error ? "Disconnected" : paused ? "Polling paused" : !health ? "Connecting" : health.broker !== "ready" ? "Broker disconnected" : health.consumer_state === "inactive" ? "Consumer inactive" : health.consumer_state === "unknown" ? "Consumer health unknown" : stale && (health.lag || 0) > 0 ? "Data stale" : health.recent_deliveries === 0 && health.lag === 0 ? "Connected · no new activity" : "Live";
  const zone = data?.summary.timezone || health?.timezone || "Asia/Colombo";
  const metrics = data?.summary.metrics;
  const statusWarning = Boolean(error || health && (health.broker !== "ready" || health.consumer_state !== "active" || stale && (health.lag || 0) > 0));

  return <section className="live-monitoring">
    <div className="page-heading"><div><p className="eyebrow">STREAM INTELLIGENCE</p><h1>Live monitoring<span>.</span></h1><p className="page-description">Follow customer events, model decisions and the flow between them.</p></div><button className="button button-secondary" onClick={() => setRevision(n => n + 1)} disabled={loading}><RefreshCw size={15} className={loading ? "animate-spin" : ""} />Refresh now</button></div>
    <div className="live-status panel"><span className={`live-indicator ${statusWarning ? "live-warning" : ""}`}><Radio size={15} /><strong role="status" data-testid="live-status">{state}</strong></span><span>Last refresh: <strong>{snapshot ? when(snapshot.receivedAt, zone) : "Waiting"}</strong></span><label>Refresh<select aria-label="Analytics refresh interval" value={poll} onChange={e => setPoll(Number(e.target.value))}>{Array.from(new Set([initialPoll,1000,5000,10000,30000])).sort((a,b) => a-b).map(n => <option key={n} value={n}>{n/1000}s</option>)}</select></label><label className="live-pause"><input type="checkbox" checked={paused} onChange={e => setPaused(e.target.checked)} />Pause polling</label></div>
    {error && <div className="live-alert" role="alert"><strong>{error}</strong><p>{data ? "Showing the last successful result. These values are not current." : "No live values are available for this selection. Check the analytics API and database."}</p></div>}
    {!error && health?.consumer_state === "inactive" && <div className="live-alert"><strong>The analytics consumer is not active.</strong><p>{stale ? "Stored data is stale. " : ""}Start the analytics consumer to save new events. Zero lag alone does not mean the consumer is running.</p></div>}
    {!error && health?.broker === "unavailable" && <div className="live-alert"><strong>Kafka is disconnected.</strong><p>Database history is available; incoming activity and consumer lag cannot be confirmed.</p></div>}
    <form className="panel live-filters" onSubmit={e => { e.preventDefault(); setOffset(0); setFilters({ ...draft }); }}>
      <label>Engine<select aria-label="Analytics engine" value={draft.engine} onChange={e => setDraft({ ...draft,engine:e.target.value,model_version:"" })}><option value="sklearn">scikit-learn</option><option value="spark">Spark</option></select></label>
      <label>Model version<select aria-label="Analytics model version" value={draft.model_version} onChange={e => setDraft({ ...draft,model_version:e.target.value })}><option value="">Latest model</option>{snapshot?.summary.available_models.filter(m => m.engine === draft.engine).map(m => <option key={m.model_version} value={m.model_version}>{m.model_version}</option>)}</select></label>
      <label>Event-time window<select aria-label="Analytics time window" value={draft.lookback_hours} onChange={e => setDraft({ ...draft,lookback_hours:e.target.value })}><option value="1">Last hour</option><option value="24">Last 24 hours</option><option value="168">Last 7 days</option><option value="744">Last 31 days</option></select></label>
      <label>Customer ID<input aria-label="Analytics customer ID" placeholder="Exact customer ID" maxLength={128} value={draft.customer_id} onChange={e => setDraft({ ...draft,customer_id:e.target.value })} /></label>
      <label>Minimum risk<input aria-label="Minimum churn probability" type="number" min="0" max="1" step="0.05" value={draft.min_probability} onChange={e => setDraft({ ...draft,min_probability:e.target.value })} required /></label>
      <label>Contract<select aria-label="Analytics contract" value={draft.contract} onChange={e => setDraft({ ...draft,contract:e.target.value })}><option value="">All contracts</option>{["Month-to-month","One year","Two year"].map(c => <option key={c}>{c}</option>)}</select></label>
      <label>Payment<select aria-label="Analytics payment method" value={draft.payment_method} onChange={e => setDraft({ ...draft,payment_method:e.target.value })}><option value="">All payment methods</option>{["Electronic check","Mailed check","Bank transfer (automatic)","Credit card (automatic)"].map(c => <option key={c}>{c}</option>)}</select></label>
      <button className="button button-primary" type="submit">Apply filters</button>
    </form>
    <div className="live-scope"><span>Engine: <strong>{data?.summary.scope.engine || filters.engine}</strong></span><span>Model: <code data-testid="live-model-version">{data?.summary.scope.model_version || "No selected model yet"}</code></span><span>Times: {zone}</span></div>
    <div className={`metric-grid ${error ? "live-old-data" : ""}`}>
      {[{ label:"Prediction events", value:metrics ? count(metrics.prediction_volume) : "—", icon:Activity, note:"Logical results in this scope", id:"live-volume" },{ label:"Unique customers",value:metrics ? count(metrics.unique_customers) : "—",icon:UsersRound,note:"Customers, not deliveries",id:"live-customers" },{ label:"Predicted churn share",value:metrics ? percent(metrics.predicted_churn_share) : "—",icon:Radio,note:"Predictions labelled Yes",id:"live-churn-share" },{ label:"Mean risk",value:metrics ? percent(metrics.mean_risk) : "—",icon:Activity,note:"Average churn probability",id:"live-mean-risk" }].map(({label,value,icon:Icon,note,id}) => <div className="metric-card" key={id}><div className="metric-top"><span>{label}</span><span className="metric-icon mint"><Icon size={17} /></span></div><strong className="metric-value" data-testid={id}>{value}</strong><p>{note}</p></div>)}
    </div>
    {!data && !error && <div className="panel live-empty">Loading analytics…</div>}
    {data && <>
      {metrics?.prediction_volume === 0 && <div className="panel live-empty"><Database size={23} /><h2>{health?.total_predictions === 0 ? "Empty stream" : "No activity in this selection"}</h2><p>{health?.total_predictions === 0 ? "No predictions have been ingested yet. Publish customer events and run the prediction and analytics consumers." : "The query succeeded with zero matching predictions. Try another model, time window or filter."}</p></div>}
      <div className="live-chart-grid"><section className="panel"><div className="panel-heading"><div><h2>Prediction activity</h2><p>Event-time volume · UTC {data.series.interval} buckets</p></div><span className="subtle-tag">{count(metrics?.unique_events || 0)} unique events</span></div><ActivityChart series={data.series} zone={zone} /></section>
        <section className="panel live-health"><div className="panel-heading"><div><h2>Pipeline health</h2><p>Broker and committed consumer offsets</p></div><Clock3 size={17} /></div><dl><div><dt>Kafka</dt><dd>{health?.broker || "Unknown"}</dd></div><div><dt>Consumer</dt><dd>{health?.consumer_state || "Unknown"}</dd></div><div><dt>Consumer lag</dt><dd data-testid="live-lag">{health?.lag === null || health?.lag === undefined ? "Unavailable" : `${count(health.lag)} records`}</dd></div><div><dt>Last ingestion</dt><dd>{when(health?.last_ingested_at || null,zone)}</dd></div><div><dt>Raw events in window</dt><dd>{count(data.summary.raw_event_volume)} <small>(all models)</small></dd></div><div><dt>Matched feature profiles</dt><dd>{count(metrics?.matched_raw_predictions || 0)} / {count(metrics?.prediction_volume || 0)}</dd></div></dl></section></div>
      <section className="panel live-table"><div className="panel-heading"><div><h2>Recent stream predictions</h2><p>Newest ingestion first. Expand a row to trace its event and prediction.</p></div><span className="subtle-tag">{count(data.recent.total)} results</span></div><PredictionTable rows={data.recent.items} zone={zone} /><div className="live-pagination"><button className="button button-secondary" disabled={offset === 0 || loading} onClick={() => setOffset(Math.max(0,offset-10))}>Previous</button><span>Page {Math.floor(offset/10)+1}</span><button className="button button-secondary" disabled={offset+10 >= data.recent.total || loading} onClick={() => setOffset(offset+10)}>Next</button></div></section>
      <div className="live-segment-grid">{(["contract","payment_method"] as const).map(field => <section className="panel" key={field}><div className="panel-heading"><div><h2>{field === "contract" ? "Contract segments" : "Payment segments"}</h2><p>Feature values from each original event</p></div></div><div className="live-segments">{data.summary.segments[field].map(s => <div key={s.label || "unmatched"}><span>{s.label || "Profile not received"}</span><strong>{count(s.predictions)}</strong><small>{percent(s.mean_risk)} mean risk</small></div>)}{data.summary.segments[field].length === 0 && <p>No matching segments.</p>}</div></section>)}</div>
      <section className="panel live-table"><div className="panel-heading"><div><h2>Highest-risk customers</h2><p>Latest event per customer in this scope · probability ≥ 70%</p></div></div><PredictionTable rows={data.risk.items} zone={zone} /></section>
      <p className="live-footnote">Predicted churn share and risk are model outputs, not observed churn or accuracy. Records from other engines and model versions are kept separate.</p>
    </>}
  </section>;
}

function PredictionTable({ rows, zone }: { rows: AnalyticsPrediction[]; zone: string }) {
  return <div className="table-scroll"><table><thead><tr><th>Customer</th><th>Risk</th><th>Prediction</th><th>Model</th><th>Event time</th><th>Trace</th></tr></thead><tbody>{rows.map(row => <tr key={row.prediction_id} data-testid={`stream-${row.prediction_id}`}><td><strong>{row.customer_id}</strong></td><td>{percent(row.churn_probability)}</td><td><span className={`rate-pill ${row.prediction === "Yes" ? "rate-pill-amber" : ""}`}>{row.prediction}</span></td><td><code>{row.model_version}</code></td><td>{when(row.event_ts,zone)}</td><td><details><summary>Trace event</summary><div className="live-trace"><span>Event <code>{row.event_id}</code></span><span>Prediction <code>{row.prediction_id}</code></span><span>Processed {when(row.processed_ts,zone)}</span><span>Ingested <time dateTime={row.ingested_at}>{when(row.ingested_at,zone)}</time></span><span>{row.raw_available ? `${row.contract} · ${row.payment_method}` : "Original profile not received yet"}</span></div></details></td></tr>)}{rows.length === 0 && <tr><td colSpan={6}>No matching predictions.</td></tr>}</tbody></table></div>;
}

function ActivityChart({ series, zone }: { series: AnalyticsSeries; zone: string }) {
  const max = Math.max(1,...series.items.map(p => p.predictions));
  const width = 640, height = 185;
  const step = 600/Math.max(1,series.items.length);
  return <div className="live-chart"><svg viewBox={`0 0 ${width} ${height}`} role="img" aria-label="Prediction event volume over time">{[0,.5,1].map(n => <g key={n}><line x1="30" x2="635" y1={155-n*130} y2={155-n*130} stroke="#e8ebe3" strokeDasharray="3 4" /><text x="24" y={159-n*130} textAnchor="end" fill="#89928a" fontSize="10">{Math.round(max*n)}</text></g>)}{series.items.map((p,i) => <g key={p.bucket_start}><rect x={32+i*step} y={155-p.predictions/max*130} width={Math.max(1,step*.65)} height={p.predictions/max*130} rx="2" fill="#51866b"><title>{when(p.bucket_start,zone)}: {p.predictions} predictions</title></rect>{i % Math.max(1,Math.ceil(series.items.length/5)) === 0 && <text x={32+i*step} y="178" fill="#89928a" fontSize="9">{when(p.bucket_start,zone).slice(0,-3)}</text>}</g>)}</svg></div>;
}
