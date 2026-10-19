"use client";

import { useRef, useState } from "react";
import { ArrowDownToLine, ArrowRight, Check, ChevronLeft, ChevronRight, FileSpreadsheet, LoaderCircle, RefreshCw, Search, ShieldCheck, Upload, UsersRound, X } from "lucide-react";
import { api, downloadFile } from "@/lib/api";
import type { Prediction, PredictionResponse } from "@/lib/types";

export function BatchPredictor({ connected, onPredicted, onReconnect }: { connected: boolean; onPredicted: (predictions: Prediction[]) => void; onReconnect: () => void }) {
  const [file, setFile] = useState<File | null>(null);
  const [csv, setCsv] = useState("");
  const [dragging, setDragging] = useState(false);
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState("");
  const [response, setResponse] = useState<PredictionResponse | null>(null);
  const [search, setSearch] = useState("");
  const [filter, setFilter] = useState("all");
  const [page, setPage] = useState(1);
  const input = useRef<HTMLInputElement>(null);

  async function chooseFile(selected: File | undefined) {
    setResponse(null); setError(""); setFile(null); setCsv(""); setSearch(""); setPage(1); setFilter("all");
    if (!selected) return;
    if (!selected.name.toLowerCase().endsWith(".csv")) { setError("Please choose a CSV file."); return; }
    if (selected.size > 2_000_000) { setError("Choose a CSV smaller than 2 MB."); return; }
    try { const content = await selected.text(); setCsv(content); setFile(selected); }
    catch { setError("This file could not be read. Please select it again."); }
  }
  async function predict() {
    setBusy(true); setError(""); setResponse(null);
    try {
      const results = await api<PredictionResponse>("predict/batch", { csv_text: csv });
      setResponse(results); onPredicted(results.predictions); setPage(1);
    } catch (cause) { setError(cause instanceof Error ? cause.message : "Batch prediction failed. Please try again."); }
    finally { setBusy(false); }
  }
  function downloadResults() {
    if (!response) return;
    const cell = (value: string | number) => {
      let text = String(value);
      if (typeof value === "string" && /^[=+@-]/.test(text)) text = `'${text}`;
      return `"${text.replaceAll('"', '""')}"`;
    };
    const rows = ["customerID,churn_probability,prediction,model_version", ...response.predictions.map((r) => [r.customerID || "", r.churn_probability, r.prediction, r.model_version].map(cell).join(","))];
    downloadFile(rows.join("\n") + "\n", "opsyntra-churn-predictions.csv", "text/csv;charset=utf-8");
  }
  const results = response?.predictions.filter((record) => (record.customerID || "").toLowerCase().includes(search.toLowerCase()) && (filter === "all" || record.prediction === filter)) || [];
  const totalPages = Math.max(1, Math.ceil(results.length / 10));
  const visible = results.slice((page - 1) * 10, page * 10);

  return <>
    <div className="page-heading"><div><p className="eyebrow">BATCH WORKSPACE</p><h1>A bigger picture. In one go<span>.</span></h1><p className="page-description">Upload customer profiles and bring your retention priorities into view.</p></div><a className="button button-secondary" href="/sample-customers.csv" download><ArrowDownToLine size={15} />Sample CSV</a></div>
    {!connected && <div className="snapshot-notice"><ShieldCheck size={15} /><span>Connect your local prediction service to process a batch.</span><button onClick={onReconnect}><RefreshCw size={13} />Reconnect</button></div>}
    <div className="batch-upload-layout"><section className="panel upload-panel"><div className="panel-heading"><div><h2>Your customer file</h2><p>One row per customer. No Churn label required.</p></div><span className="subtle-tag">CSV · up to 2 MB</span></div><label htmlFor="customer-csv" className={`drop-zone ${dragging ? "drop-zone-active" : ""} ${file ? "drop-zone-selected" : ""}`} onDragOver={(event) => { event.preventDefault(); if (!busy) setDragging(true); }} onDragLeave={() => setDragging(false)} onDrop={(event) => { event.preventDefault(); setDragging(false); if (!busy) void chooseFile(event.dataTransfer.files[0]); }}><input ref={input} id="customer-csv" type="file" accept=".csv,text/csv" disabled={busy} onChange={(event) => void chooseFile(event.target.files?.[0])} /><span className="upload-icon">{file ? <FileSpreadsheet size={27} strokeWidth={1.4} /> : <Upload size={27} strokeWidth={1.4} />}</span><h3>{file ? file.name : "Drop your customer CSV here"}</h3><p>{file ? `${(file.size / 1024).toFixed(1)} KB · ready to process` : "or click to browse your files"}</p><span className="upload-caption">Up to 10,000 customers per batch</span></label><div className="upload-actions"><span><ShieldCheck size={14} />Processed locally</span><button className="button button-primary" disabled={!connected || !csv || busy} onClick={() => void predict()}>{busy ? <LoaderCircle className="animate-spin" size={15} /> : <UsersRound size={15} />}{busy ? "Processing customers…" : "Run batch prediction"}{!busy && <ArrowRight size={15} />}</button></div>{error && <div role="alert" className="error-message mt-4">{error}</div>}</section><aside className="panel csv-guide"><span className="metric-icon sand"><FileSpreadsheet size={22} /></span><h2>A file your model understands.</h2><p>Include the same 19 customer features used during training.</p><ul><li><Check size={14} />Keep the original column names</li><li><Check size={14} />Customer IDs are optional</li><li><Check size={14} />Blank numeric values are imputed</li><li><Check size={14} />No target label is needed</li></ul><a className="text-link" href="/sample-customers.csv" download>Download an example<ArrowDownToLine size={14} /></a></aside></div>
    {response && <><div className="batch-summary"><div><span className="metric-icon mint"><UsersRound size={19} /></span><div><span>Customers processed</span><strong>{response.summary.total.toLocaleString()}</strong></div></div><div><span className="metric-icon sand"><FileSpreadsheet size={19} /></span><div><span>Predicted to churn</span><strong>{response.summary.predicted_churn.toLocaleString()}</strong></div></div><div><span className="metric-icon sage"><ShieldCheck size={19} /></span><div><span>Average churn probability</span><strong>{(response.summary.average_probability * 100).toFixed(1)}%</strong></div></div></div><section className="panel batch-results"><div className="panel-heading"><div><h2>Your batch, decoded</h2><p>{response.summary.total.toLocaleString()} predictions · {file?.name}</p></div><button className="button button-secondary" onClick={downloadResults}><ArrowDownToLine size={14} />Download results</button></div><div className="table-filters"><div className="search-field"><Search size={15} /><input aria-label="Search customer IDs" placeholder="Search customer IDs…" value={search} onChange={(event) => { setSearch(event.target.value); setPage(1); }} />{search && <button onClick={() => { setSearch(""); setPage(1); }} aria-label="Clear search"><X size={13} /></button>}</div><select aria-label="Filter predictions" value={filter} onChange={(event) => { setFilter(event.target.value); setPage(1); }}><option value="all">All predictions</option><option value="Yes">Predicted churn</option><option value="No">Predicted stay</option></select></div><div className="table-scroll"><table><thead><tr><th>Customer ID</th><th>Churn probability</th><th>Prediction</th><th>Model version</th></tr></thead><tbody>{visible.map((r, index) => <tr key={`${index}-${r.customerID}`}><td><strong>{r.customerID || `Customer ${(page - 1) * 10 + index + 1}`}</strong></td><td><span className="probability-cell"><span className="mini-track"><span style={{ width: `${r.churn_probability * 100}%`, background: r.prediction === "Yes" ? "#c69c59" : undefined }} /></span>{(r.churn_probability * 100).toFixed(1)}%</span></td><td><span className={`rate-pill ${r.prediction === "Yes" ? "rate-pill-amber" : ""}`}>{r.prediction === "Yes" ? "Churn" : "Stay"}</span></td><td className="font-mono text-xs" title={r.model_version}>{r.model_version.slice(0, 8)}</td></tr>)}{visible.length === 0 && <tr><td colSpan={4} className="text-center">No customers match this search.</td></tr>}</tbody></table></div><div className="pagination"><span>{results.length ? `${(page - 1) * 10 + 1}–${Math.min(page * 10, results.length)}` : "0"} of {results.length.toLocaleString()} results</span><div><button className="icon-button" aria-label="Previous page" disabled={page <= 1} onClick={() => setPage(page - 1)}><ChevronLeft size={17} /></button><span>{page} / {totalPages}</span><button className="icon-button" aria-label="Next page" disabled={page >= totalPages} onClick={() => setPage(page + 1)}><ChevronRight size={17} /></button></div></div></section></>}
  </>;
}
