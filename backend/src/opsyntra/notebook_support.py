"""Verified notebook inputs and read-only, reproducible analytics snapshots."""
import hashlib
import json
from datetime import datetime, timedelta, timezone
from pathlib import Path

import numpy as np
import pandas as pd

from .config import load_config, load_training_config


def digest(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def portable_path(root, value):
    """Reports can have Windows separators even when opened on Linux."""
    path = (Path(root) / str(value).replace("\\", "/")).resolve()
    if not path.is_relative_to(Path(root).resolve()):
        raise ValueError("Notebook input must stay inside the backend directory")
    return path


class Exports:
    def __init__(self, backend, name):
        self.backend = Path(backend).resolve()
        self.directory = self.backend / "reports/notebooks" / name
        self.directory.mkdir(parents=True, exist_ok=True)
        self.inputs = {}
        self.outputs = {}

    def input(self, path):
        path = Path(path).resolve()
        self.inputs[str(path.relative_to(self.backend)).replace("\\", "/")] = digest(path)
        return path

    def table(self, name, frame):
        path = self.directory / (name + ".csv")
        frame.to_csv(path, index=False, lineterminator="\n")
        self.outputs[path.name] = digest(path)
        return frame

    def figure(self, name, figure):
        for suffix in ("png", "svg"):
            path = self.directory / f"{name}.{suffix}"
            figure.savefig(path, dpi=160, bbox_inches="tight", metadata={"Creator": "Opsyntra"})
            self.outputs[path.name] = digest(path)

    def finish(self, **metadata):
        report = {"schema_version": 1, "inputs": self.inputs, "outputs": self.outputs, **metadata}
        (self.directory / "manifest.json").write_text(json.dumps(report, indent=2, allow_nan=False) + "\n", encoding="utf-8")
        return report


def verified_partition(backend, name, exports):
    """Read only the requested partition, never load held-out test feature rows."""
    if name not in ("train", "validation"):
        raise ValueError("Notebooks may explore train/validation only")
    config = load_config(Path(backend) / "config.yaml")
    manifest = json.loads(exports.input(config.output_dir / "manifest.json").read_text())
    for path, expected in ((config.raw_path, manifest["source_sha256"]),
                           (config.output_dir / "memberships.csv", manifest["membership_sha256"]),
                           (config.output_dir / f"{name}.csv", manifest["splits"][name]["sha256"])):
        if digest(exports.input(path)) != expected:
            raise ValueError("Input checksum differs from saved split manifest")
    frame = pd.read_csv(config.output_dir / f"{name}.csv", dtype={"customerID": "string"})
    members = pd.read_csv(config.output_dir / "memberships.csv", dtype={"customerID": "string"})
    expected_ids = set(members.loc[members.split.eq(name), "customerID"])
    if frame.customerID.duplicated().any() or set(frame.customerID) != expected_ids:
        raise ValueError("Partition does not match persisted customer membership")
    frame["TotalCharges"] = pd.to_numeric(frame.TotalCharges, errors="raise")
    if len(frame) != manifest["splits"][name]["rows"]:
        raise ValueError("Partition row count differs from manifest")
    return frame, manifest


def model_reports(backend, exports):
    reports = [json.loads(exports.input(Path(backend) / "reports" / name).read_text()) for name in
               ("model_training_report.json", "spark_training_report.json", "engine_comparison.json")]
    sk, spark, comparison = reports
    if any(r["status"] != "PASS" for r in reports) or not comparison["same_test_customers"]:
        raise ValueError("Require successful saved training/comparison reports")
    for key in ("source_sha256", "membership_sha256"):
        comparison_key = "dataset_sha256" if key == "source_sha256" else key
        if sk["provenance"][key] != spark["provenance"][key] or comparison[comparison_key] != sk["provenance"][key]:
            raise ValueError("Model reports refer to different datasets/splits")
    for engine, report in (("sklearn", sk), ("spark", spark)):
        if report["test_used_for_selection"]:
            raise ValueError("Test metrics must not select candidates")
        selected = report["selected_model"]
        if report["selection_metric"] != "average_precision" or report["candidates"][selected]["validation"]["average_precision"] != max(c["validation"]["average_precision"] for c in report["candidates"].values()):
            raise ValueError("Saved champion is inconsistent with validation AP ranking")
        if comparison[engine]["model_version"] != report["selected_model_version"]:
            raise ValueError("Comparison refers to an older selected model; regenerate engine comparison")
        if comparison[engine]["decision_threshold"] != report["decision_threshold"]:
            raise ValueError("Comparison threshold differs from training report")
        for metric in ("accuracy", "precision", "recall", "f1", "roc_auc", "average_precision"):
            np.testing.assert_allclose(comparison[engine]["test_metrics"][metric], report["selected_test_metrics"][metric], rtol=0, atol=1e-10)
    return reports


def validation_scores(backend, reports, validation, exports):
    """Inference only for sklearn; native Spark scores come from its saved CSVs."""
    import joblib
    from sklearn.metrics import average_precision_score, roc_auc_score
    from .data_validation import FEATURE_COLUMNS
    result = {}
    sk, spark, _ = reports
    training_config = load_training_config(Path(backend) / "config.yaml")
    labels = validation.Churn.map({"No": 0, "Yes": 1}).to_numpy()
    order = validation.customerID.astype(str).tolist()
    for engine, report in (("sklearn", sk), ("spark", spark)):
        for name, candidate in report["candidates"].items():
            if engine == "sklearn":
                artifact = portable_path(training_config.artifact_dir, candidate["artifact"])
                if digest(exports.input(artifact)) != candidate["artifact_sha256"]:
                    raise ValueError("Saved sklearn candidate checksum mismatch")
                model = joblib.load(artifact)
                scores = model.predict_proba(validation[list(FEATURE_COLUMNS)])[:, 1]
            else:
                path = portable_path(backend, candidate["pipeline_path"]).parent / (name + "_validation_scores.csv")
                saved = pd.read_csv(exports.input(path), dtype={"customerID": "string"}).sort_values("customerID")
                if saved.customerID.astype(str).tolist() != order:
                    raise ValueError("Spark validation customers do not match persisted membership")
                np.testing.assert_array_equal(saved.label, labels)
                scores = saved.churn_probability.to_numpy()
            np.testing.assert_allclose(average_precision_score(labels, scores), candidate["validation"]["average_precision"], atol=1e-10, rtol=0)
            np.testing.assert_allclose(roc_auc_score(labels, scores), candidate["validation"]["roc_auc"], atol=1e-10, rtol=0)
            result[(engine, name)] = (labels, scores)
    return result


def analytics_snapshot(*, engine="sklearn", model_version=None, as_of=None, hours=24, config=None):
    """Last complete UTC hours; all tables use one repeatable-read SQL snapshot.

    Hour-aligned bounds make the existing hourly and segment views exactly
    comparable with dashboard queries using these explicit start/end bounds.
    No payloads, connection strings, or write/migration credentials are exported.
    """
    from .analytics.config import AnalyticsConfig
    if engine not in ("sklearn", "spark") or type(hours) is not int or not 1 <= hours <= 744:
        raise ValueError("Use a supported engine and a 1..744 hour window")
    if as_of is None:
        as_of = datetime.now(timezone.utc)
    elif isinstance(as_of, str):
        as_of = datetime.fromisoformat(as_of.replace("Z", "+00:00"))
    if as_of.tzinfo is None:
        raise ValueError("as_of must have a timezone")
    end = as_of.astimezone(timezone.utc).replace(minute=0, second=0, microsecond=0)
    start = end - timedelta(hours=hours)
    config = config or AnalyticsConfig.from_env(required_roles=("read",))
    tables = {}
    try:
        with config.connect("read") as c, c.transaction():
            c.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY")
            c.execute("SET LOCAL statement_timeout='15s'")
            def query(sql, params=()):
                cursor = c.execute(sql, params)
                return pd.DataFrame(cursor.fetchall(), columns=[column.name for column in cursor.description])
            observed = c.execute("SELECT transaction_timestamp() AS ts").fetchone()["ts"]
            models = query("SELECT engine,model_version,max(ingested_at) AS last_ingested_at FROM analytics.prediction_events GROUP BY engine,model_version ORDER BY last_ingested_at DESC,engine,model_version")
            candidates = models.loc[models.engine.eq(engine)]
            version = model_version or (candidates.iloc[0].model_version if len(candidates) else None)
            params = (engine, version, start, end)
            where = "engine=%s AND model_version=%s AND event_ts >= %s AND event_ts < %s"
            tables["models"] = models
            tables["summary"] = query("""SELECT count(*) AS prediction_volume,count(DISTINCT event_id) AS unique_events,
                count(DISTINCT customer_id) AS unique_customers,avg((prediction='Yes')::int)::float8 AS predicted_churn_share,
                avg(churn_probability) AS mean_risk,count(*) FILTER(WHERE raw_available) AS matched_raw_predictions
                FROM analytics.prediction_details WHERE """ + where, params)
            tables["hourly"] = query("""SELECT * FROM analytics.prediction_hourly WHERE engine=%s AND model_version=%s
                AND bucket_start >= %s AND bucket_start < %s ORDER BY bucket_start""", params)
            tables["segments"] = query("""SELECT contract,payment_method,sum(prediction_volume)::bigint AS prediction_volume,
                sum(mean_risk*prediction_volume)/nullif(sum(prediction_volume),0) AS mean_risk
                FROM analytics.prediction_hourly_segments WHERE engine=%s AND model_version=%s
                AND bucket_start >= %s AND bucket_start < %s GROUP BY contract,payment_method ORDER BY prediction_volume DESC""", params)
            tables["recent"] = query("""SELECT prediction_id,event_id,customer_id,engine,model_version,churn_probability,prediction,
                event_ts,processed_ts,ingested_at,contract,payment_method FROM analytics.prediction_details WHERE """ + where + " ORDER BY ingested_at DESC,prediction_id DESC LIMIT 20", params)
            tables["risk_distribution"] = query("""SELECT CASE WHEN churn_probability < .3 THEN '0–30%%' WHEN churn_probability < .7
                THEN '30–70%%' ELSE '70–100%%' END AS risk_band,count(*) AS prediction_volume
                FROM analytics.prediction_details WHERE """ + where + " GROUP BY risk_band ORDER BY risk_band", params)
            tables["late_events"] = query("""SELECT prediction_id,event_id,event_ts,ingested_at,
                extract(epoch FROM ingested_at-event_ts)::float8 AS event_to_ingestion_seconds
                FROM analytics.prediction_details WHERE """ + where + " AND ingested_at-event_ts > interval '1 hour' ORDER BY event_to_ingestion_seconds DESC LIMIT 20", params)
            tables["late_summary"] = query("""SELECT count(*) FILTER(WHERE ingested_at-event_ts > interval '1 hour') AS over_one_hour,
                count(*) FILTER(WHERE ingested_at < event_ts) AS clock_skew_events FROM analytics.prediction_details WHERE """ + where, params)
            tables["deliveries"] = query("""WITH per_prediction AS (SELECT p.prediction_id,count(d.prediction_id) AS n
                FROM analytics.prediction_events p LEFT JOIN analytics.kafka_deliveries d USING(prediction_id)
                WHERE """ + where + """ GROUP BY p.prediction_id)
                SELECT count(*) AS logical_predictions,coalesce(sum(n),0)::bigint AS physical_deliveries,
                coalesce(sum(greatest(n-1,0)),0)::bigint AS repeated_deliveries,
                count(*) FILTER(WHERE n=0) AS predictions_without_delivery_provenance FROM per_prediction""", params)
            tables["quarantine"] = query("""SELECT event_kind,error_type,count(*) AS records FROM analytics.ingestion_errors
                WHERE ingested_at >= %s AND ingested_at < %s GROUP BY event_kind,error_type ORDER BY event_kind,error_type""", (start,end))
    except Exception as error:
        # Notebook tracebacks must never contain a DSN or driver connection error.
        raise RuntimeError("Analytics snapshot unavailable. Start PostgreSQL, apply migrations and configure the reader login; see docs/NOTEBOOKS.md.") from None
    n = int(tables["summary"].iloc[0].prediction_volume)
    if int(tables["hourly"].prediction_volume.sum()) != n or int(tables["segments"].prediction_volume.sum()) != n or int(tables["risk_distribution"].prediction_volume.sum()) != n:
        raise ValueError("Analytics view totals disagree with logical prediction counts")
    metadata = {"engine": engine, "model_version": version, "start": start.isoformat(), "end": end.isoformat(),
                "snapshot_at": observed.isoformat(), "display_timezone": config.display_timezone,
                "window": "last complete UTC hours; current partial hour excluded", "late_threshold_seconds": 3600,
                "synthetic_replay": True, "quarantine_scope": "all engines/models, by quarantine ingestion time"}
    return tables, metadata
