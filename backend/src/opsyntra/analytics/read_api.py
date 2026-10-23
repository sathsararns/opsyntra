"""Bounded, read-only analytics endpoints; no model or writer credentials needed."""
import logging
import os
import threading
import time
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from typing import Annotated, Literal

from fastapi import APIRouter, HTTPException, Query, Response
from fastapi.responses import JSONResponse
from pydantic import AwareDatetime, BaseModel, ConfigDict, Field, model_validator

from .config import AnalyticsConfig

router = APIRouter(prefix="/api/analytics", tags=["analytics"])
LOG = logging.getLogger(__name__)


class Filters(BaseModel):
    model_config = ConfigDict(extra="forbid", allow_inf_nan=False)
    engine: Literal["sklearn", "spark"] = "sklearn"
    model_version: str | None = Field(None, min_length=1, max_length=128)
    lookback_hours: int = Field(24, ge=1, le=744)
    start: AwareDatetime | None = None
    end: AwareDatetime | None = None
    customer_id: str | None = Field(None, min_length=1, max_length=128)
    min_probability: float = Field(0, ge=0, le=1)
    max_probability: float = Field(1, ge=0, le=1)
    prediction: Literal["Yes", "No"] | None = None
    contract: Literal["Month-to-month", "One year", "Two year"] | None = None
    payment_method: Literal["Electronic check", "Mailed check", "Bank transfer (automatic)", "Credit card (automatic)"] | None = None

    @model_validator(mode="after")
    def ranges(self):
        end = self.end or datetime.now(timezone.utc)
        start = self.start or end-timedelta(hours=self.lookback_hours)
        if start >= end or end-start > timedelta(days=31):
            raise ValueError("Use an increasing timestamp range of at most 31 days")
        if self.min_probability > self.max_probability:
            raise ValueError("Minimum probability exceeds maximum")
        return self


class PageFilters(Filters):
    limit: int = Field(25, ge=1, le=100)
    offset: int = Field(0, ge=0, le=100000)


class RiskFilters(PageFilters):
    threshold: float = Field(.7, ge=0, le=1)


class TimeFilters(Filters):
    interval: Literal["hour", "day"] = "hour"


def reader_config():
    config = AnalyticsConfig.from_env(required_roles=("read",))
    return replace(config, connect_timeout=min(config.connect_timeout, 2))


def read(operation):
    try:
        config = reader_config()
        with config.connect("read") as connection:
            connection.execute("SET statement_timeout='3s'")
            with connection.transaction():
                connection.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY")
                return operation(connection, config)
    except Exception as error:
        # No exception text/URLs/SQL payloads in public responses or logs.
        LOG.warning("Analytics read unavailable (%s)", type(error).__name__)
        raise HTTPException(503, "Analytics database unavailable. Check PostgreSQL, reader credentials and migrations.") from error


def scope(connection, filters):
    end = filters.end or datetime.now(timezone.utc)
    start = filters.start or end-timedelta(hours=filters.lookback_hours)
    models = connection.execute("""SELECT engine,model_version,max(ingested_at) AS last_ingested_at
        FROM analytics.prediction_events GROUP BY engine,model_version
        ORDER BY last_ingested_at DESC,engine,model_version LIMIT 100""").fetchall()
    version = filters.model_version or next((m["model_version"] for m in models if m["engine"] == filters.engine), None)
    return {"start": start, "end": end, "engine": filters.engine, "model_version": version}, models


def conditions(filters, selected, *, extras=True):
    clauses = ["engine=%s", "model_version=%s", "event_ts >= %s", "event_ts < %s"]
    parameters = [selected["engine"], selected["model_version"], selected["start"], selected["end"]]
    if extras:
        clauses += ["churn_probability >= %s", "churn_probability <= %s"]
        parameters += [filters.min_probability, filters.max_probability]
        for field, column in (("customer_id", "customer_id"), ("prediction", "prediction"), ("contract", "contract"), ("payment_method", "payment_method")):
            value = getattr(filters, field)
            if value is not None:
                clauses.append(column+"=%s")
                parameters.append(value)
    return " AND ".join(clauses), parameters


def envelope(selected, config):
    return {"scope": selected, "timezone": config.display_timezone, "generated_at": datetime.now(timezone.utc)}


@router.get("/summary")
def summary(filters: Annotated[Filters, Query()], response: Response):
    response.headers["Cache-Control"] = "no-store"
    def query(c, config):
        selected, models = scope(c, filters)
        where, params = conditions(filters, selected)
        metrics = c.execute("""SELECT count(*) AS prediction_volume,count(DISTINCT event_id) AS unique_events,
            count(DISTINCT customer_id) AS unique_customers,avg((prediction='Yes')::int)::float8 AS predicted_churn_share,
            avg(churn_probability) AS mean_risk,count(*) FILTER(WHERE raw_available) AS matched_raw_predictions,
            avg(processing_latency_seconds) AS mean_processing_latency_seconds,max(ingested_at) AS last_ingested_at
            FROM analytics.prediction_details WHERE """+where, params).fetchone()
        segments = {}
        for field in ("contract", "payment_method"):
            segments[field] = c.execute("SELECT "+field+" AS label, count(*) AS predictions,avg(churn_probability) AS mean_risk "
                "FROM analytics.prediction_details WHERE "+where+" GROUP BY "+field+" ORDER BY predictions DESC,"+field+" NULLS LAST LIMIT 20", params).fetchall()
        raw = c.execute("SELECT count(*) AS n FROM analytics.raw_customer_events WHERE event_ts >= %s AND event_ts < %s", (selected["start"],selected["end"])).fetchone()["n"]
        return {**envelope(selected,config), "metrics": metrics, "segments": segments, "available_models": models,
                "raw_event_volume": raw, "raw_event_scope": "all engines/models in the event-time window"}
    return read(query)


@router.get("/predictions")
def predictions(filters: Annotated[PageFilters, Query()], response: Response):
    response.headers["Cache-Control"] = "no-store"
    return read(lambda c, config: prediction_query(c, config, filters))


FIELDS = "prediction_id,event_id,customer_id,engine,model_version,churn_probability,prediction,event_ts,processed_ts,ingested_at,raw_available,contract,payment_method"


def prediction_query(c, config, filters, *, highest=False):
    selected, _ = scope(c, filters)
    where, params = conditions(filters, selected)
    prefix, source = "", "analytics.prediction_details"
    if highest:
        base, base_params = conditions(filters, selected, extras=False)
        prefix = "WITH latest AS (SELECT DISTINCT ON(customer_id) * FROM analytics.prediction_details WHERE "+base+" ORDER BY customer_id,event_ts DESC,ingested_at DESC,prediction_id DESC) "
        source = "latest"
        where += " AND churn_probability >= %s"
        params.append(filters.threshold)
        all_params = base_params+params
    else:
        all_params = params
    total = c.execute(prefix+"SELECT count(*) AS n FROM "+source+" WHERE "+where,all_params).fetchone()["n"]
    order = "churn_probability DESC,event_ts DESC,prediction_id DESC" if highest else "ingested_at DESC,prediction_id DESC"
    rows = c.execute(prefix+"SELECT "+FIELDS+" FROM "+source+" WHERE "+where+" ORDER BY "+order+" LIMIT %s OFFSET %s",[*all_params,filters.limit,filters.offset]).fetchall()
    return {**envelope(selected,config),"items":rows,"total":total,"limit":filters.limit,"offset":filters.offset,
            **({"threshold":filters.threshold,"selection":"latest event per customer in the selected engine/model/time scope"} if highest else {})}


@router.get("/high-risk")
def high_risk(filters: Annotated[RiskFilters, Query()], response: Response):
    response.headers["Cache-Control"] = "no-store"
    return read(lambda c, config: prediction_query(c,config,filters,highest=True))


@router.get("/timeseries")
def timeseries(filters: Annotated[TimeFilters, Query()], response: Response):
    response.headers["Cache-Control"] = "no-store"
    def query(c, config):
        selected,_ = scope(c,filters)
        where,params = conditions(filters,selected)
        rows = c.execute("""WITH actual AS (
            SELECT date_trunc(%s,event_ts,'UTC') AS bucket_start,count(*) AS predictions,
            avg((prediction='Yes')::int)::float8 AS predicted_churn_share,avg(churn_probability) AS mean_risk
            FROM analytics.prediction_details WHERE """+where+""" GROUP BY bucket_start
            ), buckets AS (SELECT generate_series(date_trunc(%s,%s::timestamptz,'UTC'),
                date_trunc(%s,%s::timestamptz-interval '1 microsecond','UTC'),%s::interval) AS bucket_start)
            SELECT b.bucket_start,coalesce(a.predictions,0) AS predictions,a.predicted_churn_share,a.mean_risk
            FROM buckets b LEFT JOIN actual a USING(bucket_start) ORDER BY b.bucket_start""",
            [filters.interval,*params,filters.interval,selected["start"],filters.interval,selected["end"],"1 "+filters.interval]).fetchall()
        return {**envelope(selected,config),"interval":filters.interval,"bucket_timezone":"UTC","items":rows}
    return read(query)


_health_lock = threading.Lock()
_broker_cache = (0.0,None,None)


def broker_health():
    """Read Kafka metadata/offsets without joining or changing the consumer group."""
    global _broker_cache
    from dotenv import dotenv_values
    from .config import ENV_FILE
    env = {**dotenv_values(ENV_FILE), **os.environ}
    group = env.get("OPSYNTRA_ANALYTICS_GROUP_ID","opsyntra-analytics-v1")
    bootstrap = env.get("OPSYNTRA_KAFKA_BOOTSTRAP_SERVERS","127.0.0.1:9092")
    topics = [env.get("OPSYNTRA_ANALYTICS_"+name+"_TOPIC",default) for name,default in
              (("RAW","telco.raw.customers"),("PREDICTION","telco.churn.predictions"),("DEADLETTER","telco.deadletter"))]
    key = (bootstrap,group,tuple(topics))
    with _health_lock:
        if _broker_cache[1] == key and time.monotonic()-_broker_cache[0] < 5:
            return _broker_cache[2]
        try:
            from confluent_kafka import TopicPartition,ConsumerGroupTopicPartitions,KafkaException,KafkaError
            from confluent_kafka.admin import AdminClient,OffsetSpec
            admin=AdminClient({"bootstrap.servers":bootstrap,"socket.timeout.ms":1500})
            meta=admin.list_topics(timeout=1.5)
            if any(t not in meta.topics or meta.topics[t].error for t in topics): raise ValueError("Topics unavailable")
            parts=[TopicPartition(t,p) for t in topics for p in meta.topics[t].partitions]
            ends=admin.list_offsets({p:OffsetSpec.latest() for p in parts},request_timeout=1.5)
            try:
                description=admin.describe_consumer_groups([group],request_timeout=1.5)[group].result(timeout=2)
                members=len(description.members)
                state="active" if members else "inactive"
                committed=admin.list_consumer_group_offsets([ConsumerGroupTopicPartitions(group,parts)],request_timeout=1.5)[group].result(timeout=2)
                if any(p.error for p in committed.topic_partitions): raise ValueError("Group offsets unavailable")
                offsets={(p.topic,p.partition):p.offset for p in committed.topic_partitions}
            except KafkaException as error:
                if error.args[0].code()!=KafkaError.GROUP_ID_NOT_FOUND: raise
                members,state,offsets=0,"inactive",{}
            lag=0
            uninitialized=0
            unknown_lag=False
            for part,future in ends.items():
                high=future.result(timeout=2).offset
                offset=offsets.get((part.topic,part.partition),-1)
                if offset < 0:
                    uninitialized+=1
                    unknown_lag=unknown_lag or high>0
                lag+=max(0,high-max(offset,0))
            result={"broker":"ready","consumer_state":state,
                    "group_id":group,"members":members,"lag":None if unknown_lag else lag,"uninitialized_partitions":uninitialized,
                    "observed_at":datetime.now(timezone.utc)}
        except Exception:
            result={"broker":"unavailable","consumer_state":"unknown","group_id":group,"members":None,"lag":None,
                    "uninitialized_partitions":None,"observed_at":datetime.now(timezone.utc)}
        _broker_cache=(time.monotonic(),key,result)
        return result


@router.get("/health")
def health(response: Response):
    response.headers["Cache-Control"]="no-store"
    try:
        def query(c, config):
            row=c.execute("""SELECT count(*) AS total_predictions,max(ingested_at) AS last_prediction_ingested_at
                FROM analytics.prediction_events""").fetchone()
            activity=c.execute("""SELECT max(ingested_at) AS last_ingested_at,
                count(*) FILTER(WHERE ingested_at > clock_timestamp()-interval '60 seconds') AS recent_deliveries
                FROM analytics.kafka_deliveries""").fetchone()
            return {"database":"ready","timezone":config.display_timezone,**row,**activity}
        data=read(query)
    except HTTPException:
        return JSONResponse({"status":"unavailable","database":"unavailable","broker":"unknown","consumer_state":"unknown",
                             "detail":"Analytics database unavailable; stored data could not be read."},status_code=503,headers={"Cache-Control":"no-store"})
    broker=broker_health()
    return {**data,**broker,"status":"ready" if broker["broker"]=="ready" else "degraded",
            "stale_after_seconds":60,"checked_at":datetime.now(timezone.utc)}
