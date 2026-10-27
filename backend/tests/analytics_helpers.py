import hashlib
from uuid import uuid4

from opsyntra.kafka.events import normalize_customer
from tests.helpers import sample_data


def raw_event(customer_id=None, contract="Month-to-month"):
    value = normalize_customer(sample_data(1).iloc[0].to_dict())
    value.update(customerID=customer_id or "TEST-" + uuid4().hex, Contract=contract,
                 schema_version=1, event_id=str(uuid4()), event_ts="2026-10-20T08:00:00Z",
                 run_id=str(uuid4()), mode="batch", source_row=0, source_sha256="a" * 64)
    return value


def prediction_event(raw, model="test-model", engine="sklearn"):
    return {"schema_version": 1, "prediction_id": hashlib.sha256((engine + model + raw["event_id"]).encode()).hexdigest(),
            "event_id": raw["event_id"], "customerID": raw["customerID"], "event_ts": raw["event_ts"],
            "processed_ts": "2026-10-20T08:00:02Z", "engine": engine, "model_version": model,
            "churn_probability": .7, "prediction": "Yes"}


def deadletter_event():
    from types import SimpleNamespace
    from opsyntra.kafka.consumption import deadletter_job
    message = SimpleNamespace(topic=lambda: "telco.raw.customers", partition=lambda: 0,
                              offset=lambda: int(uuid4().hex[:12], 16), value=lambda: b"not-json", key=lambda: b"bad-json")
    return deadletter_job(message, "Malformed JSON", {"topics": {"deadletter": "telco.deadletter"}}, "test-cluster")["payload"]
