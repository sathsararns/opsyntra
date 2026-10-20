"""Normalize supported Kafka v1 records into immutable logical analytics facts."""
import copy
import hashlib
import json
import math
from dataclasses import dataclass
from datetime import datetime, timezone
from uuid import UUID

from ..data_validation import FEATURE_COLUMNS
from ..kafka_events import normalize_customer, validate_event


class EventConflict(ValueError):
    """An immutable logical ID or Kafka coordinate was reused inconsistently."""


def text(value, name):
    if not isinstance(value, str) or not value.strip() or value != value.strip():
        raise ValueError(f"{name} must be a nonempty normalized string")
    return value


def checksum(value, name):
    if not isinstance(value, str) or len(value) != 64 or any(c not in "0123456789abcdef" for c in value):
        raise ValueError(f"{name} must be a lowercase SHA-256")
    return value


def timestamp(value):
    result = datetime.fromisoformat(text(value, "timestamp").replace("Z", "+00:00"))
    if result.tzinfo is None:
        raise ValueError("Timestamp must include its UTC offset")
    return result.astimezone(timezone.utc).isoformat(timespec="microseconds").replace("+00:00", "Z")


def canonical_hash(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, ensure_ascii=False,
                                    allow_nan=False, separators=(",", ":")).encode()).hexdigest()


@dataclass(frozen=True)
class Source:
    cluster_id: str
    topic: str
    partition: int
    offset: int

    def __post_init__(self):
        text(self.cluster_id, "cluster_id")
        text(self.topic, "topic")
        for name in ("partition", "offset"):
            if type(getattr(self, name)) is not int or not 0 <= getattr(self, name) <= (2**31-1 if name == "partition" else 2**63-1):
                raise ValueError(f"Invalid Kafka {name}")


def normalize(kind, payload, key):
    if kind not in ("raw", "prediction", "deadletter") or not isinstance(payload, dict):
        raise ValueError("Expected a supported JSON event object")
    value = copy.deepcopy(payload)
    # JSON encoding also rejects non-finite numbers and non-JSON values.
    canonical_hash(value)
    if type(value.get("schema_version")) is not int or value["schema_version"] != 1:
        raise ValueError("Unsupported analytics event schema_version")
    if kind == "raw":
        if key != value.get("customerID"):
            raise ValueError("Raw event requires a matching customerID key")
        try:
            validate_event(value, key)
        except (AttributeError, TypeError) as error:
            raise ValueError("Raw event contains invalid field types") from error
        value.update(normalize_customer(value))
        value["run_id"] = str(UUID(value["run_id"]))
    elif kind == "prediction":
        fields = {"schema_version", "prediction_id", "event_id", "customerID", "engine", "model_version",
                  "churn_probability", "prediction", "event_ts", "processed_ts"}
        if set(value) != fields:
            raise ValueError("Prediction fields do not match the supported v1 contract")
        checksum(value["prediction_id"], "prediction_id")
        text(value["customerID"], "customerID")
        text(value["model_version"], "model_version")
        if key != value["customerID"] or value["engine"] not in ("sklearn", "spark"):
            raise ValueError("Invalid prediction key or engine")
        p = value["churn_probability"]
        if type(p) not in (float, int) or not math.isfinite(p) or not 0 <= p <= 1 or value["prediction"] not in ("Yes", "No"):
            raise ValueError("Invalid churn probability or prediction label")
        value["churn_probability"] = float(p)
        value["processed_ts"] = timestamp(value["processed_ts"])
    else:
        fields = {"schema_version", "event_id", "processed_ts", "original_event_id", "customerID", "error", "source",
                  "raw_value_is_null", "raw_key_is_null", "raw_value_base64", "raw_key_base64",
                  "raw_value_truncated", "raw_key_truncated", "raw_value_sha256"}
        if set(value) != fields or not isinstance(value["source"], dict):
            raise ValueError("Dead-letter fields do not match the supported v1 contract")
        if set(value["source"]) != {"cluster_id", "topic", "partition", "offset"}:
            raise ValueError("Invalid dead-letter source")
        Source(**value["source"])
        if not isinstance(value["error"], dict) or set(value["error"]) != {"type", "message"}:
            raise ValueError("Invalid dead-letter error")
        text(value["error"]["type"], "error.type")
        text(value["error"]["message"], "error.message")
        checksum(value["raw_value_sha256"], "raw_value_sha256")
        import base64
        for name in ("raw_value", "raw_key"):
            if not isinstance(value[name + "_base64"], str):
                raise ValueError("Dead-letter bytes must be base64 strings")
            decoded = base64.b64decode(value[name + "_base64"], validate=True)
            for suffix in ("_is_null", "_truncated"):
                if type(value[name + suffix]) is not bool:
                    raise ValueError("Dead-letter flags must be booleans")
            if value[name + "_is_null"] and (decoded or value[name + "_truncated"]):
                raise ValueError("Null dead-letter bytes cannot contain data or be truncated")
            if name == "raw_value" and not value[name + "_truncated"] and hashlib.sha256(decoded).hexdigest() != value["raw_value_sha256"]:
                raise ValueError("Dead-letter raw bytes do not match their checksum")
        if value["original_event_id"] is not None:
            value["original_event_id"] = str(UUID(text(value["original_event_id"], "original_event_id")))
        if value["customerID"] is not None:
            text(value["customerID"], "customerID")
        value["processed_ts"] = timestamp(value["processed_ts"])
        if key != payload["event_id"]:
            raise ValueError("Dead-letter key differs from its stable event ID")
    value["event_id"] = str(UUID(text(value["event_id"], "event_id")))
    if kind != "deadletter":
        value["event_ts"] = timestamp(value["event_ts"])
    return value


def logical_hash(kind, value):
    # A crash before the inference ledger commit may regenerate processed_ts.
    # It is delivery metadata, not a different logical result. Keep the first.
    identity = {k: v for k, v in value.items() if k != "processed_ts"}
    return canonical_hash(identity)


def features(value):
    return {name: value[name] for name in FEATURE_COLUMNS}
