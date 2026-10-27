"""Flat Telco event contract shared by publishing and future consumers."""
import csv
import hashlib
import json
import math
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from uuid import UUID, uuid5

from opsyntra.data.validation import CATEGORICAL_VALUES, FEATURE_COLUMNS, IDENTIFIER, NUMERIC_COLUMNS, TARGET

CUSTOMER_COLUMNS = (IDENTIFIER, *FEATURE_COLUMNS)
METADATA_COLUMNS = {"schema_version", "event_id", "event_ts", "run_id", "mode", "source_row", "source_sha256"}


def utc_now():
    return datetime.now(timezone.utc).isoformat(timespec="microseconds").replace("+00:00", "Z")


def file_sha256(path):
    digest = hashlib.sha256()
    with Path(path).open("rb") as source:
        while block := source.read(1024 * 1024):
            digest.update(block)
    return digest.hexdigest()


def normalize_customer(row):
    customer = {}
    if not set(CUSTOMER_COLUMNS) <= set(row):
        raise ValueError("Customer record is missing required input columns")
    for name in CUSTOMER_COLUMNS + ((TARGET,) if TARGET in row else ()):
        value = row[name]
        text = "" if value is None else str(value).strip()
        if not text:
            if name == "TotalCharges":
                customer[name] = None
                continue
            raise ValueError(f"{name}: missing values are not permitted")
        if name in NUMERIC_COLUMNS:
            if isinstance(value, bool):
                raise ValueError(f"{name}: booleans are not numeric input")
            try:
                number = float(text)
            except (TypeError, ValueError) as error:
                raise ValueError(f"{name}: invalid numeric value") from error
            if not math.isfinite(number) or number < 0:
                raise ValueError(f"{name}: values must be finite and nonnegative")
            if name in ("SeniorCitizen", "tenure"):
                if not number.is_integer() or number > 2**63 - 1:
                    raise ValueError(f"{name}: integer required")
                number = int(number)
            if name == "SeniorCitizen" and number not in (0, 1):
                raise ValueError("SeniorCitizen must be 0 or 1")
            customer[name] = number
        else:
            if name in CATEGORICAL_VALUES and text not in CATEGORICAL_VALUES[name]:
                raise ValueError(f"{name}: unexpected category {text!r}")
            customer[name] = text
    return customer


def customer_rows(path):
    with Path(path).open("r", encoding="utf-8-sig", newline="") as source:
        reader = csv.DictReader(source)
        columns = reader.fieldnames or []
        allowed = set(CUSTOMER_COLUMNS) | {TARGET}
        if (len(columns) != len(set(columns)) or not set(CUSTOMER_COLUMNS) <= set(columns)
                or not set(columns) <= allowed):
            raise ValueError("CSV needs customerID and all 19 predictors, optional Churn, and no duplicate/extra headers")
        for index, row in enumerate(reader):
            if None in row or any(value is None for value in row.values()):
                raise ValueError(f"CSV record {index + 1}: incorrect number of fields")
            try:
                yield index, normalize_customer(row)
            except ValueError as error:
                raise ValueError(f"CSV record {index + 1}: {error}") from error


@dataclass(frozen=True)
class CustomerSource:
    path: Path
    sha256: str
    rows: int

    @classmethod
    def inspect(cls, path):
        path = Path(path).resolve()
        before = file_sha256(path)
        seen = set()
        for _, row in customer_rows(path):
            if row[IDENTIFIER] in seen:
                raise ValueError(f"Duplicate customerID: {row[IDENTIFIER]}")
            seen.add(row[IDENTIFIER])
        if not seen:
            raise ValueError("Source CSV is empty")
        if before != file_sha256(path):
            raise ValueError("Source CSV changed during validation")
        return cls(path, before, len(seen))


def make_event(customer, *, run_id, source, row_index, sequence, mode, started_at, rng=None, jitter_seconds=60):
    if mode == "batch":
        timestamp = datetime.fromisoformat(started_at.replace("Z", "+00:00")) + timedelta(microseconds=row_index)
        identity = f"batch:{source.sha256}:{row_index}:{customer[IDENTIFIER]}"
    else:
        timestamp = datetime.now(timezone.utc) - timedelta(seconds=rng.uniform(0, jitter_seconds))
        identity = f"streaming:{sequence}"
    return {**customer, "schema_version": 1, "event_id": str(uuid5(UUID(run_id), identity)),
            "run_id": run_id, "mode": mode, "source_row": row_index, "source_sha256": source.sha256,
            "event_ts": timestamp.isoformat(timespec="microseconds").replace("+00:00", "Z")}


def encode_event(event):
    return json.dumps(event, ensure_ascii=False, allow_nan=False, separators=(",", ":")).encode("utf-8")


def validate_event(event, key=None):
    """Validate wire types too: blanks become null, never NaN or numeric strings."""
    expected = set(CUSTOMER_COLUMNS) | METADATA_COLUMNS | ({TARGET} if TARGET in event else set())
    if set(event) != expected:
        raise ValueError("Event schema is incomplete or has unexpected fields")
    customer = normalize_customer(event)
    for name in NUMERIC_COLUMNS:
        value = event[name]
        if name == "TotalCharges" and value is None:
            continue
        if type(value) not in (int, float) or (name in ("SeniorCitizen", "tenure") and type(value) is not int):
            raise ValueError(f"{name}: JSON numeric type required")
    if any(event[name] != value for name, value in customer.items()):
        raise ValueError("Customer values must be normalized")
    if type(event["schema_version"]) is not int or event["schema_version"] != 1 or event["mode"] not in ("batch", "streaming"):
        raise ValueError("Unsupported event version or mode")
    UUID(event["event_id"])
    UUID(event["run_id"])
    if type(event["source_row"]) is not int or event["source_row"] < 0:
        raise ValueError("Invalid source row")
    if len(event["source_sha256"]) != 64 or any(c not in "0123456789abcdef" for c in event["source_sha256"]):
        raise ValueError("Invalid source checksum")
    timestamp = datetime.fromisoformat(event["event_ts"].replace("Z", "+00:00"))
    if timestamp.tzinfo is None or timestamp.utcoffset() != timedelta(0):
        raise ValueError("event_ts must be a UTC timestamp")
    if key is not None and key != event[IDENTIFIER]:
        raise ValueError("Kafka message key differs from customerID")
    return event
