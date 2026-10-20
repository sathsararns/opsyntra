"""A complete Kafka batch becomes durable before any input acknowledgment."""
import base64
import hashlib

from psycopg import DataError

from ..kafka_consumption import strict_json
from .contracts import EventConflict, ProvenanceConflict, Source, canonical_hash
from .storage import AnalyticsStore


def record_digest(key, value):
    return canonical_hash({"key_null": key is None, "value_null": value is None,
                           "key_sha": hashlib.sha256(key or b"").hexdigest(),
                           "value_sha": hashlib.sha256(value or b"").hexdigest()})


def coordinates(source):
    return source.cluster_id, source.topic, source.partition, source.offset


def quarantine(connection, source, kind, key, value, reason):
    # An already accepted coordinate changing into a rejection is corruption,
    # not an ordinary poison message that may safely be acknowledged.
    if connection.execute('''SELECT 1 FROM analytics.kafka_deliveries
        WHERE cluster_id=%s AND topic=%s AND partition=%s AND "offset"=%s''', coordinates(source)).fetchone():
        raise ProvenanceConflict("Accepted Kafka coordinate changed into an invalid record")
    error_type = "event_conflict" if isinstance(reason, EventConflict) else "invalid_record"
    inserted = connection.execute('''INSERT INTO analytics.ingestion_errors
        (cluster_id,topic,partition,"offset",event_kind,error_type,error_message,record_sha256,
         raw_value_sha256,raw_key_sha256,raw_value_base64,raw_key_base64,
         raw_value_is_null,raw_key_is_null,raw_value_truncated,raw_key_truncated)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        ON CONFLICT DO NOTHING RETURNING 1''', (*coordinates(source), kind, error_type,
        str(reason)[:500], record_digest(key, value), hashlib.sha256(value or b"").hexdigest(),
        hashlib.sha256(key or b"").hexdigest(), base64.b64encode((value or b"")[:65536]).decode(),
        base64.b64encode((key or b"")[:4096]).decode(), value is None, key is None,
        len(value or b"") > 65536, len(key or b"") > 4096)).fetchone() is not None
    if not inserted:
        row = connection.execute('''SELECT record_sha256,event_kind FROM analytics.ingestion_errors
            WHERE cluster_id=%s AND topic=%s AND partition=%s AND "offset"=%s''', coordinates(source)).fetchone()
        if (row["record_sha256"], row["event_kind"]) != (record_digest(key, value), kind):
            raise ProvenanceConflict("Concurrent quarantine reused a coordinate with different contents")
    return inserted


def process_batch(connection, records, topics, cluster_id, *, max_event_bytes=262144, fault_hook=lambda stage: None):
    """No Kafka calls here. A failed outer transaction cannot acknowledge inputs."""
    store = AnalyticsStore(connection)
    counts = {"records": len(records), "raw_inserted": 0, "prediction_inserted": 0, "deadletter_inserted": 0,
              "logical_duplicates": 0, "deliveries_inserted": 0, "rejected": 0, "quarantine_inserted": 0,
              "quarantine_duplicates": 0, "event_conflicts": 0}
    by_topic = {topics["raw"]: "raw", topics["predictions"]: "prediction", topics["deadletter"]: "deadletter"}
    with connection.transaction():
        for message in records:
            source = Source(cluster_id, message.topic(), message.partition(), message.offset())
            kind = by_topic[source.topic]  # unknown topics are configuration failures
            key, value = message.key(), message.value()
            lock = int.from_bytes(hashlib.sha256(repr(coordinates(source)).encode()).digest()[:8], signed=True)
            connection.execute("SELECT pg_advisory_xact_lock(%s)", (lock,))
            previous = connection.execute('''SELECT record_sha256,event_kind,error_type FROM analytics.ingestion_errors
                WHERE cluster_id=%s AND topic=%s AND partition=%s AND "offset"=%s''', coordinates(source)).fetchone()
            if previous:
                if (previous["record_sha256"], previous["event_kind"]) != (record_digest(key, value), kind):
                    raise ProvenanceConflict("Quarantined Kafka coordinate changed contents or event kind")
                counts["rejected"] += 1
                counts["quarantine_duplicates"] += 1
                counts["event_conflicts"] += previous["error_type"] == "event_conflict"
                continue
            try:
                # Savepoint also rolls back a PostgreSQL data/encoding error
                # before the quarantine row is inserted in the outer transaction.
                with connection.transaction():
                    if key is None or value is None:
                        raise ValueError("Analytics events require a key/value; tombstones are rejected")
                    if len(value) > max_event_bytes or len(key) > 4096:
                        raise ValueError("Analytics message exceeds the configured size limit")
                    decoded_key = key.decode("utf-8")
                    payload = strict_json(value.decode("utf-8"))
                    result = store.ingest(kind, payload, source=source, key=decoded_key)
            except ProvenanceConflict:
                raise
            except (ValueError, TypeError, AttributeError, OverflowError, RecursionError, DataError) as error:
                inserted = quarantine(connection, source, kind, key, value, error)
                counts["rejected"] += 1
                counts["quarantine_inserted"] += inserted
                counts["quarantine_duplicates"] += not inserted
                counts["event_conflicts"] += isinstance(error, EventConflict)
                continue
            counts[kind + "_inserted"] += result["logical_inserted"]
            counts["logical_duplicates"] += not result["logical_inserted"]
            counts["deliveries_inserted"] += result["delivery_inserted"]
        fault_hook("before_db_commit")
    # Includes ambiguous-commit replay: caller must retry without Kafka commits.
    fault_hook("after_db_commit")
    return counts
