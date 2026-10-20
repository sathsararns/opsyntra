"""Atomic append-only ingestion; caller may nest it in a Kafka batch transaction."""
from psycopg import sql
from psycopg.errors import UniqueViolation
from psycopg.types.json import Jsonb

from .contracts import EventConflict, Source, canonical_hash, features, logical_hash, normalize


class AnalyticsStore:
    def __init__(self, connection):
        self.connection = connection

    def _identity(self, value):
        values = (value["event_id"], value["customerID"], value["event_ts"])
        self.connection.execute("""INSERT INTO analytics.customer_events(event_id,customer_id,event_ts)
            VALUES (%s,%s,%s) ON CONFLICT (event_id) DO NOTHING""", values)
        row = self.connection.execute("""SELECT customer_id = %s AND event_ts = %s::timestamptz AS matches
            FROM analytics.customer_events WHERE event_id = %s""", (values[1], values[2], values[0])).fetchone()
        if not row["matches"]:
            raise EventConflict("event_id reused with another customer or event timestamp")

    def _insert(self, table, identity, fields):
        columns = list(fields)
        query = sql.SQL("INSERT INTO analytics.{} ({}) VALUES ({}) ON CONFLICT ({}) DO NOTHING RETURNING 1").format(
            sql.Identifier(table), sql.SQL(",").join(map(sql.Identifier, columns)),
            sql.SQL(",").join(sql.Placeholder() for _ in columns), sql.Identifier(identity))
        inserted = self.connection.execute(query, tuple(fields.values())).fetchone() is not None
        if not inserted:
            row = self.connection.execute(sql.SQL("SELECT content_sha256 FROM analytics.{} WHERE {} = %s").format(
                sql.Identifier(table), sql.Identifier(identity)), (fields[identity],)).fetchone()
            if row["content_sha256"] != fields["content_sha256"]:
                raise EventConflict(f"{identity} reused with different logical contents")
        return inserted

    def _delivery(self, kind, value, source, key, wire_hash):
        names = {"raw": "raw_event_id", "prediction": "prediction_id", "deadletter": "deadletter_event_id"}
        identity = value["prediction_id"] if kind == "prediction" else value["event_id"]
        coordinates = (source.cluster_id, source.topic, source.partition, source.offset)
        statement = sql.SQL('''INSERT INTO analytics.kafka_deliveries
            (cluster_id,topic,partition,"offset",{},message_key,payload_sha256)
            VALUES (%s,%s,%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING RETURNING 1''').format(sql.Identifier(names[kind]))
        inserted = self.connection.execute(statement, (*coordinates, identity, key, wire_hash)).fetchone() is not None
        if not inserted:
            row = self.connection.execute('''SELECT raw_event_id,prediction_id,deadletter_event_id,message_key,payload_sha256
                FROM analytics.kafka_deliveries WHERE cluster_id=%s AND topic=%s AND partition=%s AND "offset"=%s''', coordinates).fetchone()
            if (str(row[names[kind]]), row["message_key"], row["payload_sha256"]) != (identity, key, wire_hash):
                raise EventConflict("Kafka source coordinates reused with different contents")
        return inserted

    def ingest(self, kind, payload, *, source, key):
        if not isinstance(source, Source):
            raise ValueError("A validated Kafka Source is required")
        value = normalize(kind, payload, key)
        common = {"event_id": value["event_id"], "schema_version": value["schema_version"],
                  "content_sha256": logical_hash(kind, value), "payload": Jsonb(payload)}
        try:
            # Nested transactions are savepoints. An error rolls back identity,
            # logical fact AND physical provenance, including on a reused offset.
            with self.connection.transaction():
                if kind != "deadletter":
                    self._identity(value)
                    common.update(customer_id=value["customerID"], event_ts=value["event_ts"])
                if kind == "raw":
                    fields = {**common, "features": Jsonb(features(value)), "observed_churn": value.get("Churn"),
                              **{name: value[name] for name in ("run_id", "mode", "source_row", "source_sha256")}}
                    inserted = self._insert("raw_customer_events", "event_id", fields)
                elif kind == "prediction":
                    fields = {**common, **{name: value[name] for name in ("prediction_id", "engine", "model_version",
                              "churn_probability", "prediction", "processed_ts")}}
                    inserted = self._insert("prediction_events", "prediction_id", fields)
                else:
                    s = Source(**value["source"])
                    fields = {**common, "original_event_id": value["original_event_id"], "customer_id": value["customerID"],
                              "error_type": value["error"]["type"], "error_message": value["error"]["message"],
                              "source_cluster_id": s.cluster_id, "source_topic": s.topic, "source_partition": s.partition,
                              "source_offset": s.offset, "raw_value_sha256": value["raw_value_sha256"],
                              "processed_ts": value["processed_ts"]}
                    inserted = self._insert("deadletter_events", "event_id", fields)
                delivery = self._delivery(kind, value, source, key, canonical_hash(payload))
        except UniqueViolation as error:
            raise EventConflict("Another logical ID already identifies this event/model or dead-letter source") from error
        return {"logical_inserted": inserted, "delivery_inserted": delivery,
                "logical_id": value["prediction_id"] if kind == "prediction" else value["event_id"]}
