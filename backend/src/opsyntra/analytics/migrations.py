"""Forward-only transactional migrations with advisory locking and SHA checks."""
import hashlib
from pathlib import Path

DIRECTORY = Path(__file__).resolve().parents[3] / "migrations"


def migrate(connection, directory=DIRECTORY):
    files = sorted(Path(directory).glob("[0-9][0-9][0-9][0-9]_*.sql"))
    if not files:
        raise ValueError("No analytics migration files found")
    versions = [p.name[:4] for p in files]
    if len(versions) != len(set(versions)):
        raise ValueError("Duplicate migration versions")
    result = []
    # One transaction makes first install, checksum checks and upgrades atomic.
    with connection.transaction():
        connection.execute("SELECT pg_advisory_xact_lock(7261062002)")
        connection.execute("CREATE SCHEMA IF NOT EXISTS analytics")
        connection.execute("""CREATE TABLE IF NOT EXISTS analytics.schema_migrations (
            version text PRIMARY KEY, name text NOT NULL, sha256 text NOT NULL,
            applied_at timestamptz NOT NULL DEFAULT clock_timestamp())""")
        applied = {row["version"]: row for row in connection.execute("SELECT * FROM analytics.schema_migrations")}
        if set(applied) - set(versions):
            raise ValueError("Database contains migrations missing from this checkout")
        for path in files:
            # Git may check out CRLF on Windows and LF in Linux containers.
            # Hash canonical UTF-8/LF SQL so the same migration is portable.
            content = path.read_text(encoding="utf-8").encode("utf-8")
            digest = hashlib.sha256(content).hexdigest()
            version = path.name[:4]
            if version in applied:
                if (applied[version]["sha256"], applied[version]["name"]) != (digest, path.name):
                    raise ValueError(f"Applied migration changed: {path.name}")
                result.append({"version": version, "name": path.name, "status": "already_applied", "sha256": digest})
                continue
            if applied and version < max(applied):
                raise ValueError("Refusing an out-of-order migration")
            connection.execute(content.decode("utf-8"), prepare=False)
            connection.execute("INSERT INTO analytics.schema_migrations(version,name,sha256) VALUES (%s,%s,%s)",
                               (version, path.name, digest))
            result.append({"version": version, "name": path.name, "status": "applied", "sha256": digest})
    return result
