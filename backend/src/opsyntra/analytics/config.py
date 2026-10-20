"""Explicit server-side credentials. Never expose DSNs in logs or the browser."""
import os
from dataclasses import dataclass, field
from pathlib import Path
from zoneinfo import ZoneInfo

ENV_FILE = Path(__file__).resolve().parents[3] / ".env"


@dataclass(frozen=True)
class AnalyticsConfig:
    write_url: str = field(repr=False)
    read_url: str = field(repr=False)
    migration_url: str = field(repr=False)
    display_timezone: str = "Asia/Colombo"
    connect_timeout: int = 10

    @classmethod
    def from_env(cls, env=None, env_file=ENV_FILE):
        if env is None:
            from dotenv import dotenv_values
            env = {**dotenv_values(env_file), **os.environ}
        names = ["OPSYNTRA_ANALYTICS_WRITE_URL", "OPSYNTRA_ANALYTICS_READ_URL", "OPSYNTRA_ANALYTICS_MIGRATION_URL"]
        values = [env.get(name, "") for name in names]
        for name, value in zip(names, values):
            if not isinstance(value, str) or not value.strip() or "CHANGE_ME" in value:
                raise ValueError(f"Set {name} to a real PostgreSQL connection URL")
            if not value.startswith(("postgresql://", "postgres://")):
                raise ValueError(f"{name} must be a PostgreSQL URL")
        zone = env.get("OPSYNTRA_ANALYTICS_TIMEZONE", "Asia/Colombo")
        ZoneInfo(zone)
        timeout = int(env.get("OPSYNTRA_ANALYTICS_CONNECT_TIMEOUT", "10"))
        if not 1 <= timeout <= 120:
            raise ValueError("Analytics connection timeout must be between 1 and 120 seconds")
        return cls(*values, display_timezone=zone, connect_timeout=timeout)

    def connect(self, role="read"):
        import psycopg
        from psycopg.rows import dict_row
        if role not in ("read", "write", "migration"):
            raise ValueError("Unknown analytics connection role")
        connection = psycopg.connect(getattr(self, role + "_url"), autocommit=True,
                                     connect_timeout=self.connect_timeout, row_factory=dict_row)
        connection.execute("SET TIME ZONE 'UTC'")
        connection.execute("SET statement_timeout = '30s'")
        if role == "read":
            connection.execute("SET default_transaction_read_only = on")
        return connection
