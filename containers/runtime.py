"""Container commands; preserve named-volume data and keep service credentials separate."""
import hashlib
import json
import logging
import os
import shutil
import sys
from pathlib import Path
from urllib.parse import quote

import yaml

ROOT = Path(__file__).resolve().parent


def digest(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def prepare_config():
    config = yaml.safe_load((ROOT / "config.yaml").read_text())
    paths = {
        "data": ("raw_path", "normalized_path"),
        "splitting": ("output_dir",),
        "training": ("artifact_dir", "report_path", "comparison_path"),
        "spark": ("artifact_dir", "report_path", "comparison_path"),
        "orchestration": ("artifact_dir", "prediction_input"),
        "kafka": ("checkpoint", "report_dir"),
    }
    for section, keys in paths.items():
        for key in keys:
            config[section][key] = str(ROOT / config[section][key])
    config["report_path"] = str(ROOT / config["report_path"])
    for key in ("state_dir", "report_dir"):
        config["kafka"]["consumer"][key] = str(ROOT / config["kafka"]["consumer"][key])
    tracking = config["training"]["tracking"]
    tracking["database"] = str(ROOT / "tracking/mlflow.db")
    tracking["artifact_dir"] = str(ROOT / "mlartifacts")
    # Paths are configurable without a WSL venv or a Windows drive mounted in containers.
    if os.getenv("OPSYNTRA_ARTIFACT_DIR"):
        base = Path(os.environ["OPSYNTRA_ARTIFACT_DIR"]).resolve()
        config["training"]["artifact_dir"] = str(base / "models")
        config["spark"]["artifact_dir"] = str(base / "spark")
        config["orchestration"]["artifact_dir"] = str(base / "airflow")
        config["kafka"]["checkpoint"] = str(base / "kafka/checkpoints/batch.json")
        config["kafka"]["report_dir"] = str(base / "kafka/runs")
        config["kafka"]["consumer"]["state_dir"] = str(base / "kafka/consumer_state")
        config["kafka"]["consumer"]["report_dir"] = str(base / "kafka/consumer_runs")
    path = Path(os.environ.get("OPSYNTRA_CONFIG", "/tmp/opsyntra-config.yaml"))
    path.write_text(yaml.safe_dump(config), encoding="utf-8")
    return path, config


def role_url(role):
    password = os.environ[f"ANALYTICS_{role.upper()}_PASSWORD"]
    if not password or password.startswith("CHANGE_ME"):
        raise ValueError("Replace the analytics password placeholders in the root .env")
    return f"postgresql://opsyntra_{role}:{quote(password, safe='')}@postgres:5432/opsyntra"


def database_init():
    import psycopg
    from psycopg import sql
    from psycopg.rows import dict_row
    from opsyntra.analytics.migrations import migrate
    passwords = [os.environ[key] for key in ("POSTGRES_PASSWORD", "ANALYTICS_OWNER_PASSWORD", "ANALYTICS_WRITE_PASSWORD", "ANALYTICS_READ_PASSWORD")]
    if len(set(passwords)) != 4 or any(not p or p.startswith("CHANGE_ME") for p in passwords):
        raise ValueError("Set four distinct non-placeholder database passwords in the root .env")
    with psycopg.connect(host="postgres", dbname="postgres", user="postgres", password=passwords[0], autocommit=True) as c:
        for group in ("opsyntra_analytics_reader", "opsyntra_analytics_writer"):
            if not c.execute("SELECT 1 FROM pg_roles WHERE rolname=%s", (group,)).fetchone():
                c.execute(sql.SQL("CREATE ROLE {} NOLOGIN").format(sql.Identifier(group)))
        for role in ("owner", "write", "read"):
            name = "opsyntra_" + role
            if not c.execute("SELECT 1 FROM pg_roles WHERE rolname=%s", (name,)).fetchone():
                c.execute(sql.SQL("CREATE ROLE {} LOGIN").format(sql.Identifier(name)))
            c.execute(sql.SQL("ALTER ROLE {} PASSWORD {}").format(sql.Identifier(name), sql.Literal(os.environ[f"ANALYTICS_{role.upper()}_PASSWORD"])))
        for role, group in (("write", "writer"), ("read", "reader")):
            c.execute(sql.SQL("GRANT {} TO {}").format(sql.Identifier("opsyntra_analytics_" + group), sql.Identifier("opsyntra_" + role)))
        existing = c.execute("SELECT pg_get_userbyid(datdba) FROM pg_database WHERE datname='opsyntra'").fetchone()
        if existing and existing[0] != "opsyntra_owner":
            raise ValueError("Existing opsyntra database has an unexpected owner; refusing to change it")
        if not existing:
            c.execute("CREATE DATABASE opsyntra OWNER opsyntra_owner")
        c.execute("REVOKE ALL ON DATABASE opsyntra FROM PUBLIC")
        c.execute("GRANT CONNECT ON DATABASE opsyntra TO opsyntra_owner, opsyntra_write, opsyntra_read")
    with psycopg.connect(role_url("owner"), autocommit=True, row_factory=dict_row) as c:
        c.execute("REVOKE CREATE ON SCHEMA public FROM PUBLIC")
        result = migrate(c)
    print(json.dumps({"database": "ready", "migrations": result}))


def model_init(path, config, force=False, source=Path("/input/customers.csv")):
    from opsyntra.data_pipeline import run_pipeline
    from opsyntra.model_training import run_training
    from opsyntra.kafka_config import atomic_json
    destination = Path(config["data"]["raw_path"])
    source = Path(source)
    if not source.is_file():
        raise ValueError("Mount the Telco CSV at /input/customers.csv using OPSYNTRA_DATASET_PATH")
    source_sha = digest(source)
    if destination.exists() and digest(destination) != source_sha:
        raise ValueError("Dataset differs from the persistent volume; use a new Compose project for another dataset")
    destination.parent.mkdir(parents=True, exist_ok=True)
    if not destination.exists():
        shutil.copyfile(source, destination)
    model = Path(config["training"]["artifact_dir"]) / "best_pipeline.joblib"
    receipt = model.parent / "container-training.json"
    expected = {"source_sha256": source_sha, "config_sha256": digest(path)}
    if model.exists() and not force:
        saved = json.loads(receipt.read_text()) if receipt.exists() else {}
        if any(saved.get(k) != v for k, v in expected.items()) or saved.get("model_sha256") != digest(model):
            raise ValueError("Model receipt does not match data/config/artifact; inspect before explicit retraining")
        print(json.dumps({"model": "reused", **saved}))
        return
    run_pipeline(path)
    run_training(path)
    atomic_json(receipt, {**expected, "model_sha256": digest(model)})
    print(json.dumps({"model": "trained", **expected, "model_sha256": digest(model)}))


def main():
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    path, config = prepare_config()
    command, *args = sys.argv[1:] or ["api"]
    if command == "db-init":
        database_init()
        return
    if command in ("model-init", "train"):
        model_init(path, config, force=command == "train")
        return
    if command == "topics":
        from pipelines.kafka_topics import ensure_topics
        from opsyntra.kafka_config import load_kafka_config
        print(json.dumps(ensure_topics(load_kafka_config(path))))
        return
    if command in ("analytics", "api", "health-analytics"):
        role = "read" if command == "api" else "write"
        os.environ["OPSYNTRA_ANALYTICS_" + role.upper() + "_URL"] = role_url(role)
    if command.startswith("health-"):
        from confluent_kafka.admin import AdminClient
        metadata = AdminClient({"bootstrap.servers": os.environ["OPSYNTRA_KAFKA_BOOTSTRAP_SERVERS"]}).list_topics(timeout=5)
        if not metadata.brokers:
            raise ValueError("Broker not ready")
        if command == "health-analytics":
            from opsyntra.analytics.config import AnalyticsConfig
            with AnalyticsConfig.from_env(required_roles=("write",)).connect("write") as c:
                c.execute("SELECT 1 FROM analytics.prediction_events LIMIT 1")
        else:
            if not (Path(config["training"]["artifact_dir"]) / "best_pipeline.joblib").exists():
                raise ValueError("Selected model not ready")
        return
    if command == "airflow":
        os.execv("/home/airflow/.local/bin/airflow", ["airflow", "standalone", *args])
    commands = {
        "api": ["-m", "uvicorn", "opsyntra.web_api:app", "--host", "0.0.0.0", "--port", "8000"],
        "inference": ["consumer.py", "--mode", "streaming", "--config", str(path)],
        "analytics": ["analytics_consumer.py", "--mode", "streaming", "--config", str(path)],
        "producer": ["producer.py", "--config", str(path)],
        "spark-train": ["-m", "pipelines.spark_training_pipeline", "--config", str(path)],
        "mlflow": ["-m", "mlflow", "server", "--host", "0.0.0.0", "--port", "5000", "--backend-store-uri", "sqlite:////app/backend/tracking/mlflow.db", "--default-artifact-root", "/app/backend/mlartifacts"],
    }
    if command == "inference" and os.getenv("OPSYNTRA_INFERENCE_GROUP_ID"):
        commands["inference"] += ["--group-id", os.environ["OPSYNTRA_INFERENCE_GROUP_ID"]]
    if command == "python":
        arguments = args
    else:
        arguments = commands[command] + args
    os.execv(sys.executable, [sys.executable, *arguments])


if __name__ == "__main__":
    main()
