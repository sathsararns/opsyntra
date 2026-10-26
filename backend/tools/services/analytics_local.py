"""WSL-only dedicated PostgreSQL cluster; never modifies other project services.

Local development helper, run as WSL root. Credentials are generated once into
ignored files; no passwords are passed on a process command line or printed.
"""
import argparse
import hashlib
import json
import os
import pwd
import secrets
import subprocess
from pathlib import Path

BACKEND = Path(__file__).resolve().parents[2]
STATE = BACKEND / ".analytics-runtime" / "local.json"


def run(command, *, input=None, check=True):
    return subprocess.run(command, input=input, text=True, capture_output=True, check=check)


def pg(state, name, *arguments, check=True):
    return run(["runuser", "-u", "postgres", "--", str(Path(state["bin"]) / name), *map(str, arguments)], check=check)


def admin(state, database, statement):
    # Suppress query echo/errors: bootstrap SQL contains generated passwords.
    result = run(["runuser", "-u", "postgres", "--", str(Path(state["bin"]) / "psql"),
                  "-X", "-q", "-v", "ON_ERROR_STOP=1", "-h", state["socket"], "-p", str(state["port"]),
                  "-U", "postgres", "-d", database], input=statement, check=False)
    if result.returncode:
        raise RuntimeError("Local PostgreSQL provisioning failed; SQL/credentials suppressed")
    return result.stdout


def start(state):
    if pg(state, "pg_ctl", "-D", state["data"], "status", check=False).returncode == 0:
        return
    pg(state, "pg_ctl", "-D", state["data"], "-l", str(Path(state["data"]).parent / "server.log"), "-w", "start")


def setup(port):
    if STATE.exists():
        state = json.loads(STATE.read_text())
        if not (BACKEND / ".env").exists() or "OPSYNTRA_ANALYTICS_TEST_READ_URL=" not in (BACKEND / ".env").read_text():
            raise ValueError("Existing local cluster has incomplete configuration; inspect ignored local.json/.env. No data was replaced.")
        start(state)
        print(f"Existing dedicated PostgreSQL preserved on port {state['port']}")
        return
    env_path = BACKEND / ".env"
    if env_path.exists() and "OPSYNTRA_ANALYTICS_" in env_path.read_text():
        raise ValueError("Analytics .env configuration already exists; use it with analytics_db migrate or choose another checkout")
    candidates = sorted(Path("/usr/lib/postgresql").glob("*/bin/initdb"), key=lambda p: int(p.parts[-3]))
    if not candidates:
        raise ValueError("Install PostgreSQL in WSL first: apt-get install postgresql postgresql-client")
    identity = hashlib.sha256(str(BACKEND).encode()).hexdigest()[:12]
    runtime = Path("/var/lib/postgresql") / ("opsyntra-" + identity)
    if runtime.exists():
        raise ValueError("Untracked local cluster directory exists; refusing to replace its data")
    account = pwd.getpwnam("postgres")
    runtime.mkdir(mode=0o700)
    os.chown(runtime, account.pw_uid, account.pw_gid)
    socket = runtime / "socket"
    socket.mkdir(mode=0o700)
    os.chown(socket, account.pw_uid, account.pw_gid)
    state = {"bin": str(candidates[-1].parent), "data": str(runtime / "data"), "socket": str(socket), "port": port,
             "passwords": {role: secrets.token_urlsafe(32) for role in ("owner", "ingest", "api")}}
    # Save before initialization so a failed start never causes silent recreation.
    STATE.parent.mkdir(exist_ok=True)
    STATE.write_text(json.dumps(state, indent=2) + "\n")
    pg(state, "initdb", "-D", state["data"], "--encoding=UTF8", "--locale=C.UTF-8", "--auth-local=trust", "--auth-host=scram-sha-256")
    config = Path(state["data"]) / "postgresql.conf"
    with config.open("a") as destination:
        destination.write(f"\nport = {port}\nlisten_addresses = '127.0.0.1'\nunix_socket_directories = '{socket}'\ntimezone = 'UTC'\n")
    start(state)
    statements = ["CREATE ROLE opsyntra_analytics_reader NOLOGIN;", "CREATE ROLE opsyntra_analytics_writer NOLOGIN;"]
    for role, password in state["passwords"].items():
        name = {"owner": "opsyntra_analytics_owner", "ingest": "opsyntra_analytics_ingest", "api": "opsyntra_analytics_api"}[role]
        statements.append(f"CREATE ROLE {name} LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION PASSWORD '{password}';")
    statements.extend(["GRANT opsyntra_analytics_reader TO opsyntra_analytics_api;",
                       "GRANT opsyntra_analytics_writer TO opsyntra_analytics_ingest;"])
    admin(state, "postgres", "\n".join(statements))
    for database in ("opsyntra_analytics", "opsyntra_analytics_test"):
        admin(state, "postgres", f"CREATE DATABASE {database} OWNER opsyntra_analytics_owner;")
        admin(state, database, f"""REVOKE ALL ON DATABASE {database} FROM PUBLIC;
            GRANT CONNECT ON DATABASE {database} TO opsyntra_analytics_owner, opsyntra_analytics_ingest, opsyntra_analytics_api;
            REVOKE CREATE ON SCHEMA public FROM PUBLIC;""")
    lines = ["", "# Generated local analytics credentials; keep this file out of Git."]
    for prefix, database in (("", "opsyntra_analytics"), ("TEST_", "opsyntra_analytics_test")):
        for access, role in (("MIGRATION", "owner"), ("WRITE", "ingest"), ("READ", "api")):
            username = "opsyntra_analytics_" + role
            lines.append(f"OPSYNTRA_ANALYTICS_{prefix}{access}_URL=postgresql://{username}:{state['passwords'][role]}@127.0.0.1:{port}/{database}")
    lines += ["OPSYNTRA_ANALYTICS_TIMEZONE=Asia/Colombo", "OPSYNTRA_ANALYTICS_CONNECT_TIMEOUT=10", ""]
    with env_path.open("a") as destination:
        destination.write("\n".join(lines))
    print(f"Dedicated PostgreSQL ready on 127.0.0.1:{port}; separate production/demo and test databases; credentials in backend/.env")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("task", choices=("setup", "start", "stop", "status"))
    parser.add_argument("--port", type=int, default=5433)
    args = parser.parse_args()
    if os.name == "nt" or os.geteuid() != 0:
        parser.error("Run this local cluster helper as root inside Ubuntu WSL")
    if not 1024 <= args.port <= 65535:
        parser.error("Choose an unprivileged port")
    if args.task == "setup":
        setup(args.port)
        return
    if not STATE.exists():
        parser.error("Run setup first")
    state = json.loads(STATE.read_text())
    if args.task == "start":
        start(state)
        print(f"Dedicated PostgreSQL running on port {state['port']}")
    elif args.task == "stop":
        if pg(state, "pg_ctl", "-D", state["data"], "status", check=False).returncode == 0:
            pg(state, "pg_ctl", "-D", state["data"], "-m", "fast", "-w", "stop")
        print("Dedicated PostgreSQL stopped; data retained")
    else:
        running = pg(state, "pg_ctl", "-D", state["data"], "status", check=False).returncode == 0
        print(json.dumps({"running": running, "port": state["port"], "data": state["data"]}))
        if not running:
            raise SystemExit(1)


if __name__ == "__main__":
    main()
