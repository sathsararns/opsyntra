"""Project-specific, persistent local KRaft broker in Ubuntu WSL (Java 17+)."""
import argparse
import hashlib
import json
import os
import re
import signal
import socket
import subprocess
import tarfile
import time
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
VERSION = "4.3.1"
BASE = Path.home() / ".local/share/opsyntra"
INSTALL = BASE / f"kafka_2.13-{VERSION}"
STATE = BASE / ("kafka-home-" + hashlib.sha256(str(ROOT).encode()).hexdigest()[:10])
LOCAL = ROOT / ".kafka-runtime"


def setup():
    if (INSTALL / "bin/kafka-server-start.sh").exists():
        print(f"Kafka {VERSION} is already installed: {INSTALL}", flush=True)
        return
    BASE.mkdir(parents=True, exist_ok=True)
    filename = f"kafka_2.13-{VERSION}.tgz"
    url = f"https://downloads.apache.org/kafka/{VERSION}/{filename}"
    archive = BASE / filename
    print(f"Downloading Apache Kafka {VERSION} from Apache; verifying SHA512.", flush=True)
    with urllib.request.urlopen(url + ".sha512", timeout=30) as response:
        checksum_text = response.read().decode().split(":", 1)[-1]
    expected = "".join(re.findall(r"\b[0-9A-Fa-f]{8}\b", checksum_text)).lower()
    if len(expected) != 128:
        raise ValueError("Unexpected Apache SHA512 checksum format")
    partial = archive.with_suffix(".part")
    digest = hashlib.sha512()
    with urllib.request.urlopen(url, timeout=60) as response, partial.open("wb") as output:
        while block := response.read(1024 * 1024):
            output.write(block)
            digest.update(block)
    if digest.hexdigest() != expected:
        raise ValueError("Kafka download failed SHA512 verification")
    partial.replace(archive)
    with tarfile.open(archive) as bundle:
        bundle.extractall(BASE, filter="data")
    print(f"Installed verified Apache Kafka {VERSION}: {INSTALL}", flush=True)


def port_open():
    try:
        with socket.create_connection(("127.0.0.1", 9092), timeout=1):
            return True
    except OSError:
        return False


def environment():
    return {**os.environ, "KAFKA_OPTS": "-Djava.net.preferIPv4Stack=true", "KAFKA_HEAP_OPTS": "-Xms256m -Xmx512m", "LOG_DIR": str(STATE / "logs")}


def command(script, *args):
    return subprocess.run([str(INSTALL / "bin" / script), *args], env=environment(),
                          cwd=STATE, text=True, capture_output=True, check=True)


def start():
    if port_open():
        if not (STATE / "service.pid").exists():
            raise RuntimeError("Port 9092 is occupied by another service. This launcher will not replace it.")
        print("Kafka is already listening on 127.0.0.1:9092", flush=True)
        return
    if not INSTALL.exists():
        raise RuntimeError("Run start-kafka.ps1 -Task setup first")
    STATE.mkdir(parents=True, exist_ok=True)
    config = STATE / "server.properties"
    config.write_text("\n".join([
        "process.roles=broker,controller", "node.id=1",
        "controller.quorum.bootstrap.servers=127.0.0.1:9093",
        "listeners=PLAINTEXT://127.0.0.1:9092,CONTROLLER://127.0.0.1:9093",
        "advertised.listeners=PLAINTEXT://127.0.0.1:9092",
        "controller.listener.names=CONTROLLER", "inter.broker.listener.name=PLAINTEXT",
        "listener.security.protocol.map=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT",
        f"log.dirs={STATE / 'data'}", "num.partitions=3", "auto.create.topics.enable=false",
        "offsets.topic.replication.factor=1", "transaction.state.log.replication.factor=1",
        "transaction.state.log.min.isr=1", "group.initial.rebalance.delay.ms=0",
        "log.retention.hours=168", "log.segment.bytes=104857600", "num.recovery.threads.per.data.dir=1", "",
    ]))
    if not (STATE / "data/meta.properties").exists():
        cluster_id = command("kafka-storage.sh", "random-uuid").stdout.strip()
        formatted = command("kafka-storage.sh", "format", "--standalone", "-t", cluster_id, "-c", str(config))
        print(formatted.stdout, flush=True)
    with (STATE / "broker.log").open("a") as log:
        process = subprocess.Popen([str(INSTALL / "bin/kafka-server-start.sh"), str(config)],
                                   env=environment(), cwd=STATE, stdin=subprocess.DEVNULL,
                                   stdout=log, stderr=subprocess.STDOUT, start_new_session=True)
    (STATE / "service.pid").write_text(str(process.pid))
    LOCAL.mkdir(exist_ok=True)
    (LOCAL / "service.json").write_text(json.dumps({"version": VERSION, "pid": process.pid,
        "home": str(STATE), "bootstrap_servers": "127.0.0.1:9092", "log": str(STATE / "broker.log")}, indent=2))
    for _ in range(90):
        if process.poll() is not None:
            raise RuntimeError(f"Broker exited; inspect {STATE / 'broker.log'}")
        if port_open():
            print("Kafka listening on 127.0.0.1:9092; topic setup verifies broker readiness.", flush=True)
            return
        time.sleep(1)
    raise TimeoutError(f"Broker startup timed out; inspect {STATE / 'broker.log'}")


def stop():
    pidfile = STATE / "service.pid"
    if pidfile.exists():
        pid = int(pidfile.read_text())
        cmd = Path(f"/proc/{pid}/cmdline")
        if cmd.exists() and str(STATE / "server.properties").encode() in cmd.read_bytes():
            os.killpg(pid, signal.SIGTERM)
            for _ in range(100):
                if not port_open():
                    break
                time.sleep(.1)
            else:
                raise TimeoutError("Broker has not finished stopping; inspect its log before restarting")
            print("Sent graceful stop to this project's Kafka broker; stored data is retained.")
        pidfile.unlink(missing_ok=True)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("task", choices=["setup", "start", "stop", "status"])
    args = parser.parse_args()
    if args.task == "status":
        print("listening on 127.0.0.1:9092" if port_open() else "not running")
    else:
        globals()[args.task]()
