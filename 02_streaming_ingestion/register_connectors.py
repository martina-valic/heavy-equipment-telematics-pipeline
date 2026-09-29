"""Registers every connector in connectors/ with the Kafka Connect REST API.

Idempotent: each connector is created or updated with PUT /connectors/<name>/config, so re-running
never fails on "already exists". Transient errors (409 while the worker rebalances, 5xx while it
starts) are retried. After registering, it waits for each connector and its tasks to reach
RUNNING and exits non-zero with the task's stack trace if one fails.

Connector files hold no secrets. Sensitive values are ${env:VAR} placeholders that the Connect
worker resolves from its own environment (EnvVarConfigProvider), and this script refuses to
register a config that has a literal value for a sensitive key.

Usage (from the repo root, stdlib only):
    python 02_streaming_ingestion/register_connectors.py [--connect-url http://localhost:8083]
"""

import argparse
import json
import os
import re
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path

CONNECTORS_DIR = Path(__file__).resolve().parent / "connectors"
SENSITIVE_KEY = re.compile(r"(private\.key|passphrase|password|secret|token)", re.IGNORECASE)
PLACEHOLDER = re.compile(r"\$\{(\w+):(?:[^}:]*:)?([^}]+)\}")
RETRYABLE_STATUS = {404, 409, 500, 502, 503, 504}


class ConnectError(RuntimeError):
    pass


def load_connectors(directory: Path = CONNECTORS_DIR) -> list[dict]:
    connectors = []
    for path in sorted(directory.glob("*.json")):
        spec = json.loads(path.read_text(encoding="utf-8"))
        if not spec.get("name") or not isinstance(spec.get("config"), dict):
            raise ValueError(f"{path.name}: expected an object with 'name' and 'config'")
        check_no_literal_secrets(spec["name"], spec["config"])
        connectors.append(spec)
    if not connectors:
        raise ValueError(f"No connector files found in {directory}")
    return connectors


def check_no_literal_secrets(name: str, config: dict) -> None:
    for key, value in config.items():
        if SENSITIVE_KEY.search(key) and not PLACEHOLDER.fullmatch(str(value)):
            raise ValueError(f"{name}: '{key}' must be a ${{env:VAR}} placeholder, not a literal value")


def env_placeholders(config: dict) -> set[str]:
    """Names of the environment variables a config references through ${env:VAR}."""
    return {m.group(2) for value in config.values() for m in PLACEHOLDER.finditer(str(value)) if m.group(1) == "env"}


class ConnectClient:
    def __init__(self, base_url: str, retries: int = 30, backoff: float = 3.0):
        self.base_url = base_url.rstrip("/")
        self.retries = retries
        self.backoff = backoff

    def request(self, method: str, path: str, body: dict | None = None, retry: bool = True):
        data = json.dumps(body).encode("utf-8") if body is not None else None
        attempts = self.retries if retry else 1
        for attempt in range(1, attempts + 1):
            req = urllib.request.Request(
                self.base_url + path, data=data, method=method,
                headers={"Content-Type": "application/json", "Accept": "application/json"},
            )
            try:
                with urllib.request.urlopen(req, timeout=30) as resp:
                    payload = resp.read()
                    return json.loads(payload) if payload else None
            except urllib.error.HTTPError as exc:
                detail = exc.read().decode("utf-8", "replace")
                if exc.code not in RETRYABLE_STATUS or attempt == attempts:
                    raise ConnectError(f"{method} {path} -> HTTP {exc.code}: {detail}") from exc
            except (urllib.error.URLError, ConnectionError, TimeoutError) as exc:
                if attempt == attempts:
                    raise ConnectError(f"{method} {path} failed: {exc}") from exc
            time.sleep(self.backoff)

    def wait_until_ready(self) -> None:
        info = self.request("GET", "/")
        print(f"Kafka Connect {info.get('version')} is up at {self.base_url}")

    def installed_plugins(self) -> set[str]:
        return {p["class"] for p in self.request("GET", "/connector-plugins")}

    def upsert(self, name: str, config: dict) -> bool:
        """Create or update a connector. Returns True if it already existed."""
        existed = name in self.request("GET", "/connectors")
        self.request("PUT", f"/connectors/{name}/config", config)
        return existed

    def wait_until_running(self, name: str, timeout: float) -> None:
        deadline = time.monotonic() + timeout
        while True:
            status = self.request("GET", f"/connectors/{name}/status")
            states = [status["connector"]["state"]] + [t["state"] for t in status.get("tasks", [])]
            failed = [t for t in status.get("tasks", []) if t["state"] == "FAILED"]
            if status["connector"]["state"] == "FAILED":
                failed.append(status["connector"])
            if failed:
                trace = failed[0].get("trace", "(no trace)")
                raise ConnectError(f"{name} FAILED:\n{trace}")
            if status.get("tasks") and all(s == "RUNNING" for s in states):
                return
            if time.monotonic() > deadline:
                raise ConnectError(f"{name} did not reach RUNNING within {timeout:.0f}s (states: {states})")
            time.sleep(2)


def load_local_env() -> None:
    try:
        from dotenv import load_dotenv
    except ImportError:
        return
    load_dotenv(Path(__file__).resolve().parents[1] / ".env")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--connect-url", default=None, help="default: $KAFKA_CONNECT_URL or http://localhost:8083")
    parser.add_argument("--timeout", type=float, default=120, help="seconds to wait for each connector to run")
    args = parser.parse_args()

    load_local_env()
    client = ConnectClient(args.connect_url or os.getenv("KAFKA_CONNECT_URL", "http://localhost:8083"))

    try:
        connectors = load_connectors()
        client.wait_until_ready()

        installed = client.installed_plugins()
        for spec in connectors:
            cls = spec["config"]["connector.class"]
            if cls not in installed:
                raise ConnectError(f"{spec['name']}: plugin {cls} is not installed on the Connect worker")
            # The worker resolves these, not this script. A missing local value usually means the
            # worker is missing it too (Compose and Kubernetes both load it from the same .env).
            missing = sorted(v for v in env_placeholders(spec["config"]) if not os.getenv(v))
            if missing:
                print(f"WARNING {spec['name']}: not set in local environment/.env: {', '.join(missing)}")

        for spec in connectors:
            existed = client.upsert(spec["name"], spec["config"])
            print(f"{'Updated' if existed else 'Created'} connector {spec['name']}")

        for spec in connectors:
            client.wait_until_running(spec["name"], args.timeout)
            print(f"{spec['name']}: RUNNING")
    except (ConnectError, ValueError) as exc:
        print(f"ERROR {exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
