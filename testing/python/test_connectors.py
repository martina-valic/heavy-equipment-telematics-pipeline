"""Module 2 unit tests: connector configs stay consistent with the topics, the Snowflake setup and
.env.example, and connector registration is idempotent. No Kafka or Snowflake needed."""

import importlib.util
import json
import re
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import pytest

from conftest import REPO_ROOT
from register_connectors import (
    ConnectClient,
    ConnectError,
    check_no_literal_secrets,
    env_placeholders,
    load_connectors,
)

CREATE_TOPICS = (REPO_ROOT / "kafka" / "create_topics.sh").read_text(encoding="utf-8")
SETUP_SQL = (REPO_ROOT / "02_streaming_ingestion" / "snowflake" / "setup.sql").read_text(encoding="utf-8")
ENV_EXAMPLE = (REPO_ROOT / ".env.example").read_text(encoding="utf-8")
CONNECTORS = load_connectors()
SNOWFLAKE_CLASS = "com.snowflake.kafka.connector.SnowflakeStreamingSinkConnector"


def created_topics() -> set[str]:
    return set(re.findall(r"^create_topic (\S+)", CREATE_TOPICS, re.MULTILINE))


def test_one_connector_per_contract_topic():
    topics = {spec["config"]["topics"] for spec in CONNECTORS}
    assert topics == {"telematics.equipment.telemetry.v1", "telematics.equipment.telemetry.dlq.v1"}


@pytest.mark.parametrize("spec", CONNECTORS, ids=lambda s: s["name"])
class TestConnectorConfig:
    def test_uses_v4_streaming_connector_without_schematization(self, spec):
        config = spec["config"]
        assert config["connector.class"] == SNOWFLAKE_CLASS
        assert config["snowflake.enable.schematization"] == "false"
        assert config["snowflake.autocreate.table.type"] == "none"
        assert config["snowflake.authenticator"] == "snowflake_jwt"

    def test_source_and_error_topics_are_created(self, spec):
        config = spec["config"]
        assert config["topics"] in created_topics()
        assert config["errors.deadletterqueue.topic.name"] in created_topics()

    def test_target_table_is_created_and_granted_in_setup_sql(self, spec):
        topic, table = spec["config"]["snowflake.topic2table.map"].split(":")
        assert topic == spec["config"]["topics"]
        assert f"CREATE TABLE IF NOT EXISTS TELEMATICS.BRONZE.{table} (" in SETUP_SQL
        assert re.search(rf"GRANT INSERT\s+ON TABLE\s+TELEMATICS\.BRONZE\.{table}\s", SETUP_SQL)

    def test_secrets_are_env_placeholders(self, spec):
        config = spec["config"]
        assert config["snowflake.private.key"] == "${env:SNOWFLAKE_PRIVATE_KEY}"
        check_no_literal_secrets(spec["name"], config)

    def test_every_env_placeholder_is_documented_in_env_example(self, spec):
        for var in env_placeholders(spec["config"]):
            assert re.search(rf"^{var}=", ENV_EXAMPLE, re.MULTILINE), f"{var} missing from .env.example"


def test_literal_secret_is_rejected():
    with pytest.raises(ValueError, match="snowflake.private.key"):
        check_no_literal_secrets("x", {"snowflake.private.key": "MIIEvQIBADANBgkq"})


def test_env_placeholders_inside_larger_values():
    config = {"url": "${env:SNOWFLAKE_ACCOUNT}.snowflakecomputing.com:443", "user": "${env:SNOWFLAKE_USER}"}
    assert env_placeholders(config) == {"SNOWFLAKE_ACCOUNT", "SNOWFLAKE_USER"}


# --- Registration against a fake Kafka Connect REST API ---


class FakeConnect:
    """Enough of the Connect REST API for register_connectors: list, PUT config, status."""

    def __init__(self, conflicts=0, task_state="RUNNING"):
        self.connectors: dict[str, dict] = {}
        self.puts = 0
        self.conflicts = conflicts
        self.task_state = task_state

    def handle(self, method, path, body):
        if method == "GET" and path == "/connectors":
            return 200, list(self.connectors)
        if m := re.fullmatch(r"/connectors/([^/]+)/config", path):
            if self.conflicts:
                self.conflicts -= 1
                return 409, {"error_code": 409, "message": "rebalance in progress"}
            self.puts += 1
            created = m.group(1) not in self.connectors
            self.connectors[m.group(1)] = body
            return (201 if created else 200), {"name": m.group(1), "config": body}
        if m := re.fullmatch(r"/connectors/([^/]+)/status", path):
            task = {"id": 0, "state": self.task_state, "trace": "java.lang.Exception: boom"}
            return 200, {"name": m.group(1), "connector": {"state": "RUNNING"}, "tasks": [task]}
        return 404, {"error_code": 404, "message": "not found"}


@pytest.fixture
def fake_connect():
    fake = FakeConnect()

    class Handler(BaseHTTPRequestHandler):
        def _serve(self):
            length = int(self.headers.get("Content-Length") or 0)
            body = json.loads(self.rfile.read(length)) if length else None
            status, payload = fake.handle(self.command, self.path, body)
            data = json.dumps(payload).encode()
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)

        do_GET = do_PUT = _serve

        def log_message(self, *args):
            pass

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    fake.url = f"http://127.0.0.1:{server.server_port}"
    yield fake
    server.shutdown()


def test_upsert_is_idempotent(fake_connect):
    client = ConnectClient(fake_connect.url, retries=3, backoff=0)
    config = {"connector.class": SNOWFLAKE_CLASS}
    assert client.upsert("sink", config) is False
    assert client.upsert("sink", config) is True
    assert list(fake_connect.connectors) == ["sink"]


def test_upsert_retries_while_worker_rebalances(fake_connect):
    fake_connect.conflicts = 2
    ConnectClient(fake_connect.url, retries=3, backoff=0).upsert("sink", {})
    assert fake_connect.puts == 1


def test_upsert_gives_up_after_retries(fake_connect):
    fake_connect.conflicts = 5
    with pytest.raises(ConnectError, match="HTTP 409"):
        ConnectClient(fake_connect.url, retries=2, backoff=0).upsert("sink", {})


def test_failed_task_surfaces_its_trace(fake_connect):
    fake_connect.task_state = "FAILED"
    with pytest.raises(ConnectError, match="boom"):
        ConnectClient(fake_connect.url, retries=1, backoff=0).wait_until_running("sink", timeout=5)


# --- Key pair helper ---


@pytest.fixture(scope="module")
def keypair_module():
    path = REPO_ROOT / "02_streaming_ingestion" / "snowflake" / "generate_keypair.py"
    spec = importlib.util.spec_from_file_location("generate_keypair", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_pem_body_strips_header_footer_and_newlines(keypair_module):
    pem = "-----BEGIN PRIVATE KEY-----\nAAAA\nBBBB\n-----END PRIVATE KEY-----\n"
    assert keypair_module.pem_body(pem) == "AAAABBBB"


def test_set_env_var_replaces_or_appends(keypair_module, tmp_path):
    env = tmp_path / ".env"
    env.write_text("A=1\nSNOWFLAKE_PRIVATE_KEY=old\nB=2\n", encoding="utf-8")
    keypair_module.set_env_var(env, "SNOWFLAKE_PRIVATE_KEY", "new")
    keypair_module.set_env_var(env, "C", "3")
    assert env.read_text(encoding="utf-8") == "A=1\nSNOWFLAKE_PRIVATE_KEY=new\nB=2\nC=3\n"
