"""Module 3 unit tests: the Debezium source, the Postgres init scripts, the CDC topics, the CDC sink
and the data contract stay consistent with each other. No Postgres, Kafka or Snowflake needed."""

import copy
import json
import random
import re

import pytest
from jsonschema import Draft202012Validator, FormatChecker

from conftest import REPO_ROOT
from configure_env import PASSWORD_VARS, configure
from equipment_config import EQUIPMENT_FLEET
from generate_seed import SEED, SEED_SQL_PATH, equipment_rows, render
from register_connectors import check_no_literal_secrets, load_connectors

INIT_DIR = REPO_ROOT / "03_cdc_migration" / "postgres" / "init"
SCHEMA_SQL = (INIT_DIR / "01_schema.sql").read_text(encoding="utf-8")
ROLES_SH = (INIT_DIR / "02_roles.sh").read_text(encoding="utf-8")
CREATE_TOPICS = (REPO_ROOT / "kafka" / "create_topics.sh").read_text(encoding="utf-8")
ENV_EXAMPLE = (REPO_ROOT / ".env.example").read_text(encoding="utf-8")
CONTRACT = json.loads(
    (REPO_ROOT / "documentation" / "data_contracts" / "legacy_fleet_cdc.v1.schema.json").read_text(encoding="utf-8")
)
CONNECTORS = {spec["name"]: spec["config"] for spec in load_connectors()}
SOURCE = CONNECTORS["debezium-source-legacy-fleet"]
SINK = CONNECTORS["snowflake-sink-legacy-cdc"]


def created_topics() -> set[str]:
    return set(re.findall(r"^create_topic (\S+)", CREATE_TOPICS, re.MULTILINE))


def captured_tables() -> list[str]:
    return SOURCE["table.include.list"].split(",")


def routed_topic(table: str) -> str:
    """The topic a captured table ends up in: Debezium's default name, then the RegexRouter SMT."""
    default = f"{SOURCE['topic.prefix']}.{table}"
    java_replacement = SOURCE["transforms.route.replacement"].replace("$1", r"\1")
    routed = re.sub(f"^{SOURCE['transforms.route.regex']}$", java_replacement, default)
    assert routed != default, f"RegexRouter does not match {default}"
    return routed


# --- Debezium source ---


def test_source_uses_pgoutput_with_a_precreated_publication():
    assert SOURCE["connector.class"] == "io.debezium.connector.postgresql.PostgresConnector"
    assert SOURCE["plugin.name"] == "pgoutput"
    assert SOURCE["publication.autocreate.mode"] == "disabled"
    assert f"CREATE PUBLICATION {SOURCE['publication.name']} FOR TABLE" in SCHEMA_SQL


def test_publication_covers_exactly_the_captured_tables():
    tables = re.search(r"CREATE PUBLICATION \w+ FOR TABLE ([^;]+);", SCHEMA_SQL).group(1)
    assert sorted(t.strip() for t in tables.split(",")) == sorted(captured_tables())


@pytest.mark.parametrize("table", captured_tables())
def test_captured_table_is_created_with_full_replica_identity(table):
    assert f"CREATE TABLE IF NOT EXISTS {table} (" in SCHEMA_SQL
    assert re.search(rf"ALTER TABLE {re.escape(table)}\s+REPLICA IDENTITY FULL;", SCHEMA_SQL)


@pytest.mark.parametrize("table", captured_tables())
def test_cdc_user_can_snapshot_captured_table(table):
    grant = re.search(r"GRANT SELECT ON ([\w., ]+) TO %I', :'cdc_user'", ROLES_SH).group(1)
    assert table in [t.strip() for t in grant.split(",")]


def test_cdc_user_has_replication_but_app_user_does_not():
    assert "ALTER ROLE %I WITH LOGIN REPLICATION PASSWORD %L', :'cdc_user'" in ROLES_SH
    assert "ALTER ROLE %I WITH LOGIN NOREPLICATION PASSWORD %L', :'app_user'" in ROLES_SH


@pytest.mark.parametrize("table", captured_tables())
def test_routed_cdc_topic_is_created_with_v1_suffix(table):
    topic = routed_topic(table)
    assert re.fullmatch(r"telematics\.legacy\.\w+\.cdc\.v1", topic)
    assert topic in created_topics()


def test_heartbeat_topic_is_created():
    # Broker auto-creation is off, so Debezium's heartbeat topic must be created up front.
    assert SOURCE["heartbeat.interval.ms"]
    assert f"__debezium-heartbeat.{SOURCE['topic.prefix']}" in created_topics()


def test_source_keeps_the_full_envelope_as_schemaless_json():
    assert "ExtractNewRecordState" not in json.dumps(SOURCE)
    assert SOURCE["value.converter"] == "org.apache.kafka.connect.json.JsonConverter"
    assert SOURCE["value.converter.schemas.enable"] == "false"
    assert SOURCE["key.converter.schemas.enable"] == "false"
    # Tombstones (null values) would land in Bronze as empty rows; the 'd' event carries the delete.
    assert SOURCE["tombstones.on.delete"] == "false"
    assert SOURCE["decimal.handling.mode"] == "string"


def test_source_password_is_an_env_placeholder():
    assert SOURCE["database.password"] == "${env:POSTGRES_CDC_PASSWORD}"
    check_no_literal_secrets("debezium-source-legacy-fleet", SOURCE)


def test_roles_script_variables_are_documented_in_env_example():
    for var in set(re.findall(r"\$\{?(POSTGRES_[A-Z_]+)", ROLES_SH)):
        assert re.search(rf"^{var}=", ENV_EXAMPLE, re.MULTILINE), f"{var} missing from .env.example"


# --- CDC sink ---


def test_sink_reads_every_routed_cdc_topic():
    assert sorted(SINK["topics"].split(",")) == sorted(routed_topic(t) for t in captured_tables())


def test_sink_keys_are_the_debezium_json_keys():
    # Debezium writes JSON keys; the sink stores them as a string in RECORD_METADATA:key.
    assert SINK["key.converter"] == "org.apache.kafka.connect.storage.StringConverter"


# --- Seed data ---


def test_committed_seed_sql_is_up_to_date():
    committed = SEED_SQL_PATH.read_text(encoding="utf-8").replace("\r\n", "\n")
    assert committed == render(equipment_rows(random.Random(SEED))), (
        "03_seed.sql is stale: run python 03_cdc_migration/postgres/generate_seed.py"
    )


def test_seed_matches_the_telemetry_fleet():
    rows = equipment_rows(random.Random(SEED))
    keys = ("equipment_id", "equipment_serial_number", "type_name", "category")
    assert [{k: r[k] for k in keys} for r in rows] == [{k: m[k] for k in keys} for m in EQUIPMENT_FLEET]
    assert all(r["status"] in {"ACTIVE", "MAINTENANCE", "STANDBY"} for r in rows)
    assert sum(r["status"] == "ACTIVE" for r in rows) > len(rows) / 2


# --- Data contract ---


def envelope(table: str, op: str, before: dict | None, after: dict | None) -> dict:
    return {
        "before": before, "after": after, "op": op, "ts_ms": 1790724848167, "transaction": None,
        "source": {
            "version": "3.7.0.Final", "connector": "postgresql", "name": "telematics.legacy",
            "ts_ms": 1790724848099, "snapshot": "true" if op == "r" else "false", "db": "telematics_legacy",
            "schema": "fleet", "table": table, "txId": 773, "lsn": 28221432,
            "sequence": "[null,\"28221432\"]", "origin": None,
        },
    }


EQUIPMENT_ROW = {
    "equipment_id": 1, "equipment_serial_number": "BOLT-001", "type_name": "Roof_Bolter", "category": "BEV",
    "manufacturer": "MacLean", "model": "975 Roof Bolter EV", "commissioned_on": 18506,
    "assigned_site": "SURFACE_SHOP", "status": "ACTIVE", "service_interval_hours": 250,
    "created_at": "2026-09-29T23:32:52.110089Z", "updated_at": "2026-09-29T23:32:52.110089Z",
}
LOG_ROW = {
    "log_id": 1, "equipment_id": 1, "reading_at": "2026-07-01T18:00:00.000000Z", "hour_meter": "2251.2",
    "source": "SHIFT_REPORT", "recorded_by": "shift_supervisor",
    "created_at": "2026-09-29T23:32:52.113705Z", "updated_at": "2026-09-29T23:32:52.113705Z",
}


@pytest.fixture(scope="module")
def cdc_validator():
    Draft202012Validator.check_schema(CONTRACT)
    return Draft202012Validator(CONTRACT, format_checker=FormatChecker())


@pytest.mark.parametrize("event", [
    envelope("equipment", "r", None, EQUIPMENT_ROW),
    envelope("equipment", "u", EQUIPMENT_ROW, {**EQUIPMENT_ROW, "status": "MAINTENANCE"}),
    envelope("hour_meter_logs", "c", None, LOG_ROW),
    envelope("hour_meter_logs", "u", LOG_ROW, {**LOG_ROW, "hour_meter": "2251.3"}),
    envelope("hour_meter_logs", "d", LOG_ROW, None),
], ids=["snapshot", "equipment-update", "log-insert", "log-correction", "log-delete"])
def test_contract_accepts_valid_events(cdc_validator, event):
    assert not list(cdc_validator.iter_errors(event))


def test_contract_allows_new_debezium_envelope_fields(cdc_validator):
    event = envelope("hour_meter_logs", "c", None, LOG_ROW)
    event["ts_us"] = 1790724848167898
    event["source"]["ts_ns"] = 1790724848099538000
    assert not list(cdc_validator.iter_errors(event))


def broken(event: dict, change) -> dict:
    event = copy.deepcopy(event)
    change(event)
    return event


@pytest.mark.parametrize("event", [
    broken(envelope("equipment", "c", None, EQUIPMENT_ROW), lambda e: e["after"].update(fleet_owner="x")),
    broken(envelope("equipment", "c", None, EQUIPMENT_ROW), lambda e: e["after"].pop("status")),
    broken(envelope("hour_meter_logs", "c", None, LOG_ROW), lambda e: e["after"].update(hour_meter=2251.2)),
    envelope("hour_meter_logs", "d", LOG_ROW, LOG_ROW),
    envelope("hour_meter_logs", "u", None, LOG_ROW),
    envelope("equipment", "c", None, LOG_ROW),
    broken(envelope("equipment", "c", None, EQUIPMENT_ROW), lambda e: e.update(op="x")),
    broken(envelope("equipment", "c", None, EQUIPMENT_ROW), lambda e: e["source"].update(db="other")),
], ids=[
    "column-added-upstream", "column-dropped-upstream", "decimal-as-float", "delete-with-after",
    "update-without-before", "row-from-wrong-table", "unknown-op", "wrong-database",
])
def test_contract_rejects_breaking_events(cdc_validator, event):
    assert list(cdc_validator.iter_errors(event))


# --- configure_env ---


@pytest.fixture
def env_files(tmp_path):
    example = tmp_path / ".env.example"
    example.write_text(
        "KAFKA_BOOTSTRAP_SERVERS=localhost:9092\nPOSTGRES_DB=telematics_legacy\n"
        "POSTGRES_APP_USER=legacy_app\nPOSTGRES_PASSWORD=\nPOSTGRES_CDC_PASSWORD=\nPOSTGRES_APP_PASSWORD=\n",
        encoding="utf-8",
    )
    env = tmp_path / ".env"
    env.write_text("KAFKA_BOOTSTRAP_SERVERS=broker:9092\nPOSTGRES_APP_PASSWORD=keep-me\n", encoding="utf-8")
    return env, example


def test_configure_env_copies_defaults_and_generates_missing_passwords(env_files):
    env, example = env_files
    changes = configure(env, example)
    text = env.read_text(encoding="utf-8")
    assert changes == {
        "POSTGRES_DB": "copied", "POSTGRES_APP_USER": "copied",
        "POSTGRES_PASSWORD": "generated", "POSTGRES_CDC_PASSWORD": "generated",
    }
    assert "KAFKA_BOOTSTRAP_SERVERS=broker:9092" in text  # other settings untouched
    assert "POSTGRES_APP_PASSWORD=keep-me" in text  # existing password kept
    for name in PASSWORD_VARS:
        value = re.search(rf"^{name}=(.*)$", text, re.MULTILINE).group(1)
        assert re.fullmatch(r"[A-Za-z0-9_-]{24,}", value) or value == "keep-me"


def test_configure_env_is_idempotent(env_files):
    env, example = env_files
    configure(env, example)
    first = env.read_text(encoding="utf-8")
    assert configure(env, example) == {}
    assert env.read_text(encoding="utf-8") == first
