"""End-to-end ingestion SLA: events produced to Kafka must be queryable in Snowflake Bronze within
INGESTION_SLA_SECONDS (default 120). Needs Kafka, Kafka Connect with both connectors RUNNING, and
the Snowflake objects from 02_streaming_ingestion/snowflake/setup.sql. Skipped if any are missing."""

import json
import os
import random
import time

import pytest
from dotenv import load_dotenv

from anomalies import AnomalyInjector
from conftest import REPO_ROOT
from run_simulator import run
from sinks import KafkaSink

load_dotenv(REPO_ROOT / ".env")

pytestmark = [pytest.mark.integration, pytest.mark.snowflake]

SLA_SECONDS = float(os.getenv("INGESTION_SLA_SECONDS", "120"))
BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC = os.getenv("TELEMETRY_TOPIC", "telematics.equipment.telemetry.v1")
DLQ_TOPIC = os.getenv("TELEMETRY_DLQ_TOPIC", "telematics.equipment.telemetry.dlq.v1")


class RecordingSink:
    """Passes events through to Kafka and remembers what went where."""

    def __init__(self, inner):
        self.inner, self.sent, self.dlq = inner, [], []

    def send(self, key, payload):
        self.sent.append(payload)
        self.inner.send(key, payload)

    def send_dlq(self, key, record):
        self.dlq.append(record)
        self.inner.send_dlq(key, record)

    def flush(self):
        self.inner.flush()


@pytest.fixture(scope="module")
def snowflake_cursor(snowflake_connection):
    return snowflake_connection.cursor()


@pytest.fixture(scope="module")
def kafka_sink():
    try:
        return KafkaSink(BOOTSTRAP, TOPIC, DLQ_TOPIC)
    except RuntimeError as exc:
        pytest.skip(str(exc))


def wait_for_rows(cursor, table: str, id_expression: str, ids: set[str], since_ms: int) -> tuple[int, float]:
    """Poll until every id has landed or the SLA runs out. Returns (rows found, seconds waited)."""
    start = time.monotonic()
    placeholders = ", ".join(["%s"] * len(ids))
    query = (
        f"SELECT COUNT(DISTINCT {id_expression}) FROM {table} "
        f"WHERE RECORD_METADATA:CreateTime::NUMBER >= %s AND {id_expression} IN ({placeholders})"
    )
    found = 0
    while time.monotonic() - start < SLA_SECONDS:
        found = cursor.execute(query, (since_ms, *ids)).fetchone()[0]
        if found == len(ids):
            break
        time.sleep(5)
    return found, time.monotonic() - start


def test_telemetry_lands_in_bronze_within_sla(kafka_sink, simulator, validator, snowflake_cursor):
    sink = RecordingSink(kafka_sink)
    since_ms = int(time.time() * 1000) - 5_000
    run(simulator, AnomalyInjector(rate=0.0, rng=random.Random(0)), validator, sink, ticks=1, interval=0)
    ids = {event["event_id"] for event in sink.sent}
    assert len(ids) == 40

    found, waited = wait_for_rows(
        snowflake_cursor, "EQUIPMENT_TELEMETRY_RAW", "RECORD_CONTENT:event_id::STRING", ids, since_ms
    )
    assert found == len(ids), f"only {found}/{len(ids)} events landed within the {SLA_SECONDS:.0f}s SLA"
    print(f"\n40 events landed in Bronze after {waited:.1f}s (SLA {SLA_SECONDS:.0f}s)")

    # Connector metadata needed downstream: lineage back to Kafka and the message key.
    row = snowflake_cursor.execute(
        "SELECT RECORD_METADATA, RECORD_CONTENT FROM EQUIPMENT_TELEMETRY_RAW "
        "WHERE RECORD_CONTENT:event_id::STRING = %s LIMIT 1",
        (sink.sent[0]["event_id"],),
    ).fetchone()
    metadata, content = json.loads(row[0]), json.loads(row[1])
    assert metadata["topic"] == TOPIC
    assert {"partition", "offset", "CreateTime"} <= metadata.keys()
    assert metadata["key"] == content["equipment_serial_number"]


def test_contract_violations_land_in_bronze_dlq_within_sla(kafka_sink, simulator, validator, snowflake_cursor):
    sink = RecordingSink(kafka_sink)
    injector = AnomalyInjector(rate=1.0, rng=random.Random(0))
    injector.apply = lambda event: (injector._contract_violation(dict(event)), "CONTRACT_VIOLATION")
    since_ms = int(time.time() * 1000) - 5_000
    run(simulator, injector, validator, sink, ticks=1, interval=0)
    # Violations may drop event_id itself, so match on the DLQ envelope's failed_at timestamp.
    ids = {record["failed_at"] for record in sink.dlq}
    assert sink.dlq and not sink.sent

    found, waited = wait_for_rows(
        snowflake_cursor, "EQUIPMENT_TELEMETRY_DLQ_RAW", "RECORD_CONTENT:failed_at::STRING", ids, since_ms
    )
    assert found == len(ids), f"only {found}/{len(ids)} DLQ records landed within the {SLA_SECONDS:.0f}s SLA"
    print(f"\n{len(ids)} DLQ records landed in Bronze after {waited:.1f}s (SLA {SLA_SECONDS:.0f}s)")
