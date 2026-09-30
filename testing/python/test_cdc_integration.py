"""End-to-end CDC checks: changes committed in the legacy Postgres database reach Kafka through
Debezium as contract-valid change events. Needs Kafka, Postgres and the debezium-source-legacy-fleet
connector RUNNING (see 03_cdc_migration/README.md). Skipped locally if any are missing, failed in CI.
"""

import json
import os
import random
import time
import urllib.request
import uuid
from collections import Counter

import pytest
from dotenv import load_dotenv
from jsonschema import Draft202012Validator, FormatChecker

from conftest import REPO_ROOT, unavailable
from legacy_activity import run_tick

confluent_kafka = pytest.importorskip("confluent_kafka")
load_dotenv(REPO_ROOT / ".env")

pytestmark = pytest.mark.cdc

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
CONNECT_URL = os.getenv("KAFKA_CONNECT_URL", "http://localhost:8083")
CONNECTOR = "debezium-source-legacy-fleet"
EQUIPMENT_TOPIC = "telematics.legacy.equipment.cdc.v1"
LOGS_TOPIC = "telematics.legacy.hour_meter_logs.cdc.v1"
# Debezium streams within a second or two; this only bounds a hung test.
CDC_TIMEOUT_SECONDS = 60
CONTRACT = json.loads(
    (REPO_ROOT / "documentation" / "data_contracts" / "legacy_fleet_cdc.v1.schema.json").read_text(encoding="utf-8")
)


@pytest.fixture(scope="module", autouse=True)
def debezium_running():
    try:
        with urllib.request.urlopen(f"{CONNECT_URL}/connectors/{CONNECTOR}/status", timeout=5) as resp:
            status = json.loads(resp.read())
    except OSError as exc:
        unavailable(f"{CONNECTOR} is not registered at {CONNECT_URL}: {exc}")
    states = [status["connector"]["state"]] + [t["state"] for t in status.get("tasks", [])]
    if set(states) != {"RUNNING"}:
        unavailable(f"{CONNECTOR} is not RUNNING: {states}")


@pytest.fixture(scope="module")
def contract():
    return Draft202012Validator(CONTRACT, format_checker=FormatChecker())


def consumer_for(topic: str, from_beginning: bool = False):
    """A consumer assigned to every partition, at the start or the current end of each."""
    consumer = confluent_kafka.Consumer(
        {"bootstrap.servers": BOOTSTRAP, "group.id": f"cdc-it-{uuid.uuid4()}", "enable.auto.commit": False}
    )
    partitions = consumer.list_topics(topic, timeout=10).topics[topic].partitions
    assignments = []
    for p in partitions:
        low, high = consumer.get_watermark_offsets(confluent_kafka.TopicPartition(topic, p), timeout=10)
        assignments.append(confluent_kafka.TopicPartition(topic, p, low if from_beginning else high))
    consumer.assign(assignments)
    return consumer


def consume_until(consumer, done, timeout: float = CDC_TIMEOUT_SECONDS) -> list:
    """Collect (key, value) pairs until done(values) is true or the timeout runs out."""
    events, deadline = [], time.monotonic() + timeout
    while not done([value for _, value in events]) and time.monotonic() < deadline:
        msg = consumer.poll(1)
        if msg is not None and not msg.error() and msg.value() is not None:
            events.append((json.loads(msg.key()), json.loads(msg.value())))
    return events


def row_of(event: dict) -> dict:
    return event["after"] if event["after"] is not None else event["before"]


def test_initial_snapshot_published_every_machine(legacy_db, contract):
    expected = {row[0] for row in legacy_db.execute("SELECT equipment_id FROM fleet.equipment")}
    consumer = consumer_for(EQUIPMENT_TOPIC, from_beginning=True)
    try:
        def snapshot_complete(values):
            return {v["after"]["equipment_id"] for v in values if v["op"] == "r"} >= expected

        events = consume_until(consumer, snapshot_complete)
    finally:
        consumer.close()

    snapshot = [(k, v) for k, v in events if v["op"] == "r"]
    assert {v["after"]["equipment_id"] for _, v in snapshot} == expected
    for key, value in snapshot:
        assert not list(contract.iter_errors(value))
        assert key == {"equipment_id": value["after"]["equipment_id"]}


def test_insert_correct_and_void_a_reading(legacy_db, contract):
    """One reading's full life cycle arrives as c, u, d in commit order with complete row images."""
    consumer = consumer_for(LOGS_TOPIC)
    try:
        log_id, equipment_id = legacy_db.execute(
            """
            INSERT INTO fleet.hour_meter_logs (equipment_id, reading_at, hour_meter, source, recorded_by)
            SELECT equipment_id, clock_timestamp(), 9999.0, 'OPERATOR_LOG', 'cdc_integration_test'
            FROM fleet.equipment ORDER BY equipment_id LIMIT 1
            RETURNING log_id, equipment_id
            """
        ).fetchone()
        committed_ms = time.time() * 1000
        legacy_db.execute("UPDATE fleet.hour_meter_logs SET hour_meter = 9999.5 WHERE log_id = %s", (log_id,))
        legacy_db.execute("DELETE FROM fleet.hour_meter_logs WHERE log_id = %s", (log_id,))

        def life_cycle_seen(values):
            return sum(row_of(v)["log_id"] == log_id for v in values) >= 3

        events = [(k, v) for k, v in consume_until(consumer, life_cycle_seen) if row_of(v)["log_id"] == log_id]
    finally:
        consumer.close()

    assert [v["op"] for _, v in events] == ["c", "u", "d"]
    created, corrected, voided = (v for _, v in events)
    assert created["before"] is None and created["after"]["hour_meter"] == "9999.0"
    assert corrected["before"]["hour_meter"] == "9999.0" and corrected["after"]["hour_meter"] == "9999.5"
    assert voided["after"] is None and voided["before"]["hour_meter"] == "9999.5"
    assert voided["before"]["recorded_by"] == "cdc_integration_test"  # full before-image, not just the key
    assert created["source"]["lsn"] < corrected["source"]["lsn"] < voided["source"]["lsn"]
    for key, value in events:
        assert key == {"equipment_id": equipment_id}
        assert value["source"]["snapshot"] == "false"
        assert not list(contract.iter_errors(value))
    # Commit to Kafka is near-instant; the 120 s SLA is checked end to end in test_cdc_snowflake.py.
    assert created["ts_ms"] - committed_ms < CDC_TIMEOUT_SECONDS * 1000


def test_equipment_update_carries_the_previous_row(legacy_db, contract):
    equipment_id, site = legacy_db.execute(
        "SELECT equipment_id, assigned_site FROM fleet.equipment ORDER BY equipment_id DESC LIMIT 1"
    ).fetchone()
    consumer = consumer_for(EQUIPMENT_TOPIC)
    try:
        legacy_db.execute(
            "UPDATE fleet.equipment SET assigned_site = 'CDC_TEST_BAY' WHERE equipment_id = %s", (equipment_id,)
        )
        legacy_db.execute("UPDATE fleet.equipment SET assigned_site = %s WHERE equipment_id = %s", (site, equipment_id))
        events = consume_until(
            consumer, lambda values: sum(v["after"]["equipment_id"] == equipment_id for v in values) >= 2
        )
    finally:
        consumer.close()

    moved, restored = (v for _, v in events if v["after"]["equipment_id"] == equipment_id)
    assert (moved["op"], moved["before"]["assigned_site"], moved["after"]["assigned_site"]) == ("u", site, "CDC_TEST_BAY")
    assert (restored["before"]["assigned_site"], restored["after"]["assigned_site"]) == ("CDC_TEST_BAY", site)
    assert moved["after"]["updated_at"] >= moved["before"]["updated_at"]  # trigger bumped updated_at
    assert not list(contract.iter_errors(moved))


def test_legacy_activity_changes_are_all_captured(legacy_db, contract):
    consumer = consumer_for(LOGS_TOPIC)
    try:
        changes = Counter()
        for seed in range(3):
            changes += run_tick(legacy_db, random.Random(seed))  # each tick is its own transaction
        expected_log_events = (
            changes["readings_inserted"] + changes["reading_corrections"] + 2 * changes["duplicates_voided"]
        )
        assert expected_log_events > 0
        events = consume_until(consumer, lambda values: len(values) >= expected_log_events)
    finally:
        consumer.close()

    assert len(events) >= expected_log_events
    assert all(not list(contract.iter_errors(v)) for _, v in events)
