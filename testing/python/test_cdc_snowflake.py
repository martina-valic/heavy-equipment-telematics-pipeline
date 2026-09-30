"""End-to-end CDC ingestion SLA: a change committed in the legacy Postgres database must be
queryable in its Snowflake Bronze table within INGESTION_SLA_SECONDS (default 120), measured from
the commit. Needs the full stack: Postgres, Kafka, Kafka Connect with debezium-source-legacy-fleet
and snowflake-sink-legacy-cdc RUNNING, and 03_cdc_migration/snowflake/setup.sql run in Snowflake.
"""

import json
import os
import time

import pytest

pytestmark = [pytest.mark.cdc, pytest.mark.snowflake]

SLA_SECONDS = float(os.getenv("INGESTION_SLA_SECONDS", "120"))
TABLE = "LEGACY_HOUR_METER_LOGS_CDC_RAW"


@pytest.fixture(scope="module")
def snowflake_cursor(snowflake_connection):
    cursor = snowflake_connection.cursor()
    exists = cursor.execute(f"SHOW TABLES LIKE '{TABLE}' IN SCHEMA TELEMATICS.BRONZE").fetchall()
    assert exists, f"{TABLE} not found: run 03_cdc_migration/snowflake/setup.sql in Snowsight"
    return cursor


def wait_for_ops(cursor, log_id: int, expected_ops: list[str]) -> tuple[list, float]:
    """Poll until every expected op for log_id has landed or the SLA runs out."""
    start = time.monotonic()
    rows = []
    while time.monotonic() - start < SLA_SECONDS:
        rows = cursor.execute(
            f"""
            SELECT RECORD_CONTENT:op::STRING, RECORD_METADATA, RECORD_CONTENT
            FROM {TABLE}
            WHERE COALESCE(RECORD_CONTENT:after:log_id, RECORD_CONTENT:before:log_id)::NUMBER = %s
            ORDER BY RECORD_CONTENT:source:lsn::NUMBER
            """,
            (log_id,),
        ).fetchall()
        if [row[0] for row in rows] == expected_ops:
            break
        time.sleep(5)
    return rows, time.monotonic() - start


def test_reading_life_cycle_lands_in_bronze_within_sla(legacy_db, snowflake_cursor):
    log_id, equipment_id = legacy_db.execute(
        """
        INSERT INTO fleet.hour_meter_logs (equipment_id, reading_at, hour_meter, source, recorded_by)
        SELECT equipment_id, clock_timestamp(), 9999.0, 'OPERATOR_LOG', 'cdc_sla_test'
        FROM fleet.equipment ORDER BY equipment_id LIMIT 1
        RETURNING log_id, equipment_id
        """
    ).fetchone()
    legacy_db.execute("DELETE FROM fleet.hour_meter_logs WHERE log_id = %s", (log_id,))

    rows, waited = wait_for_ops(snowflake_cursor, log_id, ["c", "d"])
    assert [row[0] for row in rows] == ["c", "d"], (
        f"only {[row[0] for row in rows]} landed for log_id {log_id} within the {SLA_SECONDS:.0f}s SLA"
    )
    print(f"\ninsert and delete landed in Bronze after {waited:.1f}s (SLA {SLA_SECONDS:.0f}s)")

    for _, metadata, content in rows:
        metadata, content = json.loads(metadata), json.loads(content)
        assert metadata["topic"] == "telematics.legacy.hour_meter_logs.cdc.v1"
        assert json.loads(metadata["key"]) == {"equipment_id": equipment_id}
        commit_to_push = (metadata["SnowflakeConnectorPushTime"] - content["source"]["ts_ms"]) / 1000
        assert commit_to_push < SLA_SECONDS
    deleted = json.loads(rows[1][2])
    assert deleted["after"] is None and deleted["before"]["recorded_by"] == "cdc_sla_test"
