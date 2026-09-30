"""Simulates the legacy fleet-management application writing to telematics_legacy.

Every tick is one transaction, like a batch of screens saved by site staff. It produces all three
change types Debezium has to capture:

  INSERT  end-of-shift hour-meter readings for working machines
  UPDATE  a machine moves between ACTIVE, MAINTENANCE and STANDBY or to another site, and
          clerks correct a mistyped hour-meter reading
  DELETE  clerks void a reading that was entered twice

Connects as the application user (POSTGRES_APP_USER), which has DML rights on the fleet tables only.

Usage (from the repo root):
    python 03_cdc_migration/legacy_activity.py [--interval 10] [--ticks N] [--seed 7]
"""

import argparse
import logging
import os
import random
import signal
import time
from collections import Counter
from pathlib import Path

import psycopg
from dotenv import load_dotenv

REPO_ROOT = Path(__file__).resolve().parents[1]
logger = logging.getLogger("legacy_activity")

READING_PROBABILITY = 0.25  # per working machine per tick
STATUS_CHANGE_PROBABILITY = 0.20
SITE_CHANGE_PROBABILITY = 0.10
CORRECTION_PROBABILITY = 0.15
DUPLICATE_VOID_PROBABILITY = 0.10
SITES = ("LEVEL_2400", "LEVEL_2600", "LEVEL_2800", "RAMP_NORTH", "SURFACE_SHOP")
# Machines never leave the fleet here: DECOMMISSIONED is left for manual demos.
NEXT_STATUS = {"ACTIVE": ("MAINTENANCE", "STANDBY"), "MAINTENANCE": ("ACTIVE",), "STANDBY": ("ACTIVE",)}


def connect() -> psycopg.Connection:
    return psycopg.connect(
        host=os.getenv("POSTGRES_HOST", "localhost"),
        port=int(os.getenv("POSTGRES_PORT", "5432")),
        dbname=os.getenv("POSTGRES_DB", "telematics_legacy"),
        user=os.getenv("POSTGRES_APP_USER", "legacy_app"),
        password=os.environ["POSTGRES_APP_PASSWORD"],
        application_name="legacy_activity",
        connect_timeout=10,
    )


def log_readings(cur: psycopg.Cursor, rng: random.Random) -> int:
    cur.execute(
        """
        SELECT e.equipment_id, COALESCE(MAX(l.hour_meter), 0)
        FROM fleet.equipment e
        LEFT JOIN fleet.hour_meter_logs l USING (equipment_id)
        WHERE e.status = 'ACTIVE'
        GROUP BY e.equipment_id
        """
    )
    written = 0
    for equipment_id, hours in cur.fetchall():
        if rng.random() >= READING_PROBABILITY:
            continue
        source = rng.choice(("SHIFT_REPORT", "OPERATOR_LOG"))
        cur.execute(
            """
            INSERT INTO fleet.hour_meter_logs (equipment_id, reading_at, hour_meter, source, recorded_by)
            VALUES (%s, clock_timestamp(), %s, %s, %s)
            """,
            (equipment_id, round(float(hours) + rng.uniform(0.5, 12.0), 1), source,
             "shift_supervisor" if source == "SHIFT_REPORT" else "operator"),
        )
        written += 1
    return written


def change_status(cur: psycopg.Cursor, rng: random.Random) -> int:
    cur.execute("SELECT equipment_id, status FROM fleet.equipment WHERE status <> 'DECOMMISSIONED' ORDER BY equipment_id")
    equipment_id, status = rng.choice(cur.fetchall())
    cur.execute(
        "UPDATE fleet.equipment SET status = %s WHERE equipment_id = %s",
        (rng.choice(NEXT_STATUS[status]), equipment_id),
    )
    return cur.rowcount


def change_site(cur: psycopg.Cursor, rng: random.Random) -> int:
    cur.execute("SELECT equipment_id, assigned_site FROM fleet.equipment WHERE status = 'ACTIVE' ORDER BY equipment_id")
    rows = cur.fetchall()
    if not rows:
        return 0
    equipment_id, site = rng.choice(rows)
    cur.execute(
        "UPDATE fleet.equipment SET assigned_site = %s WHERE equipment_id = %s",
        (rng.choice([s for s in SITES if s != site]), equipment_id),
    )
    return cur.rowcount


def correct_reading(cur: psycopg.Cursor, rng: random.Random) -> int:
    """Fix a typo in one of the latest readings by a tenth of an hour or two."""
    cur.execute("SELECT log_id FROM fleet.hour_meter_logs ORDER BY log_id DESC LIMIT 50")
    rows = cur.fetchall()
    if not rows:
        return 0
    cur.execute(
        """
        UPDATE fleet.hour_meter_logs
        SET hour_meter = GREATEST(hour_meter + %s, 0), recorded_by = 'records_clerk'
        WHERE log_id = %s
        """,
        (rng.choice((-0.2, -0.1, 0.1, 0.2)), rng.choice(rows)[0]),
    )
    return cur.rowcount


def void_duplicate(cur: psycopg.Cursor, rng: random.Random) -> int:
    """Enter a reading twice by mistake, then void the copy."""
    cur.execute(
        """
        SELECT equipment_id, hour_meter, source, recorded_by
        FROM fleet.hour_meter_logs ORDER BY log_id DESC LIMIT 1
        """
    )
    latest = cur.fetchone()
    if latest is None:
        return 0
    cur.execute(
        """
        INSERT INTO fleet.hour_meter_logs (equipment_id, reading_at, hour_meter, source, recorded_by)
        VALUES (%s, clock_timestamp(), %s, %s, %s) RETURNING log_id
        """,
        latest,
    )
    duplicate_id = cur.fetchone()[0]
    cur.execute("DELETE FROM fleet.hour_meter_logs WHERE log_id = %s", (duplicate_id,))
    return cur.rowcount


def run_tick(conn: psycopg.Connection, rng: random.Random) -> Counter:
    """One transaction of legacy-app activity. Returns rows changed per kind of change."""
    changes = Counter()
    with conn.transaction(), conn.cursor() as cur:
        changes["readings_inserted"] = log_readings(cur, rng)
        if rng.random() < STATUS_CHANGE_PROBABILITY:
            changes["status_updates"] = change_status(cur, rng)
        if rng.random() < SITE_CHANGE_PROBABILITY:
            changes["site_updates"] = change_site(cur, rng)
        if rng.random() < CORRECTION_PROBABILITY:
            changes["reading_corrections"] = correct_reading(cur, rng)
        if rng.random() < DUPLICATE_VOID_PROBABILITY:
            changes["duplicates_voided"] = void_duplicate(cur, rng)
    return changes


def main() -> None:
    load_dotenv(REPO_ROOT / ".env")
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--interval", type=float, default=float(os.getenv("LEGACY_ACTIVITY_INTERVAL_SECONDS", "10")))
    parser.add_argument("--ticks", type=int, default=None, help="stop after N transactions (default: run forever)")
    parser.add_argument("--seed", type=int, default=None, help="for reproducible runs")
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    stop = False

    def request_stop(*_):
        nonlocal stop
        stop = True

    signal.signal(signal.SIGINT, request_stop)
    signal.signal(signal.SIGTERM, request_stop)

    rng = random.Random(args.seed)
    tick = 0
    with connect() as conn:
        logger.info("Connected to %s as %s", conn.info.dbname, conn.info.user)
        while not stop and (args.ticks is None or tick < args.ticks):
            tick += 1
            changes = run_tick(conn, rng)
            logger.info("tick %d: %s", tick, ", ".join(f"{k}={v}" for k, v in changes.items() if v) or "no changes")
            if args.ticks is None or tick < args.ticks:
                time.sleep(args.interval)
    logger.info("Stopped after %d tick(s)", tick)


if __name__ == "__main__":
    main()
