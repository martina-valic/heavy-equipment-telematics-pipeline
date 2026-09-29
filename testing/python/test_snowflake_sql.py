"""Runs every SQL test in testing/snowflake/ against Snowflake.

Each .sql file is a data test: its last statement selects the rows that violate an expectation, so
zero rows means pass (the same convention as dbt singular tests). Files may run setup statements
first, e.g. SHOW GRANTS followed by a RESULT_SCAN query. Session variables available to every
file: $sla_seconds (INGESTION_SLA_SECONDS, default 120).

Needs only Snowflake, not Kafka: skipped unless the setup from 02_streaming_ingestion is done.
"""

import os

import pytest

from conftest import REPO_ROOT

SQL_TEST_DIR = REPO_ROOT / "testing" / "snowflake"
SQL_TESTS = sorted(SQL_TEST_DIR.glob("*.sql"))
MAX_ROWS_SHOWN = 5

pytestmark = pytest.mark.snowflake


@pytest.fixture(scope="module")
def session(snowflake_connection):
    sla_seconds = float(os.getenv("INGESTION_SLA_SECONDS", "120"))
    snowflake_connection.cursor().execute(f"SET sla_seconds = {sla_seconds}")
    return snowflake_connection


def test_sql_tests_exist():
    assert SQL_TESTS, f"no .sql files in {SQL_TEST_DIR}"


@pytest.mark.parametrize("sql_file", SQL_TESTS, ids=lambda p: p.stem)
def test_sql_returns_no_violations(session, sql_file):
    cursors = session.execute_string(sql_file.read_text(encoding="utf-8"), remove_comments=True)
    result = cursors[-1]
    columns = [col[0] for col in result.description]
    rows = result.fetchall()
    shown = "\n".join(str(dict(zip(columns, row))) for row in rows[:MAX_ROWS_SHOWN])
    assert not rows, f"{sql_file.name} returned {len(rows)} violating row(s):\n{shown}"
