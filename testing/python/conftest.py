import os
import random
import sys
from pathlib import Path

import pytest
from dotenv import load_dotenv

# Module folders start with a digit, so they aren't importable as packages; put them on the path.
REPO_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO_ROOT / "01_telemetry_simulator"))
sys.path.insert(0, str(REPO_ROOT / "02_streaming_ingestion"))

from contract import load_validator  # noqa: E402
from simulator import FleetSimulator  # noqa: E402


@pytest.fixture(scope="session")
def validator():
    return load_validator()


@pytest.fixture
def rng():
    return random.Random(42)


@pytest.fixture
def simulator(rng):
    return FleetSimulator(interval_seconds=5.0, rng=rng)


@pytest.fixture(scope="session")
def snowflake_connection():
    """Connects as the connector's service user (key-pair auth). Skips if Snowflake isn't set up."""
    load_dotenv(REPO_ROOT / ".env")
    snowflake_connector = pytest.importorskip("snowflake.connector")
    serialization = pytest.importorskip("cryptography.hazmat.primitives.serialization")
    key_path = REPO_ROOT / "secrets" / "snowflake_rsa_key.p8"
    if not os.getenv("SNOWFLAKE_ACCOUNT") or not key_path.exists():
        pytest.skip("Snowflake not configured: set SNOWFLAKE_* in .env and run generate_keypair.py")
    try:
        conn = snowflake_connector.connect(
            account=os.environ["SNOWFLAKE_ACCOUNT"],
            user=os.getenv("SNOWFLAKE_USER", "TELEMATICS_KAFKA_CONNECTOR"),
            private_key=serialization.load_pem_private_key(key_path.read_bytes(), password=None),
            role=os.getenv("SNOWFLAKE_ROLE", "TELEMATICS_INGEST_ROLE"),
            warehouse=os.getenv("SNOWFLAKE_WAREHOUSE", "TELEMATICS_WH"),
            database=os.getenv("SNOWFLAKE_DATABASE", "TELEMATICS"),
            schema=os.getenv("SNOWFLAKE_BRONZE_SCHEMA", "BRONZE"),
            login_timeout=30,
        )
    except snowflake_connector.errors.Error as exc:
        pytest.skip(f"Cannot connect to Snowflake: {exc}")
    with conn:
        yield conn
