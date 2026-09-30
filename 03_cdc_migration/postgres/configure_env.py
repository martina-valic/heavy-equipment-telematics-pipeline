"""Adds the Module 3 settings to .env and fills in the legacy database passwords.

Idempotent: values already in .env are never changed, so re-running never locks you out of an
existing database. Each run:
  1. creates .env from .env.example if it does not exist yet,
  2. copies any missing POSTGRES_* / LEGACY_* setting from .env.example (users, database, ports),
  3. sets every empty password to a strong random value:
       POSTGRES_PASSWORD      superuser, only used by the container to initialise the database
       POSTGRES_CDC_PASSWORD  Debezium replication user (resolved by Kafka Connect as ${env:VAR})
       POSTGRES_APP_PASSWORD  legacy application user (legacy_activity.py and the tests)

Usage (from the repo root):
    python 03_cdc_migration/postgres/configure_env.py
"""

import re
import secrets
import sys
from pathlib import Path

from dotenv import dotenv_values

REPO_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO_ROOT / "02_streaming_ingestion" / "snowflake"))

from generate_keypair import set_env_var  # noqa: E402

ENV_PATH = REPO_ROOT / ".env"
ENV_EXAMPLE_PATH = REPO_ROOT / ".env.example"
MODULE_SETTING = re.compile(r"^(POSTGRES|LEGACY)_[A-Z_]+$")
PASSWORD_VARS = ("POSTGRES_PASSWORD", "POSTGRES_CDC_PASSWORD", "POSTGRES_APP_PASSWORD")


def configure(env_path: Path = ENV_PATH, example_path: Path = ENV_EXAMPLE_PATH) -> dict[str, str]:
    """Bring env_path up to date. Returns {name: 'copied' | 'generated'} for what changed."""
    changes = {}
    current = dotenv_values(env_path) if env_path.exists() else {}
    for name, default in dotenv_values(example_path).items():
        if MODULE_SETTING.match(name) and name not in current and default:
            set_env_var(env_path, name, default)
            changes[name] = "copied"
    current = dotenv_values(env_path) if env_path.exists() else {}
    for name in PASSWORD_VARS:
        if not current.get(name):
            # URL-safe alphabet: no quotes, spaces or '$', so it survives dotenv, Compose and JDBC.
            set_env_var(env_path, name, secrets.token_urlsafe(24))
            changes[name] = "generated"
    return changes


def main() -> None:
    changes = configure()
    if not changes:
        print("Module 3 settings and passwords already set in .env, nothing changed")
    for name, change in changes.items():
        print(f"{name}: {change}")
    if any(change == "generated" for change in changes.values()):
        print("If the Postgres volume already exists, apply new passwords with 02_roles.sh (see the Module 3 README).")


if __name__ == "__main__":
    main()
