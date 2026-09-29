"""Loads the equipment telemetry data contract and validates payloads against it."""

import json
from datetime import datetime, timezone
from pathlib import Path

from jsonschema import Draft202012Validator, FormatChecker

CONTRACT_NAME = "equipment_telemetry.v1"
SCHEMA_VERSION = "1.0.0"
CONTRACT_PATH = (
    Path(__file__).resolve().parents[1]
    / "documentation"
    / "data_contracts"
    / f"{CONTRACT_NAME}.schema.json"
)


def load_validator(path: Path = CONTRACT_PATH) -> Draft202012Validator:
    schema = json.loads(path.read_text(encoding="utf-8"))
    Draft202012Validator.check_schema(schema)
    # FormatChecker enforces uuid and date-time (date-time needs rfc3339-validator installed).
    return Draft202012Validator(schema, format_checker=FormatChecker())


def validate(validator: Draft202012Validator, payload: dict) -> list[str]:
    """Return a list of human-readable contract violations; empty means valid."""
    errors = sorted(validator.iter_errors(payload), key=lambda e: e.json_path)
    return [f"{error.json_path}: {error.message}" for error in errors]


def build_dlq_record(payload: dict, errors: list[str]) -> dict:
    return {
        "failed_at": datetime.now(timezone.utc).isoformat(),
        "contract": CONTRACT_NAME,
        "errors": errors,
        "original_payload": payload,
    }
