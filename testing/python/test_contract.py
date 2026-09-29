import copy

import pytest

from contract import build_dlq_record, validate


@pytest.fixture
def event(simulator):
    return simulator.tick()[0]


def reading_with_unit(event, unit):
    return next(r for r in event["readings"] if r["unit"] == unit)


def test_valid_event_passes(event, validator):
    assert validate(validator, event) == []


def test_null_sensor_value_is_allowed(event, validator):
    event["readings"][0]["value"] = None
    assert validate(validator, event) == []


def test_out_of_range_values_are_allowed(event, validator):
    # Business ranges are enforced in Silver, not by the contract.
    reading_with_unit(event, "DEGC")["value"] = 999.0
    reading_with_unit(event, "PSI")["value"] = -20.0
    assert validate(validator, event) == []


@pytest.mark.parametrize(
    "mutate",
    [
        pytest.param(lambda e: e.pop("equipment_serial_number"), id="missing-required-field"),
        pytest.param(lambda e: e.update(unexpected="x"), id="unknown-field"),
        pytest.param(lambda e: e.update(event_timestamp="yesterday"), id="bad-timestamp"),
        pytest.param(lambda e: e.update(event_id="not-a-uuid"), id="bad-uuid"),
        pytest.param(lambda e: e.update(category="Hybrid"), id="unknown-category"),
        pytest.param(lambda e: e.update(operating_state="SLEEPING"), id="unknown-state"),
        pytest.param(lambda e: e.update(equipment_serial_number="scoop_1"), id="bad-serial"),
        pytest.param(lambda e: e.update(sequence_number=0), id="zero-sequence"),
        pytest.param(lambda e: e.update(readings=[]), id="empty-readings"),
        pytest.param(lambda e: reading_with_unit(e, "PSI").update(value="ERR"), id="string-in-numeric"),
        pytest.param(lambda e: reading_with_unit(e, "CODE").update(value=3), id="number-in-code"),
        pytest.param(lambda e: reading_with_unit(e, "BOOL").update(value=2), id="bool-out-of-domain"),
        pytest.param(lambda e: reading_with_unit(e, "PSI").update(unit="BAR"), id="unknown-unit"),
        pytest.param(lambda e: e["readings"][0].pop("subsystem"), id="reading-missing-subsystem"),
    ],
)
def test_contract_violations_are_rejected(event, validator, mutate):
    mutate(event)
    assert validate(validator, event) != []


def test_dlq_record_wraps_original_payload(event, validator):
    broken = copy.deepcopy(event)
    del broken["event_timestamp"]
    record = build_dlq_record(broken, validate(validator, broken))
    assert record["contract"] == "equipment_telemetry.v1"
    assert record["original_payload"] == broken
    assert any("event_timestamp" in error for error in record["errors"])
