from collections import Counter

from contract import validate
from equipment_config import EQUIPMENT_FLEET
from simulator import ENGINE_HOURS_TAG, UPTIME_TAG, FleetSimulator


def by_tag(event):
    return {r["tag"]: r["value"] for r in event["readings"]}


def test_fleet_has_40_machines_with_unique_keys():
    assert len(EQUIPMENT_FLEET) == 40
    assert [m["equipment_id"] for m in EQUIPMENT_FLEET] == list(range(1, 41))
    assert len({m["equipment_serial_number"] for m in EQUIPMENT_FLEET}) == 40


def test_fleet_matches_dim_equipment_layout():
    categories = Counter(m["category"] for m in EQUIPMENT_FLEET)
    assert categories == {"BEV": 17, "Diesel": 23}
    assert EQUIPMENT_FLEET[16] == {
        "equipment_id": 17,
        "equipment_serial_number": "SCOOP-001",
        "type_name": "Scooptram_LHD",
        "category": "BEV",
    }


def test_tick_emits_one_event_per_machine(simulator):
    events = simulator.tick()
    assert len(events) == 40
    assert len({e["event_id"] for e in events}) == 40


def test_clean_events_satisfy_contract(simulator, validator):
    for _ in range(50):
        for event in simulator.tick():
            assert validate(validator, event) == []


def test_readings_stay_within_state_ranges(simulator):
    for _ in range(20):
        for machine, event in zip(simulator.machines, simulator.tick()):
            for reading in event["readings"]:
                if reading["tag"] in machine.counters:
                    continue
                unit, _, _, ranges = machine.signals[reading["tag"]]
                bounds = ranges[event["operating_state"]]
                if unit == "CODE":
                    assert reading["value"] in bounds
                else:
                    assert bounds[0] <= reading["value"] <= bounds[1]


def test_sequence_numbers_and_counters_are_monotonic(simulator):
    previous = {e["equipment_serial_number"]: e for e in simulator.tick()}
    for _ in range(30):
        for event in simulator.tick():
            prior = previous[event["equipment_serial_number"]]
            assert event["sequence_number"] == prior["sequence_number"] + 1
            assert by_tag(event)[UPTIME_TAG] == by_tag(prior)[UPTIME_TAG] + 5
            assert by_tag(event)[ENGINE_HOURS_TAG] >= by_tag(prior)[ENGINE_HOURS_TAG]
            previous[event["equipment_serial_number"]] = event


def test_machines_transition_between_states(simulator):
    seen = Counter()
    for _ in range(200):
        seen.update(e["operating_state"] for e in simulator.tick())
    assert set(seen) == {"IDLE", "OPERATIONAL", "FAULT"}


def test_same_seed_produces_same_stream():
    import random

    first = FleetSimulator(rng=random.Random(7)).tick()
    second = FleetSimulator(rng=random.Random(7)).tick()
    strip = lambda events: [{k: v for k, v in e.items() if k != "event_timestamp"} for e in events]
    assert strip(first) == strip(second)
