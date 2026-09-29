import copy
import random

import pytest

from anomalies import ANOMALY_WEIGHTS, AnomalyInjector
from contract import validate
from run_simulator import run

CONTRACT_VALID_KINDS = [k for k in ANOMALY_WEIGHTS if k != "CONTRACT_VIOLATION"]


class RecordingSink:
    def __init__(self):
        self.sent, self.dlq = [], []

    def send(self, key, payload):
        self.sent.append(payload)

    def send_dlq(self, key, record):
        self.dlq.append(record)

    def flush(self):
        pass


def inject(kind, event, seed=0):
    injector = AnomalyInjector(rate=1.0, rng=random.Random(seed))
    return getattr(injector, f"_{kind.lower()}")(copy.deepcopy(event))


def test_rate_must_be_a_probability():
    with pytest.raises(ValueError):
        AnomalyInjector(rate=1.5)


def test_zero_rate_leaves_events_untouched(simulator):
    injector = AnomalyInjector(rate=0.0, rng=random.Random(1))
    event = simulator.tick()[0]
    assert injector.apply(event) == ([event], None)


def test_injection_does_not_mutate_the_original(simulator):
    event = simulator.tick()[0]
    snapshot = repr(event)
    AnomalyInjector(rate=1.0, rng=random.Random(1)).apply(event)
    assert repr(event) == snapshot


@pytest.mark.parametrize("kind", CONTRACT_VALID_KINDS)
def test_data_quality_anomalies_still_pass_contract(kind, simulator, validator):
    for seed, event in enumerate(simulator.tick()):
        for payload in inject(kind, event, seed):
            assert validate(validator, payload) == []


def test_temperature_spike_exceeds_any_normal_range(simulator):
    event = simulator.tick()[0]
    [spiked] = inject("TEMPERATURE_SPIKE", event)
    assert max(r["value"] for r in spiked["readings"] if r["unit"] == "DEGC") >= 250


def test_pressure_drop_is_non_positive(simulator):
    event = simulator.tick()[0]
    [dropped] = inject("PRESSURE_DROP", event)
    assert min(r["value"] for r in dropped["readings"] if r["unit"] == "PSI") <= 0


def test_null_and_missing_readings(simulator):
    event = simulator.tick()[0]
    [nulled] = inject("NULL_READINGS", event)
    assert 1 <= sum(r["value"] is None for r in nulled["readings"]) <= 3
    [trimmed] = inject("MISSING_READINGS", event)
    assert 2 <= len(event["readings"]) - len(trimmed["readings"]) <= 5


def test_duplicate_event_repeats_event_id(simulator):
    event = simulator.tick()[0]
    first, second = inject("DUPLICATE_EVENT", event)
    assert first == second


def test_contract_violation_is_rejected(simulator, validator):
    for seed, event in enumerate(simulator.tick()):
        [broken] = inject("CONTRACT_VIOLATION", event, seed)
        assert validate(validator, broken) != []


def test_run_routes_violations_to_dlq_and_hits_target_rate(simulator, validator):
    injector = AnomalyInjector(rate=0.075, rng=random.Random(3))
    sink = RecordingSink()
    totals = run(simulator, injector, validator, sink, ticks=100, interval=0)

    anomalies = sum(totals[k] for k in ANOMALY_WEIGHTS)
    assert 0.05 <= anomalies / 4000 <= 0.10
    assert totals["dlq"] == totals["CONTRACT_VIOLATION"] == len(sink.dlq)
    assert totals["sent"] == 4000 - totals["dlq"] + totals["DUPLICATE_EVENT"] == len(sink.sent)
    assert all(validate(validator, payload) == [] for payload in sink.sent)
