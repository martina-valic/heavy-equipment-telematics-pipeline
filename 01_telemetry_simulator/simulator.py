"""Stateful telemetry generation for the equipment fleet.

Each machine runs a small Markov state machine (IDLE / OPERATIONAL / FAULT) and on every
tick samples its signals from the state-specific ranges in SIGNAL_REGISTRY.
"""

import random
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone

from contract import SCHEMA_VERSION
from equipment_config import EQUIPMENT_FLEET, SIGNAL_REGISTRY

STATES = ("IDLE", "OPERATIONAL", "FAULT")
INITIAL_STATE_WEIGHTS = (0.35, 0.60, 0.05)

# Per-tick transition probabilities; the remaining probability keeps the current state.
STATE_TRANSITIONS = {
    "IDLE": {"OPERATIONAL": 0.10, "FAULT": 0.002},
    "OPERATIONAL": {"IDLE": 0.03, "FAULT": 0.005},
    "FAULT": {"IDLE": 0.05},
}

# Signals that accumulate over the machine's life instead of being re-sampled each tick.
ENGINE_HOURS_TAG = "NS=2;s=Engine_Cumulative_Hours"
UPTIME_TAG = "NS=2;s=System_Uptime_Seconds"
CUMULATIVE_TAGS = {ENGINE_HOURS_TAG, UPTIME_TAG}

INTEGER_UNITS = {"BOOL", "COUNT", "SEC", "RPM"}


def sample_value(unit: str, bounds: tuple, rng: random.Random):
    if unit == "CODE":
        return rng.choice(bounds)
    low, high = bounds
    if unit in INTEGER_UNITS:
        return rng.randint(int(low), int(high))
    return round(rng.uniform(low, high), 2)


@dataclass
class Machine:
    equipment_id: int
    equipment_serial_number: str
    type_name: str
    category: str
    signals: dict
    state: str
    sequence_number: int = 0
    counters: dict = field(default_factory=dict)


class FleetSimulator:
    def __init__(
        self,
        interval_seconds: float = 5.0,
        rng: random.Random | None = None,
        fleet: list[dict] = EQUIPMENT_FLEET,
    ):
        self.interval_seconds = interval_seconds
        self.rng = rng or random.Random()
        self.machines = [self._build_machine(spec) for spec in fleet]

    def _build_machine(self, spec: dict) -> Machine:
        signals = {**SIGNAL_REGISTRY["UNIVERSAL_CORE"], **SIGNAL_REGISTRY[spec["type_name"]]}
        state = self.rng.choices(STATES, weights=INITIAL_STATE_WEIGHTS)[0]
        counters = {
            tag: sample_value(signals[tag][0], signals[tag][3][state], self.rng)
            for tag in CUMULATIVE_TAGS
        }
        return Machine(**spec, signals=signals, state=state, counters=counters)

    def _advance_state(self, machine: Machine) -> None:
        roll = self.rng.random()
        for next_state, probability in STATE_TRANSITIONS[machine.state].items():
            if roll < probability:
                machine.state = next_state
                return
            roll -= probability

    def _advance_counters(self, machine: Machine) -> None:
        machine.counters[UPTIME_TAG] += int(self.interval_seconds)
        if machine.state != "FAULT":
            machine.counters[ENGINE_HOURS_TAG] = round(
                machine.counters[ENGINE_HOURS_TAG] + self.interval_seconds / 3600, 4
            )

    def _emit(self, machine: Machine, now: datetime) -> dict:
        self._advance_state(machine)
        self._advance_counters(machine)
        machine.sequence_number += 1

        readings = []
        for tag, (unit, _description, subsystem, ranges) in machine.signals.items():
            if tag in machine.counters:
                value = machine.counters[tag]
            else:
                value = sample_value(unit, ranges[machine.state], self.rng)
            readings.append({"tag": tag, "value": value, "unit": unit, "subsystem": subsystem})

        return {
            "schema_version": SCHEMA_VERSION,
            "event_id": str(uuid.UUID(int=self.rng.getrandbits(128), version=4)),
            "event_timestamp": now.isoformat(),
            "sequence_number": machine.sequence_number,
            "equipment_id": machine.equipment_id,
            "equipment_serial_number": machine.equipment_serial_number,
            "type_name": machine.type_name,
            "category": machine.category,
            "operating_state": machine.state,
            "readings": readings,
        }

    def tick(self, now: datetime | None = None) -> list[dict]:
        """Advance every machine one interval and return one event per machine."""
        now = now or datetime.now(timezone.utc)
        return [self._emit(machine, now) for machine in self.machines]
