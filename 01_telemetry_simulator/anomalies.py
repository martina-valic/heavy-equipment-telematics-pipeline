"""Injects data-quality anomalies into a share of telemetry events.

Most anomalies stay contract-valid so they land in Bronze and are caught by Silver-tier
dbt tests. CONTRACT_VIOLATION produces an event that fails validation and is routed to the DLQ.
"""

import copy
import random

# Relative weights of each anomaly kind once an event is selected for injection.
ANOMALY_WEIGHTS = {
    "TEMPERATURE_SPIKE": 0.20,
    "PRESSURE_DROP": 0.20,
    "NULL_READINGS": 0.20,
    "MISSING_READINGS": 0.15,
    "DUPLICATE_EVENT": 0.15,
    "CONTRACT_VIOLATION": 0.10,
}


class AnomalyInjector:
    def __init__(self, rate: float, rng: random.Random | None = None):
        if not 0.0 <= rate <= 1.0:
            raise ValueError(f"Anomaly rate must be between 0 and 1, got {rate}")
        self.rate = rate
        self.rng = rng or random.Random()

    def apply(self, event: dict) -> tuple[list[dict], str | None]:
        """Return the event(s) to send and the anomaly kind applied (None if clean)."""
        if self.rng.random() >= self.rate:
            return [event], None
        kind = self.rng.choices(list(ANOMALY_WEIGHTS), weights=list(ANOMALY_WEIGHTS.values()))[0]
        return getattr(self, f"_{kind.lower()}")(copy.deepcopy(event)), kind

    def _readings_with_unit(self, event: dict, unit: str) -> list[dict]:
        return [r for r in event["readings"] if r["unit"] == unit and r["value"] is not None]

    def _temperature_spike(self, event: dict) -> list[dict]:
        reading = self.rng.choice(self._readings_with_unit(event, "DEGC"))
        reading["value"] = round(self.rng.uniform(250, 999), 2)
        return [event]

    def _pressure_drop(self, event: dict) -> list[dict]:
        reading = self.rng.choice(self._readings_with_unit(event, "PSI"))
        reading["value"] = round(self.rng.uniform(-50, 0), 2)
        return [event]

    def _null_readings(self, event: dict) -> list[dict]:
        for reading in self.rng.sample(event["readings"], k=self.rng.randint(1, 3)):
            reading["value"] = None
        return [event]

    def _missing_readings(self, event: dict) -> list[dict]:
        drop = self.rng.randint(2, 5)
        keep = self.rng.sample(event["readings"], k=len(event["readings"]) - drop)
        event["readings"] = [r for r in event["readings"] if r in keep]
        return [event]

    def _duplicate_event(self, event: dict) -> list[dict]:
        return [event, copy.deepcopy(event)]

    def _contract_violation(self, event: dict) -> list[dict]:
        if self.rng.random() < 0.5:
            del event[self.rng.choice(["equipment_serial_number", "event_timestamp", "operating_state"])]
        else:
            numeric = [r for r in event["readings"] if r["unit"] not in ("CODE", "BOOL")]
            self.rng.choice(numeric)["value"] = self.rng.choice(["ERR", "N/A", "#VALUE!"])
        return [event]
