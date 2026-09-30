"""Renders seeds/signal_catalog.csv: every signal each equipment type reports, with its expected range.

The catalog comes from the telemetry simulator's SIGNAL_REGISTRY (01_telemetry_simulator/
equipment_config.py), so Silver checks readings against the same definitions the machines use.
One row per (type_name, tag):

  min_expected / max_expected  lowest and highest value across IDLE, OPERATIONAL and FAULT. FAULT
                               is a legitimate state, so its ranges count as expected values.
                               Empty for CODE signals and for the open end of cumulative counters.
  allowed_values               the valid codes of a CODE signal, separated by "|"
  is_cumulative                counters that only grow (engine hours, uptime): no upper bound

Silver flags a reading outside these bounds as out of range, and an event missing some of its
type's signals as incomplete. A unit test fails if the committed file is out of date.

Usage (from the repo root):
    python 04_data_warehouse/generate_signal_catalog.py
"""

import csv
import io
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT / "01_telemetry_simulator"))

from equipment_config import EQUIPMENT_FLEET, SIGNAL_REGISTRY  # noqa: E402
from simulator import CUMULATIVE_TAGS  # noqa: E402

CATALOG_PATH = Path(__file__).resolve().parent / "seeds" / "signal_catalog.csv"
TAG_PREFIX = "NS=2;s="
COLUMNS = (
    "type_name", "tag", "signal_name", "unit", "unit_description", "subsystem",
    "min_expected", "max_expected", "allowed_values", "is_cumulative",
)


def number(value) -> str:
    """Integers without a trailing .0, so the CSV reads like the registry."""
    return str(int(value)) if float(value).is_integer() else str(value)


def catalog_rows() -> list[dict]:
    type_names = sorted({machine["type_name"] for machine in EQUIPMENT_FLEET})
    rows = []
    for type_name in type_names:
        signals = {**SIGNAL_REGISTRY["UNIVERSAL_CORE"], **SIGNAL_REGISTRY[type_name]}
        for tag, (unit, unit_description, subsystem, ranges) in signals.items():
            row = {
                "type_name": type_name,
                "tag": tag,
                "signal_name": tag.removeprefix(TAG_PREFIX),
                "unit": unit,
                "unit_description": unit_description,
                "subsystem": subsystem,
                "min_expected": "",
                "max_expected": "",
                "allowed_values": "",
                "is_cumulative": str(tag in CUMULATIVE_TAGS).lower(),
            }
            if unit == "CODE":
                codes = sorted({code for bounds in ranges.values() for code in bounds})
                row["allowed_values"] = "|".join(codes)
            elif tag in CUMULATIVE_TAGS:
                row["min_expected"] = "0"
            else:
                row["min_expected"] = number(min(low for low, _ in ranges.values()))
                row["max_expected"] = number(max(high for _, high in ranges.values()))
            rows.append(row)
    return rows


def render(rows: list[dict]) -> str:
    buffer = io.StringIO()
    writer = csv.DictWriter(buffer, fieldnames=COLUMNS, lineterminator="\n")
    writer.writeheader()
    writer.writerows(rows)
    return buffer.getvalue()


def main() -> None:
    CATALOG_PATH.parent.mkdir(exist_ok=True)
    CATALOG_PATH.write_text(render(catalog_rows()), encoding="utf-8", newline="\n")
    print(f"Wrote {CATALOG_PATH.relative_to(REPO_ROOT)}")


if __name__ == "__main__":
    main()
