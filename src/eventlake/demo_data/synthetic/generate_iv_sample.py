"""Generate ``iv_sample.csv``: illustrative option implied volatilities, NOT market data.

Run from the repository root:

    uv run python src/eventlake/demo_data/synthetic/generate_iv_sample.py

For each demo event the file holds one observation time (21:00 UTC on the last weekday that
ends at least a day before the event) and two weekly SPY expiries: the first Friday whose
close is after the event, and the Friday one week later. The implied vol of an expiry is the
total-variance formula the solver inverts,

    iv = sqrt((diffuse**2 * T + event_move**2) / T),   T = years from the observation to the
                                                        expiry close, ACT/365,

with a diffuse vol of 13-16 % and an event move of 0.6-0.9 % chosen from the event's position
in the list below. The numbers are invented; they only exercise the implied-move column.
"""

import csv
import json
import math
from datetime import UTC, date, datetime, time, timedelta
from pathlib import Path

from eventlake.calendar import load_events

HERE = Path(__file__).resolve().parent
EVENT_MOVE = {"FOMC": 0.008, "CPI": 0.006}


def next_friday(day: date) -> date:
    return day + timedelta(days=(4 - day.weekday()) % 7)


def observation_time(event_day: date) -> datetime:
    day = event_day - timedelta(days=2)
    while day.weekday() >= 5:
        day -= timedelta(days=1)
    return datetime.combine(day, time(21, 0), tzinfo=UTC)


def main() -> None:
    manifest = json.loads((HERE.parent / "manifest.json").read_text())
    wanted = set(manifest["events"])
    rows: list[dict[str, str]] = []
    for i, event in enumerate(e for e in load_events() if e.event_id in wanted):
        diffuse = 0.13 + 0.01 * (i % 4)
        move = EVENT_MOVE[event.kind] + 0.0005 * (i % 3)
        at = observation_time(event.scheduled_time.date())
        front = next_friday(event.scheduled_time.date())
        for expiry in (front, front + timedelta(days=7)):
            close = datetime.combine(expiry, time(21, 0), tzinfo=UTC)
            t = (close - at).total_seconds() / (365 * 86400)
            iv = math.sqrt((diffuse**2 * t + move**2) / t)
            rows.append(
                {
                    "event_id": event.event_id,
                    "observed_at": at.isoformat(),
                    "underlying": "SPY",
                    "expiry": expiry.isoformat(),
                    "atm_iv": f"{iv:.4f}",
                }
            )
    with (HERE / "iv_sample.csv").open("w", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)


if __name__ == "__main__":
    main()
