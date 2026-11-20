"""Real-world event calendar: FOMC decisions, CPI and jobs releases, daily weather highs.

Scheduled release dates live in packaged CSVs (``date,source_url``). Release times are local
to America/New_York and converted to UTC here so daylight saving is handled by ``zoneinfo``.
"""

import csv
import json
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, date, datetime, time, timedelta
from importlib import resources
from typing import Any, Literal, get_args
from zoneinfo import ZoneInfo

from eventlake.store import Store
from eventlake.types import utc

EventKind = Literal["FOMC", "CPI", "NFP", "WEATHER_HIGH"]

_NY = ZoneInfo("America/New_York")
_RELEASE_TIME: dict[str, time] = {
    "FOMC": time(14, 0),
    "CPI": time(8, 30),
    "NFP": time(8, 30),
}
_CSV_FILES = {"FOMC": "fomc.csv", "CPI": "cpi.csv", "NFP": "nfp.csv"}
_WARN_BEFORE_END = timedelta(days=30)


@dataclass(frozen=True)
class Event:
    event_id: str
    kind: EventKind
    scheduled_time: datetime
    attrs: Mapping[str, Any]


def _load_kind(kind: str) -> list[Event]:
    text = (
        resources.files("eventlake")
        .joinpath("calendar_data", _CSV_FILES[kind])
        .read_text(encoding="utf-8")
    )
    out: list[Event] = []
    for row in csv.DictReader(text.splitlines()):
        day = date.fromisoformat(row["date"])
        local = datetime.combine(day, _RELEASE_TIME[kind], tzinfo=_NY)
        out.append(
            Event(
                event_id=f"{kind}:{day:%Y-%m-%d}",
                kind=kind,  # type: ignore[arg-type]
                scheduled_time=local.astimezone(UTC),
                attrs={"source_url": row["source_url"]},
            )
        )
    return out


def load_events(kinds: Sequence[EventKind] | None = None) -> list[Event]:
    """Scheduled events from the packaged CSVs, sorted by time then id."""
    wanted = list(kinds) if kinds is not None else list(_CSV_FILES)
    events: list[Event] = []
    for kind in wanted:
        if kind not in _CSV_FILES:
            if kind in get_args(EventKind):
                continue
            raise ValueError(f"unknown event kind: {kind}")
        events.extend(_load_kind(kind))
    return sorted(events, key=lambda e: (e.scheduled_time, e.event_id))


def calendar_coverage_end(kind: EventKind) -> date:
    """The last scheduled date of ``kind`` in the packaged calendar."""
    if kind not in _CSV_FILES:
        raise ValueError(f"no packaged schedule for event kind: {kind}")
    return max(e.scheduled_time.astimezone(_NY).date() for e in _load_kind(kind))


def calendar_warnings(now: datetime) -> list[str]:
    """One line per kind whose schedule ends within 30 days of ``now``, or already has."""
    today = utc(now).date()
    out: list[str] = []
    for kind in sorted(_CSV_FILES):
        end = calendar_coverage_end(kind)  # type: ignore[arg-type]
        if today >= end - _WARN_BEFORE_END:
            out.append(f"calendar: {kind} schedule ends {end.isoformat()}")
    return out


def weather_event(station: str, day: date) -> Event:
    """Daily-high event for ``station`` on ``day``; settles at 05:00 UTC the next day."""
    when = datetime.combine(day + timedelta(days=1), time(5, 0), tzinfo=UTC)
    return Event(
        event_id=f"WEATHER_HIGH:{station}:{day:%Y-%m-%d}",
        kind="WEATHER_HIGH",
        scheduled_time=when,
        attrs={"station": station, "date": day.isoformat()},
    )


def upsert_events(store: Store, events: Iterable[Event]) -> int:
    """Insert events whose id is not stored yet; existing ids are left untouched."""
    inserted = 0
    for e in events:
        if store.execute("SELECT 1 FROM events WHERE event_id = ?", [e.event_id]):
            continue
        store.execute(
            "INSERT INTO events (event_id, kind, scheduled_time, attrs) VALUES (?,?,?,?)",
            [e.event_id, e.kind, utc(e.scheduled_time), json.dumps(dict(e.attrs), sort_keys=True)],
        )
        inserted += 1
    return inserted
