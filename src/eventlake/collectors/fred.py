"""FRED collector: daily and monthly macro series from the keyless CSV endpoint."""

import csv
import io
from collections.abc import Iterator, Mapping
from dataclasses import dataclass
from datetime import UTC, date, datetime, time, timedelta
from functools import cache
from typing import Any

from eventlake.calendar import Event, EventKind, load_events
from eventlake.http import HttpClient, RateLimiter
from eventlake.types import KtQuality, Observation, ValidationError, utc

BASE_URL = "https://fred.stlouisfed.org"
FEED = "fred"
MISSING = frozenset({"", "."})  # the live CSV leaves missing values empty; "." is the legacy marker

Known = tuple[datetime, KtQuality, dict[str, Any]]


@dataclass(frozen=True)
class DailyLag:
    """A daily series: assumed known ``lag`` after the end of the observation day."""

    lag: timedelta

    def knowledge(self, day: date) -> Known:
        start = datetime.combine(day, time(), tzinfo=UTC)
        return start + timedelta(days=1) + self.lag, "assumed", {}


@cache
def _releases(kind: EventKind) -> tuple[Event, ...]:
    return tuple(load_events([kind]))


def _next_month(day: date) -> datetime:
    first = date(day.year + day.month // 12, day.month % 12 + 1, 1)
    return datetime.combine(first, time(), tzinfo=UTC)


def _first_of_month(when: datetime) -> datetime:
    return datetime.combine(utc(when).date().replace(day=1), time(), tzinfo=UTC)


@dataclass(frozen=True)
class MonthlyRelease:
    """A monthly series dated the first of its reference month, published by a calendar release.

    Known at the first ``kind`` release in the packaged calendar after the reference month ends
    (``source``). The calendar covers reference months from the month before its first release
    onward; for a month outside that coverage, or with no later release in the calendar, the
    series is known ``fallback`` after the month ends (``assumed``). One release is assumed to
    carry each reference month.
    """

    kind: EventKind
    fallback: timedelta = timedelta(days=45)

    def knowledge(self, day: date) -> Known:
        month_end = _next_month(day)
        releases = _releases(self.kind)
        covered = bool(releases) and month_end >= _first_of_month(releases[0].scheduled_time)
        for event in releases if covered else ():
            if event.scheduled_time > month_end:
                return event.scheduled_time, "source", {"event_id": event.event_id}
        return month_end + self.fallback, "assumed", {}


KnowledgeRule = DailyLag | MonthlyRelease

FRED_SERIES: Mapping[str, KnowledgeRule] = {
    "SP500": DailyLag(timedelta(0)),
    "DFF": DailyLag(timedelta(days=1)),
    "CPIAUCSL": MonthlyRelease("CPI"),
}


class FredCollector:
    feed = FEED

    def __init__(
        self,
        series: Mapping[str, KnowledgeRule] = FRED_SERIES,
        http: HttpClient | None = None,
    ) -> None:
        self._series = dict(series)
        self._http = http if http is not None else HttpClient(BASE_URL, limiter=RateLimiter(2, 1.0))

    def fetch(self, start: datetime, end: datetime) -> Iterator[Any]:
        """One record per CSV row dated inside ``[start, end]``; the endpoint returns it all."""
        first, last = utc(start).date(), utc(end).date()
        for series in self._series:
            text = self._http.get_text("/graph/fredgraph.csv", {"id": series})
            for row in csv.DictReader(io.StringIO(text)):
                day = date.fromisoformat(row["observation_date"])
                if first <= day <= last:
                    yield {"series": series, "date": row["observation_date"], "value": row[series]}

    def normalize(self, raw: Any, fetched_at: datetime) -> list[Observation]:
        series = str(raw["series"])
        text = str(raw["value"]).strip()
        if text in MISSING:
            return []
        try:
            value = float(text)
        except ValueError as exc:
            raise ValidationError(f"{series}: non-numeric value {text!r}") from exc
        day = date.fromisoformat(raw["date"])
        event = datetime.combine(day, time(), tzinfo=UTC)
        known, quality, attrs = self._series[series].knowledge(day)
        if known > utc(fetched_at):
            return []
        return [
            Observation(
                feed=FEED,
                series_id=series,
                event_time=event,
                knowledge_time=known,
                value=value,
                kt_quality=quality,
                attrs=attrs,
            )
        ]
