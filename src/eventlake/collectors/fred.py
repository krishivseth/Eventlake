"""FRED collector: daily macro series from the keyless CSV endpoint."""

import csv
import io
from collections.abc import Iterator, Mapping
from datetime import UTC, date, datetime, time, timedelta
from typing import Any

from eventlake.http import HttpClient, RateLimiter
from eventlake.types import Observation, ValidationError, utc

BASE_URL = "https://fred.stlouisfed.org"
FEED = "fred"
MISSING = frozenset({"", "."})  # the live CSV leaves missing values empty; "." is the legacy marker

# Extra delay after the day following the observation date before the value is assumed known.
FRED_SERIES: Mapping[str, timedelta] = {
    "SP500": timedelta(0),
    "DFF": timedelta(days=1),
    "CPIAUCSL": timedelta(days=14),
}


class FredCollector:
    feed = FEED

    def __init__(
        self,
        series: Mapping[str, timedelta] = FRED_SERIES,
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
        event = datetime.combine(date.fromisoformat(raw["date"]), time(), tzinfo=UTC)
        known = event + timedelta(days=1) + self._series[series]
        if known > utc(fetched_at):
            return []
        return [
            Observation(
                feed=FEED,
                series_id=series,
                event_time=event,
                knowledge_time=known,
                value=value,
                kt_quality="assumed",
            )
        ]
