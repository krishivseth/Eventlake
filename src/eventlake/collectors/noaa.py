"""NOAA collector: daily maximum temperature from the NCEI Access Data Service."""

from collections.abc import Iterator, Sequence
from datetime import UTC, date, datetime, time, timedelta
from typing import Any

from eventlake.http import HttpClient, RateLimiter
from eventlake.types import Observation, ValidationError, utc

BASE_URL = "https://www.ncei.noaa.gov"
FEED = "noaa"
TMAX_RANGE_F = (-80.0, 140.0)

NOAA_STATIONS: tuple[str, ...] = ("USW00094728",)  # New York Central Park


class NoaaCollector:
    feed = FEED

    def __init__(
        self, stations: Sequence[str] = NOAA_STATIONS, http: HttpClient | None = None
    ) -> None:
        self._stations = tuple(stations)
        self._http = http if http is not None else HttpClient(BASE_URL, limiter=RateLimiter(2, 1.0))

    def fetch(self, start: datetime, end: datetime) -> Iterator[Any]:
        for station in self._stations:
            rows = self._http.get_json(
                "/access/services/data/v1",
                {
                    "dataset": "daily-summaries",
                    "stations": station,
                    "startDate": utc(start).date().isoformat(),
                    "endDate": utc(end).date().isoformat(),
                    "dataTypes": "TMAX",
                    "units": "standard",
                    "format": "json",
                },
            )
            yield from rows

    def normalize(self, raw: Any, fetched_at: datetime) -> list[Observation]:
        if "TMAX" not in raw:
            return []  # the station did not report that day
        station = str(raw.get("STATION", ""))
        try:
            tmax = float(raw["TMAX"])
            day = date.fromisoformat(raw["DATE"])
        except (KeyError, TypeError, ValueError) as exc:
            raise ValidationError(f"{station}: malformed TMAX/DATE field ({exc!r})") from exc
        low, high = TMAX_RANGE_F
        if not low <= tmax <= high:
            raise ValidationError(f"{station}: TMAX {tmax} outside [{low}, {high}] F")
        event = datetime.combine(day, time(), tzinfo=UTC)
        known = event + timedelta(days=1, hours=12)
        if known > utc(fetched_at):
            return []
        return [
            Observation(
                feed=FEED,
                series_id=f"{station}:TMAX",
                event_time=event,
                knowledge_time=known,
                value=tmax,
                kt_quality="assumed",
            )
        ]
