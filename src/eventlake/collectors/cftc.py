"""CFTC collector: Traders in Financial Futures positioning for the E-mini S&P 500."""

from collections.abc import Iterator, Sequence
from datetime import UTC, datetime, time, timedelta
from typing import Any
from zoneinfo import ZoneInfo

from eventlake.http import HttpClient, RateLimiter
from eventlake.types import Observation, ValidationError, parse_utc, utc

BASE_URL = "https://publicreporting.cftc.gov"
DATASET = "gpe5-46if"  # Traders in Financial Futures, futures only
FEED = "cftc"
PAGE_LIMIT = 5000
PUBLISH_TZ = ZoneInfo("America/New_York")
PUBLISH_TIME = time(15, 30)

CFTC_CONTRACTS: tuple[str, ...] = ("13874A",)  # E-MINI S&P 500
CFTC_FIELDS: tuple[str, ...] = (
    "asset_mgr_positions_long",
    "asset_mgr_positions_short",
    "lev_money_positions_long",
    "lev_money_positions_short",
)


def _publication_time(as_of: datetime) -> datetime:
    """First Friday on/after ``as_of`` + 3 days, 15:30 New York (holiday delays not modelled)."""
    earliest = as_of.date() + timedelta(days=3)
    friday = earliest + timedelta(days=(4 - earliest.weekday()) % 7)
    return datetime.combine(friday, PUBLISH_TIME, tzinfo=PUBLISH_TZ).astimezone(UTC)


class CftcCollector:
    feed = FEED

    def __init__(
        self, contracts: Sequence[str] = CFTC_CONTRACTS, http: HttpClient | None = None
    ) -> None:
        self._contracts = tuple(contracts)
        self._http = http if http is not None else HttpClient(BASE_URL, limiter=RateLimiter(2, 1.0))

    def fetch(self, start: datetime, end: datetime) -> Iterator[Any]:
        codes = ",".join(f"'{c}'" for c in self._contracts)
        lo = utc(start).strftime("%Y-%m-%dT00:00:00")
        hi = utc(end).strftime("%Y-%m-%dT00:00:00")
        where = (
            f"cftc_contract_market_code in({codes}) "
            f"AND report_date_as_yyyy_mm_dd between '{lo}' and '{hi}'"
        )
        yield from self._http.get_json(
            f"/resource/{DATASET}.json",
            {"$where": where, "$order": "report_date_as_yyyy_mm_dd", "$limit": PAGE_LIMIT},
        )

    def normalize(self, raw: Any, fetched_at: datetime) -> list[Observation]:
        code = str(raw.get("cftc_contract_market_code", "")).strip()
        try:
            event = parse_utc(raw["report_date_as_yyyy_mm_dd"] + "+00:00")
        except (KeyError, TypeError, ValueError) as exc:
            raise ValidationError(f"{code}: malformed report date ({exc!r})") from exc
        known = _publication_time(event)
        values: dict[str, float] = {}
        for name in CFTC_FIELDS:
            try:
                value = float(raw[name])
            except (KeyError, TypeError, ValueError) as exc:
                raise ValidationError(f"{code}:{name}: missing or non-numeric ({exc!r})") from exc
            if value < 0:
                raise ValidationError(f"{code}:{name}: negative position {value}")
            values[name] = value
        if known > utc(fetched_at):
            return []  # the report is not published yet
        return [
            Observation(
                feed=FEED,
                series_id=f"{code}:{name}",
                event_time=event,
                knowledge_time=known,
                value=value,
                kt_quality="source",
                attrs={"contract": str(raw.get("contract_market_name", ""))},
            )
            for name, value in values.items()
        ]
