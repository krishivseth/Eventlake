"""Kalshi prediction-market collector: daily YES-price candles for a fixed set of series."""

from collections.abc import Iterator, Sequence
from datetime import UTC, datetime
from typing import Any

from eventlake.entities import Market
from eventlake.http import HttpClient, RateLimiter
from eventlake.types import Observation, ValidationError, parse_utc, utc

BASE_URL = "https://api.elections.kalshi.com/trade-api/v2"
FEED = "kalshi"
DAY_MINUTES = 1440
PAGE_LIMIT = 1000

# Fed rate decision (one market family per meeting), monthly CPI, NYC daily high temperature.
KALSHI_SERIES: tuple[str, ...] = ("KXFEDDECISION", "KXCPI", "KXHIGHNY")


def _market(raw: dict[str, Any], series: str) -> Market:
    ticker = str(raw["ticker"])
    return Market(
        market_id=f"{FEED}:{ticker}",
        venue="kalshi",
        venue_ticker=ticker,
        series_ticker=series,
        title=str(raw["title"]),
        close_time=parse_utc(raw["close_time"]),
        attrs={
            "event_ticker": raw.get("event_ticker"),
            "open_time": raw.get("open_time"),
            "status": raw.get("status"),
        },
    )


class KalshiCollector:
    feed = FEED

    def __init__(
        self, series: Sequence[str] = KALSHI_SERIES, http: HttpClient | None = None
    ) -> None:
        self._series = tuple(series)
        self._http = (
            http if http is not None else HttpClient(BASE_URL, limiter=RateLimiter(10, 1.0))
        )

    def _iter_markets(self, series: str, min_close: datetime) -> Iterator[dict[str, Any]]:
        """Every market of ``series`` closing at or after ``min_close``, following the cursor."""
        cursor = ""
        while True:
            params: dict[str, Any] = {
                "series_ticker": series,
                "min_close_ts": int(utc(min_close).timestamp()),
                "limit": PAGE_LIMIT,
            }
            if cursor:
                params["cursor"] = cursor
            page = self._http.get_json("/markets", params)
            yield from page.get("markets", [])
            cursor = page.get("cursor") or ""
            if not cursor:
                return

    def list_markets(self, start: datetime, end: datetime) -> list[Market]:
        """Markets closing in ``[start, end]``, plus open ones closing after ``end``."""
        start, end = utc(start), utc(end)
        out: list[Market] = []
        for series in self._series:
            for raw in self._iter_markets(series, start):
                try:
                    market = _market(raw, series)
                except (KeyError, TypeError, ValueError):
                    continue  # one malformed entry must not abort the listing
                if market.close_time < start:
                    continue
                if market.close_time <= end or raw.get("status") in ("active", "open"):
                    out.append(market)
        return out

    def fetch(self, start: datetime, end: datetime) -> Iterator[Any]:
        """One record per daily candle of every market whose life overlaps the window."""
        start, end = utc(start), utc(end)
        for series in self._series:
            for raw in self._iter_markets(series, start):
                opened = parse_utc(raw["open_time"]) if raw.get("open_time") else start
                if opened > end:
                    continue
                ticker = raw["ticker"]
                body = self._http.get_json(
                    f"/series/{series}/markets/{ticker}/candlesticks",
                    {
                        "start_ts": int(max(start, opened).timestamp()),
                        "end_ts": int(end.timestamp()),
                        "period_interval": DAY_MINUTES,
                    },
                )
                for candle in body.get("candlesticks", []):
                    yield {"series": series, "ticker": ticker, "candle": candle}

    def normalize(self, raw: Any, fetched_at: datetime) -> list[Observation]:
        candle = raw["candle"]
        close = candle["price"].get("close_dollars")
        if close is None:
            return []  # nothing traded in this period
        probability = float(close)
        if not 0.0 <= probability <= 1.0:
            raise ValidationError(f"{raw['ticker']}: probability {probability} outside [0, 1]")
        at = datetime.fromtimestamp(int(candle["end_period_ts"]), UTC)
        if at > utc(fetched_at):
            return []  # the period has not ended, so there is no observation yet
        return [
            Observation(
                feed=FEED,
                series_id=str(raw["ticker"]),
                event_time=at,
                knowledge_time=at,
                value=probability,
                kt_quality="source",
                attrs={"series": raw["series"]},
            )
        ]
