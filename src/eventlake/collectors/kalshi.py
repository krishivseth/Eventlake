"""Kalshi prediction-market collector: daily YES-price candles for a fixed set of series."""

from collections.abc import Callable, Iterator, Sequence
from datetime import UTC, datetime, timedelta
from typing import Any

from eventlake.entities import Market
from eventlake.http import HttpClient, HttpError, RateLimiter
from eventlake.types import Observation, ValidationError, parse_utc, utc

BASE_URL = "https://api.elections.kalshi.com/trade-api/v2"
FEED = "kalshi"
DAY_MINUTES = 1440
PAGE_LIMIT = 1000
# The historical cutoff trails the present by weeks; a window starting later than this is live-only.
_RECENT = timedelta(days=30)

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
        self,
        series: Sequence[str] = KALSHI_SERIES,
        http: HttpClient | None = None,
        *,
        clock: Callable[[], datetime] = lambda: datetime.now(UTC),
    ) -> None:
        self._series = tuple(series)
        self._clock = clock
        self._http = (
            http if http is not None else HttpClient(BASE_URL, limiter=RateLimiter(10, 1.0))
        )

    def _pages(self, path: str, params: dict[str, Any]) -> Iterator[dict[str, Any]]:
        """Every market of a listing endpoint, following the cursor."""
        cursor = ""
        while True:
            page_params = dict(params, **({"cursor": cursor} if cursor else {}))
            page = self._http.get_json(path, page_params)
            yield from page.get("markets", [])
            cursor = page.get("cursor") or ""
            if not cursor:
                return

    def iter_markets(
        self, series: str, min_close: datetime
    ) -> Iterator[tuple[dict[str, Any], bool]]:
        """Markets of ``series`` closing at or after ``min_close`` as ``(raw, is_live)``.

        Settled markets older than the exchange's historical cutoff are served only by the
        ``/historical`` endpoints, so those are listed too when the window reaches back past
        the cutoff. A ticker listed live is never repeated from the historical listing.
        """
        seen: set[str] = set()
        params: dict[str, Any] = {
            "series_ticker": series,
            "min_close_ts": int(utc(min_close).timestamp()),
            "limit": PAGE_LIMIT,
        }
        for raw in self._pages("/markets", params):
            seen.add(str(raw.get("ticker")))
            yield raw, True
        try:
            cutoff = parse_utc(self._http.get_json("/historical/cutoff")["market_settled_ts"])
        except HttpError:
            if utc(min_close) >= utc(self._clock()) - _RECENT:
                return  # far newer than any cutoff: the live listing is complete
            raise  # the window may need history, so do not return a partial listing
        if utc(min_close) >= cutoff:
            return
        for raw in self._pages(
            "/historical/markets", {"series_ticker": series, "limit": PAGE_LIMIT}
        ):
            try:
                closes = parse_utc(raw["close_time"])
            except (KeyError, TypeError, ValueError):
                continue  # one malformed entry must not abort the listing
            if closes >= utc(min_close) and str(raw.get("ticker")) not in seen:
                yield raw, False

    def list_markets(self, start: datetime, end: datetime) -> list[Market]:
        """Markets closing in ``[start, end]``, plus open ones closing after ``end``."""
        start, end = utc(start), utc(end)
        out: list[Market] = []
        for series in self._series:
            for raw, _live in self.iter_markets(series, start):
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
            for raw, live in self.iter_markets(series, start):
                opened = parse_utc(raw["open_time"]) if raw.get("open_time") else start
                if opened > end:
                    continue
                for candle in self.candles(series, raw, live, max(start, opened), end):
                    yield {"series": series, "ticker": raw["ticker"], "candle": candle}

    def candles(
        self, series: str, raw: dict[str, Any], live: bool, start: datetime, end: datetime
    ) -> list[dict[str, Any]]:
        """Daily candles of one market from the live or the historical endpoint."""
        ticker = raw["ticker"]
        path = (
            f"/series/{series}/markets/{ticker}/candlesticks"
            if live
            else f"/historical/markets/{ticker}/candlesticks"
        )
        body = self._http.get_json(
            path,
            {
                "start_ts": int(utc(start).timestamp()),
                "end_ts": int(utc(end).timestamp()),
                "period_interval": DAY_MINUTES,
            },
        )
        found: list[dict[str, Any]] = body.get("candlesticks", [])
        return found

    def normalize(self, raw: Any, fetched_at: datetime) -> list[Observation]:
        candle = raw["candle"]
        price = candle["price"]
        # Markets settled before the historical cutoff report ``close``, not ``close_dollars``.
        close = price.get("close_dollars", price.get("close"))
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
