"""Tradier options collector: at-the-money implied volatility per underlying and expiry."""

import os
from collections.abc import Callable, Iterator, Sequence
from datetime import UTC, date, datetime, timedelta
from typing import Any, Literal

from eventlake.http import HttpClient, RateLimiter
from eventlake.types import Observation, ValidationError, utc

DEFAULT_BASE = "https://sandbox.tradier.com/v1"
FEED = "tradier.options"


def occ_symbol(underlying: str, expiry: date, right: Literal["C", "P"], strike: float) -> str:
    """OCC option symbol, e.g. ``SPY260130C00601000``."""
    return f"{underlying}{expiry:%y%m%d}{right}{round(strike * 1000):08d}"


def _as_list(value: Any) -> list[Any]:
    """Tradier returns a bare object for one item, a list for several and null for none."""
    if value is None:
        return []
    return list(value) if isinstance(value, list) else [value]


def _quotes(body: Any) -> list[dict[str, Any]]:
    container = body.get("quotes") if isinstance(body, dict) else None
    if not isinstance(container, dict):
        return []
    return [q for q in _as_list(container.get("quote")) if isinstance(q, dict)]


def _mid(quote: dict[str, Any]) -> float:
    return (float(quote["bid"]) + float(quote["ask"])) / 2


def _mid_iv(quote: dict[str, Any]) -> float | None:
    greeks = quote.get("greeks")
    iv = greeks.get("mid_iv") if isinstance(greeks, dict) else None
    return float(iv) if iv is not None and float(iv) > 0 else None


class TradierOptionsCollector:
    feed = FEED

    def __init__(
        self,
        underlyings: Sequence[str] = ("SPY",),
        n_expiries: int = 6,
        http: HttpClient | None = None,
        *,
        clock: Callable[[], datetime] = lambda: datetime.now(UTC),
    ) -> None:
        """``TRADIER_TOKEN`` and ``TRADIER_BASE`` are read here, not at import.

        The token is only required when no ``http`` client is supplied.
        """
        self._underlyings = tuple(underlyings)
        self._n_expiries = n_expiries
        self._clock = clock
        if http is None:
            token = os.environ.get("TRADIER_TOKEN")
            if not token:
                raise RuntimeError("TRADIER_TOKEN is required to query the Tradier API")
            http = HttpClient(
                os.environ.get("TRADIER_BASE", DEFAULT_BASE),
                headers={"Authorization": f"Bearer {token}", "Accept": "application/json"},
                limiter=RateLimiter(120, 60.0),
            )
        self._http = http

    def fetch(self, start: datetime, end: datetime) -> Iterator[Any]:
        """A snapshot of the chain now, one record per underlying and expiry.

        The window is ignored, except that nothing is yielded when ``end`` is earlier than the
        current time minus one day, since a past window cannot be answered from live quotes.
        """
        start, end = utc(start), utc(end)
        now = utc(self._clock())
        if end < now - timedelta(days=1):
            return
        for underlying in self._underlyings:
            spot = self._spot(underlying)
            if spot is None:
                continue
            for expiry in self._expiries(underlying, now.date()):
                yield self._record(underlying, expiry, spot)

    def _spot(self, underlying: str) -> float | None:
        for q in _quotes(self._http.get_json("/markets/quotes", {"symbols": underlying})):
            price = q.get("last") if q.get("last") is not None else q.get("close")
            if price is not None:
                return float(price)
        return None

    def _expiries(self, underlying: str, today: date) -> list[date]:
        body = self._http.get_json("/markets/options/expirations", {"symbol": underlying})
        container = body.get("expirations") if isinstance(body, dict) else None
        dates = _as_list(container.get("date")) if isinstance(container, dict) else []
        upcoming = sorted(d for d in map(date.fromisoformat, dates) if d >= today)
        return upcoming[: self._n_expiries]

    def _strikes(self, underlying: str, expiry: date) -> list[float]:
        body = self._http.get_json(
            "/markets/options/strikes",
            {"symbol": underlying, "expiration": expiry.isoformat()},
        )
        container = body.get("strikes") if isinstance(body, dict) else None
        return [float(s) for s in _as_list(container.get("strike"))] if container else []

    def _record(self, underlying: str, expiry: date, spot: float) -> dict[str, Any]:
        record: dict[str, Any] = {
            "underlying": underlying,
            "expiry": expiry.isoformat(),
            "spot": spot,
            "strike": None,
            "call": None,
            "put": None,
        }
        strikes = self._strikes(underlying, expiry)
        if not strikes:
            return record
        strike = min(strikes, key=lambda s: abs(s - spot))
        record["strike"] = strike
        symbols = {
            "call": occ_symbol(underlying, expiry, "C", strike),
            "put": occ_symbol(underlying, expiry, "P", strike),
        }
        body = self._http.get_json(
            "/markets/quotes", {"symbols": ",".join(symbols.values()), "greeks": "true"}
        )
        for q in _quotes(body):
            for side, symbol in symbols.items():
                if q.get("symbol") == symbol:
                    record[side] = q
        return record

    def normalize(self, raw: Any, fetched_at: datetime) -> list[Observation]:
        series = f"{raw['underlying']}:{raw['expiry']}:atm_iv"
        call, put = raw.get("call"), raw.get("put")
        if not isinstance(call, dict) or not isinstance(put, dict):
            raise ValidationError(f"{series}: call or put quote missing")
        call_iv, put_iv = _mid_iv(call), _mid_iv(put)
        if call_iv is None or put_iv is None:
            raise ValidationError(f"{series}: mid_iv missing or not positive")
        try:
            straddle = _mid(call) + _mid(put)
        except (KeyError, TypeError, ValueError) as exc:
            raise ValidationError(f"{series}: bid/ask missing") from exc
        at = utc(fetched_at)
        return [
            Observation(
                feed=FEED,
                series_id=series,
                event_time=at,
                knowledge_time=at,
                value=(call_iv + put_iv) / 2,
                kt_quality="observed",
                attrs={
                    "spot": float(raw["spot"]),
                    "strike": float(raw["strike"]),
                    "straddle_mid": straddle,
                },
            )
        ]
