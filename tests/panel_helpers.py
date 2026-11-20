import math
from datetime import UTC, date, datetime, time, timedelta

from eventlake.calendar import Event, upsert_events
from eventlake.entities import Market, upsert_markets
from eventlake.store import Store
from eventlake.types import Observation

FOMC1 = Event("FOMC:2026-01-28", "FOMC", datetime(2026, 1, 28, 19, 0, tzinfo=UTC), {})
FOMC2 = Event("FOMC:2026-03-18", "FOMC", datetime(2026, 3, 18, 18, 0, tzinfo=UTC), {})
CPI = Event("CPI:2026-02-11", "CPI", datetime(2026, 2, 11, 13, 30, tzinfo=UTC), {})
DIFFUSE, MOVE = 0.15, 0.012


def _utc(y: int, m: int, d: int, h: int = 0) -> datetime:
    return datetime(y, m, d, h, tzinfo=UTC)


def _iv(expiry: date, at: datetime) -> float:
    close = datetime.combine(expiry, time(21, 0), tzinfo=UTC)
    t = (close - at).total_seconds() / (365 * 86400)
    return math.sqrt((DIFFUSE**2 * t + MOVE**2) / t)


def _iv_rows(front: date, back: date, at: datetime) -> list[Observation]:
    return [
        Observation("tradier.options", f"SPY:{e:%Y-%m-%d}:atm_iv", at, at, _iv(e, at))
        for e in (front, back)
    ]


def _kalshi(ticker: str, close: datetime) -> Market:
    return Market(f"kalshi:{ticker}", "kalshi", ticker, "KXFEDDECISION", ticker, close, {})


def _poly(slug: str, token: str, close: datetime) -> Market:
    return Market(
        f"polymarket:{slug}", "polymarket", slug, "fed-decision", slug, close,
        {"yes_token_id": token},
    )  # fmt: skip


def _prob(feed: str, sid: str, day: datetime, p: float) -> Observation:
    return Observation(feed, sid, day, day, p, "source")


def _close(day: datetime, value: float, known: datetime | None = None) -> Observation:
    return Observation("fred", "SP500", day, known or day + timedelta(days=1), value)


def _link(store: Store, event_id: str, market_id: str) -> None:
    store.execute(
        "INSERT INTO links (event_id, market_id, rule, linked_at) VALUES (?,?,?,?)",
        [event_id, market_id, "test", _utc(2026, 1, 1)],
    )


def _build() -> Store:
    store = Store()
    upsert_events(store, [FOMC1, FOMC2, CPI])
    upsert_markets(
        store,
        [
            _kalshi("KXFED-26JAN-HOLD", _utc(2026, 1, 28, 19)),
            _poly("fed-jan-cut", "tok-cut", _utc(2026, 1, 28, 19)),
            _kalshi("KXFED-26MAR-HOLD", _utc(2026, 3, 18, 18)),
            _poly("fed-mar-cut", "tok-mar", _utc(2026, 3, 18, 18)),
        ],
    )
    _link(store, FOMC1.event_id, "kalshi:KXFED-26JAN-HOLD")
    _link(store, FOMC1.event_id, "polymarket:fed-jan-cut")
    _link(store, FOMC2.event_id, "kalshi:KXFED-26MAR-HOLD")
    _link(store, FOMC2.event_id, "polymarket:fed-mar-cut")
    obs: list[Observation] = []
    for d in (26, 27):
        day = _utc(2026, 1, d)
        obs.append(_prob("kalshi", "KXFED-26JAN-HOLD", day, 0.71))
        obs.append(_prob("polymarket", "tok-cut", day, 0.22))
    for d in (25, 26, 27, 29):
        obs.append(_close(_utc(2026, 1, d), 5000.0 + d))
    obs.append(_close(_utc(2026, 1, 28), 5100.0))
    obs += _iv_rows(date(2026, 1, 30), date(2026, 2, 13), _utc(2026, 1, 26, 21))
    obs += _iv_rows(date(2026, 3, 20), date(2026, 4, 3), _utc(2026, 3, 16, 21))
    obs.append(_prob("kalshi", "KXFED-26MAR-HOLD", _utc(2026, 3, 16), 0.5))
    obs.append(_prob("polymarket", "tok-mar", _utc(2026, 3, 16), 0.4))
    store.append(obs)
    return store
