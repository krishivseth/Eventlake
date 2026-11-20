"""Prediction-market entities stored alongside events."""

import json
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from typing import Any, Literal, cast
from zoneinfo import ZoneInfo

from eventlake.calendar import Event, EventKind, upsert_events, weather_event
from eventlake.store import Store
from eventlake.types import utc

_NY = ZoneInfo("America/New_York")
WEATHER_STATION = "USW00094728"  # Central Park, the settlement station for NYC daily highs


@dataclass(frozen=True)
class Market:
    market_id: str  # f"{venue}:{venue_ticker}"
    venue: Literal["kalshi", "polymarket"]
    venue_ticker: str
    series_ticker: str
    title: str
    close_time: datetime
    attrs: Mapping[str, Any]


def upsert_markets(store: Store, markets: Iterable[Market]) -> int:
    """Insert markets whose id is not stored yet; existing ids are left untouched."""
    inserted = 0
    for m in markets:
        if store.execute("SELECT 1 FROM markets WHERE market_id = ?", [m.market_id]):
            continue
        store.execute(
            "INSERT INTO markets (market_id, venue, venue_ticker, series_ticker, title,"
            " close_time, attrs) VALUES (?,?,?,?,?,?,?)",
            [
                m.market_id,
                m.venue,
                m.venue_ticker,
                m.series_ticker,
                m.title,
                utc(m.close_time),
                json.dumps(dict(m.attrs), sort_keys=True),
            ],
        )
        inserted += 1
    return inserted


@dataclass(frozen=True)
class LinkRule:
    venue: str
    series_ticker: str
    kind: EventKind
    tolerance: timedelta

    @property
    def name(self) -> str:
        return f"{self.venue}:{self.series_ticker}"


_DECISION_TOLERANCE = timedelta(hours=36)
_WEATHER_TOLERANCE = timedelta(hours=24)

LINK_RULES: tuple[LinkRule, ...] = (
    LinkRule("kalshi", "KXFEDDECISION", "FOMC", _DECISION_TOLERANCE),
    LinkRule("kalshi", "KXCPI", "CPI", _DECISION_TOLERANCE),
    LinkRule("kalshi", "KXHIGHNY", "WEATHER_HIGH", _WEATHER_TOLERANCE),
    LinkRule("polymarket", "fed-decision", "FOMC", _DECISION_TOLERANCE),
)


def _row_to_market(row: tuple[Any, ...]) -> Market:
    market_id, venue, venue_ticker, series_ticker, title, close_time, attrs = row
    return Market(
        market_id=market_id,
        venue=venue,
        venue_ticker=venue_ticker,
        series_ticker=series_ticker,
        title=title,
        close_time=close_time,
        attrs=json.loads(attrs) if attrs else {},
    )


_MARKET_COLUMNS = (
    "m.market_id, m.venue, m.venue_ticker, m.series_ticker, m.title, m.close_time, m.attrs"
)


def markets_for_event(store: Store, event_id: str) -> list[Market]:
    rows = store.execute(
        f"SELECT {_MARKET_COLUMNS} FROM markets m JOIN links l ON l.market_id = m.market_id"
        " WHERE l.event_id = ? ORDER BY m.market_id",
        [event_id],
    )
    return [_row_to_market(r) for r in rows]


def unlinked_markets(store: Store) -> list[Market]:
    rows = store.execute(
        f"SELECT {_MARKET_COLUMNS} FROM markets m"
        " WHERE NOT EXISTS (SELECT 1 FROM links l WHERE l.market_id = m.market_id)"
        " ORDER BY m.market_id"
    )
    return [_row_to_market(r) for r in rows]


def _weather_day(close_time: datetime) -> date:
    """The New York settlement day of a daily-high market.

    These markets stop trading shortly after local midnight ending their day, so stepping back
    twelve hours lands in the middle of that day whatever the UTC offset.
    """
    return (utc(close_time) - timedelta(hours=12)).astimezone(_NY).date()


def _candidate_events(store: Store, rule: LinkRule, market: Market) -> list[Event]:
    if rule.kind == "WEATHER_HIGH":
        event = weather_event(WEATHER_STATION, _weather_day(market.close_time))
        return [event]
    rows = store.execute(
        "SELECT event_id, kind, scheduled_time FROM events WHERE kind = ?", [rule.kind]
    )
    return [Event(r[0], cast(EventKind, r[1]), r[2], {}) for r in rows]


def _closest_event(rule: LinkRule, market: Market, events: list[Event]) -> Event | None:
    close = utc(market.close_time)
    in_range = [e for e in events if abs(close - utc(e.scheduled_time)) <= rule.tolerance]
    if not in_range:
        return None
    return min(
        in_range, key=lambda e: (abs(close - utc(e.scheduled_time)), e.scheduled_time, e.event_id)
    )


def link_markets(store: Store, *, now: datetime) -> int:
    """Attach each unlinked market to the event its rule names; returns links inserted.

    Only insertion happens here: a market that already has a link is never moved, and a market
    with no matching rule or no event within tolerance stays unlinked. Calendar events must
    already be in the store, except daily-high events, which are created on demand.
    """
    linked_at = utc(now)
    rules = {(r.venue, r.series_ticker): r for r in LINK_RULES}
    inserted = 0
    for market in unlinked_markets(store):
        rule = rules.get((market.venue, market.series_ticker))
        if rule is None:
            continue
        event = _closest_event(rule, market, _candidate_events(store, rule, market))
        if event is None:
            continue
        if rule.kind == "WEATHER_HIGH":
            upsert_events(store, [event])
        store.execute(
            "INSERT INTO links (event_id, market_id, rule, linked_at) VALUES (?,?,?,?)",
            [event.event_id, market.market_id, rule.name, linked_at],
        )
        inserted += 1
    return inserted
