"""Prediction-market entities stored alongside events."""

import json
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Literal

from eventlake.store import Store
from eventlake.types import utc


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
