"""The ``eventlake demo`` pipeline: replay recorded public data, then show the panel two ways.

Offline mode replays raw vendor records saved under ``demo_data/`` through the real
collectors' ``normalize`` and the real runner. Option implied vols are an illustrative
synthetic sample (see ``demo_data/synthetic/README.md``) and are always labelled as such.
"""

import csv
import json
from collections.abc import Callable, Iterator, Sequence
from dataclasses import dataclass
from datetime import datetime, timedelta
from importlib import resources
from typing import Any

from eventlake.calendar import Event, EventKind, load_events, upsert_events
from eventlake.collectors import REGISTRY
from eventlake.collectors.kalshi import KalshiCollector
from eventlake.collectors.polymarket import GAMMA_EVENTS, PolymarketCollector
from eventlake.entities import Market, link_markets, upsert_markets
from eventlake.health import POLICIES, FeedStatus, check_feed
from eventlake.panel import PanelRow, eve_panel, event_panel
from eventlake.runner import Collector, run_collector
from eventlake.store import Store
from eventlake.types import Observation, parse_utc, utc

IMPLIED_HEADER = "implied move (synthetic sample)"
_OPTIONS_FEED = "tradier.options"
_UNKNOWN = "—"
_KINDS: tuple[EventKind, ...] = ("FOMC", "CPI")
_LIVE_DAYS = 120
_MIN_PER_KIND = 4


def _data(name: str) -> Any:
    return json.loads(resources.files("eventlake").joinpath("demo_data", name).read_text("utf-8"))


def manifest() -> dict[str, Any]:
    data: dict[str, Any] = _data("manifest.json")
    return data


def default_now() -> datetime:
    return parse_utc(manifest()["default_now"])


class ReplayCollector:
    """Serves saved raw records under the real collector's feed name and ``normalize``."""

    def __init__(self, real: Collector, records: Sequence[Any]) -> None:
        self.feed = real.feed
        self._real = real
        self._records = list(records)

    def fetch(self, start: datetime, end: datetime) -> Iterator[Any]:
        yield from self._records

    def normalize(self, raw: Any, fetched_at: datetime) -> list[Observation]:
        return self._real.normalize(raw, fetched_at)


class SyntheticIvCollector:
    """Loads the illustrative IV sample as ``tradier.options`` observations, kt 'assumed'."""

    feed = _OPTIONS_FEED

    def fetch(self, start: datetime, end: datetime) -> Iterator[Any]:
        text = resources.files("eventlake").joinpath("demo_data", "synthetic", "iv_sample.csv")
        yield from csv.DictReader(text.read_text("utf-8").splitlines())

    def normalize(self, raw: Any, fetched_at: datetime) -> list[Observation]:
        at = parse_utc(raw["observed_at"])
        return [
            Observation(
                feed=_OPTIONS_FEED,
                series_id=f"{raw['underlying']}:{raw['expiry']}:atm_iv",
                event_time=at,
                knowledge_time=at,
                value=float(raw["atm_iv"]),
                kt_quality="assumed",
                attrs={"synthetic": True},
            )
        ]


class _SavedHttp:
    """Answers the listing calls of the real collectors from saved responses; no network."""

    def __init__(self, kalshi: dict[str, list[Any]] | None, polymarket: list[Any] | None) -> None:
        self._kalshi = kalshi or {}
        self._polymarket = polymarket or []

    def get_json(self, path: str, params: dict[str, Any] | None = None) -> Any:
        params = params or {}
        if path == GAMMA_EVENTS:
            first_closed_page = params.get("closed") == "true" and params.get("offset") == 0
            return self._polymarket if first_closed_page else []
        return {"markets": self._kalshi.get(params.get("series_ticker", ""), [])}


@dataclass(frozen=True)
class Source:
    collector: Collector
    markets: list[Market]


def _offline_sources(start: datetime, now: datetime) -> list[Source]:
    k_markets = _data("kalshi_markets.json")
    p_events = _data("polymarket_events.json")
    kalshi_list = KalshiCollector(http=_SavedHttp(k_markets, None))  # type: ignore[arg-type]
    poly_list = PolymarketCollector(http=_SavedHttp(None, p_events))  # type: ignore[arg-type]
    replays: list[tuple[Collector, str]] = [
        (KalshiCollector(), "kalshi_candles.json"),
        (PolymarketCollector(), "polymarket_history.json"),
        (REGISTRY["fred"](), "fred_sp500.json"),
        (REGISTRY["noaa"](), "noaa_tmax.json"),
    ]
    listed = {
        "kalshi": kalshi_list.list_markets(start, now),
        "polymarket": poly_list.list_markets(start, now),
    }
    return [
        Source(ReplayCollector(real, _data(name)), listed.get(real.feed, []))
        for real, name in replays
    ]


def _live_sources(start: datetime, now: datetime) -> list[Source]:
    out: list[Source] = []
    for feed in ("kalshi", "polymarket", "fred", "noaa"):
        collector = REGISTRY[feed]()
        lister: Any = getattr(collector, "list_markets", None)
        out.append(Source(collector, lister(start, now) if lister else []))
    return out


def build_store(offline: bool, now: datetime) -> tuple[Store, list[Event]]:
    """A fresh in-memory store loaded through the real runner; returns it with its events."""
    now = utc(now)
    if offline:
        wanted = set(manifest()["events"])
        events = [e for e in load_events() if e.event_id in wanted]
        start = min(e.scheduled_time for e in events) - timedelta(days=30)
        sources = _offline_sources(start, now)
    else:
        start = now - timedelta(days=_LIVE_DAYS)
        events = [e for e in load_events(_KINDS) if start <= e.scheduled_time <= now]
        sources = _live_sources(start, now)
    store = Store()
    upsert_events(store, events)
    for source in sources:
        upsert_markets(store, source.markets)
    link_markets(store, now=now)
    clock: Callable[[], datetime] = lambda: now  # noqa: E731
    first = start - timedelta(days=365)
    collectors: list[Collector] = [s.collector for s in sources]
    if offline:
        collectors.append(SyntheticIvCollector())
    for collector in collectors:
        run_collector(collector, store, first, now, now=clock)
    return store, events


def pct(value: float | None, places: int) -> str:
    return _UNKNOWN if value is None else f"{value * 100:.{places}f}%"


def _stamp(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%dT%H:%MZ")


def _short(market_id: str | None, width: int = 50) -> str:
    if market_id is None:
        return _UNKNOWN
    return market_id if len(market_id) <= width else market_id[: width - 1] + "…"


def _panel_cells(rows: Sequence[PanelRow]) -> tuple[list[str], list[list[str]]]:
    headers = ["event", "as of (UTC)", "top market", "P(yes)", IMPLIED_HEADER, "realised move"]
    body = [
        [
            r.event_id,
            _stamp(r.as_of),
            _short(r.market_id),
            pct(r.market_prob, 1),
            pct(r.implied_move, 2),
            pct(r.realized_move, 2),
        ]
        for r in rows
    ]
    return headers, body


def _changes(eve: Sequence[PanelRow], now: Sequence[PanelRow]) -> tuple[int, int]:
    """Cells (market prob, implied move, realised move) that went empty->filled, filled->empty."""
    filled = emptied = 0
    now_by_id = {r.event_id: r for r in now}
    for before in eve:
        after = now_by_id[before.event_id]
        for a, b in (
            (before.market_prob, after.market_prob),
            (before.implied_move, after.implied_move),
            (before.realized_move, after.realized_move),
        ):
            filled += a is None and b is not None
            emptied += a is not None and b is None
    return filled, emptied


def eve_view_is_stable(store: Store, eve: Sequence[PanelRow]) -> bool:
    """True if re-reading each eve row at its own ``as_of`` through ``event_panel`` agrees."""
    for row in eve:
        again = [
            r for r in event_panel(store, row.as_of, (row.kind,)) if r.event_id == row.event_id
        ]
        if again != [row]:
            return False
    return True


def demo_health(store: Store, now: datetime, offline: bool) -> list[FeedStatus]:
    feeds = ["fred", "kalshi", "noaa", "polymarket"] + ([_OPTIONS_FEED] if offline else [])
    return [check_feed(store, POLICIES[f], now) for f in sorted(feeds)]


def render_demo(
    offline: bool, now: datetime, table: Callable[..., str], health: Callable[..., str]
) -> str:
    store, events = build_store(offline, now)
    try:
        eve = eve_panel(store, until=now)
        later = event_panel(store, now)
        statuses = demo_health(store, now, offline)
        stable = eve_view_is_stable(store, eve)
    finally:
        store.close()
    out: list[str] = []
    if offline:
        meta = manifest()
        out += [
            "eventlake offline demo",
            f"Replaying public data recorded {meta['recorded']} (Kalshi, Polymarket, FRED SP500,"
            " NOAA) through the real collectors and runner. No network is used.",
            f"Demo clock fixed at {_stamp(now)}.",
        ]
    else:
        out += ["eventlake demo (live data)", f"Clock {_stamp(now)}."]
    out.append("")
    out.append("1. As known the day before each event")
    out.append(table(*_panel_cells(eve)))
    if offline:
        out.append(
            f"The '{IMPLIED_HEADER}' column is computed by the real solver from illustrative"
            " inputs; the other columns come from recorded public data."
        )
    else:
        out.append(f"No option history is available here, so '{IMPLIED_HEADER}' stays empty.")
    for kind in _KINDS:
        priced = sum(1 for r in eve if r.kind == kind and r.market_prob is not None)
        if priced < _MIN_PER_KIND:
            out.append(f"Note: only {priced} {kind} events here have a recorded market price.")
    out += ["", "2. Same events, as known now", table(*_panel_cells(later))]
    filled, emptied = _changes(eve, later)
    realised = sum(
        1
        for a in eve
        for b in later
        if a.event_id == b.event_id and a.realized_move is None and b.realized_move is not None
    )
    out += [
        "",
        "3. What changed",
        f"{filled} cells went from empty to filled once later data arrived"
        f" ({realised} are realised moves, known only after the close).",
        f"{emptied} cells went from filled to empty: an implied move is only defined before its"
        " event, and a market price older than 7 days counts as unknown.",
        "No value in table 1 changed: each row is read at its own as-of time, so later"
        " data cannot enter it."
        if stable
        else "WARNING: a value in table 1 differs when re-read at its as-of time.",
        "",
        "Feed health" + (" (runs are stamped with the demo clock)" if offline else ""),
        health(statuses),
    ]
    if offline:
        out += [
            "",
            f"{_OPTIONS_FEED} is the synthetic sample; cftc and edgar are not part of the demo.",
            "A noaa gap warning is genuine: the recorded summaries trail the demo clock.",
        ]
    return "\n".join(out)
