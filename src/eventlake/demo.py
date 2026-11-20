"""The ``eventlake demo`` pipeline: replay recorded public data, then show the panel two ways.

Offline mode replays raw vendor records saved under ``demo_data/`` through the real
collectors' ``normalize`` and the real runner. Option implied vols are an illustrative
synthetic sample (see ``demo_data/synthetic/README.md``) and are always labelled as such.
"""

import csv
import json
import unicodedata
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
from eventlake.panel import PanelRow, eve_panel
from eventlake.runner import Collector, run_collector
from eventlake.store import Store
from eventlake.tables import format_table, render_health
from eventlake.types import Observation, parse_utc, utc

IMPLIED_HEADER = "implied move (synthetic sample)"
_OPTIONS_FEED = "tradier.options"
_UNKNOWN = "-"
_AFTER = timedelta(days=2)
_TITLE_WIDTH = 48
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
        if path == "/historical/cutoff":  # the saved listing already holds every market
            return {"market_settled_ts": "1970-01-01T00:00:00Z"}
        return {"markets": self._kalshi.get(params.get("series_ticker", ""), [])}


class KnownBy:
    """Wraps a collector so only observations known at or before ``cutoff`` are kept."""

    def __init__(self, inner: Collector, cutoff: datetime) -> None:
        self.feed = inner.feed
        self._inner = inner
        self._cutoff = utc(cutoff)

    def fetch(self, start: datetime, end: datetime) -> Iterator[Any]:
        return self._inner.fetch(start, end)

    def normalize(self, raw: Any, fetched_at: datetime) -> list[Observation]:
        return [
            o for o in self._inner.normalize(raw, fetched_at) if o.knowledge_time <= self._cutoff
        ]


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


def build_store(
    offline: bool, now: datetime, cutoff: datetime | None = None
) -> tuple[Store, list[Event]]:
    """A fresh in-memory store loaded through the real runner; returns it with its events.

    With ``cutoff``, only observations known at or before it are loaded; events, markets and
    links are loaded exactly as without it.
    """
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
        wrapped = KnownBy(collector, cutoff) if cutoff is not None else collector
        run_collector(wrapped, store, first, now, now=clock)
    return store, events


def pct(value: float | None, places: int) -> str:
    return _UNKNOWN if value is None else f"{value * 100:.{places}f}%"


def _stamp(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%dT%H:%MZ")


_PUNCTUATION = str.maketrans("\u2018\u2019\u201c\u201d\u2013\u2014", "''\"\"--")


def _ascii(text: str) -> str:
    """Readable ASCII: accents dropped, common typographic punctuation mapped, rest '?'."""
    decomposed = unicodedata.normalize("NFKD", text.translate(_PUNCTUATION))
    kept = "".join(c for c in decomposed if not unicodedata.combining(c))
    return kept.encode("ascii", "replace").decode("ascii")


def _market_label(row: PanelRow) -> str:
    """``venue: title`` with the title cut to ``_TITLE_WIDTH`` characters."""
    if row.market_id is None:
        return _UNKNOWN
    venue = row.market_id.split(":", 1)[0]
    title = " ".join(_ascii(row.market_title or row.market_id).split())
    if len(title) > _TITLE_WIDTH:
        title = title[: _TITLE_WIDTH - 3] + "..."
    return f"{venue}: {title}"


def _panel_cells(rows: Sequence[PanelRow]) -> tuple[list[str], list[list[str]]]:
    headers = ["event", "as of (UTC)", "top market", "market prob", IMPLIED_HEADER, "realised move"]
    body = [
        [
            r.event_id,
            _stamp(r.as_of),
            _market_label(r),
            pct(r.market_prob, 1),
            pct(r.implied_move, 2),
            pct(r.realized_move, 2),
        ]
        for r in rows
    ]
    return headers, body


_CELLS = (
    ("market prob", "market_prob"),
    (IMPLIED_HEADER, "implied_move"),
    ("realised move", "realized_move"),
)


def _change_rows(before: Sequence[PanelRow], after: Sequence[PanelRow]) -> list[list[str]]:
    """Per column: events whose cell went empty->filled, filled->empty, or changed value."""
    after_by_id = {r.event_id: r for r in after}
    rows: list[list[str]] = []
    for label, field in _CELLS:
        filled = emptied = changed = 0
        for old in before:
            a, b = getattr(old, field), getattr(after_by_id[old.event_id], field)
            filled += a is None and b is not None
            emptied += a is not None and b is None
            changed += a is not None and b is not None and a != b
        rows.append([label, str(filled), str(emptied), str(changed)])
    return rows


_FIELDS = ("market_id", "market_prob", "implied_move", "realized_move")


def eve_view_mismatches(offline: bool, now: datetime, eve: Sequence[PanelRow]) -> list[str]:
    """Recompute each eve row from a store holding only data known at its as-of time.

    Returns one line per row whose recomputed value differs, naming the event and fields.
    """
    problems: list[str] = []
    for row in eve:
        store, _ = build_store(offline, now, cutoff=row.as_of)
        try:
            again = [r for r in eve_panel(store, until=now) if r.event_id == row.event_id]
        finally:
            store.close()
        if again == [row]:
            continue
        other = again[0] if again else None
        diffs = [
            f"{f} (full {getattr(row, f)!r}, truncated {getattr(other, f, None)!r})"
            for f in _FIELDS
            if getattr(row, f) != getattr(other, f, None)
        ]
        problems.append(f"{row.event_id}: " + ", ".join(diffs or ["row missing or other field"]))
    return problems


def demo_health(store: Store, now: datetime, offline: bool) -> list[FeedStatus]:
    feeds = ["fred", "kalshi", "noaa", "polymarket"] + ([_OPTIONS_FEED] if offline else [])
    return [check_feed(store, POLICIES[f], now) for f in sorted(feeds)]


def render_demo(offline: bool, now: datetime) -> tuple[str, bool]:
    """The demo text and whether every self-check passed."""
    store, events = build_store(offline, now)
    try:
        shown_until = now - _AFTER  # both tables cover the same events
        eve = eve_panel(store, until=shown_until)
        later = eve_panel(store, lead=-_AFTER, until=shown_until)
        too_recent = [e.event_id for e in events if e.scheduled_time > shown_until]
        statuses = demo_health(store, now, offline)
    finally:
        store.close()
    problems = eve_view_mismatches(offline, now, eve)
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
    out.append(format_table(*_panel_cells(eve)))
    if offline:
        out.append(
            f"The '{IMPLIED_HEADER}' column is computed by the real solver from illustrative"
            " inputs; the other columns come from recorded public data."
        )
    else:
        out.append(f"No option history is available here, so '{IMPLIED_HEADER}' stays empty.")
    if too_recent:
        out.append(f"Not shown: {', '.join(too_recent)} (less than two days old)")
    for kind in _KINDS:
        priced = sum(1 for r in eve if r.kind == kind and r.market_prob is not None)
        if priced < _MIN_PER_KIND:
            out.append(f"Note: only {priced} {kind} events here have a recorded market price.")
    out += [
        "",
        "2. The same events, as known two days after each",
        format_table(*_panel_cells(later)),
    ]
    out += [
        "",
        "3. What changed between the two tables (number of events)",
        format_table(
            ["column", "empty to filled", "filled to empty", "value changed"],
            _change_rows(eve, later),
        ),
        "A realised move exists only once the close is known; an implied move is only defined"
        " before its event.",
        *(
            [
                "Each row in table 1 was recomputed from a store holding only data known at its"
                " as-of time, and matched."
            ]
            if not problems
            else [
                f"WARNING: table 1 row not reproduced from data known then: {p}" for p in problems
            ]
        ),
        "",
        "Feed health" + (" (runs are stamped with the demo clock)" if offline else ""),
        render_health(statuses),
    ]
    if offline:
        out += [
            "",
            f"{_OPTIONS_FEED} is the synthetic sample; cftc and edgar are not part of the demo.",
            "A noaa gap warning is genuine: the recorded summaries trail the demo clock.",
        ]
    return "\n".join(out), not problems
