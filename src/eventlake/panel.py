"""The event panel: per event, what the market said, what options implied, what happened.

Every row is computed from a single ``Snapshot`` at ``as_of``, so a row for a given ``as_of``
never changes when later data arrives.
"""

from collections.abc import Sequence
from dataclasses import astuple, dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import cast

import pyarrow as pa
import pyarrow.parquet as pq

from eventlake.calendar import Event, EventKind
from eventlake.derived import implied_event_move, realized_move
from eventlake.entities import Market, markets_for_event
from eventlake.store import Snapshot, Store
from eventlake.types import utc

_HORIZON = timedelta(days=30)
_MAX_PROB_AGE = timedelta(days=7)
_UNDERLYING = "SPY"
_DEFAULT_KINDS: tuple[str, ...] = ("FOMC", "CPI")


@dataclass(frozen=True)
class PanelRow:
    event_id: str
    kind: str
    scheduled_time: datetime
    as_of: datetime
    market_id: str | None
    market_title: str | None
    market_prob: float | None
    implied_move: float | None
    realized_move: float | None


def stored_events(store: Store, kinds: Sequence[str]) -> list[Event]:
    """Events of the given kinds from the store's ``events`` table, sorted by time then id."""
    if not kinds:
        return []
    marks = ",".join("?" for _ in kinds)
    rows = store.execute(
        "SELECT event_id, kind, scheduled_time FROM events"
        f" WHERE kind IN ({marks}) ORDER BY scheduled_time, event_id",
        list(kinds),
    )
    return [Event(r[0], cast(EventKind, r[1]), utc(r[2]), {}) for r in rows]


def _series_id(market: Market) -> str | None:
    if market.venue == "kalshi":
        return market.venue_ticker
    token = market.attrs.get("yes_token_id")
    return str(token) if token else None


def _market_prob(snap: Snapshot, market: Market) -> float | None:
    series_id = _series_id(market)
    if series_id is None:
        return None
    obs = snap.latest(market.venue, series_id)
    if obs is None or obs.event_time < snap.ts - _MAX_PROB_AGE:
        return None
    return obs.value


def _top_market(store: Store, snap: Snapshot, event: Event) -> tuple[Market, float] | None:
    priced = [
        (m, p)
        for m in markets_for_event(store, event.event_id)
        if (p := _market_prob(snap, m)) is not None
    ]
    if not priced:
        return None
    return min(priced, key=lambda mp: (-mp[1], mp[0].market_id))  # ties: lowest market_id


def event_panel(
    store: Store, as_of: datetime, kinds: Sequence[str] = _DEFAULT_KINDS
) -> list[PanelRow]:
    """One row per stored event of ``kinds`` scheduled at most 30 days after ``as_of``.

    ``market_*`` describes the linked market with the highest probability in the snapshot.
    A market's probability counts only if its latest observation is at most 7 days older than
    ``as_of``; a market that stopped trading earlier is unknown, and when no linked market is
    known the three ``market_*`` fields are ``None``. ``implied_move`` and ``realized_move``
    come from ``derived`` and are ``None`` when not knowable at ``as_of``.
    """
    as_of = utc(as_of)
    snap = store.as_of(as_of)
    rows: list[PanelRow] = []
    for event in stored_events(store, kinds):
        if event.scheduled_time > as_of + _HORIZON:
            continue
        top = _top_market(store, snap, event)
        rows.append(
            PanelRow(
                event_id=event.event_id,
                kind=event.kind,
                scheduled_time=event.scheduled_time,
                as_of=as_of,
                market_id=top[0].market_id if top else None,
                market_title=top[0].title if top else None,
                market_prob=top[1] if top else None,
                implied_move=implied_event_move(snap, _UNDERLYING, event),
                realized_move=realized_move(snap, event),
            )
        )
    return rows


def eve_panel(
    store: Store,
    lead: timedelta = timedelta(days=1),
    kinds: Sequence[str] = _DEFAULT_KINDS,
    until: datetime | None = None,
) -> list[PanelRow]:
    """For each event scheduled at or before ``until`` (default now), its row at ``lead`` before."""
    limit = utc(until) if until is not None else datetime.now(UTC)
    out: list[PanelRow] = []
    for event in stored_events(store, kinds):
        if event.scheduled_time > limit:
            continue
        rows = event_panel(store, event.scheduled_time - lead, kinds)
        out.extend(r for r in rows if r.event_id == event.event_id)
    return out


_SCHEMA = pa.schema(
    [
        ("event_id", pa.string()),
        ("kind", pa.string()),
        ("scheduled_time", pa.timestamp("us", tz="UTC")),
        ("as_of", pa.timestamp("us", tz="UTC")),
        ("market_id", pa.string()),
        ("market_title", pa.string()),
        ("market_prob", pa.float64()),
        ("implied_move", pa.float64()),
        ("realized_move", pa.float64()),
    ]
)


def export_parquet(rows: Sequence[PanelRow], path: Path) -> None:
    """Write panel rows to a Parquet file; timestamps are UTC and missing values are nulls."""
    columns = list(zip(*(astuple(r) for r in rows), strict=True)) if rows else [()] * len(_SCHEMA)
    table = pa.Table.from_arrays(
        [pa.array(col, type=f.type) for col, f in zip(columns, _SCHEMA, strict=True)],
        schema=_SCHEMA,
    )
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(table, path)
