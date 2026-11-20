"""Quantities derived from a point-in-time snapshot: implied and realised event moves.

Every function reads observations only through the ``Snapshot`` it is given, so none of them
can use information that was not yet knowable at ``snap.ts``. They return numbers or ``None``.
"""

import math
from datetime import UTC, date, datetime, time, timedelta
from typing import Literal
from zoneinfo import ZoneInfo

from eventlake.calendar import Event
from eventlake.store import Snapshot
from eventlake.types import utc

_NY = ZoneInfo("America/New_York")
_YEAR_S = 365 * 86400.0
_MAX_AGE = timedelta(days=4)
_MIN_GAP = timedelta(days=7)
_OPTIONS_FEED = "tradier.options"
_FLAT_TOL = 1e-12  # relative to the front variance, so equal IVs are not read as a tiny event

Status = Literal["ok", "flat_term", "inverted"]


def solve_event_move(
    front_iv: float, back_iv: float, front_t: float, back_t: float
) -> tuple[float, float, Status]:
    """Split two ATM implied vols into a diffuse vol and a one-off event move.

    Total variance is ``diffuse**2 * t + m**2`` for an event inside the front expiry. Times are
    in years (ACT/365). Returns ``(diffuse_vol, event_move, status)``.
    """
    if back_t <= front_t:
        raise ValueError("back_t must be greater than front_t")
    var_f = front_iv**2 * front_t
    var_b = back_iv**2 * back_t
    diffuse_sq = (var_b - var_f) / (back_t - front_t)
    if diffuse_sq <= 0:
        return math.nan, math.nan, "inverted"
    diffuse = math.sqrt(diffuse_sq)
    move_sq = var_f - diffuse_sq * front_t
    if move_sq <= _FLAT_TOL * var_f:
        return diffuse, 0.0, "flat_term"
    return diffuse, math.sqrt(move_sq), "ok"


def _expiry_close(expiry: date) -> datetime:
    """The 16:00 New York close on the expiry date, in UTC."""
    return datetime.combine(expiry, time(16, 0), tzinfo=_NY).astimezone(UTC)


def implied_event_move(snap: Snapshot, underlying: str, event: Event) -> float | None:
    """The event move implied by the front and back ATM IVs known at ``snap.ts``.

    Front is the earliest expiry after the event; back is the earliest expiry at least seven
    days after the front. Both observations must be at most four days old. ``None`` when either
    leg is missing or stale, the snapshot is not before the event, or the term structure is
    inverted.
    """
    if snap.ts >= utc(event.scheduled_time):
        return None
    prefix = f"{underlying}:"
    expiries: list[date] = []
    for series_id in snap.series_ids(_OPTIONS_FEED, prefix):
        parts = series_id.removeprefix(prefix).split(":")
        if len(parts) == 2 and parts[1] == "atm_iv":
            try:
                expiries.append(date.fromisoformat(parts[0]))
            except ValueError:
                continue
    expiries.sort()
    front = next((e for e in expiries if _expiry_close(e) > utc(event.scheduled_time)), None)
    if front is None:
        return None
    back = next((e for e in expiries if _expiry_close(e) - _expiry_close(front) >= _MIN_GAP), None)
    if back is None:
        return None
    legs: list[tuple[float, float]] = []
    for expiry in (front, back):
        obs = snap.latest(_OPTIONS_FEED, f"{underlying}:{expiry:%Y-%m-%d}:atm_iv")
        if obs is None or obs.event_time < snap.ts - _MAX_AGE:
            return None
        t = (_expiry_close(expiry) - obs.event_time).total_seconds() / _YEAR_S
        if t <= 0:
            return None
        legs.append((obs.value, t))
    (front_iv, front_t), (back_iv, back_t) = legs
    if back_t <= front_t:
        return None
    _, move, status = solve_event_move(front_iv, back_iv, front_t, back_t)
    return move if status in ("ok", "flat_term") else None


def realized_move(
    snap: Snapshot, event: Event, *, feed: str = "fred", series_id: str = "SP500"
) -> float | None:
    """``abs(close_D / close_prev - 1)`` for the event's New York calendar day ``D``.

    ``None`` when either close is not in the snapshot, which includes any ``snap.ts`` before the
    close for day ``D`` was knowable.
    """
    day = utc(event.scheduled_time).astimezone(_NY).date()
    event_time = datetime.combine(day, time(), tzinfo=UTC)
    rows = snap.series(feed, series_id)
    current = next((o for o in rows if o.event_time == event_time), None)
    previous = [o for o in rows if o.event_time < event_time]
    if current is None or not previous or previous[-1].value == 0:
        return None
    return abs(current.value / previous[-1].value - 1)
