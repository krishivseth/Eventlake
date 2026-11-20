import math
from datetime import UTC, date, datetime, time, timedelta

import pytest

from eventlake.calendar import Event
from eventlake.derived import (
    implied_event_move,
    iv_from_atm_straddle,
    realized_move,
    solve_event_move,
)
from eventlake.store import Store
from eventlake.types import Observation

EVENT = Event("CPI:2026-01-28", "CPI", datetime(2026, 1, 28, 13, 30, tzinfo=UTC), {})
OBS_AT = datetime(2026, 1, 26, 21, 0, tzinfo=UTC)
SNAP_AT = datetime(2026, 1, 26, 22, 0, tzinfo=UTC)
DIFFUSE, MOVE = 0.15, 0.012


def _t(expiry: date, at: datetime) -> float:
    close = datetime.combine(expiry, time(21, 0), tzinfo=UTC)
    return (close - at).total_seconds() / (365 * 86400)


def _iv(expiry: date, at: datetime = OBS_AT) -> float:
    return math.sqrt((DIFFUSE**2 * _t(expiry, at) + MOVE**2) / _t(expiry, at))


def _row(
    expiry: date, iv: float, at: datetime = OBS_AT, known: datetime | None = None
) -> Observation:
    return Observation(
        feed="tradier.options",
        series_id=f"SPY:{expiry:%Y-%m-%d}:atm_iv",
        event_time=at,
        knowledge_time=known or at,
        value=iv,
    )


FRONT, BACK = date(2026, 1, 30), date(2026, 2, 13)


def _store(*extra: Observation) -> Store:
    store = Store()
    store.append(
        [
            _row(date(2026, 1, 27), 0.9),  # expires before the event
            _row(FRONT, _iv(FRONT)),
            _row(date(2026, 2, 3), 0.9),  # only 4 days after the front: not a back leg
            _row(BACK, _iv(BACK)),
            *extra,
        ]
    )
    return store


def test_solver_recovers_planted_event_move() -> None:
    ft, bt = 3 / 365, 17 / 365
    front_iv = math.sqrt((DIFFUSE**2 * ft + MOVE**2) / ft)
    back_iv = math.sqrt((DIFFUSE**2 * bt + MOVE**2) / bt)
    diffuse, move, status = solve_event_move(front_iv, back_iv, ft, bt)
    assert status == "ok"
    assert diffuse == pytest.approx(DIFFUSE, abs=1e-9)
    assert move == pytest.approx(MOVE, abs=1e-9)


def test_solver_inverted_term_structure() -> None:
    diffuse, move, status = solve_event_move(0.3, 0.1, 3 / 365, 17 / 365)
    assert status == "inverted"
    assert math.isnan(diffuse) and math.isnan(move)


def test_solver_flat_term() -> None:
    diffuse, move, status = solve_event_move(0.2, 0.2, 3 / 365, 17 / 365)
    assert status == "flat_term"
    assert diffuse == pytest.approx(0.2)
    assert move == 0.0


@pytest.mark.parametrize(("ft", "bt"), [(0.1, 0.1), (0.1, 0.05)])
def test_solver_rejects_bad_times(ft: float, bt: float) -> None:
    with pytest.raises(ValueError):
        solve_event_move(0.2, 0.2, ft, bt)


def test_straddle_inversion_roundtrip() -> None:
    from statistics import NormalDist

    sigma, t, spot = 0.2, 10 / 365, 600.0
    price = 2 * spot * (2 * NormalDist().cdf(sigma * math.sqrt(t) / 2) - 1)
    assert iv_from_atm_straddle(price, spot, t) == pytest.approx(sigma, abs=1e-9)


@pytest.mark.parametrize(
    ("straddle", "spot", "t"),
    [(0.0, 600.0, 0.1), (5.0, 0.0, 0.1), (5.0, 600.0, 0.0), (2000.0, 600.0, 0.1)],
)
def test_straddle_inversion_rejects_impossible_inputs(
    straddle: float, spot: float, t: float
) -> None:
    with pytest.raises(ValueError):
        iv_from_atm_straddle(straddle, spot, t)


def test_implied_move_recovers_planted_move() -> None:
    move = implied_event_move(_store().as_of(SNAP_AT), "SPY", EVENT)
    assert move == pytest.approx(MOVE, abs=1e-9)


def test_implied_move_none_after_event() -> None:
    store = _store()
    assert implied_event_move(store.as_of(EVENT.scheduled_time), "SPY", EVENT) is None
    assert (
        implied_event_move(store.as_of(EVENT.scheduled_time + timedelta(days=1)), "SPY", EVENT)
        is None
    )


def test_implied_move_none_when_leg_missing() -> None:
    store = Store()
    store.append([_row(FRONT, _iv(FRONT))])
    assert implied_event_move(store.as_of(SNAP_AT), "SPY", EVENT) is None
    assert implied_event_move(Store().as_of(SNAP_AT), "SPY", EVENT) is None


def test_implied_move_none_when_leg_stale() -> None:
    old = SNAP_AT - timedelta(days=6)
    store = Store()
    store.append([_row(FRONT, _iv(FRONT, old), at=old), _row(BACK, _iv(BACK))])
    assert implied_event_move(store.as_of(SNAP_AT), "SPY", EVENT) is None


def test_implied_move_uses_only_snapshot() -> None:
    later = SNAP_AT + timedelta(hours=5)
    base = implied_event_move(_store().as_of(SNAP_AT), "SPY", EVENT)
    with_future = _store(_row(FRONT, 0.99, at=later))
    assert implied_event_move(with_future.as_of(SNAP_AT), "SPY", EVENT) == base
    assert implied_event_move(with_future.as_of(later), "SPY", EVENT) != base


def test_implied_move_none_when_term_structure_inverted() -> None:
    store = Store()
    store.append([_row(FRONT, 0.4), _row(BACK, 0.1)])
    assert implied_event_move(store.as_of(SNAP_AT), "SPY", EVENT) is None


def _closes() -> Store:
    store = Store()
    for day, close in ((date(2026, 1, 27), 600.0), (date(2026, 1, 28), 606.0)):
        event = datetime.combine(day, time(), tzinfo=UTC)
        store.append(
            [Observation("fred", "SP500", event, event + timedelta(days=1), close, "assumed")]
        )
    return store


def test_realized_move_null_before_close_known() -> None:
    store = _closes()
    assert realized_move(store.as_of(datetime(2026, 1, 28, 20, 0, tzinfo=UTC)), EVENT) is None
    assert realized_move(
        store.as_of(datetime(2026, 1, 29, 0, 0, tzinfo=UTC)), EVENT
    ) == pytest.approx(0.01)


def test_realized_move_value() -> None:
    snap = _closes().as_of(datetime(2026, 2, 1, tzinfo=UTC))
    assert realized_move(snap, EVENT) == pytest.approx(0.01)


def test_realized_move_uses_new_york_day() -> None:
    late = Event("X", "FOMC", datetime(2026, 1, 29, 2, 0, tzinfo=UTC), {})  # 21:00 on Jan 28 in NY
    snap = _closes().as_of(datetime(2026, 2, 1, tzinfo=UTC))
    assert realized_move(snap, late) == pytest.approx(0.01)


def test_realized_move_none_without_previous_close() -> None:
    store = Store()
    event = datetime(2026, 1, 28, tzinfo=UTC)
    store.append([Observation("fred", "SP500", event, event + timedelta(days=1), 606.0, "assumed")])
    assert realized_move(store.as_of(datetime(2026, 2, 1, tzinfo=UTC)), EVENT) is None
