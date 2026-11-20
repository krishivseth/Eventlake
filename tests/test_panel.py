from datetime import date, datetime, timedelta
from pathlib import Path

import pyarrow.parquet as pq
import pytest

from eventlake.panel import PanelRow, eve_panel, event_panel, export_parquet
from eventlake.types import Observation
from panel_helpers import CPI, FOMC1, MOVE, _build, _prob, _utc


def _row(rows: list[PanelRow], event_id: str) -> PanelRow:
    return next(r for r in rows if r.event_id == event_id)


def test_eve_row_has_prob_and_implied_but_no_realized() -> None:
    rows = eve_panel(_build(), until=_utc(2026, 2, 1))
    r = _row(rows, FOMC1.event_id)
    assert r.as_of == FOMC1.scheduled_time - timedelta(days=1)
    assert r.market_prob == 0.71
    assert r.implied_move == pytest.approx(MOVE, rel=0.05)
    assert r.realized_move is None
    assert [x.event_id for x in rows] == [FOMC1.event_id]


def test_later_as_of_fills_realized_and_keeps_eve_values_unchanged() -> None:
    store = _build()
    eve = _row(eve_panel(store, until=_utc(2026, 2, 1)), FOMC1.event_id)
    later = _row(event_panel(store, _utc(2026, 2, 5)), FOMC1.event_id)
    assert later.realized_move == pytest.approx(5100.0 / 5027.0 - 1)
    assert later.implied_move is None  # the event has passed
    again = _row(eve_panel(store, until=_utc(2026, 2, 1)), FOMC1.event_id)
    assert again == eve


def test_market_prob_is_highest_linked_market() -> None:
    r = _row(event_panel(_build(), _utc(2026, 1, 27, 19)), FOMC1.event_id)
    assert r.market_prob == 0.71
    assert r.market_id == "kalshi:KXFED-26JAN-HOLD"
    assert r.market_title == "KXFED-26JAN-HOLD"


def test_polymarket_probability_is_read_by_yes_token() -> None:
    store = _build()
    store.append([_prob("polymarket", "tok-cut", _utc(2026, 1, 27, 12), 0.9)])
    r = _row(event_panel(store, _utc(2026, 1, 27, 19)), FOMC1.event_id)
    assert (r.market_id, r.market_prob) == ("polymarket:fed-jan-cut", 0.9)


def test_event_without_linked_market_has_null_market() -> None:
    r = _row(event_panel(_build(), _utc(2026, 2, 1), kinds=("CPI",)), CPI.event_id)
    assert (r.market_id, r.market_title, r.market_prob) == (None, None, None)


def test_market_probability_older_than_seven_days_is_unknown() -> None:
    store = _build()
    fresh = _row(event_panel(store, _utc(2026, 1, 27) + timedelta(days=6)), FOMC1.event_id)
    stale = _row(event_panel(store, _utc(2026, 1, 27) + timedelta(days=8)), FOMC1.event_id)
    assert fresh.market_prob == 0.71
    assert (stale.market_id, stale.market_prob) == (None, None)


def test_event_window_is_thirty_days_ahead() -> None:
    rows = event_panel(_build(), _utc(2026, 1, 1), kinds=("FOMC", "CPI"))
    assert [r.event_id for r in rows] == [FOMC1.event_id]
    assert all(r.as_of == _utc(2026, 1, 1) and r.scheduled_time.tzinfo for r in rows)


def test_naive_datetimes_rejected() -> None:
    store = _build()
    with pytest.raises(ValueError):
        event_panel(store, datetime(2026, 1, 1))
    with pytest.raises(ValueError):
        eve_panel(store, until=datetime(2026, 1, 1))


def test_panel_no_leak() -> None:
    store = _build()
    as_of = _utc(2026, 1, 27, 19)
    before = event_panel(store, as_of)
    later = as_of + timedelta(days=1)
    leak: list[Observation] = [
        Observation("kalshi", "KXFED-26JAN-HOLD", _utc(2026, 1, 27), later, 0.05),  # revision
        Observation("kalshi", "KXFED-26JAN-HOLD", _utc(2026, 1, 28), later, 0.05),
        Observation("polymarket", "tok-cut", _utc(2026, 1, 28), later, 0.99),
        Observation("fred", "SP500", _utc(2026, 1, 28), later + timedelta(days=3), 1.0),
        Observation("fred", "SP500", _utc(2026, 1, 27), later, 1.0),  # revision
    ]
    for e in (date(2026, 1, 30), date(2026, 2, 13)):
        t = _utc(2026, 1, 27, 20)
        leak.append(Observation("tradier.options", f"SPY:{e:%Y-%m-%d}:atm_iv", t, later, 0.9))
        leak.append(
            Observation("tradier.options", f"SPY:{e:%Y-%m-%d}:atm_iv", _utc(2026, 1, 26, 21),
                        later, 0.8)  # revision of a used event_time
        )  # fmt: skip
    store.append(leak)
    assert event_panel(store, as_of) == before


def test_export_parquet_roundtrip(tmp_path: Path) -> None:
    rows = event_panel(_build(), _utc(2026, 2, 5))
    path = tmp_path / "panel.parquet"
    export_parquet(rows, path)
    table = pq.read_table(path)
    assert table.num_rows == len(rows)
    assert table.column_names == [
        "event_id", "kind", "scheduled_time", "as_of", "market_id", "market_title",
        "market_prob", "implied_move", "realized_move",
    ]  # fmt: skip
    assert str(table.schema.field("as_of").type) == "timestamp[us, tz=UTC]"
    first = table.to_pylist()[0]
    assert first["event_id"] == rows[0].event_id
    assert first["scheduled_time"] == rows[0].scheduled_time


def test_export_parquet_empty_and_nulls(tmp_path: Path) -> None:
    path = tmp_path / "empty.parquet"
    export_parquet([], path)
    assert pq.read_table(path).num_rows == 0
    rows = event_panel(_build(), _utc(2026, 2, 1), kinds=("CPI",))
    export_parquet(rows, path)
    assert pq.read_table(path).to_pylist()[0]["market_prob"] is None
