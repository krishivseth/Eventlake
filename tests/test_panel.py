from datetime import date, datetime, timedelta
from pathlib import Path

import pyarrow.parquet as pq
import pytest

from eventlake.entities import upsert_markets
from eventlake.panel import PanelRow, eve_panel, event_panel, export_parquet
from eventlake.types import Observation
from panel_helpers import CPI, FOMC1, MOVE, build_store, kalshi_market, link, prob_obs, utc_at


def _row(rows: list[PanelRow], event_id: str) -> PanelRow:
    return next(r for r in rows if r.event_id == event_id)


def test_eve_row_has_prob_and_implied_but_no_realized() -> None:
    rows = eve_panel(build_store(), until=utc_at(2026, 2, 1))
    r = _row(rows, FOMC1.event_id)
    assert r.as_of == FOMC1.scheduled_time - timedelta(days=1)
    assert r.market_prob == 0.71
    assert r.implied_move == pytest.approx(MOVE, rel=0.05)
    assert r.realized_move is None
    assert [x.event_id for x in rows] == [FOMC1.event_id]


def test_later_as_of_fills_realized_and_keeps_eve_values_unchanged() -> None:
    store = build_store()
    eve = _row(eve_panel(store, until=utc_at(2026, 2, 1)), FOMC1.event_id)
    later = _row(event_panel(store, utc_at(2026, 2, 5)), FOMC1.event_id)
    assert later.realized_move == pytest.approx(5100.0 / 5027.0 - 1)
    assert later.implied_move is None  # the event has passed
    again = _row(eve_panel(store, until=utc_at(2026, 2, 1)), FOMC1.event_id)
    assert again == eve


def test_market_prob_is_highest_linked_market() -> None:
    r = _row(event_panel(build_store(), utc_at(2026, 1, 27, 19)), FOMC1.event_id)
    assert r.market_prob == 0.71
    assert r.market_id == "kalshi:KXFED-26JAN-HOLD"
    assert r.market_title == "KXFED-26JAN-HOLD"


def test_polymarket_probability_is_read_by_yes_token() -> None:
    store = build_store()
    store.append([prob_obs("polymarket", "tok-cut", utc_at(2026, 1, 27, 12), 0.9)])
    r = _row(event_panel(store, utc_at(2026, 1, 27, 19)), FOMC1.event_id)
    assert (r.market_id, r.market_prob) == ("polymarket:fed-jan-cut", 0.9)


def test_event_without_linked_market_has_null_market() -> None:
    r = _row(event_panel(build_store(), utc_at(2026, 2, 1), kinds=("CPI",)), CPI.event_id)
    assert (r.market_id, r.market_title, r.market_prob) == (None, None, None)


def test_market_probability_older_than_seven_days_is_unknown() -> None:
    store = build_store()
    fresh = _row(event_panel(store, utc_at(2026, 1, 27) + timedelta(days=6)), FOMC1.event_id)
    stale = _row(event_panel(store, utc_at(2026, 1, 27) + timedelta(days=8)), FOMC1.event_id)
    assert fresh.market_prob == 0.71
    assert (stale.market_id, stale.market_prob) == (None, None)


def test_event_window_is_thirty_days_ahead() -> None:
    rows = event_panel(build_store(), utc_at(2026, 1, 1), kinds=("FOMC", "CPI"))
    assert [r.event_id for r in rows] == [FOMC1.event_id]
    assert all(r.as_of == utc_at(2026, 1, 1) and r.scheduled_time.tzinfo for r in rows)


def test_naive_datetimes_rejected() -> None:
    store = build_store()
    with pytest.raises(ValueError):
        event_panel(store, datetime(2026, 1, 1))
    with pytest.raises(ValueError):
        eve_panel(store, until=datetime(2026, 1, 1))


@pytest.mark.parametrize(
    ("as_of", "expect_implied", "expect_realized"),
    [(utc_at(2026, 1, 27, 19), True, False), (utc_at(2026, 2, 2), False, True)],
)
def test_panel_no_leak(as_of: datetime, expect_implied: bool, expect_realized: bool) -> None:
    store = build_store()
    before = event_panel(store, as_of)
    row = _row(before, FOMC1.event_id)
    assert row.market_prob is not None  # the panel is not trivially all-None
    assert (row.implied_move is not None) is expect_implied
    assert (row.realized_move is not None) is expect_realized
    later = as_of + timedelta(days=1)
    leak: list[Observation] = [
        # later revisions of event_times the panel uses
        Observation("kalshi", "KXFED-26JAN-HOLD", utc_at(2026, 1, 27), later, 0.05),
        Observation("kalshi", "KXFED-26JAN-HOLD", utc_at(2026, 1, 26), later, 0.05),
        Observation("fred", "SP500", utc_at(2026, 1, 28), later, 1.0),
        Observation("fred", "SP500", utc_at(2026, 1, 27), later, 1.0),
        # new event_times known only later
        Observation("polymarket", "tok-cut", as_of + timedelta(hours=1), later, 0.99),
        Observation("fred", "SP500", later - timedelta(hours=1), later, 1.0),
    ]
    for e in (date(2026, 1, 30), date(2026, 2, 13)):
        sid = f"SPY:{e:%Y-%m-%d}:atm_iv"
        leak.append(Observation("tradier.options", sid, utc_at(2026, 1, 26, 21), later, 0.8))
        leak.append(Observation("tradier.options", sid, as_of - timedelta(hours=1), later, 0.9))
    store.append(leak)
    assert event_panel(store, as_of) == before


def test_late_link_can_only_surface_prices_already_known() -> None:
    # Links are reference data, read as stored (documented behaviour): linking a market later
    # can change the market shown in a past row, but only to one priced at that as_of.
    store = build_store()
    as_of = utc_at(2026, 1, 27, 19)
    before = _row(event_panel(store, as_of), FOMC1.event_id)
    close = utc_at(2026, 1, 28, 19)
    upsert_markets(store, [kalshi_market("KXFED-26JAN-LATE", close)])
    link(store, FOMC1.event_id, "kalshi:KXFED-26JAN-LATE")
    store.append([prob_obs("kalshi", "KXFED-26JAN-LATE", utc_at(2026, 1, 28), 0.95)])
    assert _row(event_panel(store, as_of), FOMC1.event_id) == before  # (a) priced only later
    upsert_markets(store, [kalshi_market("KXFED-26JAN-KNOWN", close)])
    link(store, FOMC1.event_id, "kalshi:KXFED-26JAN-KNOWN")
    store.append([prob_obs("kalshi", "KXFED-26JAN-KNOWN", utc_at(2026, 1, 26), 0.8)])
    after = _row(event_panel(store, as_of), FOMC1.event_id)  # (b) priced at as_of, higher
    assert (after.market_id, after.market_prob) == ("kalshi:KXFED-26JAN-KNOWN", 0.8)


def test_eve_panel_lead_beyond_horizon_still_returns_row() -> None:
    rows = eve_panel(build_store(), lead=timedelta(days=45), until=utc_at(2026, 2, 1))
    assert [r.event_id for r in rows] == [FOMC1.event_id]
    assert rows[0].as_of == FOMC1.scheduled_time - timedelta(days=45)


def test_export_parquet_roundtrip(tmp_path: Path) -> None:
    rows = event_panel(build_store(), utc_at(2026, 2, 5))
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
    rows = event_panel(build_store(), utc_at(2026, 2, 1), kinds=("CPI",))
    export_parquet(rows, path)
    assert pq.read_table(path).to_pylist()[0]["market_prob"] is None
