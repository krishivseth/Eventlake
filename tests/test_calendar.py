from datetime import UTC, date, datetime

from eventlake.calendar import load_events, upsert_events, weather_event
from eventlake.entities import Market, upsert_markets
from eventlake.store import Store


def _by_id(kind: str) -> dict:  # type: ignore[type-arg]
    return {e.event_id: e for e in load_events([kind])}  # type: ignore[list-item]


def test_fomc_release_time_respects_dst() -> None:
    ev = _by_id("FOMC")
    assert ev["FOMC:2026-01-28"].scheduled_time == datetime(2026, 1, 28, 19, 0, tzinfo=UTC)
    assert ev["FOMC:2026-06-17"].scheduled_time == datetime(2026, 6, 17, 18, 0, tzinfo=UTC)


def test_cpi_release_is_0830_et() -> None:
    # BLS moved the January 2026 CPI to Feb 13 after the early-2026 funding lapse.
    ev = _by_id("CPI")
    assert ev["CPI:2026-02-13"].scheduled_time == datetime(2026, 2, 13, 13, 30, tzinfo=UTC)
    assert ev["CPI:2026-07-14"].scheduled_time == datetime(2026, 7, 14, 12, 30, tzinfo=UTC)


def test_shutdown_affected_releases_use_actual_dates() -> None:
    cpi, nfp = _by_id("CPI"), _by_id("NFP")
    assert "CPI:2025-10-24" in cpi and "CPI:2025-12-18" in cpi
    assert not any(i.startswith(("CPI:2025-11", "CPI:2025-10-1")) for i in cpi)
    assert "NFP:2025-11-20" in nfp and "NFP:2025-12-16" in nfp
    assert not any(i.startswith("NFP:2025-10") for i in nfp)


def test_eight_fomc_meetings_per_year() -> None:
    events = load_events(["FOMC"])
    for year in (2025, 2026):
        assert sum(1 for e in events if e.scheduled_time.year == year) == 8


def test_events_sorted_and_ids_unique() -> None:
    events = load_events()
    times = [e.scheduled_time for e in events]
    assert times == sorted(times)
    ids = [e.event_id for e in events]
    assert len(ids) == len(set(ids))


def test_every_row_has_source_url() -> None:
    for e in load_events():
        assert str(e.attrs["source_url"]).startswith("https://")


def test_kind_filter() -> None:
    assert {e.kind for e in load_events(["NFP"])} == {"NFP"}


def test_upsert_events_is_idempotent() -> None:
    s = Store()
    events = load_events()
    assert upsert_events(s, events) == len(events)
    assert upsert_events(s, events) == 0
    row = s.execute("SELECT kind, scheduled_time FROM events WHERE event_id = 'CPI:2026-02-13'")
    assert row == [("CPI", datetime(2026, 2, 13, 13, 30, tzinfo=UTC))]


def test_upsert_markets_is_idempotent() -> None:
    s = Store()
    m = Market(
        "kalshi:KXCPI-26FEB",
        "kalshi",
        "KXCPI-26FEB",
        "KXCPI",
        "CPI above 0.3%",
        datetime(2026, 2, 13, 13, 30, tzinfo=UTC),
        {},
    )
    assert upsert_markets(s, [m]) == 1
    assert upsert_markets(s, [m]) == 0


def test_weather_event_id_and_time() -> None:
    e = weather_event("USW00094728", date(2026, 7, 4))
    assert e.event_id == "WEATHER_HIGH:USW00094728:2026-07-04"
    assert e.scheduled_time == datetime(2026, 7, 5, 5, 0, tzinfo=UTC)
