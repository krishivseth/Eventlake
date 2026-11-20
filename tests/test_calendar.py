import csv
from datetime import UTC, date, datetime
from importlib import resources

import pytest

from eventlake.calendar import (
    calendar_coverage_end,
    calendar_warnings,
    load_events,
    upsert_events,
    weather_event,
)
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
    for year in (2025, 2026, 2027):
        assert sum(1 for e in events if e.scheduled_time.year == year) == 8


def test_2027_schedules_from_the_official_pages() -> None:
    fomc, cpi, nfp = _by_id("FOMC"), _by_id("CPI"), _by_id("NFP")
    assert fomc["FOMC:2027-01-27"].scheduled_time == datetime(2027, 1, 27, 19, 0, tzinfo=UTC)
    assert fomc["FOMC:2027-11-17"].attrs["source_url"].startswith("https://www.federalreserve")
    assert cpi["CPI:2027-07-14"].scheduled_time == datetime(2027, 7, 14, 12, 30, tzinfo=UTC)
    assert nfp["NFP:2027-12-03"].scheduled_time == datetime(2027, 12, 3, 13, 30, tzinfo=UTC)
    for kind in (cpi, nfp):
        assert sum(1 for e in kind.values() if e.scheduled_time.year == 2027) == 12


def test_coverage_end_per_kind() -> None:
    assert calendar_coverage_end("FOMC") == date(2027, 11, 17)
    assert calendar_coverage_end("CPI") == date(2027, 12, 10)
    assert calendar_coverage_end("NFP") == date(2027, 12, 3)
    with pytest.raises(ValueError):
        calendar_coverage_end("WEATHER_HIGH")


def test_warnings_within_thirty_days_of_or_past_the_end() -> None:
    assert calendar_warnings(datetime(2027, 10, 17, tzinfo=UTC)) == []
    assert calendar_warnings(datetime(2027, 10, 18, tzinfo=UTC)) == [
        "calendar: FOMC schedule ends 2027-11-17"
    ]
    assert calendar_warnings(datetime(2028, 3, 1, tzinfo=UTC)) == [
        "calendar: CPI schedule ends 2027-12-10",
        "calendar: FOMC schedule ends 2027-11-17",
        "calendar: NFP schedule ends 2027-12-03",
    ]


@pytest.mark.parametrize("name", ["fomc.csv", "cpi.csv", "nfp.csv"])
def test_calendar_csv_dates_are_sorted_unique_weekdays(name: str) -> None:
    text = resources.files("eventlake").joinpath("calendar_data", name).read_text("utf-8")
    days = [date.fromisoformat(r["date"]) for r in csv.DictReader(text.splitlines())]
    assert days and all(d.weekday() < 5 for d in days)
    assert len(days) == len(set(days))
    assert days == sorted(days)


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
