from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest
import responses

from eventlake.calendar import load_events
from eventlake.collectors import REGISTRY
from eventlake.collectors.fred import FRED_SERIES, DailyLag, FredCollector
from eventlake.runner import run_collector
from eventlake.store import Store
from eventlake.types import Observation, ValidationError

FIX = Path(__file__).parent / "fixtures" / "fred"
URL = "https://fred.stlouisfed.org/graph/fredgraph.csv"
START = datetime(2026, 1, 26, tzinfo=UTC)
END = datetime(2026, 2, 20, tzinfo=UTC)
FETCHED = datetime(2026, 3, 1, tzinfo=UTC)


def _register(series: str) -> None:
    responses.add(
        responses.GET,
        URL,
        body=(FIX / f"{series}.csv").read_text(),
        match=[responses.matchers.query_param_matcher({"id": series})],
    )


@responses.activate
def test_normalize_fixture_values() -> None:
    _register("SP500")
    c = FredCollector(series={"SP500": FRED_SERIES["SP500"]})
    records = list(c.fetch(START, END))
    got = {
        (o.event_time, o.knowledge_time, o.value)
        for r in records
        for o in c.normalize(r, FETCHED)
        if o.event_time.day in (26, 28) and o.event_time.month == 1
    }
    assert got == {
        (datetime(2026, 1, 26, tzinfo=UTC), datetime(2026, 1, 27, tzinfo=UTC), 6950.23),
        (datetime(2026, 1, 28, tzinfo=UTC), datetime(2026, 1, 29, tzinfo=UTC), 6978.03),
    }


@responses.activate
def test_fetch_filters_rows_to_window() -> None:
    _register("SP500")
    records = list(FredCollector(series={"SP500": DailyLag(timedelta(0))}).fetch(START, END))
    dates = [r["date"] for r in records]
    assert dates[0] == "2026-01-26"
    assert dates[-1] == "2026-02-20"
    assert len(dates) == 20


def _release(event_id: str) -> datetime:
    (event,) = [e for e in load_events(["CPI"]) if e.event_id == event_id]
    return event.scheduled_time


def _cpi(day: str, fetched: datetime = FETCHED) -> list[Observation]:
    return FredCollector().normalize(
        {"series": "CPIAUCSL", "date": day, "value": "326.588"}, fetched
    )


def test_cpi_known_at_the_release_after_its_reference_month() -> None:
    (obs,) = _cpi("2026-01-01")
    release = _release("CPI:2026-02-13")
    assert release == datetime(2026, 2, 13, 13, 30, tzinfo=UTC)
    assert (obs.event_time, obs.knowledge_time) == (datetime(2026, 1, 1, tzinfo=UTC), release)
    assert obs.kt_quality == "source"
    assert obs.attrs == {"event_id": "CPI:2026-02-13", "value_vintage": "current"}
    assert obs.value == 326.588
    store = Store()
    store.append([obs])
    assert store.as_of(release - timedelta(days=1)).series("fred", "CPIAUCSL") == []
    assert store.as_of(release).series("fred", "CPIAUCSL") == [obs]


def test_cpi_before_its_release_is_not_yet_knowable() -> None:
    assert _cpi("2026-01-01", datetime(2026, 2, 13, 13, tzinfo=UTC)) == []


def test_cpi_cancelled_or_delayed_release_maps_to_the_next_existing_one() -> None:
    # September 2025 was published late, on 2025-10-24; October 2025 had no release of its own,
    # so the first release after that month ends is the one on 2025-12-18.
    (september,) = _cpi("2025-09-01")
    (october,) = _cpi("2025-10-01")
    assert september.knowledge_time == _release("CPI:2025-10-24")
    assert october.knowledge_time == _release("CPI:2025-12-18")
    assert october.attrs["event_id"] == "CPI:2025-12-18"


def test_cpi_outside_the_calendar_uses_the_assumed_fallback() -> None:
    (obs,) = _cpi("2024-06-01")
    assert obs.knowledge_time == datetime(2024, 7, 1, tzinfo=UTC) + timedelta(days=45)
    assert obs.kt_quality == "assumed"
    assert "event_id" not in obs.attrs
    (first,) = _cpi("2024-12-01")  # published by the calendar's first release
    assert first.attrs["event_id"] == "CPI:2025-01-15"
    (last,) = _cpi("2027-11-01", datetime(2028, 3, 1, tzinfo=UTC))
    assert last.attrs["event_id"] == "CPI:2027-12-10"  # the calendar's last release
    (beyond,) = _cpi("2027-12-01", datetime(2028, 3, 1, tzinfo=UTC))  # no later release
    assert beyond.knowledge_time == datetime(2028, 1, 1, tzinfo=UTC) + timedelta(days=45)
    assert beyond.kt_quality == "assumed"


def test_daily_series_keep_the_next_day_rule() -> None:
    (dff,) = FredCollector().normalize(
        {"series": "DFF", "date": "2026-02-02", "value": "3.64"}, FETCHED
    )
    assert dff.knowledge_time == datetime(2026, 2, 4, tzinfo=UTC)
    assert dff.attrs == {"value_vintage": "current"}


def test_kt_quality_is_declared() -> None:
    (obs,) = FredCollector().normalize(
        {"series": "DFF", "date": "2026-02-02", "value": "3.64"}, FETCHED
    )
    assert obs.kt_quality == "assumed"
    assert obs.feed == "fred"
    assert obs.series_id == "DFF"


def test_every_observation_is_marked_as_a_current_vintage() -> None:
    c = FredCollector()
    for series, day in (("SP500", "2026-01-28"), ("DFF", "2026-02-02"), ("CPIAUCSL", "2026-01-01")):
        (obs,) = c.normalize({"series": series, "date": day, "value": "1.5"}, FETCHED)
        assert obs.attrs["value_vintage"] == "current", series
    (fallback,) = c.normalize({"series": "CPIAUCSL", "date": "2024-06-01", "value": "1.5"}, FETCHED)
    assert fallback.attrs == {"value_vintage": "current"}


def test_fred_missing_value_yields_nothing() -> None:
    c = FredCollector()
    assert c.normalize({"series": "SP500", "date": "2026-02-16", "value": "."}, FETCHED) == []
    assert c.normalize({"series": "SP500", "date": "2026-02-16", "value": ""}, FETCHED) == []


def test_fred_sp500_known_next_utc_midnight() -> None:
    (obs,) = FredCollector().normalize(
        {"series": "SP500", "date": "2026-01-28", "value": "6900.0"}, FETCHED
    )
    assert obs.event_time == datetime(2026, 1, 28, tzinfo=UTC)
    assert obs.knowledge_time == datetime(2026, 1, 29, tzinfo=UTC)


def test_not_yet_knowable_is_dropped() -> None:
    raw = {"series": "SP500", "date": "2026-01-28", "value": "6900.0"}
    assert FredCollector().normalize(raw, datetime(2026, 1, 28, 23, tzinfo=UTC)) == []


def test_registered() -> None:
    assert isinstance(REGISTRY["fred"](), FredCollector)


@responses.activate
def test_run_collector_end_to_end() -> None:
    for s in FRED_SERIES:
        _register(s)
    store = Store()
    result = run_collector(FredCollector(), store, START, END, now=lambda: FETCHED)
    assert result.status == "ok", result.error
    snap = store.as_of(FETCHED)
    assert set(snap.series_ids("fred")) == {"SP500", "DFF"}  # CPI row for 2026-01 is outside window
    sp = snap.series("fred", "SP500")
    assert len(sp) == 19  # 20 rows minus the empty Presidents Day row
    assert all(o.kt_quality == "assumed" for o in sp)


def test_non_numeric_value_is_validation_error() -> None:
    with pytest.raises(ValidationError, match="SP500"):
        FredCollector().normalize(
            {"series": "SP500", "date": "2026-01-28", "value": "abc"}, FETCHED
        )


@responses.activate
def test_malformed_row_met_during_fetch_is_quarantined_and_the_run_continues() -> None:
    text = (FIX / "SP500.csv").read_text().replace("2026-01-27,", "27/01/2026,", 1)
    responses.add(responses.GET, URL, body=text)
    store = Store()
    collector = FredCollector(series={"SP500": FRED_SERIES["SP500"]})
    result = run_collector(collector, store, START, END, now=lambda: FETCHED)
    assert (result.status, result.rows_quarantined) == ("partial", 1)
    (reason,) = store.execute("SELECT reason FROM quarantine")[0]
    assert "fred: SP500: malformed observation_date" in reason
    days = [o.event_time.day for o in store.as_of(FETCHED).series("fred", "SP500")]
    assert 28 in days and 27 not in days
