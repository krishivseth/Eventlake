from datetime import UTC, datetime, timedelta
from pathlib import Path

import responses

from eventlake.collectors import REGISTRY
from eventlake.collectors.fred import FRED_SERIES, FredCollector
from eventlake.runner import run_collector
from eventlake.store import Store

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
    assert len(got) == 2
    (jan28,) = [g for g in got if g[0].day == 28]
    assert jan28 == (datetime(2026, 1, 28, tzinfo=UTC), datetime(2026, 1, 29, tzinfo=UTC), jan28[2])
    assert 5000 < jan28[2] < 9000


@responses.activate
def test_fetch_filters_rows_to_window() -> None:
    _register("SP500")
    records = list(FredCollector(series={"SP500": timedelta(0)}).fetch(START, END))
    dates = [r["date"] for r in records]
    assert dates[0] == "2026-01-26"
    assert dates[-1] == "2026-02-20"
    assert len(dates) == 20


def test_cpi_lag_is_added_after_one_day() -> None:
    c = FredCollector()
    (obs,) = c.normalize(
        {"series": "CPIAUCSL", "date": "2026-01-01", "value": "326.588"},
        datetime(2026, 3, 1, tzinfo=UTC),
    )
    assert obs.knowledge_time == datetime(2026, 1, 16, tzinfo=UTC)
    assert obs.value == 326.588


def test_kt_quality_is_declared() -> None:
    (obs,) = FredCollector().normalize(
        {"series": "DFF", "date": "2026-02-02", "value": "3.64"}, FETCHED
    )
    assert obs.kt_quality == "assumed"
    assert obs.feed == "fred"
    assert obs.series_id == "DFF"


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
