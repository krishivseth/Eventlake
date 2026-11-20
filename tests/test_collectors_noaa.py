import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, urlsplit

import pytest
import responses

from eventlake.collectors import REGISTRY
from eventlake.collectors.noaa import NoaaCollector
from eventlake.runner import run_collector
from eventlake.store import Store
from eventlake.types import ValidationError

FIX = Path(__file__).parent / "fixtures" / "noaa"
URL = "https://www.ncei.noaa.gov/access/services/data/v1"
START = datetime(2026, 1, 20, tzinfo=UTC)
END = datetime(2026, 2, 5, tzinfo=UTC)
FETCHED = datetime(2026, 3, 1, tzinfo=UTC)


def _rows() -> list[dict[str, Any]]:
    data: list[dict[str, Any]] = json.loads((FIX / "daily_USW00094728_tmax.json").read_text())
    return data


def _register() -> None:
    responses.add(responses.GET, URL, json=_rows())


@responses.activate
def test_normalize_fixture_values() -> None:
    _register()
    c = NoaaCollector()
    obs = [o for r in c.fetch(START, END) for o in c.normalize(r, FETCHED)]
    first = obs[0]
    assert (first.event_time, first.knowledge_time, first.value) == (
        datetime(2026, 1, 20, tzinfo=UTC),
        datetime(2026, 1, 21, 12, tzinfo=UTC),
        26.0,
    )
    by_day = {o.event_time.day: o for o in obs if o.event_time.month == 1}
    assert by_day[22].value == 47.0
    assert by_day[22].knowledge_time == datetime(2026, 1, 23, 12, tzinfo=UTC)
    assert first.series_id == "USW00094728:TMAX"


@responses.activate
def test_fetch_sends_station_window_and_units() -> None:
    _register()
    list(NoaaCollector().fetch(START, END))
    query = parse_qs(urlsplit(responses.calls[0].request.url).query)
    assert query == {
        "dataset": ["daily-summaries"],
        "stations": ["USW00094728"],
        "startDate": ["2026-01-20"],
        "endDate": ["2026-02-05"],
        "dataTypes": ["TMAX"],
        "units": ["standard"],
        "format": ["json"],
    }


def test_kt_quality_is_declared() -> None:
    (obs,) = NoaaCollector().normalize(
        {"DATE": "2026-01-20", "STATION": "USW00094728", "TMAX": "26"}, FETCHED
    )
    assert obs.kt_quality == "assumed"
    assert obs.feed == "noaa"


def test_noaa_implausible_temperature_rejected() -> None:
    raw = {"DATE": "2026-01-20", "STATION": "USW00094728", "TMAX": "999"}
    with pytest.raises(ValidationError):
        NoaaCollector().normalize(raw, FETCHED)


def test_missing_tmax_yields_nothing() -> None:
    assert (
        NoaaCollector().normalize({"DATE": "2026-01-20", "STATION": "USW00094728"}, FETCHED) == []
    )


def test_not_yet_knowable_is_dropped() -> None:
    raw = {"DATE": "2026-01-20", "STATION": "USW00094728", "TMAX": "26"}
    assert NoaaCollector().normalize(raw, datetime(2026, 1, 21, 11, tzinfo=UTC)) == []


def test_registered() -> None:
    assert isinstance(REGISTRY["noaa"](), NoaaCollector)


@responses.activate
def test_run_collector_end_to_end() -> None:
    _register()
    store = Store()
    result = run_collector(NoaaCollector(), store, START, END, now=lambda: FETCHED)
    assert result.status == "ok", result.error
    assert result.rows_written == len(_rows())
    snap = store.as_of(FETCHED)
    assert snap.latest("noaa", "USW00094728:TMAX") is not None
    assert store.as_of(datetime(2026, 1, 21, tzinfo=UTC)).series("noaa", "USW00094728:TMAX") == []
