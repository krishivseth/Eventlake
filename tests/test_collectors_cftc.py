import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, urlsplit

import pytest
import responses

from eventlake.collectors import REGISTRY
from eventlake.collectors.cftc import CFTC_CONTRACTS, CftcCollector
from eventlake.runner import run_collector
from eventlake.store import Store
from eventlake.types import ValidationError

FIX = Path(__file__).parent / "fixtures" / "cftc"
URL = "https://publicreporting.cftc.gov/resource/gpe5-46if.json"
START = datetime(2026, 2, 25, tzinfo=UTC)
END = datetime(2026, 3, 20, tzinfo=UTC)
FETCHED = datetime(2026, 3, 30, tzinfo=UTC)
SERIES = "13874A:lev_money_positions_short"


def _rows() -> list[dict[str, Any]]:
    data: list[dict[str, Any]] = json.loads((FIX / "tff_13874A.json").read_text())
    return data


def _register() -> None:
    responses.add(responses.GET, URL, json=_rows())


@responses.activate
def test_normalize_fixture_values() -> None:
    _register()
    c = CftcCollector()
    obs = [o for r in c.fetch(START, END) for o in c.normalize(r, FETCHED)]
    assert len(obs) == 3 * 4
    got = {(o.event_time, o.knowledge_time, o.value) for o in obs if o.series_id == SERIES}
    assert got == {
        (datetime(2026, 3, 3, tzinfo=UTC), datetime(2026, 3, 6, 20, 30, tzinfo=UTC), 563464.0),
        (datetime(2026, 3, 10, tzinfo=UTC), datetime(2026, 3, 13, 19, 30, tzinfo=UTC), 506286.0),
        (datetime(2026, 3, 17, tzinfo=UTC), datetime(2026, 3, 20, 19, 30, tzinfo=UTC), 490238.0),
    }


@responses.activate
def test_fetch_filters_to_contract_and_window() -> None:
    _register()
    list(CftcCollector().fetch(START, END))
    query = parse_qs(urlsplit(responses.calls[0].request.url).query)
    assert query["$where"] == [
        "cftc_contract_market_code in('13874A') "
        "AND report_date_as_yyyy_mm_dd between '2026-02-25T00:00:00' and '2026-03-20T00:00:00'"
    ]
    assert CFTC_CONTRACTS == ("13874A",)


def test_kt_quality_is_declared() -> None:
    raw = _rows()[1]
    obs = CftcCollector().normalize(raw, FETCHED)
    assert {o.kt_quality for o in obs} == {"source"}
    assert {o.feed for o in obs} == {"cftc"}


def test_cftc_rejects_negative_positions() -> None:
    raw = {**_rows()[1], "lev_money_positions_long": "-5"}
    with pytest.raises(ValidationError):
        CftcCollector().normalize(raw, FETCHED)


def test_cftc_knowledge_time_is_following_friday() -> None:
    obs = CftcCollector().normalize(_rows()[1], FETCHED)  # as-of Tue 2026-03-10
    assert {o.knowledge_time for o in obs} == {datetime(2026, 3, 13, 19, 30, tzinfo=UTC)}


def test_cftc_not_visible_before_publication() -> None:
    store = Store()
    c = CftcCollector()
    batch = c.normalize(_rows()[1], FETCHED)
    store.append(batch, ingested_at=FETCHED)
    thu = datetime(2026, 3, 12, 23, tzinfo=UTC)
    assert store.as_of(thu).latest("cftc", SERIES) is None
    fri = datetime(2026, 3, 13, 20, tzinfo=UTC)
    got = store.as_of(fri).latest("cftc", SERIES)
    assert got is not None and got.value == 506286.0


def test_report_not_yet_published_is_dropped() -> None:
    thu = datetime(2026, 3, 12, 23, tzinfo=UTC)
    assert CftcCollector().normalize(_rows()[1], thu) == []


def test_registered() -> None:
    assert isinstance(REGISTRY["cftc"](), CftcCollector)


@responses.activate
def test_run_collector_end_to_end() -> None:
    _register()
    store = Store()
    result = run_collector(CftcCollector(), store, START, END, now=lambda: FETCHED)
    assert result.status == "ok", result.error
    assert result.rows_fetched == 3
    assert result.rows_written == 12
    assert len(store.as_of(FETCHED).series("cftc", SERIES)) == 3


def test_malformed_field_is_validation_error() -> None:
    raw = {**_rows()[1], "asset_mgr_positions_long": "n/a"}
    with pytest.raises(ValidationError, match="asset_mgr_positions_long"):
        CftcCollector().normalize(raw, FETCHED)
    missing = {k: v for k, v in _rows()[1].items() if k != "lev_money_positions_short"}
    with pytest.raises(ValidationError, match="lev_money_positions_short"):
        CftcCollector().normalize(missing, FETCHED)


def test_monday_as_of_maps_to_friday() -> None:
    raw = {**_rows()[1], "report_date_as_yyyy_mm_dd": "2026-03-09T00:00:00.000"}  # Monday
    obs = CftcCollector().normalize(raw, FETCHED)
    assert {o.knowledge_time for o in obs} == {datetime(2026, 3, 13, 19, 30, tzinfo=UTC)}
