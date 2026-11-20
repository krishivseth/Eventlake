import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pytest
import responses

from eventlake.collectors import REGISTRY
from eventlake.collectors.edgar import EDGAR_CIKS, EdgarCollector
from eventlake.runner import run_collector
from eventlake.store import Store
from eventlake.types import ValidationError

FIX = Path(__file__).parent / "fixtures" / "edgar"
URL = "https://data.sec.gov/submissions/CIK0000320193.json"
START = datetime(2026, 11, 1, tzinfo=UTC)
END = datetime(2026, 11, 30, tzinfo=UTC)
FETCHED = datetime(2026, 12, 1, tzinfo=UTC)
SERIES = "0000320193:form4_count"


@pytest.fixture(autouse=True)
def _contact(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("EVENTLAKE_CONTACT", "tester tester@example.invalid")


def _body() -> dict[str, Any]:
    data: dict[str, Any] = json.loads((FIX / "submissions_CIK0000320193.json").read_text())
    return data


def _normalize_all(c: EdgarCollector) -> list[Any]:
    return [o for r in c.fetch(START, END) for o in c.normalize(r, FETCHED)]


def test_edgar_requires_contact(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("EVENTLAKE_CONTACT")
    with pytest.raises(RuntimeError, match="EVENTLAKE_CONTACT is required"):
        EdgarCollector()
    with pytest.raises(RuntimeError, match="EVENTLAKE_CONTACT is required"):
        REGISTRY["edgar"]()


def test_registered() -> None:
    assert isinstance(REGISTRY["edgar"](), EdgarCollector)
    assert EDGAR_CIKS == ("0000320193",)


@responses.activate
def test_sends_contact_user_agent() -> None:
    responses.add(responses.GET, URL, json=_body())
    _normalize_all(EdgarCollector())
    assert responses.calls[0].request.headers["User-Agent"] == (
        "eventlake tester tester@example.invalid"
    )


@responses.activate
def test_normalize_fixture_values() -> None:
    responses.add(responses.GET, URL, json=_body())
    obs = _normalize_all(EdgarCollector())
    got = {(o.event_time, o.knowledge_time, o.value) for o in obs}
    assert got == {
        (datetime(2026, 11, 5, tzinfo=UTC), datetime(2026, 11, 5, 23, 30, 13, tzinfo=UTC), 1.0),
        (datetime(2026, 11, 12, tzinfo=UTC), datetime(2026, 11, 12, 23, 58, 20, tzinfo=UTC), 1.0),
        (datetime(2026, 11, 18, tzinfo=UTC), datetime(2026, 11, 18, 23, 30, 10, tzinfo=UTC), 1.0),
        (datetime(2026, 11, 19, tzinfo=UTC), datetime(2026, 11, 19, 23, 30, 21, tzinfo=UTC), 1.0),
    }
    assert {o.series_id for o in obs} == {SERIES}


def test_kt_quality_is_declared() -> None:
    raw = {"cik": "0000320193", "start": START, "end": END, "recent": _body()["filings"]["recent"]}
    obs = EdgarCollector().normalize(raw, FETCHED)
    assert {o.kt_quality for o in obs} == {"source"}
    assert {o.feed for o in obs} == {"edgar"}


def test_edgar_counts_form4_per_day() -> None:
    recent = {
        "filingDate": ["2026-11-05", "2026-11-05", "2026-11-05", "2026-11-06"],
        "form": ["4", "4", "8-K", "4/A"],
        "acceptanceDateTime": [
            "2026-11-05T14:00:00.000Z",
            "2026-11-05T21:15:00.000Z",
            "2026-11-05T23:59:00.000Z",
            "2026-11-06T10:00:00.000Z",
        ],
    }
    raw = {"cik": "0000320193", "start": START, "end": END, "recent": recent}
    (obs,) = EdgarCollector().normalize(raw, FETCHED)
    assert obs.value == 2.0
    assert obs.knowledge_time == datetime(2026, 11, 5, 21, 15, tzinfo=UTC)


def test_acceptance_before_filing_date_is_clamped_to_event_time() -> None:
    recent = {
        "filingDate": ["2026-11-06"],
        "form": ["4"],
        "acceptanceDateTime": ["2026-11-05T23:30:00.000Z"],
    }
    raw = {"cik": "0000320193", "start": START, "end": END, "recent": recent}
    (obs,) = EdgarCollector().normalize(raw, FETCHED)
    assert obs.knowledge_time == obs.event_time == datetime(2026, 11, 6, tzinfo=UTC)


def test_not_yet_accepted_day_is_dropped() -> None:
    raw = {"cik": "0000320193", "start": START, "end": END, "recent": _body()["filings"]["recent"]}
    obs = EdgarCollector().normalize(raw, datetime(2026, 11, 12, 23, 0, tzinfo=UTC))
    assert [o.event_time.day for o in obs] == [5]


@responses.activate
def test_malformed_submissions_are_quarantined() -> None:
    body = _body()
    body["filings"]["recent"]["form"] = body["filings"]["recent"]["form"][:-1]  # ragged arrays
    responses.add(responses.GET, URL, json=body)
    store = Store()
    result = run_collector(EdgarCollector(), store, START, END, now=lambda: FETCHED)
    assert result.status == "partial", result.error
    assert result.rows_quarantined == 1
    assert result.rows_written == 0


@responses.activate
def test_missing_filings_key_is_quarantined() -> None:
    responses.add(responses.GET, URL, json={"cik": "0000320193"})
    result = run_collector(EdgarCollector(), Store(), START, END, now=lambda: FETCHED)
    assert result.rows_quarantined == 1


@responses.activate
def test_run_collector_end_to_end() -> None:
    responses.add(responses.GET, URL, json=_body())
    store = Store()
    result = run_collector(EdgarCollector(), store, START, END, now=lambda: FETCHED)
    assert result.status == "ok", result.error
    assert result.rows_written == 4
    assert len(store.as_of(FETCHED).series("edgar", SERIES)) == 4


def test_malformed_acceptance_time_is_validation_error() -> None:
    recent = {"filingDate": ["2026-11-05"], "form": ["4"], "acceptanceDateTime": ["soon"]}
    raw = {"cik": "0000320193", "start": START, "end": END, "recent": recent}
    with pytest.raises(ValidationError, match="0000320193"):
        EdgarCollector().normalize(raw, FETCHED)
    del recent["form"]
    with pytest.raises(ValidationError, match="form"):
        EdgarCollector().normalize(raw, FETCHED)
