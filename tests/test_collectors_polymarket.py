import json
import re
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pytest
import responses

from eventlake.collectors import REGISTRY
from eventlake.collectors.polymarket import POLYMARKET_TAGS, PolymarketCollector
from eventlake.runner import run_collector
from eventlake.store import Store
from eventlake.types import ValidationError

FIX = Path(__file__).parent / "fixtures" / "polymarket"
GAMMA = "https://gamma-api.polymarket.com/events"
CLOB = "https://clob.polymarket.com/prices-history"
START = datetime(2026, 10, 1, tzinfo=UTC)
END = datetime(2026, 11, 30, tzinfo=UTC)
FETCHED = datetime(2026, 12, 1, tzinfo=UTC)
LIFE_START = datetime(2026, 6, 1, tzinfo=UTC)  # the recorded price points are from June


def _events() -> list[dict[str, Any]]:
    data: list[dict[str, Any]] = json.loads((FIX / "events_fed.json").read_text())
    return data


def _register_events(open_events: list[dict[str, Any]] | None = None) -> None:
    def cb(request: Any) -> tuple[int, dict[str, str], str]:
        if request.params["closed"] == "true" and request.params.get("offset", "0") == "0":
            return 200, {}, json.dumps(_events())
        return 200, {}, json.dumps(open_events or [])

    responses.add_callback(responses.GET, GAMMA, callback=cb, content_type="application/json")


@responses.activate
def test_list_markets_parses_fixture() -> None:
    _register_events()
    markets = PolymarketCollector(tag_slugs=["fed"]).list_markets(START, END)
    assert len(markets) == 7
    first = markets[0]
    assert first.venue == "polymarket"
    assert first.series_ticker == "fed-decision"
    assert first.close_time == datetime(2026, 10, 29, 3, 59, tzinfo=UTC)
    assert first.attrs["yes_token_id"].startswith("17010377994663817312")
    others = {m.series_ticker for m in markets if m.series_ticker != "fed-decision"}
    assert others == {"how-many-dissent-at-the-october-fed-meeting"}


@responses.activate
def test_pagination_follows_offset() -> None:
    events = _events()
    pages = [events[:1], events[1:], []]

    def cb(request: Any) -> tuple[int, dict[str, str], str]:
        if request.params["closed"] == "false":
            return 200, {}, "[]"
        return 200, {}, json.dumps(pages[int(request.params["offset"]) // 1])

    responses.add_callback(responses.GET, GAMMA, callback=cb, content_type="application/json")
    collector = PolymarketCollector(tag_slugs=["fed"], page_size=1)
    markets = collector.list_markets(START, END)
    assert len(markets) == 7
    closed_calls = [c for c in responses.calls if "closed=true" in c.request.url]
    assert len(closed_calls) == 3  # two full pages, then the empty page that ends the walk


@responses.activate
def test_normalize_scales_to_unit_probability() -> None:
    raw = {"token_id": "tok1", "point": {"t": 1781827225, "p": 0.055}}
    (obs,) = PolymarketCollector().normalize(raw, FETCHED)
    assert obs.feed == "polymarket"
    assert obs.series_id == "tok1"
    assert obs.value == 0.055
    assert obs.event_time == obs.knowledge_time == datetime.fromtimestamp(1781827225, UTC)
    assert obs.kt_quality == "source"


def test_normalize_rejects_out_of_range() -> None:
    raw = {"token_id": "tok1", "point": {"t": 1781827225, "p": 1.4}}
    with pytest.raises(ValidationError):
        PolymarketCollector().normalize(raw, FETCHED)


@responses.activate
def test_run_collector_end_to_end() -> None:
    _register_events()
    history = json.loads((FIX / "prices_history.json").read_text())
    responses.add(responses.GET, re.compile(re.escape(CLOB)), json=history)
    store = Store()
    result = run_collector(
        PolymarketCollector(tag_slugs=["fed"]), store, LIFE_START, END, now=lambda: FETCHED
    )
    assert result.status == "ok", result.error
    assert result.rows_fetched > 0
    assert result.rows_written > 0
    snap = store.as_of(FETCHED)
    token = _events()[0]["markets"][0]["clobTokenIds"]
    yes_token = json.loads(token)[0]
    assert snap.series("polymarket", yes_token)[0].value == 0.055


@responses.activate
def test_fetch_windows_points_and_skips_markets_outside_life() -> None:
    _register_events()
    history = json.loads((FIX / "prices_history.json").read_text())
    responses.add(responses.GET, re.compile(re.escape(CLOB)), json=history)
    # A window before any of these markets opened yields no price requests.
    before = list(
        PolymarketCollector(tag_slugs=["fed"]).fetch(
            datetime(2026, 1, 1, tzinfo=UTC), datetime(2026, 2, 1, tzinfo=UTC)
        )
    )
    assert before == []


def test_registered_and_default_tags() -> None:
    assert isinstance(REGISTRY["polymarket"](), PolymarketCollector)
    assert "fed" in POLYMARKET_TAGS
