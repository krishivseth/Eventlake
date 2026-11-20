import json
import re
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, urlsplit

import pytest
import responses

from eventlake.collectors import REGISTRY
from eventlake.collectors.kalshi import KALSHI_SERIES, KalshiCollector
from eventlake.http import HttpClient, HttpError
from eventlake.runner import run_collector
from eventlake.store import Store
from eventlake.types import ValidationError

FIX = Path(__file__).parent / "fixtures" / "kalshi"
BASE = "https://api.elections.kalshi.com/trade-api/v2"
START = datetime(2026, 10, 20, tzinfo=UTC)
END = datetime(2026, 11, 1, tzinfo=UTC)
FETCHED = datetime(2026, 11, 2, tzinfo=UTC)


def _load(name: str) -> dict[str, Any]:
    data: dict[str, Any] = json.loads((FIX / name).read_text())
    return data


def _cutoff() -> None:
    responses.add(responses.GET, f"{BASE}/historical/cutoff", json=_load("cutoff.json"))


def _markets_only_fed() -> Any:
    """Serve the markets fixture for the Fed series and nothing for the others."""

    def cb(request: Any) -> tuple[int, dict[str, str], str]:
        series = request.params["series_ticker"]
        body = _load("markets_kxfeddecision.json") if series == "KXFEDDECISION" else {"markets": []}
        return 200, {}, json.dumps(body)

    return cb


def _register_markets() -> None:
    responses.add_callback(
        responses.GET,
        f"{BASE}/markets",
        callback=_markets_only_fed(),
        content_type="application/json",
    )


@responses.activate
def test_list_markets_parses_fixture() -> None:
    _cutoff()
    _register_markets()
    markets = KalshiCollector().list_markets(START, END)
    assert len(markets) == 3
    first = markets[0]
    assert first.market_id == "kalshi:KXFEDDECISION-26OCT-H26"
    assert first.venue == "kalshi"
    assert first.series_ticker == "KXFEDDECISION"
    assert first.close_time == datetime(2026, 10, 28, 17, 59, tzinfo=UTC)
    assert first.close_time.tzinfo is not None


@responses.activate
def test_list_markets_filters_to_window() -> None:
    _cutoff()
    _register_markets()
    late = datetime(2026, 11, 5, tzinfo=UTC)
    assert KalshiCollector().list_markets(late, datetime(2026, 11, 30, tzinfo=UTC)) == []


@responses.activate
def test_pagination_follows_cursor() -> None:
    _cutoff()
    full = _load("markets_kxfeddecision.json")
    page1 = {"cursor": "abc", "markets": full["markets"][:2]}
    page2 = {"cursor": "", "markets": full["markets"][2:]}
    responses.add(responses.GET, f"{BASE}/markets", json=page1)
    responses.add(responses.GET, f"{BASE}/markets", json=page2)
    markets = KalshiCollector(series=["KXFEDDECISION"]).list_markets(START, END)
    assert len(markets) == 3
    pages = [c for c in responses.calls if urlsplit(c.request.url).path.endswith("/markets")]
    assert len(pages) == 2
    assert "cursor=abc" in pages[1].request.url
    assert "cursor" not in pages[0].request.url


def _normalize_first(collector: KalshiCollector) -> list[Any]:
    candles = _load("candlesticks_h0.json")
    raw = {
        "series": "KXFEDDECISION",
        "ticker": candles["ticker"],
        "candle": candles["candlesticks"][0],
    }
    return collector.normalize(raw, FETCHED)


def test_normalize_scales_to_unit_probability() -> None:
    (obs,) = _normalize_first(KalshiCollector())
    assert obs.feed == "kalshi"
    assert obs.series_id == "KXFEDDECISION-26OCT-H0"
    assert obs.value == 0.86  # price.close_dollars "0.8600" in the recorded candle
    assert obs.event_time == obs.knowledge_time == datetime.fromtimestamp(1792468800, UTC)
    assert obs.kt_quality == "source"


def test_normalize_rejects_out_of_range() -> None:
    raw = {
        "series": "KXFEDDECISION",
        "ticker": "KXFEDDECISION-26OCT-H0",
        "candle": {"end_period_ts": 1792468800, "price": {"close_dollars": "1.4000"}},
    }
    with pytest.raises(ValidationError):
        KalshiCollector().normalize(raw, FETCHED)


def test_normalize_skips_candle_without_trades() -> None:
    raw = {
        "series": "KXFEDDECISION",
        "ticker": "KXFEDDECISION-26OCT-H0",
        "candle": {"end_period_ts": 1792468800, "price": {}},
    }
    assert KalshiCollector().normalize(raw, FETCHED) == []


@responses.activate
def test_run_collector_end_to_end() -> None:
    _cutoff()
    _register_markets()
    responses.add(
        responses.GET,
        re.compile(rf"{re.escape(BASE)}/series/KXFEDDECISION/markets/[^/]+/candlesticks"),
        json=_load("candlesticks_h0.json"),
    )
    store = Store()
    result = run_collector(KalshiCollector(), store, START, END, now=lambda: FETCHED)
    assert result.status == "ok", result.error
    assert result.rows_fetched == 12  # 3 markets x 4 candles
    assert result.rows_written > 0
    snap = store.as_of(FETCHED)
    assert snap.series_ids("kalshi", "KXFEDDECISION-26OCT") != []
    values = [o.value for sid in snap.series_ids("kalshi") for o in snap.series("kalshi", sid)]
    assert all(0.0 <= v <= 1.0 for v in values)


def test_registered_and_series_cover_three_event_families() -> None:
    assert isinstance(REGISTRY["kalshi"](), KalshiCollector)
    assert set(KALSHI_SERIES) == {"KXFEDDECISION", "KXCPI", "KXHIGHNY"}


CANDLES = re.compile(
    r"https://api\.elections\.kalshi\.com/trade-api/v2/series/KXFEDDECISION/markets/[^/]+/candlesticks"
)


def _fed_page_with_open_time(ticker: str, open_time: str) -> dict[str, Any]:
    page = _load("markets_kxfeddecision.json")
    for m in page["markets"]:
        if m["ticker"] == ticker:
            m["open_time"] = open_time
    return page


@responses.activate
def test_fetch_skips_market_opened_after_end() -> None:
    _cutoff()
    late = "KXFEDDECISION-26OCT-H25"
    page = _fed_page_with_open_time(late, "2026-12-01T00:00:00Z")
    responses.add(responses.GET, f"{BASE}/markets", json=page)
    responses.add(responses.GET, CANDLES, json=_load("candlesticks_h0.json"))
    records = list(KalshiCollector(series=["KXFEDDECISION"]).fetch(START, END))
    paths = [
        urlsplit(c.request.url).path for c in responses.calls if "candlesticks" in c.request.url
    ]
    assert len(paths) == 2
    assert not any(late in p for p in paths)
    assert len(records) == 2 * 4


@responses.activate
def test_fetch_sends_window_and_daily_interval() -> None:
    _cutoff()
    responses.add(responses.GET, f"{BASE}/markets", json=_load("markets_kxfeddecision.json"))
    responses.add(responses.GET, CANDLES, json=_load("candlesticks_h0.json"))
    list(KalshiCollector(series=["KXFEDDECISION"]).fetch(START, END))
    candle_calls = [c for c in responses.calls if "candlesticks" in c.request.url]
    query = parse_qs(urlsplit(candle_calls[0].request.url).query)
    assert query == {
        "start_ts": [str(int(START.timestamp()))],
        "end_ts": [str(int(END.timestamp()))],
        "period_interval": ["1440"],
    }
    markets_query = parse_qs(urlsplit(responses.calls[0].request.url).query)
    assert markets_query["series_ticker"] == ["KXFEDDECISION"]
    assert markets_query["min_close_ts"] == [str(int(START.timestamp()))]


def test_normalize_skips_candle_that_has_not_ended() -> None:
    candles = _load("candlesticks_h0.json")
    raw = {
        "series": "KXFEDDECISION",
        "ticker": candles["ticker"],
        "candle": candles["candlesticks"][0],
    }
    ends = datetime.fromtimestamp(candles["candlesticks"][0]["end_period_ts"], UTC)
    assert KalshiCollector().normalize(raw, ends - timedelta(seconds=1)) == []
    assert len(KalshiCollector().normalize(raw, ends)) == 1


@responses.activate
def test_list_markets_skips_malformed_market() -> None:
    _cutoff()
    page = _load("markets_kxfeddecision.json")
    del page["markets"][0]["close_time"]
    responses.add(responses.GET, f"{BASE}/markets", json=page)
    markets = KalshiCollector(series=["KXFEDDECISION"]).list_markets(START, END)
    assert [m.venue_ticker for m in markets] == [
        "KXFEDDECISION-26OCT-H25",
        "KXFEDDECISION-26OCT-H0",
    ]


def test_normalize_reads_recorded_historical_candle() -> None:
    body = _load("historical_candlesticks.json")
    candle = body["candlesticks"][0]
    assert "close_dollars" not in candle["price"]  # the historical shape
    raw = {"series": "KXCPI", "ticker": body["ticker"], "candle": candle}
    (obs,) = KalshiCollector().normalize(raw, FETCHED + timedelta(days=300))
    assert obs.series_id == body["ticker"]
    assert obs.value == float(candle["price"]["close"])


HIST_START = datetime(2026, 8, 1, tzinfo=UTC)
HIST_END = datetime(2026, 8, 31, tzinfo=UTC)
HIST_CANDLES = re.compile(rf"{re.escape(BASE)}/historical/markets/[^/]+/candlesticks")


def _cpi_only_historical(live: dict[str, Any] | None = None) -> None:
    _cutoff()
    responses.add(responses.GET, f"{BASE}/markets", json=live or {"markets": []})
    responses.add(
        responses.GET, f"{BASE}/historical/markets", json=_load("historical_markets_kxcpi.json")
    )


@responses.activate
def test_list_markets_falls_back_to_historical_before_the_cutoff() -> None:
    _cpi_only_historical()
    markets = KalshiCollector(series=["KXCPI"]).list_markets(HIST_START, HIST_END)
    assert [m.venue_ticker for m in markets] == [
        "KXCPI-26JUL-T-0.4",
        "KXCPI-26JUL-T-0.3",
        "KXCPI-26JUL-T-0.2",
    ]
    assert markets[0].close_time == datetime(2026, 8, 12, 12, 25, tzinfo=UTC)
    hist = [c for c in responses.calls if "/historical/markets" in c.request.url]
    assert "min_close_ts" not in hist[0].request.url


@responses.activate
def test_historical_listing_is_skipped_at_or_after_the_cutoff() -> None:
    _cutoff()
    responses.add(responses.GET, f"{BASE}/markets", json={"markets": []})
    KalshiCollector(series=["KXCPI"]).list_markets(START, END)
    assert not any("/historical/markets" in c.request.url for c in responses.calls)


@responses.activate
def test_historical_listing_drops_markets_closing_before_the_window() -> None:
    _cpi_only_historical()
    late = datetime(2026, 8, 13, tzinfo=UTC)
    assert KalshiCollector(series=["KXCPI"]).list_markets(late, HIST_END) == []


@responses.activate
def test_historical_listing_does_not_repeat_a_live_ticker() -> None:
    hist = _load("historical_markets_kxcpi.json")
    _cpi_only_historical({"markets": [hist["markets"][0]]})
    markets = KalshiCollector(series=["KXCPI"]).list_markets(HIST_START, HIST_END)
    assert [m.venue_ticker for m in markets].count("KXCPI-26JUL-T-0.4") == 1
    assert len(markets) == 3


@responses.activate
def test_fetch_uses_historical_candlesticks_for_historical_markets() -> None:
    _cpi_only_historical()
    responses.add(responses.GET, HIST_CANDLES, json=_load("historical_candlesticks.json"))
    collector = KalshiCollector(series=["KXCPI"])
    records = list(collector.fetch(HIST_START, HIST_END))
    paths = [
        urlsplit(c.request.url).path for c in responses.calls if "candlesticks" in c.request.url
    ]
    assert len(paths) == 3 and all("/historical/markets/" in p for p in paths)
    assert len(records) == 3 * 5
    assert collector.normalize(records[0], FETCHED + timedelta(days=300))[0].value > 0


@responses.activate
def test_run_collector_ingests_historical_markets() -> None:
    _cpi_only_historical()
    responses.add(responses.GET, HIST_CANDLES, json=_load("historical_candlesticks.json"))
    store = Store()
    now = datetime(2026, 11, 20, tzinfo=UTC)
    result = run_collector(KalshiCollector(series=["KXCPI"]), store, HIST_START, HIST_END,
                           now=lambda: now)  # fmt: skip
    assert result.status == "ok", result.error
    assert result.rows_written > 0
    assert "KXCPI-26JUL-T-0.4" in store.as_of(now).series_ids("kalshi")


def _no_sleep_http() -> HttpClient:
    return HttpClient(BASE, sleep=lambda _s: None)


@responses.activate
def test_recent_window_lists_live_markets_when_the_cutoff_endpoint_fails() -> None:
    responses.add(responses.GET, f"{BASE}/historical/cutoff", status=500)
    responses.add(responses.GET, f"{BASE}/markets", json=_load("markets_kxfeddecision.json"))
    collector = KalshiCollector(
        series=["KXFEDDECISION"], http=_no_sleep_http(), clock=lambda: FETCHED
    )
    assert len(collector.list_markets(START, END)) == 3


@responses.activate
def test_window_that_may_need_history_fails_loudly_when_the_cutoff_endpoint_fails() -> None:
    responses.add(responses.GET, f"{BASE}/historical/cutoff", status=500)
    responses.add(responses.GET, f"{BASE}/markets", json={"markets": []})
    collector = KalshiCollector(series=["KXCPI"], http=_no_sleep_http(), clock=lambda: FETCHED)
    with pytest.raises(HttpError):
        collector.list_markets(HIST_START, HIST_END)


@responses.activate
def test_malformed_market_met_during_fetch_is_quarantined_and_the_run_continues() -> None:
    _cutoff()
    page = _fed_page_with_open_time("KXFEDDECISION-26OCT-H26", "yesterday")
    del page["markets"][1]["ticker"]
    responses.add(responses.GET, f"{BASE}/markets", json=page)
    responses.add(responses.GET, CANDLES, json=_load("candlesticks_h0.json"))
    store = Store()
    collector = KalshiCollector(series=["KXFEDDECISION"])
    result = run_collector(collector, store, START, END, now=lambda: FETCHED)
    assert (result.status, result.rows_quarantined) == ("partial", 2)
    assert result.rows_written > 0  # the third market's candles are stored
    reasons = sorted(r[0] for r in store.execute("SELECT reason FROM quarantine"))
    assert "kalshi: KXFEDDECISION-26OCT-H26: malformed open_time" in reasons[0]
    assert "kalshi: KXFEDDECISION: malformed ticker" in reasons[1]


def test_null_price_is_validation_error_naming_the_field() -> None:
    raw = {
        "series": "KXFEDDECISION",
        "ticker": "KXFEDDECISION-26OCT-H0",
        "candle": {"end_period_ts": 1792468800, "price": None},
    }
    with pytest.raises(ValidationError, match="kalshi: KXFEDDECISION-26OCT-H0: malformed price"):
        KalshiCollector().normalize(raw, FETCHED)
