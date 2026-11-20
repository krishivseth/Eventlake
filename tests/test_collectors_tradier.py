import json
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any

import pytest
import responses

from eventlake.collectors import REGISTRY
from eventlake.collectors.tradier import (
    FEED,
    TradierOptionsCollector,
    occ_symbol,
)
from eventlake.http import HttpClient
from eventlake.runner import run_collector
from eventlake.store import Store
from eventlake.types import ValidationError

FIX = Path(__file__).parent / "fixtures" / "tradier"
BASE = "https://sandbox.tradier.com/v1"
NOW = datetime(2026, 11, 19, 15, 0, tzinfo=UTC)


@pytest.fixture(autouse=True)
def _token(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("TRADIER_TOKEN", "test-token")
    monkeypatch.delenv("TRADIER_BASE", raising=False)


def _load(name: str) -> Any:
    return json.loads((FIX / name).read_text())


def _serve(options: dict[str, Any] | None = None, expirations: Any = None) -> None:
    """Serve the fixtures; ``options`` overrides the option-quote body per expiry code."""
    overrides = options or {}

    def quotes(request: Any) -> tuple[int, dict[str, str], str]:
        symbols = request.params["symbols"]
        if "," not in symbols:
            return 200, {}, json.dumps(_load("quotes_spot.json"))
        assert request.params["greeks"] == "true"
        code = symbols[3:9]
        body = overrides[code] if code in overrides else _load(f"quotes_SPY{code}.json")
        return 200, {}, json.dumps(body)

    responses.add_callback(
        responses.GET, f"{BASE}/markets/quotes", callback=quotes, content_type="application/json"
    )
    responses.add(
        responses.GET,
        f"{BASE}/markets/options/expirations",
        json=expirations if expirations is not None else _load("expirations.json"),
    )
    responses.add(responses.GET, f"{BASE}/markets/options/strikes", json=_load("strikes.json"))


def _collector(**kw: Any) -> TradierOptionsCollector:
    return TradierOptionsCollector(clock=lambda: NOW, **kw)


def test_occ_symbol() -> None:
    assert occ_symbol("SPY", date(2026, 1, 30), "C", 601.0) == "SPY260130C00601000"
    assert occ_symbol("SPY", date(2026, 1, 30), "P", 600.5) == "SPY260130P00600500"


@responses.activate
def test_tradier_normalize_fixture() -> None:
    _serve()
    store = Store()
    result = run_collector(_collector(n_expiries=2), store, NOW, NOW, now=lambda: NOW)
    assert (result.status, result.rows_fetched, result.rows_written) == ("ok", 2, 2)
    snap = store.as_of(NOW)
    assert snap.series_ids(FEED, "SPY:") == [
        "SPY:2026-11-20:atm_iv",
        "SPY:2026-11-23:atm_iv",
    ]  # the 2026-11-18 expiry is before the run date
    obs = snap.latest(FEED, "SPY:2026-11-20:atm_iv")
    assert obs is not None
    assert obs.value == pytest.approx(0.21)
    assert obs.event_time == obs.knowledge_time == NOW
    assert obs.kt_quality == "observed"
    assert obs.attrs["spot"] == 600.4
    assert obs.attrs == {"spot": 600.4, "strike": 600.0}
    second = snap.latest(FEED, "SPY:2026-11-23:atm_iv")
    assert second is not None
    assert second.value == pytest.approx(0.18)
    auth = responses.calls[0].request.headers["Authorization"]
    assert auth == "Bearer test-token"


@responses.activate
def test_tradier_single_object_response() -> None:
    """A lone expiration string, and a lone quote object, are accepted as one-item lists."""
    call_only = {"quotes": {"quote": _load("quotes_SPY261123.json")["quotes"]["quote"][0]}}
    _serve(options={"261123": call_only}, expirations={"expirations": {"date": "2026-11-23"}})
    store = Store()
    result = run_collector(_collector(n_expiries=2), store, NOW, NOW, now=lambda: NOW)
    assert (result.rows_fetched, result.rows_written, result.rows_quarantined) == (1, 0, 1)
    reason = store.execute("SELECT reason FROM quarantine")[0][0]
    assert "SPY:2026-11-23:atm_iv" in reason


@responses.activate
def test_missing_leg_quarantines_only_that_expiry() -> None:
    call_only = {"quotes": {"quote": _load("quotes_SPY261123.json")["quotes"]["quote"][0]}}
    _serve(options={"261123": call_only})
    store = Store()
    result = run_collector(_collector(n_expiries=2), store, NOW, NOW, now=lambda: NOW)
    assert (result.status, result.rows_written, result.rows_quarantined) == ("partial", 1, 1)
    assert store.as_of(NOW).series_ids(FEED, "SPY:") == ["SPY:2026-11-20:atm_iv"]


@responses.activate
def test_null_option_quotes_quarantine() -> None:
    _serve(options={"261120": {"quotes": None}})
    store = Store()
    result = run_collector(_collector(n_expiries=1), store, NOW, NOW, now=lambda: NOW)
    assert (result.rows_written, result.rows_quarantined) == (0, 1)


def _raw(call_iv: Any, put_iv: Any) -> dict[str, Any]:
    quotes = _load("quotes_SPY261120.json")["quotes"]["quote"]
    quotes[0]["greeks"] = {"mid_iv": call_iv} if call_iv is not None else None
    quotes[1]["greeks"] = {"mid_iv": put_iv}
    return {
        "underlying": "SPY",
        "expiry": "2026-11-20",
        "spot": 600.4,
        "strike": 600.0,
        "call": quotes[0],
        "put": quotes[1],
    }


@pytest.mark.parametrize(("call_iv", "put_iv"), [(None, 0.2), (0.2, 0.0), (-0.1, 0.2), (0.2, None)])
def test_bad_iv_is_rejected(call_iv: Any, put_iv: Any) -> None:
    with pytest.raises(ValidationError, match="SPY:2026-11-20:atm_iv"):
        _collector().normalize(_raw(call_iv, put_iv), NOW)


@responses.activate
def test_spot_falls_back_to_close() -> None:
    _serve()
    responses.replace(
        responses.GET,
        f"{BASE}/markets/quotes",
        json={"quotes": {"quote": {"symbol": "SPY", "last": None, "close": 599.1}}},
    )
    raws = list(_collector(n_expiries=1).fetch(NOW, NOW))
    assert raws[0]["spot"] == 599.1
    assert raws[0]["strike"] == 599.0


@responses.activate
def test_fetch_ignores_pure_historical_window() -> None:
    _serve()
    assert list(_collector().fetch(NOW - timedelta(days=30), NOW - timedelta(days=2))) == []
    assert len(responses.calls) == 0


def test_tradier_requires_token(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("TRADIER_TOKEN")
    with pytest.raises(RuntimeError, match="TRADIER_TOKEN is required"):
        TradierOptionsCollector()
    assert REGISTRY[FEED] is TradierOptionsCollector  # registering never reads the token


@responses.activate
def test_base_url_comes_from_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("TRADIER_BASE", "https://example.test/v1")
    responses.add(
        responses.GET,
        "https://example.test/v1/markets/quotes",
        json={"quotes": None},
    )
    (raw,) = _collector().fetch(NOW, NOW)
    assert len(responses.calls) == 1
    with pytest.raises(ValidationError, match="no spot quote for SPY"):
        _collector().normalize(raw, NOW)


@responses.activate
def test_empty_base_env_falls_back_to_the_default(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("TRADIER_BASE", "")
    responses.add(responses.GET, f"{BASE}/markets/quotes", json={"quotes": None})
    list(_collector().fetch(NOW, NOW))
    assert responses.calls[0].request.url.startswith(f"{BASE}/markets/quotes")


@responses.activate
def test_underlying_without_spot_is_quarantined_and_the_run_is_partial() -> None:
    _serve()
    responses.replace(responses.GET, f"{BASE}/markets/quotes", json={"quotes": None})
    store = Store()
    result = run_collector(_collector(underlyings=("SPX", "SPY")), store, NOW, NOW,
                           now=lambda: NOW)  # fmt: skip
    assert (result.status, result.rows_written, result.rows_quarantined) == ("partial", 0, 2)
    reasons = sorted(r[0] for r in store.execute("SELECT reason FROM quarantine"))
    assert "no spot quote for SPX" in reasons[0] and "no spot quote for SPY" in reasons[1]


@responses.activate
def test_malformed_expiration_is_quarantined_and_the_rest_are_stored() -> None:
    expirations = _load("expirations.json")
    expirations["expirations"]["date"].insert(0, "soon")
    _serve(expirations=expirations)
    store = Store()
    result = run_collector(_collector(n_expiries=2), store, NOW, NOW, now=lambda: NOW)
    assert (result.status, result.rows_written, result.rows_quarantined) == ("partial", 2, 1)
    (reason,) = store.execute("SELECT reason FROM quarantine")[0]
    assert "tradier.options: SPY: malformed expiration" in reason


def test_injected_http_needs_no_token(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("TRADIER_TOKEN")
    TradierOptionsCollector(http=HttpClient(BASE))
