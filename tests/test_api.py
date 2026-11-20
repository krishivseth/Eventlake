from datetime import UTC, datetime, timedelta

from fastapi.testclient import TestClient

from eventlake.api import create_app
from eventlake.entities import upsert_markets
from eventlake.health import POLICIES
from eventlake.panel import event_panel
from eventlake.store import Store
from panel_helpers import build_store, kalshi_market, utc_at


def _client() -> TestClient:
    return TestClient(create_app(build_store()))


def test_health_and_feeds() -> None:
    client = _client()
    assert client.get("/health").json() == {"status": "ok"}


def test_feeds_reports_health_per_feed() -> None:
    now = datetime(2026, 3, 11, 12, tzinfo=UTC)
    store = Store()
    store.execute(
        "INSERT INTO feed_runs (run_id, feed, started_at, finished_at, status, rows_fetched,"
        " rows_written, rows_quarantined, error) VALUES ('r1','kalshi',?,?,'ok',5,5,0,NULL)",
        [now - timedelta(hours=3), now - timedelta(hours=2)],
    )
    body = TestClient(create_app(store, clock=lambda: now)).get("/feeds").json()
    by_feed = {f["feed"]: f for f in body}
    assert set(by_feed) == set(POLICIES)
    assert by_feed["kalshi"]["status"] == "ok"
    assert by_feed["fred"] == {"feed": "fred", "status": "not_configured", "checks": []}
    assert [f["feed"] for f in body] == sorted(POLICIES)
    assert {c["name"] for c in by_feed["kalshi"]["checks"]} >= {"freshness", "gaps"}
    assert set(by_feed["kalshi"]["checks"][0]) == {"name", "status", "detail"}


def test_markets_endpoint_filters_on_linked() -> None:
    store = build_store()
    upsert_markets(store, [kalshi_market("KXFED-27JAN-HOLD", utc_at(2027, 1, 27, 19))])
    client = TestClient(create_app(store))
    every = client.get("/markets").json()
    linked = client.get("/markets", params={"linked": "true"}).json()
    unlinked = client.get("/markets", params={"linked": "false"}).json()
    assert linked and unlinked
    assert sorted(m["market_id"] for m in linked + unlinked) == sorted(
        m["market_id"] for m in every
    )
    assert all(m["event_id"] for m in linked)
    assert [m["market_id"] for m in unlinked] == ["kalshi:KXFED-27JAN-HOLD"]
    assert all(m["event_id"] is None for m in unlinked)
    assert set(every[0]) == {
        "market_id",
        "venue",
        "series_ticker",
        "title",
        "close_time",
        "event_id",
    }
    assert every[0]["close_time"].endswith("+00:00")


def test_as_of_defaults_to_the_app_clock() -> None:
    store = build_store()
    early = utc_at(2026, 1, 27, 12)
    client = TestClient(create_app(store, clock=lambda: early))
    q = {"feed": "fred", "series_id": "SP500"}
    default = client.get("/observations", params=q).json()
    explicit = client.get("/observations", params={**q, "as_of": early.isoformat()}).json()
    assert default == explicit and len(default) == 2


def test_api_panel_matches_function() -> None:
    store = build_store()
    client = TestClient(create_app(store))
    as_of = utc_at(2026, 1, 27, 19)
    body = client.get("/panel", params={"as_of": as_of.isoformat(), "kinds": "FOMC"}).json()
    rows = event_panel(store, as_of, kinds=("FOMC",))
    assert [b["event_id"] for b in body] == [r.event_id for r in rows]
    assert [b["market_prob"] for b in body] == [r.market_prob for r in rows]
    assert [b["implied_move"] for b in body] == [r.implied_move for r in rows]
    assert datetime.fromisoformat(body[0]["as_of"]) == as_of
    assert body[0]["scheduled_time"].endswith(("Z", "+00:00"))


def test_api_rejects_naive_as_of() -> None:
    client = _client()
    for path in ("/panel", "/observations"):
        extra = {"feed": "fred", "series_id": "SP500"} if path == "/observations" else {}
        r = client.get(path, params={"as_of": "2026-01-27T19:00:00", **extra})
        assert r.status_code == 422
        r = client.get(path, params={"as_of": "garbage", **extra})
        assert r.status_code == 422


def test_api_observations_respects_as_of() -> None:
    client = _client()
    q = {"feed": "fred", "series_id": "SP500"}
    early = client.get("/observations", params={**q, "as_of": "2026-01-27T12:00:00Z"}).json()
    late = client.get("/observations", params={**q, "as_of": "2026-02-05T00:00:00+00:00"}).json()
    assert [o["event_time"][:10] for o in early] == ["2026-01-25", "2026-01-26"]
    assert len(late) == 5
    assert late[0]["value"] == 5025.0


def test_api_events_filter_by_kind() -> None:
    client = _client()
    body = client.get("/events", params={"kind": "CPI"}).json()
    assert [e["event_id"] for e in body] == ["CPI:2026-02-11"]
    assert len(client.get("/events").json()) == 3


def test_api_out_of_range_as_of_is_422_not_500() -> None:
    r = _client().get("/panel", params={"as_of": "9999-12-31T23:59:59-05:00"})
    assert r.status_code == 422
    r = _client().get("/panel", params={"as_of": "2026-01-27T19:00:00 02:00"})
    assert r.status_code == 422 and "%2B" in r.json()["detail"]
