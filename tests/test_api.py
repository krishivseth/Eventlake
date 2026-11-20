from datetime import datetime

from fastapi.testclient import TestClient

from eventlake.api import create_app
from eventlake.panel import event_panel
from panel_helpers import _build, _utc


def _client() -> tuple[TestClient, object]:
    store = _build()
    return TestClient(create_app(store)), store


def test_health_and_feeds() -> None:
    client, _ = _client()
    assert client.get("/health").json() == {"status": "ok"}
    assert client.get("/feeds").json() == []


def test_api_panel_matches_function() -> None:
    store = _build()
    client = TestClient(create_app(store))
    as_of = _utc(2026, 1, 27, 19)
    body = client.get("/panel", params={"as_of": as_of.isoformat(), "kinds": "FOMC"}).json()
    rows = event_panel(store, as_of, kinds=("FOMC",))
    assert [b["event_id"] for b in body] == [r.event_id for r in rows]
    assert [b["market_prob"] for b in body] == [r.market_prob for r in rows]
    assert [b["implied_move"] for b in body] == [r.implied_move for r in rows]
    assert datetime.fromisoformat(body[0]["as_of"]) == as_of
    assert body[0]["scheduled_time"].endswith(("Z", "+00:00"))


def test_api_rejects_naive_as_of() -> None:
    client, _ = _client()
    for path in ("/panel", "/observations"):
        extra = {"feed": "fred", "series_id": "SP500"} if path == "/observations" else {}
        r = client.get(path, params={"as_of": "2026-01-27T19:00:00", **extra})
        assert r.status_code == 422
        r = client.get(path, params={"as_of": "garbage", **extra})
        assert r.status_code == 422


def test_api_observations_respects_as_of() -> None:
    client, _ = _client()
    q = {"feed": "fred", "series_id": "SP500"}
    early = client.get("/observations", params={**q, "as_of": "2026-01-27T12:00:00Z"}).json()
    late = client.get("/observations", params={**q, "as_of": "2026-02-05T00:00:00+00:00"}).json()
    assert [o["event_time"][:10] for o in early] == ["2026-01-25", "2026-01-26"]
    assert len(late) == 5
    assert late[0]["value"] == 5025.0


def test_api_events_filter_by_kind() -> None:
    client, _ = _client()
    body = client.get("/events", params={"kind": "CPI"}).json()
    assert [e["event_id"] for e in body] == ["CPI:2026-02-11"]
    assert len(client.get("/events").json()) == 3
