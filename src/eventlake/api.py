"""Read-only HTTP API over the store.

Every endpoint is ``async def`` so FastAPI runs it on the event loop thread. DuckDB
connections are not safe to share across threads, and one loop thread serialises all store
access without a lock. Handlers are short, synchronous reads.
"""

from dataclasses import asdict
from datetime import UTC, datetime
from typing import Any

from fastapi import FastAPI, HTTPException

from eventlake.panel import event_panel, stored_events
from eventlake.store import Store
from eventlake.types import utc

_ALL_KINDS = ("FOMC", "CPI", "NFP", "WEATHER_HIGH")


def _iso(dt: datetime) -> str:
    return utc(dt).isoformat()


def _parse_as_of(raw: str | None) -> datetime:
    if raw is None:
        return datetime.now(UTC)
    try:
        parsed = datetime.fromisoformat(raw)
    except ValueError:
        raise HTTPException(422, "as_of must be ISO-8601 with a UTC offset") from None
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise HTTPException(422, "as_of must include a UTC offset")
    return parsed.astimezone(UTC)


def create_app(store: Store) -> FastAPI:
    app = FastAPI(title="eventlake")

    @app.get("/health")
    async def health() -> dict[str, str]:
        return {"status": "ok"}

    @app.get("/events")
    async def events(kind: str | None = None) -> list[dict[str, Any]]:
        found = stored_events(store, [kind] if kind else _ALL_KINDS)
        return [
            {"event_id": e.event_id, "kind": e.kind, "scheduled_time": _iso(e.scheduled_time)}
            for e in found
        ]

    @app.get("/observations")
    async def observations(
        feed: str, series_id: str, as_of: str | None = None
    ) -> list[dict[str, Any]]:
        snap = store.as_of(_parse_as_of(as_of))
        return [
            {
                "event_time": _iso(o.event_time),
                "knowledge_time": _iso(o.knowledge_time),
                "value": o.value,
                "kt_quality": o.kt_quality,
                "attrs": dict(o.attrs),
            }
            for o in snap.series(feed, series_id)
        ]

    @app.get("/panel")
    async def panel(as_of: str | None = None, kinds: str | None = None) -> list[dict[str, Any]]:
        when = _parse_as_of(as_of)
        wanted = tuple(k for k in kinds.split(",") if k) if kinds else ("FOMC", "CPI")
        rows = event_panel(store, when, wanted)
        return [
            {
                **asdict(r),
                "scheduled_time": _iso(r.scheduled_time),
                "as_of": _iso(r.as_of),
            }
            for r in rows
        ]

    @app.get("/feeds")
    async def feeds() -> list[dict[str, Any]]:
        return []

    return app
