"""Collector protocol and the single ingest path from a data feed into the store."""

import uuid
from collections.abc import Callable, Iterator
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any, Literal, Protocol

from eventlake.store import Store
from eventlake.types import Observation, ValidationError, utc

Status = Literal["ok", "partial", "failed"]

_RECORD_ERRORS = (ValidationError, KeyError, TypeError, ValueError)


class Collector(Protocol):
    feed: str

    def fetch(self, start: datetime, end: datetime) -> Iterator[Any]:
        """Yield raw vendor records for the window."""
        ...

    def normalize(self, raw: Any, fetched_at: datetime) -> list[Observation]:
        """Turn one raw record into observations; may raise ``ValidationError``."""
        ...


@dataclass(frozen=True)
class RunResult:
    run_id: str
    feed: str
    status: Status
    rows_fetched: int
    rows_written: int
    rows_quarantined: int
    error: str | None


def _utc_now() -> datetime:
    return datetime.now(UTC)


def _describe(exc: BaseException) -> str:
    url = getattr(exc, "url", None)
    text = f"{type(exc).__name__}: {exc}"
    return f"{text} ({url})" if url else text


def run_collector(
    collector: Collector,
    store: Store,
    start: datetime,
    end: datetime,
    *,
    now: Callable[[], datetime] = _utc_now,
) -> RunResult:
    """Fetch, normalise and append one window; always writes exactly one ``feed_runs`` row.

    Records whose normalisation fails are quarantined and skipped. A failure of ``fetch`` or
    of the final append ends the run as ``failed`` without raising.
    """
    run_id = str(uuid.uuid4())
    started_at = utc(now())
    fetched = 0
    quarantined = 0
    written = 0
    error: str | None = None
    batch: list[Observation] = []

    try:
        for raw in collector.fetch(start, end):
            fetched += 1
            try:
                batch.extend(collector.normalize(raw, started_at))
            except _RECORD_ERRORS as exc:
                store.quarantine(collector.feed, raw, f"{type(exc).__name__}: {exc}", at=started_at)
                quarantined += 1
    except Exception as exc:
        error = _describe(exc)

    try:
        written = store.append(batch, ingested_at=started_at)
    except Exception as exc:
        written = 0
        error = error or _describe(exc)

    status: Status = "failed" if error else ("partial" if quarantined else "ok")
    store.execute(
        "INSERT INTO feed_runs (run_id, feed, started_at, finished_at, status, rows_fetched, "
        "rows_written, rows_quarantined, error) VALUES (?,?,?,?,?,?,?,?,?)",
        [
            run_id,
            collector.feed,
            started_at,
            utc(now()),
            status,
            fetched,
            written,
            quarantined,
            error,
        ],
    )
    return RunResult(run_id, collector.feed, status, fetched, written, quarantined, error)
