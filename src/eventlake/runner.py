"""Collector protocol and the single ingest path from a data feed into the store."""

import uuid
from collections.abc import Callable, Iterator
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any, Literal, Protocol
from urllib.parse import urlsplit, urlunsplit

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
    if not url:
        return text
    parts = urlsplit(str(url))
    return f"{text} ({urlunsplit((parts.scheme, parts.netloc, parts.path, '', ''))})"


def _quarantine(store: Store, feed: str, raw: Any, reason: str, at: datetime) -> None:
    """Quarantine ``raw``; if it cannot be serialised, store its ``repr`` instead."""
    try:
        store.quarantine(feed, raw, reason, at=at)
    except Exception:
        store.quarantine(feed, repr(raw), reason, at=at)


def _obs_raw(o: Observation) -> dict[str, Any]:
    return {
        "feed": o.feed,
        "series_id": o.series_id,
        "event_time": o.event_time.isoformat(),
        "knowledge_time": o.knowledge_time.isoformat(),
        "value": o.value,
        "kt_quality": o.kt_quality,
        "attrs": dict(o.attrs),
    }


def run_collector(
    collector: Collector,
    store: Store,
    start: datetime,
    end: datetime,
    *,
    now: Callable[[], datetime] = _utc_now,
) -> RunResult:
    """Fetch, normalise and append one window; always writes exactly one ``feed_runs`` row.

    Records whose normalisation fails with ``ValidationError``, ``KeyError``, ``TypeError`` or
    ``ValueError`` are quarantined and skipped. Any other exception from ``normalize`` or from
    the ``fetch`` iterator ends the run as ``failed``; observations gathered so far are still
    appended (each is valid on its own and a re-run is idempotent, so the next run completes
    the window). The batch is appended with one ``store.append`` call. If that raises
    ``ValidationError`` (nothing was written), each observation is appended individually and
    those that raise ``ValidationError`` are quarantined, so one bad row does not discard the
    rest; the run is then ``partial``. Any other ``append`` exception makes the run ``failed``
    with ``rows_written`` 0. ``run_collector`` never raises for vendor or store-validation
    errors; naive ``start``/``end`` raise ``ValueError`` before anything is recorded.
    """
    start = utc(start)
    end = utc(end)
    run_id = str(uuid.uuid4())
    started_at = utc(now())
    fetched = 0
    quarantined = 0
    written = 0
    errors: list[str] = []
    batch: list[Observation] = []

    try:
        for raw in collector.fetch(start, end):
            fetched += 1
            try:
                batch.extend(collector.normalize(raw, started_at))
            except _RECORD_ERRORS as exc:
                _quarantine(store, collector.feed, raw, f"{type(exc).__name__}: {exc}", started_at)
                quarantined += 1
    except Exception as exc:
        errors.append(_describe(exc))

    try:
        written = store.append(batch, ingested_at=started_at)
    except ValidationError:
        try:
            for o in sorted(batch, key=lambda x: x.knowledge_time):
                try:
                    written += store.append([o], ingested_at=started_at)
                except ValidationError as exc:
                    _quarantine(
                        store, collector.feed, _obs_raw(o), f"ValidationError: {exc}", started_at
                    )
                    quarantined += 1
        except Exception as exc:
            errors.append(_describe(exc))
    except Exception as exc:
        written = 0
        errors.append(_describe(exc))

    error = "; ".join(errors) if errors else None
    status: Status = "failed" if error else ("partial" if quarantined else "ok")
    try:
        finished_at = utc(now())
    except Exception:
        finished_at = started_at
    result = RunResult(run_id, collector.feed, status, fetched, written, quarantined, error)
    _record(store, result, started_at, finished_at)
    return result


def _record(store: Store, result: RunResult, started_at: datetime, finished_at: datetime) -> None:
    store.execute(
        "INSERT INTO feed_runs (run_id, feed, started_at, finished_at, status, rows_fetched, "
        "rows_written, rows_quarantined, error) VALUES (?,?,?,?,?,?,?,?,?)",
        [
            result.run_id,
            result.feed,
            started_at,
            finished_at,
            result.status,
            result.rows_fetched,
            result.rows_written,
            result.rows_quarantined,
            result.error,
        ],
    )


def record_failure(store: Store, feed: str, error: str, *, at: datetime) -> RunResult:
    """Write a ``failed`` run row for work outside ``run_collector``, such as a market listing."""
    when = utc(at)
    result = RunResult(str(uuid.uuid4()), feed, "failed", 0, 0, 0, error)
    _record(store, result, when, when)
    return result
