"""Core types. Every datetime crossing a module boundary is timezone-aware UTC."""

import math
from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any, Literal

KtQuality = Literal["observed", "source", "assumed"]


class ValidationError(ValueError):
    """Raised when a value violates a core invariant."""


def utc(dt: datetime) -> datetime:
    """Return ``dt`` converted to UTC. Naive datetimes raise ``ValueError``."""
    if dt.tzinfo is None or dt.utcoffset() is None:
        raise ValueError("datetime must be timezone-aware")
    return dt.astimezone(UTC)


def parse_utc(s: str) -> datetime:
    """Parse an ISO-8601 string with an offset or trailing ``Z`` into UTC."""
    return utc(datetime.fromisoformat(s))


@dataclass(frozen=True, slots=True)
class Observation:
    feed: str
    series_id: str
    event_time: datetime
    knowledge_time: datetime
    value: float
    kt_quality: KtQuality = "observed"
    attrs: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if not self.feed:
            raise ValidationError("feed must be non-empty")
        if not self.series_id:
            raise ValidationError("series_id must be non-empty")
        for name in ("event_time", "knowledge_time"):
            dt: datetime = getattr(self, name)
            if dt.tzinfo is None or dt.utcoffset() is None:
                raise ValidationError(f"{name} must be timezone-aware")
            object.__setattr__(self, name, dt.astimezone(UTC))
        if not math.isfinite(self.value):
            raise ValidationError("value must be finite")
        if self.knowledge_time < self.event_time:
            raise ValidationError("knowledge_time must not precede event_time")


MALFORMED = "_malformed"


def malformed(series: str, field: str, error: str, entry: Any) -> dict[str, Any]:
    """A raw record standing for a vendor entry that ``fetch`` could not parse.

    ``fetch`` yields it in place of the entry so the run continues; the collector's
    ``normalize`` passes it to ``reject_malformed``, and the runner quarantines it.
    """
    return {MALFORMED: {"series": series, "field": field, "error": error}, "entry": entry}


def reject_malformed(feed: str, raw: Any) -> None:
    """Raise ``ValidationError`` naming the feed, series and field if ``raw`` is malformed."""
    tag = raw.get(MALFORMED) if isinstance(raw, dict) else None
    if isinstance(tag, dict):
        raise ValidationError(f"{feed}: {tag['series']}: malformed {tag['field']} ({tag['error']})")


def describe(exc: BaseException) -> str:
    return f"{type(exc).__name__}: {exc}"
