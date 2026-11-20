"""Polymarket collector: daily YES-price history for markets in tagged event groups."""

import json
from collections.abc import Iterator, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

from eventlake.entities import Market
from eventlake.http import HttpClient, RateLimiter
from eventlake.types import (
    Observation,
    ValidationError,
    describe,
    malformed,
    parse_utc,
    reject_malformed,
    utc,
)

FEED = "polymarket"
GAMMA_EVENTS = "https://gamma-api.polymarket.com/events"
CLOB_HISTORY = "https://clob.polymarket.com/prices-history"
DAY_FIDELITY = 1440
PAGE_SIZE = 50

POLYMARKET_TAGS: tuple[str, ...] = ("fed",)

# Monthly "Fed Decision in <month>?" events share this slug prefix; their markets are the
# per-meeting decision outcomes and carry the series ticker the linking rules look for.
FED_DECISION_SLUG_PREFIX = "fed-decision-in-"
FED_DECISION_SERIES = "fed-decision"


def _series_ticker(event: dict[str, Any]) -> str:
    slug = str(event["slug"])
    return FED_DECISION_SERIES if slug.startswith(FED_DECISION_SLUG_PREFIX) else slug


def _yes_token(market: dict[str, Any]) -> str | None:
    """CLOB token id of the YES outcome, or None when the market is not a YES/NO market."""
    try:
        outcomes = json.loads(market["outcomes"])
        tokens = json.loads(market["clobTokenIds"])
    except (KeyError, TypeError, ValueError):
        return None
    for outcome, token in zip(outcomes, tokens, strict=False):
        if str(outcome).lower() == "yes":
            return str(token)
    return None


@dataclass(frozen=True)
class _Listing:
    """A market together with the facts needed to fetch its history."""

    market: Market
    yes_token: str
    opened: datetime | None


def _listing(event: dict[str, Any], raw: dict[str, Any]) -> _Listing | None:
    token = _yes_token(raw)
    end = raw.get("endDate") or event.get("endDate")
    if token is None or not end:
        return None
    start = raw.get("startDate") or event.get("startDate")
    market = Market(
        market_id=f"{FEED}:{raw['slug']}",
        venue="polymarket",
        venue_ticker=str(raw["slug"]),
        series_ticker=_series_ticker(event),
        title=str(raw["question"]),
        close_time=parse_utc(end),
        attrs={
            "yes_token_id": token,
            "condition_id": raw.get("conditionId"),
            "event_slug": event["slug"],
        },
    )
    return _Listing(market, token, parse_utc(start) if start else None)


class PolymarketCollector:
    feed = FEED

    def __init__(
        self,
        tag_slugs: Sequence[str] = POLYMARKET_TAGS,
        http: HttpClient | None = None,
        page_size: int = PAGE_SIZE,
    ) -> None:
        self._tags = tuple(tag_slugs)
        self._page_size = page_size
        self._http = http if http is not None else HttpClient(limiter=RateLimiter(5, 1.0))

    def _iter_events(
        self, tag: str, *, closed: bool, end_min: datetime, end_max: datetime | None
    ) -> Iterator[dict[str, Any]]:
        offset = 0
        while True:
            params: dict[str, Any] = {
                "tag_slug": tag,
                "closed": "true" if closed else "false",
                "limit": self._page_size,
                "offset": offset,
                "end_date_min": utc(end_min).strftime("%Y-%m-%dT%H:%M:%SZ"),
            }
            if end_max is not None:
                params["end_date_max"] = utc(end_max).strftime("%Y-%m-%dT%H:%M:%SZ")
            page = self._http.get_json(GAMMA_EVENTS, params)
            yield from page
            if len(page) < self._page_size:
                return
            offset += self._page_size

    def _listings(self, start: datetime, closing_by: datetime | None) -> Iterator[_Listing]:
        """Markets closing after ``start``; resolved events are bounded by ``closing_by``."""
        seen: set[str] = set()
        for tag in self._tags:
            for closed in (False, True):
                bound = closing_by if closed else None
                for event in self._iter_events(tag, closed=closed, end_min=start, end_max=bound):
                    for raw in event.get("markets", []):
                        try:
                            found = _listing(event, raw)
                        except (KeyError, TypeError, ValueError):
                            continue  # one malformed entry must not abort the listing
                        if found is None or found.market.market_id in seen:
                            continue
                        if found.market.close_time < start:
                            continue
                        if bound is not None and found.market.close_time > bound:
                            continue
                        seen.add(found.market.market_id)
                        yield found

    def list_markets(self, start: datetime, end: datetime) -> list[Market]:
        """Markets closing in ``[start, end]``, plus open ones closing after ``end``."""
        return [found.market for found in self._listings(utc(start), utc(end))]

    def fetch(self, start: datetime, end: datetime) -> Iterator[Any]:
        """One record per daily point of every market whose life overlaps the window.

        A point whose timestamp cannot be read is yielded as a malformed record.
        """
        start, end = utc(start), utc(end)
        for found in self._listings(start, None):
            if found.opened is not None and found.opened > end:
                continue
            body = self._http.get_json(
                CLOB_HISTORY,
                {"market": found.yes_token, "interval": "max", "fidelity": DAY_FIDELITY},
            )
            for point in body.get("history", []):
                try:
                    at = datetime.fromtimestamp(int(point["t"]), UTC)
                except (KeyError, TypeError, ValueError, OverflowError, OSError) as exc:
                    yield malformed(found.yes_token, "t", describe(exc), point)
                    continue
                if start <= at <= end:
                    yield {"token_id": found.yes_token, "point": point}

    def normalize(self, raw: Any, fetched_at: datetime) -> list[Observation]:
        reject_malformed(FEED, raw)
        point = raw["point"]
        probability = float(point["p"])
        if not 0.0 <= probability <= 1.0:
            raise ValidationError(f"{raw['token_id']}: probability {probability} outside [0, 1]")
        at = datetime.fromtimestamp(int(point["t"]), UTC)
        if at > utc(fetched_at):
            return []  # the period has not ended, so there is no observation yet
        return [
            Observation(
                feed=FEED,
                series_id=str(raw["token_id"]),
                event_time=at,
                knowledge_time=at,
                value=probability,
                kt_quality="source",
            )
        ]
