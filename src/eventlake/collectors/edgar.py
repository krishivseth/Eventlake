"""SEC EDGAR collector: number of Form 4 filings per filing day for a fixed set of issuers."""

import os
from collections import defaultdict
from collections.abc import Iterator, Sequence
from datetime import UTC, date, datetime, time
from typing import Any

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

BASE_URL = "https://data.sec.gov"
FEED = "edgar"
FORM = "4"

EDGAR_CIKS: tuple[str, ...] = ("0000320193",)  # an example issuer; pass your own list


def _contact() -> str:
    contact = os.environ.get("EVENTLAKE_CONTACT", "").strip()
    if not contact:
        raise RuntimeError(
            "EVENTLAKE_CONTACT is required: the SEC asks automated clients to identify "
            "themselves with a contact address"
        )
    return contact


def _form4_rows(recent: Any) -> list[tuple[Any, Any]]:
    """``(filingDate, acceptanceDateTime)`` of every Form 4 row in ``filings.recent``."""
    if not isinstance(recent, dict):
        raise ValidationError("submissions have no filings.recent table")
    try:
        dates, forms, accepted = (
            recent["filingDate"],
            recent["form"],
            recent["acceptanceDateTime"],
        )
    except KeyError as exc:
        raise ValidationError(f"filings.recent lacks column {exc}") from exc
    if not (len(dates) == len(forms) == len(accepted)):
        raise ValidationError("filings.recent columns differ in length")
    return [(d, a) for d, f, a in zip(dates, forms, accepted, strict=True) if f == FORM]


class EdgarCollector:
    feed = FEED

    def __init__(self, ciks: Sequence[str] = EDGAR_CIKS, http: HttpClient | None = None) -> None:
        self._ciks = tuple(ciks)
        self._http = (
            http
            if http is not None
            else HttpClient(
                BASE_URL,
                headers={"User-Agent": f"eventlake {_contact()}"},
                limiter=RateLimiter(8, 1.0),
            )
        )

    def fetch(self, start: datetime, end: datetime) -> Iterator[Any]:
        """One record per issuer and Form 4 filing day in the window, from ``filings.recent``.

        Only the ``recent`` table of the submissions JSON is read. A table that cannot be read
        is one malformed record for the issuer; a row whose filing date cannot be read is one
        malformed record for that row.
        """
        first, last = utc(start).date(), utc(end).date()
        for cik in self._ciks:
            body = self._http.get_json(f"/submissions/CIK{cik}.json")
            filings = body.get("filings") if isinstance(body, dict) else None
            recent = filings.get("recent") if isinstance(filings, dict) else None
            try:
                rows = _form4_rows(recent)
            except (TypeError, ValidationError) as exc:
                yield malformed(cik, "filings.recent", str(exc), {"cik": cik})
                continue
            by_day: dict[date, list[Any]] = defaultdict(list)
            for filed, at in rows:
                try:
                    day = date.fromisoformat(filed)
                except (TypeError, ValueError) as exc:
                    entry = {"filingDate": filed, "acceptanceDateTime": at}
                    yield malformed(cik, "filingDate", describe(exc), entry)
                    continue
                if first <= day <= last:
                    by_day[day].append(at)
            for day, accepted in sorted(by_day.items()):
                yield {"cik": cik, "date": day.isoformat(), "accepted": accepted}

    def normalize(self, raw: Any, fetched_at: datetime) -> list[Observation]:
        reject_malformed(FEED, raw)
        cik = str(raw["cik"])
        day = date.fromisoformat(raw["date"])
        try:
            # acceptanceDateTime ends in "Z" and is UTC: checked against the "Accepted" time,
            # in New York time, on the EDGAR index pages of filings stamped in EST and EDT.
            times = [parse_utc(at) for at in raw["accepted"]]
        except (TypeError, ValueError) as exc:
            raise ValidationError(
                f"{FEED}: {cik}: malformed acceptanceDateTime on {day} ({exc!r})"
            ) from exc
        event = datetime.combine(day, time(), tzinfo=UTC)
        # Acceptance after the filing-day cutoff can be stamped with the next business day.
        known = max(max(times), event)
        if known > utc(fetched_at):
            return []
        return [
            Observation(
                feed=FEED,
                series_id=f"{cik}:form4_count",
                event_time=event,
                knowledge_time=known,
                value=float(len(times)),
                kt_quality="source",
            )
        ]
