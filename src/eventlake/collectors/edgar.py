"""SEC EDGAR collector: number of Form 4 filings per filing day for a fixed set of issuers."""

import os
from collections import defaultdict
from collections.abc import Iterator, Sequence
from datetime import UTC, date, datetime, time
from typing import Any

from eventlake.http import HttpClient, RateLimiter
from eventlake.types import Observation, ValidationError, parse_utc, utc

BASE_URL = "https://data.sec.gov"
FEED = "edgar"
FORM = "4"

EDGAR_CIKS: tuple[str, ...] = ("0000320193",)


def _contact() -> str:
    contact = os.environ.get("EVENTLAKE_CONTACT", "").strip()
    if not contact:
        raise RuntimeError(
            "EVENTLAKE_CONTACT is required: the SEC asks automated clients to identify "
            "themselves with a contact address"
        )
    return contact


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
        """One record per issuer carrying its recent-filings table, for ``normalize`` to count."""
        for cik in self._ciks:
            body = self._http.get_json(f"/submissions/CIK{cik}.json")
            filings = body.get("filings") if isinstance(body, dict) else None
            recent = filings.get("recent") if isinstance(filings, dict) else None
            yield {"cik": cik, "start": utc(start), "end": utc(end), "recent": recent}

    def normalize(self, raw: Any, fetched_at: datetime) -> list[Observation]:
        cik = str(raw["cik"])
        recent = raw["recent"]
        if not isinstance(recent, dict):
            raise ValidationError(f"{cik}: submissions have no filings.recent table")
        try:
            dates, forms, accepted = (
                recent["filingDate"],
                recent["form"],
                recent["acceptanceDateTime"],
            )
        except KeyError as exc:
            raise ValidationError(f"{cik}: filings.recent lacks column {exc}") from exc
        if not (len(dates) == len(forms) == len(accepted)):
            raise ValidationError(f"{cik}: filings.recent columns differ in length")
        first, last = raw["start"].date(), raw["end"].date()
        by_day: dict[date, list[datetime]] = defaultdict(list)
        for filed, form, at in zip(dates, forms, accepted, strict=True):
            try:
                day = date.fromisoformat(filed)
                if form == FORM and first <= day <= last:
                    by_day[day].append(parse_utc(at))
            except (TypeError, ValueError) as exc:
                raise ValidationError(
                    f"{cik}: malformed filingDate/acceptanceDateTime ({exc!r})"
                ) from exc
        out: list[Observation] = []
        for day, times in sorted(by_day.items()):
            event = datetime.combine(day, time(), tzinfo=UTC)
            # Acceptance after the filing-day cutoff can be stamped with the next business day.
            known = max(max(times), event)
            if known > utc(fetched_at):
                continue
            out.append(
                Observation(
                    feed=FEED,
                    series_id=f"{raw['cik']}:form4_count",
                    event_time=event,
                    knowledge_time=known,
                    value=float(len(times)),
                    kt_quality="source",
                )
            )
        return out
