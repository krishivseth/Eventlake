# Design notes

## Append rule

The `observations` table has primary key `(feed, series_id, event_time, knowledge_time)`.
`Store.append` runs as one transaction and takes the incoming rows in `knowledge_time` order.
Each row is handled by one of two rules, depending on whether its exact key is already stored.

New key. The row is compared with the version visible at its knowledge time: the stored row
with the same `(feed, series_id, event_time)` and the greatest `knowledge_time` not after it.

- If that version has the same value, the row is skipped. Re-running a window therefore writes
  nothing, and resending an unchanged value with a later knowledge time adds no version.
- Otherwise the row is inserted as a new version.

Existing key (a source that re-publishes a number under the same assumed knowledge time). The
row is compared with the latest stored version of its `(feed, series_id, event_time)`, the one
with the greatest `knowledge_time` overall, not with the row at its own key.

- If the values are equal, the row is skipped.
- Otherwise the row is stored under `knowledge_time = ingested_at` with
  `kt_quality = "observed"`: the revision is dated when eventlake first saw it. So a value that
  goes V1, V2, then back to V1 is stored as three versions, and `as_of(now)` returns V1.
  `ValidationError` is raised, and the transaction rolls back, if `ingested_at` is not later
  than the key, or if the revision's own key (the same series and `event_time` at
  `ingested_at`) already holds a different value. The runner then appends row by row and
  quarantines only the offending ones.

A run whose `fetch` fails part-way still appends the observations normalised before the
failure, in the same single transaction as any other batch. This is deliberate: each of those
observations is individually valid, so storing it is never wrong, and because re-ingesting is
idempotent the next run over the same window adds exactly what the failed run missed. The run
is recorded as `failed`, so health reports it until a later run succeeds.

No code path issues `UPDATE` or `DELETE` against the store; `tests/test_store.py` checks the
source for both.

## As-of query

`Store.as_of(ts)` returns a `Snapshot` fixed at `ts`. Every snapshot read filters
`knowledge_time <= ts` first and then keeps, per `event_time`, the version with the greatest
`knowledge_time`:

```sql
SELECT ... FROM observations
WHERE feed = ? AND series_id = ? AND knowledge_time <= ?
QUALIFY row_number() OVER (PARTITION BY event_time ORDER BY knowledge_time DESC) = 1
ORDER BY event_time
```

`latest`, `at` and `series_ids` are built on the same filter, so there is no read that can see
past `ts`.

## Why derived series are not stored

The implied event move (`derived.implied_event_move`) and the realised move
(`derived.realized_move`) take a `Snapshot` and read only through it. Storing their results
would mean giving each derived value its own knowledge time, which is the maximum over every
input it used, and recomputing it whenever any input gains a revision. Computing on read
gets both for free: the result at `ts` is a function of what was known at `ts`. The inputs are
small (two IV observations per event, two closes), so the cost is negligible. The panel and the
Parquet export are built the same way: `event_panel` reads one snapshot at its `as_of`, and
`eve_panel` takes one snapshot per event at its lead time.

## Adding a collector

1. A class with a `feed` name and two methods, matching `eventlake.runner.Collector`.
   `fetch` yields raw vendor records for the window; `normalize` turns one record into
   observations and raises `ValidationError` (or `KeyError`, `TypeError`, `ValueError`) for a
   bad record, which the runner quarantines.
   `fetch` must not raise on one bad vendor entry: if it has to parse a field (to filter the
   window, say) and cannot, it yields `types.malformed(series, field, error, entry)` in the
   entry's place, and `normalize` begins with `types.reject_malformed(feed, raw)`, so the entry
   is quarantined with a reason naming the feed, series and field while the run continues.
2. A knowledge-time rule with an honest `kt_quality`: `source` when the vendor or its published
   schedule gives the time, `observed` for the fetch time, `assumed` for a fixed lag.
   `normalize` drops rows whose knowledge time is after `fetched_at`.
3. Registration: add the class to `REGISTRY` in `eventlake/collectors/__init__.py` (the CLI
   and `backfill` read it) and a `FeedPolicy` to `POLICIES` in `eventlake/health.py`.

```python
from collections.abc import Iterator
from datetime import datetime, timedelta
from typing import Any

from eventlake.http import HttpClient, RateLimiter
from eventlake.types import Observation, ValidationError, parse_utc, utc


class ExampleCollector:
    feed = "example"

    def __init__(self, http: HttpClient | None = None) -> None:
        self._http = http or HttpClient("https://api.example.org", limiter=RateLimiter(2, 1.0))

    def fetch(self, start: datetime, end: datetime) -> Iterator[Any]:
        params = {"from": utc(start).date().isoformat(), "to": utc(end).date().isoformat()}
        yield from self._http.get_json("/series", params)

    def normalize(self, raw: Any, fetched_at: datetime) -> list[Observation]:
        try:
            event = parse_utc(raw["date"] + "T00:00:00+00:00")
            value = float(raw["value"])
        except (KeyError, TypeError, ValueError) as exc:
            raise ValidationError(f"malformed record ({exc!r})") from exc
        known = event + timedelta(days=1)  # published the next day: a fixed lag
        if known > utc(fetched_at):
            return []
        return [Observation(self.feed, "example:value", event, known, value, "assumed")]
```

Tests run against a recorded response under `tests/fixtures/<feed>/`, loaded through the
`responses` library, so they never touch the network.
