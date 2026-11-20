"""Append-only point-in-time store backed by DuckDB.

Every observation row carries ``event_time`` and ``knowledge_time``. Reads go through
``Store.as_of(ts)``, which returns a ``Snapshot`` that cannot see rows whose
``knowledge_time`` is later than ``ts``.
"""

import json
from collections.abc import Iterable, Sequence
from dataclasses import replace
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, cast

import duckdb

from eventlake.types import KtQuality, Observation, ValidationError, utc

_SCHEMA = """
CREATE TABLE IF NOT EXISTS observations (
    feed TEXT NOT NULL,
    series_id TEXT NOT NULL,
    event_time TIMESTAMPTZ NOT NULL,
    knowledge_time TIMESTAMPTZ NOT NULL,
    value DOUBLE NOT NULL,
    kt_quality TEXT NOT NULL,
    attrs JSON,
    ingested_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (feed, series_id, event_time, knowledge_time)
);
CREATE TABLE IF NOT EXISTS quarantine (
    feed TEXT,
    raw JSON,
    reason TEXT,
    "at" TIMESTAMPTZ
);
CREATE TABLE IF NOT EXISTS feed_runs (
    run_id TEXT PRIMARY KEY,
    feed TEXT,
    started_at TIMESTAMPTZ,
    finished_at TIMESTAMPTZ,
    status TEXT,
    rows_fetched INTEGER,
    rows_written INTEGER,
    rows_quarantined INTEGER,
    error TEXT
);
CREATE TABLE IF NOT EXISTS events (
    event_id TEXT PRIMARY KEY,
    kind TEXT,
    scheduled_time TIMESTAMPTZ,
    attrs JSON
);
CREATE TABLE IF NOT EXISTS markets (
    market_id TEXT PRIMARY KEY,
    venue TEXT,
    venue_ticker TEXT,
    series_ticker TEXT,
    title TEXT,
    close_time TIMESTAMPTZ,
    attrs JSON
);
CREATE TABLE IF NOT EXISTS links (
    event_id TEXT,
    market_id TEXT,
    rule TEXT,
    linked_at TIMESTAMPTZ,
    PRIMARY KEY (event_id, market_id)
);
"""

_COLUMNS = "feed, series_id, event_time, knowledge_time, value, kt_quality, attrs"

_STORED_VERSION = """
SELECT value FROM observations
WHERE feed = ? AND series_id = ? AND event_time = ? AND knowledge_time <= ?
ORDER BY knowledge_time DESC LIMIT 1
"""


_SAME_KEY = """
SELECT value FROM observations
WHERE feed = ? AND series_id = ? AND event_time = ? AND knowledge_time = ?
"""


def _dumps(obj: Any) -> str:
    return json.dumps(obj, default=str, sort_keys=True)


def _to_utc(dt: datetime) -> datetime:
    return dt.astimezone(UTC)


class Store:
    def __init__(self, path: str | Path = ":memory:") -> None:
        target = str(path)
        if target != ":memory:":
            Path(target).parent.mkdir(parents=True, exist_ok=True)
        self._con = duckdb.connect(target)
        self._con.execute("SET TimeZone = 'UTC'")
        self._con.execute(_SCHEMA)

    def append(self, obs: Iterable[Observation], *, ingested_at: datetime | None = None) -> int:
        """Insert rows that change a series' value as of their knowledge time.

        Returns the number of rows inserted. The call is one transaction. A row whose key
        (feed, series_id, event_time, knowledge_time) is already stored with a different value
        is stored as a new revision with ``knowledge_time = ingested_at`` and kt_quality
        "observed"; if ``ingested_at`` is not later than that key, ``ValidationError`` is raised.
        """
        ingested = utc(ingested_at) if ingested_at is not None else datetime.now(UTC)
        rows = sorted(obs, key=lambda o: o.knowledge_time)
        inserted = 0
        self._con.execute("BEGIN TRANSACTION")
        try:
            for incoming in rows:
                o = self._resolve_collision(incoming, ingested)
                prior = self._con.execute(
                    _STORED_VERSION,
                    [o.feed, o.series_id, o.event_time, o.knowledge_time],
                ).fetchone()
                if prior is not None and prior[0] == o.value:
                    continue
                self._con.execute(
                    f"INSERT INTO observations ({_COLUMNS}, ingested_at) VALUES (?,?,?,?,?,?,?,?)",
                    [
                        o.feed,
                        o.series_id,
                        o.event_time,
                        o.knowledge_time,
                        o.value,
                        o.kt_quality,
                        _dumps(dict(o.attrs)),
                        ingested,
                    ],
                )
                inserted += 1
            self._con.execute("COMMIT")
        except BaseException:
            self._con.execute("ROLLBACK")
            raise
        return inserted

    def _resolve_collision(self, o: Observation, ingested: datetime) -> Observation:
        same_key = self._con.execute(
            _SAME_KEY, [o.feed, o.series_id, o.event_time, o.knowledge_time]
        )
        stored = same_key.fetchone()
        if stored is None or stored[0] == o.value:
            return o
        key = (o.feed, o.series_id, o.event_time.isoformat(), o.knowledge_time.isoformat())
        if ingested <= o.knowledge_time:
            raise ValidationError(
                f"changed value for existing key {key} with ingested_at not later"
            )
        revised = replace(o, knowledge_time=ingested, kt_quality="observed")
        again = self._con.execute(
            _SAME_KEY, [o.feed, o.series_id, o.event_time, revised.knowledge_time]
        ).fetchone()
        if again is not None and again[0] != o.value:
            raise ValidationError(f"revision for key {key} collides at ingested_at")
        return revised

    def quarantine(self, feed: str, raw: Any, reason: str, *, at: datetime | None = None) -> None:
        when = utc(at) if at is not None else datetime.now(UTC)
        self._con.execute(
            'INSERT INTO quarantine (feed, raw, reason, "at") VALUES (?,?,?,?)',
            [feed, _dumps(raw), reason, when],
        )

    def as_of(self, ts: datetime) -> "Snapshot":
        return Snapshot(self._con, utc(ts))

    def execute(self, sql: str, params: Sequence[Any] = ()) -> list[tuple[Any, ...]]:
        cur = self._con.execute(sql, list(params))
        if cur.description is None:
            return []
        return [
            tuple(_to_utc(v) if isinstance(v, datetime) and v.tzinfo else v for v in row)
            for row in cur.fetchall()
        ]

    def close(self) -> None:
        self._con.close()


class Snapshot:
    """A read-only view of the store as it was knowable at ``ts``."""

    def __init__(self, con: duckdb.DuckDBPyConnection, ts: datetime) -> None:
        self._con = con
        self.ts = ts

    def series(self, feed: str, series_id: str) -> list[Observation]:
        cur = self._con.execute(
            f"""
            SELECT {_COLUMNS} FROM observations
            WHERE feed = ? AND series_id = ? AND knowledge_time <= ?
            QUALIFY row_number() OVER (PARTITION BY event_time ORDER BY knowledge_time DESC) = 1
            ORDER BY event_time
            """,
            [feed, series_id, self.ts],
        )
        return [
            Observation(
                feed=r[0],
                series_id=r[1],
                event_time=_to_utc(r[2]),
                knowledge_time=_to_utc(r[3]),
                value=r[4],
                kt_quality=cast(KtQuality, r[5]),
                attrs=json.loads(r[6]) if r[6] else {},
            )
            for r in cur.fetchall()
        ]

    def latest(self, feed: str, series_id: str) -> Observation | None:
        rows = self.series(feed, series_id)
        return rows[-1] if rows else None

    def at(self, feed: str, series_id: str, event_time: datetime) -> Observation | None:
        want = utc(event_time)
        for o in self.series(feed, series_id):
            if o.event_time == want:
                return o
        return None

    def series_ids(self, feed: str, prefix: str = "") -> list[str]:
        cur = self._con.execute(
            """
            SELECT DISTINCT series_id FROM observations
            WHERE feed = ? AND knowledge_time <= ? AND starts_with(series_id, ?)
            ORDER BY series_id
            """,
            [feed, self.ts, prefix],
        )
        return [r[0] for r in cur.fetchall()]
