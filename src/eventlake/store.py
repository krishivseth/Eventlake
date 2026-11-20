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
    revision_of TIMESTAMPTZ,
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
SELECT knowledge_time, value FROM observations
WHERE feed = ? AND series_id = ? AND event_time = ? AND knowledge_time <= ?
ORDER BY knowledge_time DESC LIMIT 1
"""

_LINEAGE_CURRENT = """
SELECT knowledge_time, value FROM observations
WHERE feed = ? AND series_id = ? AND event_time = ?
  AND ((knowledge_time = ? AND revision_of IS NULL) OR revision_of = ?)
ORDER BY knowledge_time DESC LIMIT 1
"""

_AT_KNOWLEDGE_TIME = """
SELECT value FROM observations
WHERE feed = ? AND series_id = ? AND event_time = ? AND knowledge_time = ?
"""

_Key = tuple[str, str, datetime, datetime]


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
        """Store the rows that change what is known; return how many rows were inserted.

        The call is one transaction. Rows sharing an exact key (feed, series_id, event_time,
        knowledge_time) are first collapsed to the last one given; the rest are handled in
        ``knowledge_time`` order.

        A row whose key K was stored before belongs to K's lineage: the row stored at K plus
        every revision of K. It is compared with the lineage's current value, the value of its
        row with the greatest knowledge time. Equal, it is skipped. Different, it is stored as a
        revision of K with ``knowledge_time = ingested_at`` and kt_quality "observed".

        A row whose key is new is compared with the stored version visible at its knowledge
        time (the greatest ``knowledge_time`` not after it): equal, it is skipped; otherwise it
        is inserted at its own knowledge time.

        ``ValidationError`` is raised, and nothing from the call is stored, if a revision's
        ``ingested_at`` is not later than its lineage's latest knowledge time, or if a row would
        be stored where a different value already sits at the same knowledge time.
        """
        ingested = utc(ingested_at) if ingested_at is not None else datetime.now(UTC)
        last: dict[_Key, Observation] = {}
        for o in obs:
            last[(o.feed, o.series_id, o.event_time, o.knowledge_time)] = o
        rows = sorted(last.values(), key=lambda o: o.knowledge_time)
        inserted = 0
        self._con.execute("BEGIN TRANSACTION")
        try:
            for incoming in rows:
                resolved = self._resolve(incoming, ingested)
                if resolved is None:
                    continue
                o, revision_of = resolved
                self._con.execute(
                    f"INSERT INTO observations ({_COLUMNS}, ingested_at, revision_of)"
                    " VALUES (?,?,?,?,?,?,?,?,?)",
                    [
                        o.feed,
                        o.series_id,
                        o.event_time,
                        o.knowledge_time,
                        o.value,
                        o.kt_quality,
                        _dumps(dict(o.attrs)),
                        ingested,
                        revision_of,
                    ],
                )
                inserted += 1
            self._con.execute("COMMIT")
        except BaseException:
            self._con.execute("ROLLBACK")
            raise
        return inserted

    def _resolve(
        self, o: Observation, ingested: datetime
    ) -> tuple[Observation, datetime | None] | None:
        """``None`` to skip ``o``, else the row to insert and the source key it revises."""
        series = [o.feed, o.series_id, o.event_time]
        key = (o.feed, o.series_id, o.event_time.isoformat(), o.knowledge_time.isoformat())
        current = self._con.execute(
            _LINEAGE_CURRENT, [*series, o.knowledge_time, o.knowledge_time]
        ).fetchone()
        if current is None:
            prior = self._con.execute(_STORED_VERSION, [*series, o.knowledge_time]).fetchone()
            if prior is not None and prior[1] == o.value:
                return None
            if prior is not None and prior[0] == o.knowledge_time:
                raise ValidationError(f"new key {key} collides with a stored revision")
            return o, None
        latest_known, value = current
        if value == o.value:
            return None
        if ingested <= latest_known:
            raise ValidationError(
                f"changed value for existing key {key} with ingested_at not later"
                " than its latest version"
            )
        occupied = self._con.execute(_AT_KNOWLEDGE_TIME, [*series, ingested]).fetchone()
        if occupied is not None:
            if occupied[0] == o.value:
                return None
            raise ValidationError(f"revision for key {key} collides at ingested_at")
        return replace(o, knowledge_time=ingested, kt_quality="observed"), o.knowledge_time

    def quarantine(self, feed: str, raw: Any, reason: str, *, at: datetime | None = None) -> None:
        when = utc(at) if at is not None else datetime.now(UTC)
        self._con.execute(
            'INSERT INTO quarantine (feed, raw, reason, "at") VALUES (?,?,?,?)',
            [feed, _dumps(raw), reason, when],
        )

    def as_of(self, ts: datetime) -> "Snapshot":
        return Snapshot(self._con, utc(ts))

    def execute(self, sql: str, params: Sequence[Any] = ()) -> list[tuple[Any, ...]]:
        """Run SQL against the reference tables; observations are read through ``Snapshot``."""
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
