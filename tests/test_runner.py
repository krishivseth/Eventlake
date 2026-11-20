from collections.abc import Iterator
from datetime import UTC, datetime, timedelta
from typing import Any

from eventlake.http import HttpError
from eventlake.runner import RunResult, run_collector
from eventlake.store import Store
from eventlake.types import Observation, ValidationError

T0 = datetime(2026, 1, 1, tzinfo=UTC)
START = T0
END = T0 + timedelta(days=1)
RUN_AT = T0 + timedelta(days=2)


def _now() -> datetime:
    return RUN_AT


class FakeCollector:
    feed = "fake"

    def __init__(
        self,
        n: int = 3,
        bad: set[int] | None = None,
        fail_after: int | None = None,
        bad_kind: type[Exception] = KeyError,
    ) -> None:
        self.n = n
        self.bad = bad or set()
        self.fail_after = fail_after
        self.bad_kind = bad_kind

    def fetch(self, start: datetime, end: datetime) -> Iterator[Any]:
        for i in range(self.n):
            if self.fail_after is not None and i == self.fail_after:
                raise HttpError("HTTP 500", status=500, url="https://example.test/api")
            yield {"i": i}

    def normalize(self, raw: Any, fetched_at: datetime) -> list[Observation]:
        if raw["i"] in self.bad:
            raise self.bad_kind(f"bad {raw['i']}")
        event = T0 + timedelta(minutes=raw["i"])
        return [Observation("fake", f"s{raw['i']}", event, event, float(raw["i"]))]


def _runs(store: Store) -> list[tuple[Any, ...]]:
    return store.execute("SELECT run_id, status, rows_fetched, rows_written FROM feed_runs")


def test_ok_run_writes_rows_and_run_record() -> None:
    store = Store()
    res = run_collector(FakeCollector(), store, START, END, now=_now)
    assert isinstance(res, RunResult)
    assert (res.status, res.rows_fetched, res.rows_written, res.rows_quarantined) == (
        "ok",
        3,
        3,
        0,
    )
    assert res.error is None
    rows = store.execute(
        "SELECT run_id, feed, started_at, finished_at, status, error FROM feed_runs"
    )
    assert rows == [(res.run_id, "fake", RUN_AT, RUN_AT, "ok", None)]


def test_bad_record_is_quarantined_run_is_partial() -> None:
    store = Store()
    res = run_collector(FakeCollector(n=500, bad={137}), store, START, END, now=_now)
    assert (res.status, res.rows_fetched, res.rows_written, res.rows_quarantined) == (
        "partial",
        500,
        499,
        1,
    )
    q = store.execute('SELECT feed, raw, reason, "at" FROM quarantine')
    assert len(q) == 1
    assert q[0][0] == "fake"
    assert '"i": 137' in q[0][1]
    assert q[0][2] == "KeyError: 'bad 137'"
    assert q[0][3] == RUN_AT
    assert len(_runs(store)) == 1


def test_each_recoverable_error_type_is_quarantined() -> None:
    for kind in (ValidationError, TypeError, ValueError):
        store = Store()
        res = run_collector(FakeCollector(bad={1}, bad_kind=kind), store, START, END, now=_now)
        assert (res.status, res.rows_written, res.rows_quarantined) == ("partial", 2, 1)
        assert store.execute("SELECT reason FROM quarantine") == [(f"{kind.__name__}: bad 1",)]


def test_fetch_failure_marks_failed_keeps_prior() -> None:
    store = Store()
    res = run_collector(FakeCollector(n=5, fail_after=2), store, START, END, now=_now)
    assert (res.status, res.rows_fetched, res.rows_written) == ("failed", 2, 2)
    assert res.error is not None and "https://example.test/api" in res.error
    assert store.as_of(RUN_AT).series_ids("fake") == ["s0", "s1"]


def test_rerun_same_window_writes_zero() -> None:
    store = Store()
    run_collector(FakeCollector(), store, START, END, now=_now)
    res = run_collector(FakeCollector(), store, START, END, now=_now)
    assert (res.status, res.rows_fetched, res.rows_written) == ("ok", 3, 0)
    assert len(_runs(store)) == 2


def test_run_record_written_even_on_failure() -> None:
    store = Store()
    res = run_collector(FakeCollector(fail_after=0), store, START, END, now=_now)
    assert res.status == "failed"
    rows = store.execute("SELECT run_id, status, rows_written, error FROM feed_runs")
    assert len(rows) == 1
    assert rows[0][:3] == (res.run_id, "failed", 0)
    assert rows[0][3] == res.error


def test_failed_fetch_with_quarantine_is_failed_not_partial() -> None:
    store = Store()
    res = run_collector(FakeCollector(n=5, bad={0}, fail_after=3), store, START, END, now=_now)
    assert (res.status, res.rows_written, res.rows_quarantined) == ("failed", 2, 1)


def test_append_failure_marks_failed_and_still_records_run() -> None:
    store = Store()
    first = datetime(2026, 1, 1, tzinfo=UTC)

    class Revising(FakeCollector):
        def __init__(self, value: float) -> None:
            super().__init__(n=1)
            self.value = value

        def normalize(self, raw: Any, fetched_at: datetime) -> list[Observation]:
            return [Observation("fake", "s0", first, first, self.value)]

    run_collector(Revising(1.0), store, START, END, now=lambda: RUN_AT)
    backdated = first - timedelta(days=1)
    res = run_collector(Revising(2.0), store, START, END, now=lambda: backdated)
    assert (res.status, res.rows_fetched, res.rows_written) == ("failed", 1, 0)
    assert res.error is not None and "ValidationError" in res.error
    assert len(_runs(store)) == 2
