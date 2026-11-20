from collections.abc import Callable, Iterator
from datetime import UTC, datetime, timedelta
from typing import Any

import pytest

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
                raise HttpError("HTTP 500", status=500, url="https://example.test/api?key=secret")
            yield {"i": i}

    def normalize(self, raw: Any, fetched_at: datetime) -> list[Observation]:
        if raw["i"] in self.bad:
            raise self.bad_kind(f"bad {raw['i']}")
        event = T0 + timedelta(minutes=raw["i"])
        return [Observation("fake", f"s{raw['i']}", event, event, float(raw["i"]))]


def _ticking() -> Callable[[], datetime]:
    calls = iter(range(1000))
    return lambda: RUN_AT + timedelta(seconds=next(calls))


def _runs(store: Store) -> list[tuple[Any, ...]]:
    return store.execute("SELECT run_id, status, rows_fetched, rows_written FROM feed_runs")


def test_ok_run_writes_rows_and_run_record() -> None:
    store = Store()
    res = run_collector(FakeCollector(), store, START, END, now=_ticking())
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
    assert rows == [(res.run_id, "fake", RUN_AT, RUN_AT + timedelta(seconds=1), "ok", None)]
    assert rows[0][3] > rows[0][2]


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
    assert (
        res.error is not None and "https://example.test/api" in res.error and "?" not in res.error
    )
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
    assert store.execute("SELECT reason FROM quarantine") == [("KeyError: 'bad 0'",)]


class Unserialisable(FakeCollector):
    def fetch(self, start: datetime, end: datetime) -> Iterator[Any]:
        yield {(1, 2): "tuple key cannot be JSON encoded"}
        yield {"i": 1}
        yield {"i": 2}

    def normalize(self, raw: Any, fetched_at: datetime) -> list[Observation]:
        if "i" not in raw:
            raise KeyError("i")
        return super().normalize(raw, fetched_at)


def test_unserialisable_raw_is_quarantined_as_repr_and_run_continues() -> None:
    store = Store()
    res = run_collector(Unserialisable(), store, START, END, now=_now)
    assert (res.status, res.rows_fetched, res.rows_written, res.rows_quarantined) == (
        "partial",
        3,
        2,
        1,
    )
    raw = store.execute("SELECT raw FROM quarantine")[0][0]
    assert "tuple key" in raw


class Colliding(FakeCollector):
    def __init__(self, value: float) -> None:
        super().__init__(n=2)
        self.value = value

    def normalize(self, raw: Any, fetched_at: datetime) -> list[Observation]:
        value = self.value if raw["i"] == 0 else 5.0
        return [Observation("fake", f"s{raw['i']}", T0, T0, value)]


def test_colliding_observation_is_quarantined_and_good_rows_kept() -> None:
    store = Store()
    run_collector(Colliding(1.0), store, START, END, now=lambda: RUN_AT)
    backdated = T0 - timedelta(days=1)
    first = store.as_of(RUN_AT).series_ids("fake")
    assert first == ["s0", "s1"]
    store.execute("DELETE FROM observations WHERE series_id = 's1'")
    res = run_collector(Colliding(2.0), store, START, END, now=lambda: backdated)
    assert (res.status, res.rows_fetched, res.rows_written, res.rows_quarantined) == (
        "partial",
        2,
        1,
        1,
    )
    assert res.error is None
    q = store.execute("SELECT reason, raw FROM quarantine")
    assert len(q) == 1 and q[0][0].startswith("ValidationError:") and '"s0"' in q[0][1]
    assert store.as_of(RUN_AT).series_ids("fake") == ["s0", "s1"]
    assert len(_runs(store)) == 2


def test_non_validation_append_error_marks_failed_and_records_run(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    store = Store()

    def boom(*args: Any, **kwargs: Any) -> int:
        raise RuntimeError("disk full")

    monkeypatch.setattr(store, "append", boom)
    res = run_collector(FakeCollector(), store, START, END, now=_now)
    assert (res.status, res.rows_written) == ("failed", 0)
    assert res.error is not None and "RuntimeError: disk full" in res.error
    assert len(_runs(store)) == 1


def test_fetch_and_append_errors_are_both_kept(monkeypatch: pytest.MonkeyPatch) -> None:
    store = Store()

    def boom(*args: Any, **kwargs: Any) -> int:
        raise RuntimeError("disk full")

    monkeypatch.setattr(store, "append", boom)
    res = run_collector(FakeCollector(fail_after=1), store, START, END, now=_now)
    assert res.error is not None
    assert "HttpError" in res.error and "; " in res.error and "disk full" in res.error


def test_second_clock_failure_still_records_run() -> None:
    store = Store()
    calls = 0

    def clock() -> datetime:
        nonlocal calls
        calls += 1
        if calls > 1:
            raise RuntimeError("clock broke")
        return RUN_AT

    res = run_collector(FakeCollector(), store, START, END, now=clock)
    assert res.status == "ok"
    rows = store.execute("SELECT started_at, finished_at FROM feed_runs")
    assert rows == [(RUN_AT, RUN_AT)]


def test_naive_window_raises_before_any_run_record() -> None:
    store = Store()
    with pytest.raises(ValueError):
        run_collector(FakeCollector(), store, datetime(2026, 1, 1), END, now=_now)
    assert store.execute("SELECT * FROM feed_runs") == []


def test_unexpected_normalize_error_fails_run_keeping_prior_rows() -> None:
    store = Store()
    res = run_collector(
        FakeCollector(n=4, bad={2}, bad_kind=AttributeError), store, START, END, now=_now
    )
    assert (res.status, res.rows_fetched, res.rows_written) == ("failed", 3, 2)
    assert res.error is not None and "AttributeError" in res.error
    assert len(_runs(store)) == 1
