import random
from dataclasses import replace
from datetime import UTC, datetime
from pathlib import Path

import pytest

from eventlake.store import Store
from eventlake.types import Observation, ValidationError


def D(d: int, h: int = 0) -> datetime:
    return datetime(2026, 3, d, h, tzinfo=UTC)


def test_as_of_hides_future_knowledge() -> None:
    s = Store()
    s.append([Observation("f", "x", D(1), D(5), 1.0)])
    assert s.as_of(D(4)).series("f", "x") == []
    assert [o.value for o in s.as_of(D(5)).series("f", "x")] == [1.0]


def test_revision_keeps_both_versions() -> None:
    s = Store()
    s.append([Observation("f", "x", D(1), D(2), 1.0)])
    s.append([Observation("f", "x", D(1), D(9), 1.5)])
    first = s.as_of(D(3)).at("f", "x", D(1))
    second = s.as_of(D(9)).at("f", "x", D(1))
    assert first is not None and first.value == 1.0
    assert second is not None and second.value == 1.5


def test_reingest_is_idempotent() -> None:
    s = Store()
    rows = [Observation("f", "x", D(i), D(i), float(i)) for i in range(1, 6)]
    assert s.append(rows) == 5
    assert s.append(rows) == 0
    later = [replace(r, knowledge_time=D(20)) for r in rows]
    assert s.append(later) == 0


def test_roundtrip_equality_including_attrs_and_quality() -> None:
    s = Store()
    o = Observation("f", "x", D(1), D(2), 2.5, "source", {"a": 1, "b": ["z"]})
    s.append([o])
    assert s.as_of(D(2)).series("f", "x") == [o]
    assert s.as_of(D(2)).latest("f", "x") == o
    got = s.as_of(D(2)).series("f", "x")[0]
    assert got.event_time.tzinfo is not None and got.event_time.utcoffset() is not None


def test_same_call_multiple_versions_applied_in_knowledge_order() -> None:
    s = Store()
    rows = [
        Observation("f", "x", D(1), D(9), 1.5),
        Observation("f", "x", D(1), D(2), 1.0),
        Observation("f", "x", D(1), D(5), 1.0),  # same as D(2) version: skipped
        Observation("f", "x", D(1), D(12), 1.5),  # same as D(9) version: skipped
    ]
    assert s.append(rows) == 2
    assert s.as_of(D(4)).at("f", "x", D(1)).value == 1.0  # type: ignore[union-attr]
    assert s.as_of(D(10)).at("f", "x", D(1)).value == 1.5  # type: ignore[union-attr]


def test_late_arriving_older_version_is_compared_to_its_predecessor() -> None:
    s = Store()
    s.append([Observation("f", "x", D(1), D(9), 1.5)])
    # A version known earlier than the stored one has no predecessor, so it is inserted.
    assert s.append([Observation("f", "x", D(1), D(2), 1.0)]) == 1
    assert s.as_of(D(3)).at("f", "x", D(1)).value == 1.0  # type: ignore[union-attr]


def test_latest_and_series_ids() -> None:
    s = Store()
    s.append(
        [
            Observation("f", "a.1", D(1), D(1), 1.0),
            Observation("f", "a.1", D(2), D(2), 2.0),
            Observation("f", "a.2", D(1), D(1), 3.0),
            Observation("f", "b", D(1), D(8), 4.0),
            Observation("g", "a.9", D(1), D(1), 5.0),
        ]
    )
    snap = s.as_of(D(3))
    latest = snap.latest("f", "a.1")
    assert latest is not None and latest.value == 2.0
    assert snap.latest("f", "nope") is None
    assert snap.series_ids("f") == ["a.1", "a.2"]
    assert snap.series_ids("f", "a.") == ["a.1", "a.2"]
    assert s.as_of(D(9)).series_ids("f") == ["a.1", "a.2", "b"]


def test_no_leak_property() -> None:
    rng = random.Random(1234)
    s = Store()
    rows: dict[tuple[int, int, int], Observation] = {}
    while len(rows) < 300:
        ev = rng.randint(1, 20)
        kt = rng.randint(ev, 28)
        sid = rng.randint(0, 4)
        rows[(sid, ev, kt)] = Observation("f", f"s{sid}", D(ev), D(kt), round(rng.random(), 3))
    s.append(rows.values())
    for _ in range(50):
        ts = D(rng.randint(1, 28), rng.randint(0, 23))
        snap = s.as_of(ts)
        for sid in snap.series_ids("f"):
            series = snap.series("f", sid)
            assert [o.event_time for o in series] == sorted({o.event_time for o in series})
            for o in series:
                assert o.knowledge_time <= ts
            latest = snap.latest("f", sid)
            assert latest is not None and latest.knowledge_time <= ts
            at = snap.at("f", sid, series[0].event_time)
            assert at is not None and at.knowledge_time <= ts
        assert all(sid in snap.series_ids("f") for sid in snap.series_ids("f", "s"))


def test_persists_across_reopen(tmp_path: Path) -> None:
    path = tmp_path / "sub" / "lake.duckdb"
    s = Store(path)
    s.append([Observation("f", "x", D(1), D(2), 1.0)])
    s.close()
    s2 = Store(path)
    assert [o.value for o in s2.as_of(D(2)).series("f", "x")] == [1.0]
    s2.close()


def test_as_of_rejects_naive_ts() -> None:
    with pytest.raises(ValueError):
        Store().as_of(datetime(2026, 3, 1))


def test_quarantine_roundtrip() -> None:
    s = Store()
    s.quarantine("f", {"bad": 1}, "nan value", at=D(1))
    assert s.execute("SELECT feed, reason FROM quarantine") == [("f", "nan value")]


def test_execute_returns_utc_datetimes_and_accepts_params() -> None:
    s = Store()
    s.execute("INSERT INTO events VALUES (?, ?, ?, ?)", ["e1", "cpi", D(3), "{}"])
    rows = s.execute("SELECT event_id, scheduled_time FROM events WHERE event_id = ?", ["e1"])
    assert rows == [("e1", D(3))]
    assert rows[0][1].utcoffset().total_seconds() == 0  # type: ignore[union-attr]


def test_same_key_changed_value_becomes_observed_revision() -> None:
    s = Store()
    s.append([Observation("f", "x", D(1), D(2), 1.0, "source", {"v": 1})])
    revised = Observation("f", "x", D(1), D(2), 1.5, "source", {"v": 2})
    assert s.append([revised], ingested_at=D(6)) == 1
    old = s.as_of(D(5)).at("f", "x", D(1))
    new = s.as_of(D(6)).at("f", "x", D(1))
    assert old is not None and old.value == 1.0
    assert new is not None and new.value == 1.5
    assert new.kt_quality == "observed" and new.knowledge_time == D(6)
    assert new.attrs == {"v": 2}
    assert s.append([revised], ingested_at=D(7)) == 0


def test_collision_with_backdated_ingest_raises_and_rolls_back() -> None:
    s = Store()
    s.append([Observation("f", "x", D(1), D(5), 1.0)])
    batch = [Observation("f", "y", D(1), D(2), 9.0), Observation("f", "x", D(1), D(5), 2.0)]
    with pytest.raises(ValidationError, match="x"):
        s.append(batch, ingested_at=D(5))
    assert s.as_of(D(30)).series("f", "y") == []
    assert [o.value for o in s.as_of(D(30)).series("f", "x")] == [1.0]


def test_source_has_no_update_or_delete() -> None:
    root = Path(__file__).resolve().parent.parent / "src" / "eventlake"
    files = list(root.rglob("*.py"))
    assert files
    for f in files:
        text = f.read_text()
        assert "UPDATE " not in text, f
        assert "DELETE FROM" not in text, f
