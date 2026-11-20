import random
from dataclasses import replace
from datetime import UTC, datetime, timedelta
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
        for sid in range(5):
            expected: dict[datetime, float] = {}
            for (s_id, _ev, _kt), o in sorted(rows.items(), key=lambda kv: kv[0][2]):
                if s_id == sid and o.knowledge_time <= ts:
                    expected[o.event_time] = o.value  # brute force: latest kt <= ts wins
            got = {o.event_time: o.value for o in snap.series("f", f"s{sid}")}
            assert got == expected

    # Later calls re-send random keys, some with changed values (revisions). In one call no
    # two re-sent keys share a (series, event_time), so revisions cannot collide.
    final = {key: o.value for key, o in rows.items()}
    keys = sorted(rows)
    ingest = datetime(2026, 4, 1, tzinfo=UTC)
    for _ in range(20):
        ingest += timedelta(hours=1)
        batch: dict[tuple[int, int], tuple[tuple[int, int, int], float]] = {}
        for key in rng.sample(keys, 40):
            value = round(rng.random(), 3) if rng.random() < 0.5 else final[key]
            batch[(key[0], key[1])] = (key, value)
        for key, value in batch.values():
            final[key] = value
        s.append([replace(rows[k], value=v) for k, v in batch.values()], ingested_at=ingest)
        snap = s.as_of(ingest)
        for sid in snap.series_ids("f"):
            assert all(o.knowledge_time <= ingest for o in snap.series("f", sid))
    stored = s.execute("SELECT count(*) FROM observations")[0][0]
    resend = [replace(rows[key], value=value) for key, value in final.items()]
    assert s.append(resend, ingested_at=ingest + timedelta(days=1)) == 0
    assert s.execute("SELECT count(*) FROM observations")[0][0] == stored


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


def test_value_that_reverts_is_stored_as_a_new_version() -> None:
    s = Store()
    assert s.append([Observation("f", "x", D(1), D(2), 1.0)], ingested_at=D(2)) == 1
    assert s.append([Observation("f", "x", D(1), D(2), 2.0)], ingested_at=D(5)) == 1
    assert s.append([Observation("f", "x", D(1), D(2), 1.0)], ingested_at=D(8)) == 1
    stored = s.execute("SELECT knowledge_time, value FROM observations ORDER BY knowledge_time")
    assert stored == [(D(2), 1.0), (D(5), 2.0), (D(8), 1.0)]
    now = s.as_of(D(20)).at("f", "x", D(1))
    assert now is not None and now.value == 1.0 and now.kt_quality == "observed"
    assert s.as_of(D(6)).at("f", "x", D(1)).value == 2.0  # type: ignore[union-attr]
    assert s.append([Observation("f", "x", D(1), D(2), 1.0)], ingested_at=D(9)) == 0
    assert s.append([Observation("f", "x", D(1), D(2), 3.0)], ingested_at=D(11)) == 1
    assert s.append([Observation("f", "x", D(1), D(2), 2.0)], ingested_at=D(14)) == 1
    assert versions(s) == [
        (D(2), 1.0, None),
        (D(5), 2.0, D(2)),
        (D(8), 1.0, D(2)),
        (D(11), 3.0, D(2)),
        (D(14), 2.0, D(2)),
    ]
    assert values_over(s, [D(3), D(6), D(9), D(12), D(20)]) == [1.0, 2.0, 1.0, 3.0, 2.0]
    assert s.append([Observation("f", "x", D(1), D(2), 2.0)], ingested_at=D(15)) == 0
    assert len(versions(s)) == 5
    assert values_over(s, [D(20)]) == [2.0]


def versions(s: Store, series_id: str = "x") -> list[tuple[datetime, float, datetime | None]]:
    """Every stored version of ``f``/``series_id`` at event D(1), oldest knowledge first."""
    rows = s.execute(
        "SELECT knowledge_time, value, revision_of FROM observations"
        " WHERE feed = 'f' AND series_id = ? AND event_time = ? ORDER BY knowledge_time",
        [series_id, D(1)],
    )
    return [(r[0], r[1], r[2]) for r in rows]


def values_over(s: Store, times: list[datetime], series_id: str = "x") -> list[float | None]:
    """What ``as_of(t)`` returns for event D(1) at each ``t``."""
    found = [s.as_of(t).at("f", series_id, D(1)) for t in times]
    return [o.value if o is not None else None for o in found]


def obs(kt: datetime, value: float, series_id: str = "x") -> Observation:
    return Observation("f", series_id, D(1), kt, value)


def test_reingesting_an_older_vintage_is_a_noop() -> None:
    s = Store()
    assert s.append([obs(D(5), 1.0)], ingested_at=D(6)) == 1
    assert values_over(s, [D(4), D(20)]) == [None, 1.0]
    # An older vintage (earlier knowledge time) is a new key: inserted before V1.
    assert s.append([obs(D(3), 2.0)], ingested_at=D(7)) == 1
    stored = [(D(3), 2.0, None), (D(5), 1.0, None)]
    assert versions(s) == stored
    assert values_over(s, [D(2), D(4), D(20)]) == [None, 2.0, 1.0]
    assert s.append([obs(D(3), 2.0)], ingested_at=D(8)) == 0
    assert versions(s) == stored
    assert values_over(s, [D(4), D(20)]) == [2.0, 1.0]
    assert s.append([obs(D(5), 1.0), obs(D(3), 2.0)], ingested_at=D(9)) == 0
    assert versions(s) == stored
    assert values_over(s, [D(4), D(20)]) == [2.0, 1.0]


def test_duplicate_key_in_one_batch_keeps_the_last_and_is_idempotent() -> None:
    s = Store()
    batch = [obs(D(2), 1.0), obs(D(2), 2.0)]
    assert s.append(batch, ingested_at=D(3)) == 1
    assert versions(s) == [(D(2), 2.0, None)]
    assert values_over(s, [D(1), D(2), D(20)]) == [None, 2.0, 2.0]
    assert s.append(batch, ingested_at=D(4)) == 0
    assert versions(s) == [(D(2), 2.0, None)]


def test_two_lineages_revise_independently() -> None:
    s = Store()
    assert s.append([obs(D(2), 1.0), obs(D(4), 10.0)], ingested_at=D(5)) == 2
    assert versions(s) == [(D(2), 1.0, None), (D(4), 10.0, None)]
    assert values_over(s, [D(3), D(20)]) == [1.0, 10.0]
    # The source changes the D(2) vintage: a revision of D(2), observed at D(6).
    assert s.append([obs(D(2), 2.0)], ingested_at=D(6)) == 1
    assert versions(s) == [(D(2), 1.0, None), (D(4), 10.0, None), (D(6), 2.0, D(2))]
    assert values_over(s, [D(3), D(5), D(20)]) == [1.0, 10.0, 2.0]
    assert s.append([obs(D(4), 10.0)], ingested_at=D(7)) == 0
    assert s.append([obs(D(2), 2.0)], ingested_at=D(8)) == 0
    assert len(versions(s)) == 3
    assert values_over(s, [D(20)]) == [2.0]
    # Then the D(4) vintage changes: a revision of D(4), observed at D(9).
    assert s.append([obs(D(4), 20.0)], ingested_at=D(9)) == 1
    assert versions(s) == [
        (D(2), 1.0, None),
        (D(4), 10.0, None),
        (D(6), 2.0, D(2)),
        (D(9), 20.0, D(4)),
    ]
    assert values_over(s, [D(3), D(5), D(8), D(20)]) == [1.0, 10.0, 2.0, 20.0]
    assert s.append([obs(D(2), 2.0), obs(D(4), 20.0)], ingested_at=D(10)) == 0
    assert len(versions(s)) == 4


def test_revision_and_new_key_in_one_batch() -> None:
    s = Store()
    assert s.append([obs(D(2), 1.0)], ingested_at=D(3)) == 1
    # One call revises the D(2) vintage and brings a new D(6) vintage. The revision is dated
    # D(10); the new key is compared with what was visible at D(6), the D(2) source row.
    batch = [obs(D(6), 5.0), obs(D(2), 2.0)]
    assert s.append(batch, ingested_at=D(10)) == 2
    stored = [(D(2), 1.0, None), (D(6), 5.0, None), (D(10), 2.0, D(2))]
    assert versions(s) == stored
    assert values_over(s, [D(3), D(7), D(20)]) == [1.0, 5.0, 2.0]
    assert s.append(batch, ingested_at=D(11)) == 0
    assert s.append([obs(D(6), 5.0)], ingested_at=D(12)) == 0
    assert versions(s) == stored


def test_late_older_vintage_among_revisions() -> None:
    s = Store()
    assert s.append([obs(D(4), 1.0)], ingested_at=D(5)) == 1
    assert s.append([obs(D(4), 2.0)], ingested_at=D(6)) == 1
    # A vintage older than everything stored arrives after the revision.
    assert s.append([obs(D(2), 7.0)], ingested_at=D(7)) == 1
    stored = [(D(2), 7.0, None), (D(4), 1.0, None), (D(6), 2.0, D(4))]
    assert versions(s) == stored
    assert values_over(s, [D(3), D(5), D(20)]) == [7.0, 1.0, 2.0]
    assert s.append([obs(D(2), 7.0), obs(D(4), 2.0)], ingested_at=D(8)) == 0
    assert versions(s) == stored
    # The older vintage is then revised; its revision is the newest knowledge.
    assert s.append([obs(D(2), 8.0)], ingested_at=D(9)) == 1
    stored.append((D(9), 8.0, D(2)))
    assert versions(s) == stored
    assert values_over(s, [D(3), D(5), D(7), D(20)]) == [7.0, 1.0, 2.0, 8.0]
    assert s.append([obs(D(2), 8.0), obs(D(4), 2.0)], ingested_at=D(10)) == 0
    assert versions(s) == stored


def test_two_lineages_changed_in_one_call_collide_and_roll_back() -> None:
    s = Store()
    s.append([obs(D(2), 1.0), obs(D(4), 10.0)], ingested_at=D(5))
    # Both revisions would be stored at the same knowledge time D(9) with different values.
    with pytest.raises(ValidationError, match="collides"):
        s.append([obs(D(2), 2.0), obs(D(4), 20.0)], ingested_at=D(9))
    assert versions(s) == [(D(2), 1.0, None), (D(4), 10.0, None)]


def test_new_key_at_a_revision_time_with_another_value_raises() -> None:
    s = Store()
    s.append([obs(D(2), 1.0)], ingested_at=D(3))
    s.append([obs(D(2), 2.0)], ingested_at=D(6))
    with pytest.raises(ValidationError, match="collides"):
        s.append([obs(D(6), 3.0)], ingested_at=D(7))
    assert s.append([obs(D(6), 2.0)], ingested_at=D(7)) == 0
    assert versions(s) == [(D(2), 1.0, None), (D(6), 2.0, D(2))]


def test_collision_with_backdated_ingest_raises_and_rolls_back() -> None:
    s = Store()
    s.append([Observation("f", "x", D(1), D(5), 1.0)])
    batch = [Observation("f", "y", D(1), D(2), 9.0), Observation("f", "x", D(1), D(5), 2.0)]
    with pytest.raises(ValidationError, match=r"'x', '2026-03-01T00:00:00\+00:00'"):
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
