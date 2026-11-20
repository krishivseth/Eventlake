from dataclasses import replace
from datetime import UTC, datetime, timedelta
from typing import Any

import pytest

from eventlake.collectors import REGISTRY
from eventlake.health import (
    POLICIES,
    FeedPolicy,
    FeedStatus,
    check_all,
    check_feed,
    send_alert,
)
from eventlake.store import Store
from eventlake.types import Observation

NOW = datetime(2026, 3, 11, 12, 0, tzinfo=UTC)  # a Wednesday
POLICY = FeedPolicy("fake", timedelta(hours=36))


def add_run(
    store: Store,
    finished_ago: timedelta,
    *,
    status: str = "ok",
    fetched: int = 1000,
    quarantined: int = 0,
    error: str | None = None,
    feed: str = "fake",
    n: list[int] = [0],  # noqa: B006 - unique run ids
) -> None:
    n[0] += 1
    finished = NOW - finished_ago
    store.execute(
        "INSERT INTO feed_runs (run_id, feed, started_at, finished_at, status, rows_fetched,"
        " rows_written, rows_quarantined, error) VALUES (?,?,?,?,?,?,?,?,?)",
        [f"r{n[0]}", feed, finished - timedelta(minutes=1), finished, status, fetched, fetched,
         quarantined, error],
    )  # fmt: skip


def checks(status: FeedStatus) -> dict[str, Any]:
    return {c.name: c for c in status.checks}


def test_fresh_feed_is_ok() -> None:
    store = Store()
    add_run(store, timedelta(hours=2))
    status = check_feed(store, POLICY, NOW)
    assert status.status == "ok"
    assert set(checks(status)) == {"freshness", "last_run", "volume", "quarantine", "gaps"}


def test_stale_feed_fails() -> None:
    store = Store()
    add_run(store, timedelta(hours=40))
    status = check_feed(store, POLICY, NOW)
    assert checks(status)["freshness"].status == "fail"
    assert status.status == "fail"


def test_freshness_warns_beyond_half_the_limit() -> None:
    store = Store()
    add_run(store, timedelta(hours=20))
    assert checks(check_feed(store, POLICY, NOW))["freshness"].status == "warn"


def test_failed_last_run_fails_with_error_detail() -> None:
    store = Store()
    add_run(store, timedelta(hours=5))
    add_run(store, timedelta(hours=1), status="failed", fetched=0, error="HttpError: HTTP 500")
    status = check_feed(store, POLICY, NOW)
    assert checks(status)["last_run"].status == "fail"
    assert "HttpError: HTTP 500" in checks(status)["last_run"].detail
    assert status.status == "fail"


def test_volume_drop_warns() -> None:
    store = Store()
    for h in range(2, 13):
        add_run(store, timedelta(hours=h), fetched=1000)
    add_run(store, timedelta(hours=1), fetched=300)
    status = check_feed(store, POLICY, NOW)
    assert checks(status)["volume"].status == "warn"
    assert status.status == "warn"


def test_volume_is_skipped_with_fewer_than_three_prior_runs() -> None:
    store = Store()
    add_run(store, timedelta(hours=3), fetched=1000)
    add_run(store, timedelta(hours=2), fetched=1000)
    add_run(store, timedelta(hours=1), fetched=10)
    assert checks(check_feed(store, POLICY, NOW))["volume"].status == "ok"


def test_quarantine_ratio_fails() -> None:
    store = Store()
    add_run(store, timedelta(hours=1), fetched=1000, quarantined=60, status="partial")
    status = check_feed(store, POLICY, NOW)
    assert checks(status)["quarantine"].status == "fail"
    assert status.status == "fail"


def test_gap_detection_lists_missing_weekdays() -> None:
    store = Store()
    add_run(store, timedelta(hours=1))
    policy = replace(POLICY, daily_series=("S",))
    obs: list[Observation] = []
    for i in range(1, 40):
        day = datetime(2026, 3, 11, tzinfo=UTC) - timedelta(days=i)
        if day.weekday() >= 5 or day.date().isoformat() == "2026-03-04":  # drop Wed Mar 4
            continue
        obs.append(Observation("fake", "S", day, day + timedelta(days=1), 1.0))
    store.append(obs)
    gaps = checks(check_feed(store, policy, NOW))["gaps"]
    assert gaps.status == "warn"
    assert "2026-03-04" in gaps.detail
    assert "2026-03-03" not in gaps.detail
    assert "holiday" in gaps.detail


def test_never_run_feed_fails() -> None:
    status = check_feed(Store(), POLICY, NOW)
    assert checks(status)["freshness"].status == "fail"
    assert "never" in checks(status)["freshness"].detail
    assert status.status == "fail"


def test_naive_now_is_rejected() -> None:
    with pytest.raises(ValueError):
        check_feed(Store(), POLICY, datetime(2026, 3, 11))


def test_policies_cover_every_registered_feed() -> None:
    assert set(POLICIES) == set(REGISTRY)
    hours = {f: p.max_staleness for f, p in POLICIES.items()}
    assert hours["kalshi"] == timedelta(hours=36)
    assert hours["fred"] == timedelta(days=4)
    assert hours["cftc"] == timedelta(days=9)
    assert hours["edgar"] == timedelta(days=4)


def test_check_all_returns_one_status_per_policy() -> None:
    statuses = check_all(Store(), NOW)
    assert [s.feed for s in statuses] == sorted(POLICIES)
    assert all(s.status == "fail" for s in statuses)


class FakePost:
    def __init__(self, fail: bool = False) -> None:
        self.calls: list[tuple[str, Any]] = []
        self.fail = fail

    def __call__(self, url: str, **kwargs: Any) -> Any:
        self.calls.append((url, kwargs.get("json")))
        if self.fail:
            raise ConnectionError("down")
        return None


def statuses() -> list[FeedStatus]:
    return [FeedStatus("a", "ok", ()), FeedStatus("b", "fail", ()), FeedStatus("c", "warn", ())]


def test_send_alert_posts_only_failures() -> None:
    post = FakePost()
    assert send_alert(statuses(), "https://hook.test/x", http_post=post) is True
    assert len(post.calls) == 1
    text = post.calls[0][1]["text"]
    assert "b" in text and "- a " not in text and "- c " not in text


def test_send_alert_does_nothing_when_all_ok() -> None:
    post = FakePost()
    assert send_alert([FeedStatus("a", "ok", ()), FeedStatus("c", "warn", ())], "u", post) is False
    assert post.calls == []


def test_send_alert_swallows_post_errors() -> None:
    assert send_alert(statuses(), "https://hook.test/x", http_post=FakePost(fail=True)) is False
