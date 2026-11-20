from dataclasses import replace
from datetime import UTC, datetime, timedelta
from typing import Any

import pytest

from eventlake.collectors import REGISTRY
from eventlake.health import (
    POLICIES,
    Check,
    FeedPolicy,
    FeedStatus,
    check_all,
    check_feed,
    send_alert,
    unconfigured_feeds,
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
    assert set(checks(status)) == {
        "freshness",
        "last_run",
        "volume",
        "empty_runs",
        "quarantine",
        "gaps",
    }


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
    policy = replace(POLICY, daily_series=("S",), market_days=True)
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


def test_check_all_skips_feeds_that_never_ran() -> None:
    store = Store()
    assert check_all(store, NOW) == []
    assert unconfigured_feeds(store) == sorted(POLICIES)
    add_run(store, timedelta(hours=1), feed="kalshi")
    add_run(store, timedelta(hours=1), feed="cftc", status="failed", error="boom")
    assert [s.feed for s in check_all(store, NOW)] == ["cftc", "kalshi"]
    assert unconfigured_feeds(store) == sorted(set(POLICIES) - {"cftc", "kalshi"})


def test_three_empty_runs_in_a_row_fail() -> None:
    store = Store()
    add_run(store, timedelta(hours=4), fetched=50)
    for h in (3, 2, 1):
        add_run(store, timedelta(hours=h), fetched=0)
    empty = checks(check_feed(store, POLICY, NOW))["empty_runs"]
    assert empty.status == "fail" and "3" in empty.detail


def test_empty_runs_ok_with_fewer_than_three_or_one_non_empty() -> None:
    store = Store()
    add_run(store, timedelta(hours=3), fetched=0)
    add_run(store, timedelta(hours=2), fetched=0)
    assert checks(check_feed(store, POLICY, NOW))["empty_runs"].status == "ok"
    add_run(store, timedelta(hours=1, minutes=30), status="failed", fetched=0, error="x")
    assert checks(check_feed(store, POLICY, NOW))["empty_runs"].status == "ok"
    add_run(store, timedelta(hours=1), fetched=7)
    add_run(store, timedelta(minutes=30), fetched=0)
    assert checks(check_feed(store, POLICY, NOW))["empty_runs"].status == "ok"


def _empty_runs_after(policy: FeedPolicy, empty: int) -> Check:
    store = Store()
    add_run(store, timedelta(hours=empty + 1), fetched=50, feed=policy.feed)
    for h in range(empty, 0, -1):
        add_run(store, timedelta(hours=h), fetched=0, feed=policy.feed)
    check: Check = checks(check_feed(store, policy, NOW))["empty_runs"]
    return check


def test_empty_runs_limit_is_per_feed() -> None:
    assert POLICIES["cftc"].max_empty_runs == 10
    assert _empty_runs_after(POLICIES["cftc"], 5).status == "ok"
    tenth = _empty_runs_after(POLICIES["cftc"], 10)
    assert tenth.status == "fail" and "10" in tenth.detail
    edgar = _empty_runs_after(POLICIES["edgar"], 10)
    assert edgar.status == "ok" and edgar.detail == "not checked for this feed"
    assert POLICIES["kalshi"].max_empty_runs == 3
    assert _empty_runs_after(POLICIES["kalshi"], 3).status == "fail"
    assert _empty_runs_after(POLICIES["kalshi"], 2).status == "ok"


def _gap_note(policy: FeedPolicy) -> str:
    store = Store()
    add_run(store, timedelta(hours=1), feed=policy.feed)
    detail: str = checks(check_feed(store, policy, NOW))["gaps"].detail
    return detail


def test_holiday_note_only_for_market_day_series() -> None:
    assert "market holidays" in _gap_note(POLICIES["fred"])
    assert "market holidays" not in _gap_note(POLICIES["noaa"])
    assert "weekdays" in _gap_note(POLICIES["noaa"])


def test_quarantine_detail_states_its_units() -> None:
    store = Store()
    add_run(store, timedelta(hours=1), fetched=100, quarantined=2, status="partial")
    detail = checks(check_feed(store, POLICY, NOW))["quarantine"].detail
    assert detail == "2 quarantined of 100 records fetched (2.0%)"


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


def test_send_alert_returns_false_on_http_error_status() -> None:
    class Response:
        status_code = 500

    class Post(FakePost):
        def __call__(self, url: str, **kwargs: Any) -> Any:
            super().__call__(url, **kwargs)
            return Response()

    post = Post()
    assert send_alert(statuses(), "https://hook.test/x", http_post=post) is False
    assert len(post.calls) == 1


def test_send_alert_swallows_post_errors() -> None:
    assert send_alert(statuses(), "https://hook.test/x", http_post=FakePost(fail=True)) is False
