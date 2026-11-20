"""Feed health checks and failure alerts, computed from ``feed_runs`` and stored series."""

import statistics
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any, Literal

import requests

from eventlake.store import Store
from eventlake.types import utc

Level = Literal["ok", "warn", "fail"]
_RANK: dict[str, int] = {"ok": 0, "warn": 1, "fail": 2}
_GAP_WINDOW_DAYS = 30
_GAP_PUBLISH_LAG_DAYS = 2  # the newest days are the freshness check's business, not a gap
_PRIOR_RUNS = 10
_MIN_PRIOR_RUNS = 3


@dataclass(frozen=True)
class FeedPolicy:
    feed: str
    max_staleness: timedelta
    min_rows_ratio: float = 0.5
    max_quarantine_ratio: float = 0.05
    daily_series: tuple[str, ...] = ()


@dataclass(frozen=True)
class Check:
    name: str
    status: Level
    detail: str


@dataclass(frozen=True)
class FeedStatus:
    feed: str
    status: Level
    checks: tuple[Check, ...]


POLICIES: dict[str, FeedPolicy] = {
    p.feed: p
    for p in (
        FeedPolicy("kalshi", timedelta(hours=36)),
        FeedPolicy("polymarket", timedelta(hours=36)),
        FeedPolicy("tradier.options", timedelta(hours=36)),
        FeedPolicy("fred", timedelta(days=4), daily_series=("SP500",)),
        FeedPolicy("noaa", timedelta(days=4), daily_series=("USW00094728:TMAX",)),
        FeedPolicy("cftc", timedelta(days=9)),
        FeedPolicy("edgar", timedelta(days=4)),
    )
}


@dataclass(frozen=True)
class _Run:
    finished_at: datetime
    status: str
    rows_fetched: int
    rows_quarantined: int
    error: str | None


def _runs(store: Store, feed: str) -> list[_Run]:
    """Every run of ``feed``, newest first."""
    rows = store.execute(
        "SELECT finished_at, status, rows_fetched, rows_quarantined, error FROM feed_runs"
        " WHERE feed = ? ORDER BY finished_at DESC, started_at DESC, run_id",
        [feed],
    )
    return [_Run(utc(r[0]), r[1], r[2] or 0, r[3] or 0, r[4]) for r in rows]


def _age(delta: timedelta) -> str:
    hours = delta.total_seconds() / 3600
    return f"{hours:.1f} h" if hours < 48 else f"{hours / 24:.1f} d"


def _freshness(good: list[_Run], policy: FeedPolicy, now: datetime) -> Check:
    if not good:
        return Check("freshness", "fail", "never completed a successful run")
    age = now - good[0].finished_at
    detail = f"last successful run {_age(age)} ago (limit {_age(policy.max_staleness)})"
    if age > policy.max_staleness:
        return Check("freshness", "fail", detail)
    if age > policy.max_staleness / 2:
        return Check("freshness", "warn", detail)
    return Check("freshness", "ok", detail)


def _last_run(runs: list[_Run]) -> Check:
    if not runs:
        return Check("last_run", "ok", "no runs recorded")
    if runs[0].status == "failed":
        return Check("last_run", "fail", f"most recent run failed: {runs[0].error}")
    return Check("last_run", "ok", f"most recent run {runs[0].status}")


def _volume(good: list[_Run], policy: FeedPolicy) -> Check:
    prior = good[1 : 1 + _PRIOR_RUNS]
    if not good or len(prior) < _MIN_PRIOR_RUNS:
        return Check("volume", "ok", "skipped: fewer than 3 earlier successful runs")
    median = statistics.median(r.rows_fetched for r in prior)
    last = good[0].rows_fetched
    detail = f"last run fetched {last} rows; median of {len(prior)} earlier runs is {median:g}"
    if last < policy.min_rows_ratio * median:
        return Check("volume", "warn", detail)
    return Check("volume", "ok", detail)


def _quarantine(runs: list[_Run], policy: FeedPolicy) -> Check:
    if not runs or runs[0].rows_fetched == 0:
        return Check("quarantine", "ok", "no rows in the last run")
    last = runs[0]
    ratio = last.rows_quarantined / last.rows_fetched
    detail = f"{last.rows_quarantined} of {last.rows_fetched} rows quarantined ({ratio:.1%})"
    status: Level = "fail" if ratio > policy.max_quarantine_ratio else "ok"
    return Check("quarantine", status, detail)


def _gaps(store: Store, policy: FeedPolicy, now: datetime) -> Check:
    if not policy.daily_series:
        return Check("gaps", "ok", "no daily series")
    snap = store.as_of(now)
    last_day = now.date() - timedelta(days=_GAP_PUBLISH_LAG_DAYS)
    days = [last_day - timedelta(days=i) for i in range(_GAP_WINDOW_DAYS)]
    weekdays = [d for d in days if d.weekday() < 5]
    missing: list[str] = []
    for series_id in policy.daily_series:
        have = {o.event_time.date() for o in snap.series(policy.feed, series_id)}
        absent = sorted(d for d in weekdays if d not in have)
        if absent:
            missing.append(f"{series_id}: " + ", ".join(d.isoformat() for d in absent))
    note = (
        f"weekdays {min(days).isoformat()} to {max(days).isoformat()}; "
        "no holiday calendar, so market holidays show as gaps"
    )
    if missing:
        return Check("gaps", "warn", "missing " + "; ".join(missing) + f" ({note})")
    return Check("gaps", "ok", f"no missing weekdays ({note})")


def check_feed(store: Store, policy: FeedPolicy, now: datetime) -> FeedStatus:
    """Run every check for one feed as of ``now``; the feed status is the worst check."""
    now = utc(now)
    runs = _runs(store, policy.feed)
    good = [r for r in runs if r.status in ("ok", "partial")]
    found = (
        _freshness(good, policy, now),
        _last_run(runs),
        _volume(good, policy),
        _quarantine(runs, policy),
        _gaps(store, policy, now) if good else Check("gaps", "ok", "no runs recorded"),
    )
    worst: Level = max((c.status for c in found), key=lambda s: _RANK[s])
    return FeedStatus(policy.feed, worst, found)


def check_all(store: Store, now: datetime) -> list[FeedStatus]:
    """One status per policy, sorted by feed name."""
    return [check_feed(store, POLICIES[feed], now) for feed in sorted(POLICIES)]


def send_alert(
    statuses: Sequence[FeedStatus],
    webhook_url: str,
    http_post: Callable[..., Any] = requests.post,
) -> bool:
    """Post the failing feeds to ``webhook_url``; ``False`` if none failed or the POST failed."""
    failing = [s for s in statuses if s.status == "fail"]
    if not failing:
        return False
    lines = ["eventlake feed health: failing feeds"]
    for s in failing:
        reasons = "; ".join(f"{c.name}: {c.detail}" for c in s.checks if c.status == "fail")
        lines.append(f"- {s.feed} ({reasons})")
    try:
        response = http_post(webhook_url, json={"text": "\n".join(lines)}, timeout=10)
        code = getattr(response, "status_code", 200)
    except Exception:
        return False
    return isinstance(code, int) and code < 400
