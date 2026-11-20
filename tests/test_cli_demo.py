import functools
import re
import socket
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pytest
import requests
from typer.testing import CliRunner

from eventlake import cli, demo
from eventlake.panel import eve_panel
from eventlake.types import Observation, parse_utc

runner = CliRunner()
DATA = Path(demo.__file__).parent / "demo_data"
SECTIONS = (
    "1. As known the day before each event",
    "2. Same events, as known now",
    "3. What changed",
)


def run_demo(*extra: str) -> Any:
    return runner.invoke(cli.app, ["demo", "--offline", *extra])


@functools.cache
def default_output() -> str:
    result = run_demo()
    assert result.exit_code == 0, result.output
    out: str = result.output
    return out


def table_rows(text: str, heading: str) -> list[list[str]]:
    """Cells of the data rows of the table under ``heading`` (split on two or more spaces)."""
    lines = text.splitlines()
    start = lines.index(heading) + 3  # heading, header row, rule
    rows: list[list[str]] = []
    for line in lines[start:]:
        if not line.strip() or not line.startswith(("FOMC", "CPI")):
            break
        rows.append(re.split(r"\s{2,}", line.strip()))
    return rows


def test_demo_offline_runs_without_network(monkeypatch: pytest.MonkeyPatch) -> None:
    attempts: list[str] = []

    def recorder(name: str) -> Any:
        def refuse(*args: Any, **kwargs: Any) -> Any:
            attempts.append(name)
            raise OSError("network used")

        return refuse

    monkeypatch.setattr(requests.Session, "request", recorder("Session.request"))
    monkeypatch.setattr(requests.adapters.HTTPAdapter, "send", recorder("HTTPAdapter.send"))
    monkeypatch.setattr(socket.socket, "connect", recorder("socket.connect"))
    result = run_demo()
    assert result.exit_code == 0, result.output
    store, _ = demo.build_store(True, demo.default_now())
    try:
        statuses = {r[0] for r in store.execute("SELECT status FROM feed_runs")}
    finally:
        store.close()
    assert attempts == []
    assert "failed" not in statuses and statuses


def test_demo_mismatch_is_reported_and_exits_nonzero(monkeypatch: pytest.MonkeyPatch) -> None:
    real = demo.build_store

    def leaky(offline: bool, now: Any, cutoff: Any = None) -> Any:
        store, events = real(offline, now, cutoff)
        if cutoff is None:  # only the full store sees an extra IV known before the eve
            at = datetime(2026, 10, 27, 17, tzinfo=UTC)
            store.append([Observation("tradier.options", "SPY:2026-10-30:atm_iv", at, at, 0.9)])
        return store, events

    monkeypatch.setattr(demo, "build_store", leaky)
    result = run_demo()
    assert result.exit_code == 1
    assert "WARNING" in result.output and "FOMC:2026-10-28" in result.output
    assert "implied_move" in result.output
    assert "matched" not in result.output


def test_demo_offline_output_shape() -> None:
    out = default_output()
    for heading in SECTIONS:
        assert heading in out
    eve = table_rows(out, SECTIONS[0])
    now = table_rows(out, SECTIONS[1])
    assert len(eve) >= 8 and len(now) >= 8
    # columns: event, as of, top market, P(yes), implied move, realised move
    assert all(row[-1] == "—" for row in eve), "an eve-view realised move would be hindsight"
    assert sum(row[-1] != "—" for row in now) >= 8
    assert all(re.fullmatch(r"\d+\.\d%", row[3]) for row in eve)
    assert all(re.fullmatch(r"\d\.\d\d%", row[-1]) for row in now if row[-1] != "—")
    assert sum(1 for r in eve if r[0].startswith("FOMC")) >= 4
    assert sum(1 for r in eve if r[0].startswith("CPI")) >= 4
    assert "\x1b[" not in out


def test_demo_labels_implied_move_as_synthetic_everywhere() -> None:
    out = default_output()
    assert out.count("implied move (synthetic sample)") >= 3  # two headers plus the note
    assert "illustrative" in out
    tables = out.split(SECTIONS[2])[0]
    mentions = [line for line in tables.splitlines() if "implied move" in line]
    assert len(mentions) == 3 and all("synthetic sample" in line for line in mentions)


def test_demo_what_changed_counts_match_the_tables() -> None:
    out = default_output()
    eve = table_rows(out, SECTIONS[0])
    now = table_rows(out, SECTIONS[1])
    filled = sum(1 for a, b in zip(eve, now, strict=True) for i in (3, 4, 5)
                 if a[i] == "—" and b[i] != "—")  # fmt: skip
    claim = re.search(r"(\d+) cells went from empty to filled", out)
    assert claim and int(claim.group(1)) == filled
    assert "recomputed from a store holding only data known at its as-of time" in out
    assert "and matched." in out


def test_demo_is_deterministic() -> None:
    a = run_demo("--now", "2026-11-19T18:00:00+00:00")
    b = run_demo("--now", "2026-11-19T18:00:00+00:00")
    assert a.exit_code == b.exit_code == 0
    assert a.output == b.output
    assert default_output() == a.output  # the default clock is the stored one


def test_demo_default_now_is_stored_with_the_data() -> None:
    assert demo.default_now() == parse_utc("2026-11-19T18:00:00+00:00")
    assert "Demo clock fixed at 2026-11-19T18:00Z" in default_output()


def test_demo_health_lists_demo_feeds_and_synthetic_note() -> None:
    out = default_output()
    for feed in ("fred", "kalshi", "noaa", "polymarket", "tradier.options"):
        assert re.search(rf"(?m)^{re.escape(feed)}\s+(OK|WARN)\b", out), feed
    assert "synthetic sample" in out.split("Feed health")[1]
    assert not re.search(r"(?m)^\S+\s+FAIL\b", out)


def test_demo_data_is_small_and_synthetic_rows_are_assumed() -> None:
    assert sum(f.stat().st_size for f in DATA.rglob("*") if f.is_file()) < 1_000_000
    store, events = demo.build_store(True, demo.default_now())
    try:
        rows = store.execute(
            "SELECT DISTINCT feed, kt_quality FROM observations WHERE feed = 'tradier.options'"
        )
        assert rows == [("tradier.options", "assumed")]
        eve = eve_panel(store, until=demo.default_now())
        assert len(eve) == len(events) == 8
        assert all(r.market_prob is not None and r.implied_move is not None for r in eve)
    finally:
        store.close()


def test_replay_collector_delegates_normalisation() -> None:
    from eventlake.collectors.fred import FredCollector

    real = FredCollector()
    replay = demo.ReplayCollector(
        real, [{"series": "SP500", "date": "2026-11-17", "value": "7340.53"}]
    )
    assert replay.feed == "fred"
    (raw,) = list(replay.fetch(parse_utc("2026-01-01T00:00:00+00:00"), demo.default_now()))
    assert replay.normalize(raw, demo.default_now()) == real.normalize(raw, demo.default_now())
