import json
from collections.abc import Iterator
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import pyarrow.parquet as pq
import pytest
from typer.testing import CliRunner

from eventlake import cli
from eventlake.health import POLICIES
from eventlake.runner import RunResult
from eventlake.store import Store
from eventlake.tables import format_table
from eventlake.types import Observation

runner = CliRunner()
NOW = datetime(2026, 3, 11, 12, 0, tzinfo=UTC)


@pytest.fixture
def db(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    path = tmp_path / "t.duckdb"
    monkeypatch.setenv("EVENTLAKE_DB", str(path))
    monkeypatch.setattr(cli, "_now", lambda: NOW)
    return path


class Fake:
    feed = "fake"

    def __init__(self, boom: bool = False) -> None:
        self.boom = boom

    def fetch(self, start: datetime, end: datetime) -> Iterator[Any]:
        yield {"v": 1.0}
        if self.boom:
            raise RuntimeError("upstream down")

    def normalize(self, raw: Any, fetched_at: datetime) -> list[Observation]:
        t = NOW - timedelta(days=1)
        return [Observation("fake", "S", t, t, raw["v"])]


def runs(db: Path) -> list[tuple[Any, ...]]:
    store = Store(db)
    try:
        return store.execute("SELECT feed, status, rows_written FROM feed_runs")
    finally:
        store.close()


def test_ingest_success_exits_zero(db: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setitem(cli.REGISTRY, "fake", Fake)
    result = runner.invoke(cli.app, ["ingest", "fake", "--start", "2026-03-01"])
    assert result.exit_code == 0, result.output
    assert "fake: ok" in result.output
    assert runs(db) == [("fake", "ok", 1)]


def test_ingest_failed_run_exits_one(db: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setitem(cli.REGISTRY, "fake", lambda: Fake(boom=True))
    result = runner.invoke(cli.app, ["ingest", "fake"])
    assert result.exit_code == 1
    assert "upstream down" in result.output


def test_ingest_unknown_feed_and_missing_config(db: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    assert runner.invoke(cli.app, ["ingest", "nope"]).exit_code == 2
    monkeypatch.delenv("TRADIER_TOKEN", raising=False)
    result = runner.invoke(cli.app, ["ingest", "tradier.options"])
    assert result.exit_code == 1
    assert "TRADIER_TOKEN" in result.output


def test_backfill_skips_unconfigured_feeds_and_runs_the_rest(
    db: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    def refuse() -> Fake:
        raise RuntimeError("EVENTLAKE_CONTACT is required")

    monkeypatch.setattr(cli, "REGISTRY", {"fake": Fake, "edgar": refuse})
    result = runner.invoke(cli.app, ["backfill", "--start", "2026-03-01"])
    assert result.exit_code == 0, result.output
    assert "skip edgar: EVENTLAKE_CONTACT is required" in result.output
    assert "events:" in result.output and "links:" in result.output
    assert runs(db) == [("fake", "ok", 1)]
    store = Store(db)
    assert store.execute("SELECT count(*) FROM events")[0][0] > 0
    store.close()


def seed_runs(db: Path, *, skip: str | None = None) -> None:
    store = Store(db)
    for feed in POLICIES:
        if feed == skip:
            continue
        store.execute(
            "INSERT INTO feed_runs (run_id, feed, started_at, finished_at, status, rows_fetched,"
            " rows_written, rows_quarantined, error) VALUES (?,?,?,?,'ok',5,5,0,NULL)",
            [f"r-{feed}", feed, NOW - timedelta(hours=2), NOW - timedelta(hours=1)],
        )
    store.close()


def test_cli_health_exit_code(db: Path) -> None:
    seed_runs(db, skip="edgar")
    failing = runner.invoke(cli.app, ["health"])
    assert failing.exit_code == 1
    assert "edgar" in failing.output and "FAIL" in failing.output
    assert "never completed" in failing.output


def test_cli_health_zero_when_only_warnings(db: Path) -> None:
    seed_runs(db)  # fred and noaa have no observations: gap warnings only
    result = runner.invoke(cli.app, ["health"])
    assert result.exit_code == 0, result.output
    assert "WARN" in result.output


def test_cli_health_alert_uses_webhook(db: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    sent: list[tuple[str, list[str]]] = []

    def fake_alert(statuses: Any, url: str) -> bool:
        sent.append((url, [s.feed for s in statuses if s.status == "fail"]))
        return True

    monkeypatch.setattr(cli, "send_alert", fake_alert)
    monkeypatch.setenv("EVENTLAKE_ALERT_WEBHOOK", "https://hook.test/x")
    seed_runs(db, skip="cftc")
    result = runner.invoke(cli.app, ["health", "--alert"])
    assert result.exit_code == 1
    assert sent == [("https://hook.test/x", ["cftc"])]


def test_export_writes_parquet(db: Path, tmp_path: Path) -> None:
    store = Store(db)
    from eventlake.calendar import load_events, upsert_events

    upsert_events(store, load_events())
    store.close()
    out = tmp_path / "out" / "eve.parquet"
    result = runner.invoke(cli.app, ["export", str(out), "--as-of", "2026-03-01"])
    assert result.exit_code == 0, result.output
    table = pq.read_table(out)
    assert table.num_rows > 0
    assert max(table.column("scheduled_time").to_pylist()) <= datetime(2026, 3, 1, tzinfo=UTC)


def test_serve_starts_uvicorn_on_localhost(db: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    import uvicorn

    seen: dict[str, Any] = {}
    monkeypatch.setattr(uvicorn, "run", lambda app, **kw: seen.update(kw))
    assert runner.invoke(cli.app, ["serve", "--port", "8123"]).exit_code == 0
    assert seen == {"host": "127.0.0.1", "port": 8123}


def test_parse_when_forms() -> None:
    assert cli.parse_when("2026-03-01") == datetime(2026, 3, 1, tzinfo=UTC)
    assert cli.parse_when("2026-03-01T05:00:00+02:00") == datetime(2026, 3, 1, 3, tzinfo=UTC)
    assert cli.parse_when("2026-03-01T05:00:00") == datetime(2026, 3, 1, 5, tzinfo=UTC)


def test_format_table_aligns_columns() -> None:
    text = format_table(["a", "bbb"], [["xxxx", "1"], ["y", "22"]])
    assert text.splitlines() == ["a     bbb", "----  ---", "xxxx  1", "y     22"]


def test_run_result_description_includes_error() -> None:
    r = RunResult("id", "f", "failed", 1, 0, 0, "boom")
    assert cli._describe_run(r).endswith("error: boom")
    json.dumps(cli._describe_run(r))
