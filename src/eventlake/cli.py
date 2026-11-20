"""Command-line interface: ingest, backfill, health, export, serve."""

import os
from datetime import UTC, date, datetime, time, timedelta
from pathlib import Path
from typing import Annotated, Any

import typer

from eventlake.calendar import load_events, upsert_events
from eventlake.collectors import REGISTRY
from eventlake.entities import link_markets, unlinked_markets, upsert_markets
from eventlake.health import check_all, send_alert, unconfigured_feeds
from eventlake.panel import eve_panel, export_parquet
from eventlake.runner import Collector, RunResult, record_failure, run_collector
from eventlake.store import Store
from eventlake.tables import render_health
from eventlake.types import describe, utc

app = typer.Typer(
    help="Point-in-time data platform for prediction-market, options, macro and weather feeds.",
    no_args_is_help=True,
    add_completion=False,
)

DEFAULT_DB = "data/eventlake.duckdb"
_UNKNOWN = "-"


def _now() -> datetime:
    return datetime.now(UTC)


def parse_when(text: str) -> datetime:
    """Parse ``YYYY-MM-DD`` or an ISO timestamp; a value without an offset is read as UTC."""
    try:
        if len(text) == 10:
            return datetime.combine(date.fromisoformat(text), time(), tzinfo=UTC)
        parsed = datetime.fromisoformat(text)
    except ValueError:
        raise typer.BadParameter(f"not an ISO date or timestamp: {text!r}") from None
    return utc(parsed) if parsed.tzinfo else parsed.replace(tzinfo=UTC)


def _open_store() -> Store:
    return Store(os.environ.get("EVENTLAKE_DB") or DEFAULT_DB)


def _describe_run(result: RunResult) -> str:
    text = (
        f"{result.feed}: {result.status}, fetched {result.rows_fetched}, "
        f"written {result.rows_written}, quarantined {result.rows_quarantined}"
    )
    return f"{text}\n  error: {result.error}" if result.error else text


def _store_markets(
    store: Store, collector: Collector, first: datetime, last: datetime
) -> int | None:
    """List and store the collector's markets; ``None`` (with a failed run recorded) on error."""
    lister: Any = getattr(collector, "list_markets", None)
    try:
        return upsert_markets(store, lister(first, last))
    except Exception as exc:
        error = f"market listing failed: {describe(exc)}"
        typer.echo(_describe_run(record_failure(store, collector.feed, error, at=_now())))
        return None


def _link(store: Store, now: datetime) -> str:
    """Link markets and describe the result, including how many stay unlinked."""
    new = link_markets(store, now=now)
    return f"links: {new} new, unlinked: {len(unlinked_markets(store))}"


@app.command()
def ingest(
    feed: Annotated[str, typer.Argument(help="Feed name, e.g. kalshi or fred.")],
    start: Annotated[
        str | None, typer.Option(help="ISO date or timestamp (UTC); default 7d ago.")
    ] = None,
    end: Annotated[
        str | None, typer.Option(help="ISO date or timestamp (UTC); default now.")
    ] = None,
) -> None:
    """Run one collector over a window; exit 1 if the run or its market listing failed.

    For a prediction-market feed this also loads the calendar, lists and stores the markets
    for the window and links them to events after the run.
    """
    if feed not in REGISTRY:
        typer.echo(f"unknown feed {feed!r}; choose from: {', '.join(sorted(REGISTRY))}", err=True)
        raise typer.Exit(2)
    now = _now()
    first = parse_when(start) if start else now - timedelta(days=7)
    last = parse_when(end) if end else now
    try:
        collector = REGISTRY[feed]()
    except Exception as exc:
        typer.echo(f"cannot start {feed}: {exc}", err=True)
        raise typer.Exit(1) from None
    lists = hasattr(collector, "list_markets")
    listing_failed = False
    store = _open_store()
    try:
        if lists:
            upsert_events(store, load_events())
            new_markets = _store_markets(store, collector, first, last)
            listing_failed = new_markets is None
        result = run_collector(collector, store, first, last)
        typer.echo(_describe_run(result))
        if lists:
            links = _link(store, now)
            typer.echo(links if listing_failed else f"markets: {new_markets} new, {links}")
    finally:
        store.close()
    if result.status == "failed" or listing_failed:
        raise typer.Exit(1)


@app.command()
def backfill(
    start: Annotated[str, typer.Option(help="ISO date or timestamp (UTC).")] = "2025-01-01",
) -> None:
    """Load the calendar and markets, run every configured collector, then link markets.

    Exits 1 if any run or market listing failed.
    """
    first, now = parse_when(start), _now()
    store = _open_store()
    failed = False
    try:
        typer.echo(f"events: {upsert_events(store, load_events())} new")
        collectors: dict[str, Collector] = {}
        for feed in sorted(REGISTRY):
            try:
                collectors[feed] = REGISTRY[feed]()
            except Exception as exc:
                typer.echo(f"skip {feed}: {exc}")
        for feed, collector in collectors.items():
            if not hasattr(collector, "list_markets"):
                continue
            new_markets = _store_markets(store, collector, first, now)
            if new_markets is None:
                failed = True
            else:
                typer.echo(f"markets {feed}: {new_markets} new")
        for collector in collectors.values():
            result = run_collector(collector, store, first, now)
            typer.echo(_describe_run(result))
            failed = failed or result.status == "failed"
        typer.echo(_link(store, now))
    finally:
        store.close()
    if failed:
        raise typer.Exit(1)


@app.command()
def health(
    alert: Annotated[
        bool, typer.Option(help="Post failing feeds to EVENTLAKE_ALERT_WEBHOOK.")
    ] = False,
    now: Annotated[str | None, typer.Option(hidden=True)] = None,
) -> None:
    """Print feed health; exit 1 if any feed that has run fails."""
    store = _open_store()
    try:
        statuses = check_all(store, parse_when(now) if now else _now())
        never_run = unconfigured_feeds(store)
    finally:
        store.close()
    typer.echo(render_health(statuses))
    if never_run:
        typer.echo(f"Not configured (never run): {', '.join(never_run)}")
    failing = any(s.status == "fail" for s in statuses)
    if alert and failing:
        url = os.environ.get("EVENTLAKE_ALERT_WEBHOOK")
        if not url:
            typer.echo("EVENTLAKE_ALERT_WEBHOOK is not set; no alert sent", err=True)
        elif send_alert(statuses, url):
            typer.echo("alert sent")
        else:
            typer.echo("alert could not be sent", err=True)
    if failing:
        raise typer.Exit(1)


@app.command()
def export(
    path: Annotated[Path, typer.Argument(help="Output Parquet file.")],
    as_of: Annotated[str | None, typer.Option(help="Include events up to this time.")] = None,
) -> None:
    """Write the day-before-event panel to a Parquet file."""
    store = _open_store()
    try:
        rows = eve_panel(store, until=parse_when(as_of) if as_of else _now())
    finally:
        store.close()
    export_parquet(rows, path)
    typer.echo(f"wrote {len(rows)} rows to {path}")


@app.command()
def serve(
    port: Annotated[int, typer.Option(help="TCP port.")] = 8000,
    host: Annotated[
        str, typer.Option(help="Interface to bind; 0.0.0.0 exposes the API to the network.")
    ] = "127.0.0.1",
) -> None:
    """Serve the read-only HTTP API (default 127.0.0.1 only)."""
    import uvicorn

    from eventlake.api import create_app

    uvicorn.run(create_app(_open_store()), host=host, port=port)


@app.command()
def demo(
    offline: Annotated[
        bool, typer.Option(help="Replay the recorded sample data; no network.")
    ] = False,
    now: Annotated[str | None, typer.Option(hidden=True)] = None,
) -> None:
    """Show the as-of event panel: known the day before each event, and known now."""
    from eventlake.demo import default_now, render_demo

    clock = parse_when(now) if now else (default_now() if offline else _now())
    text, ok = render_demo(offline, clock)
    typer.echo(text)
    if not ok:
        raise typer.Exit(1)
