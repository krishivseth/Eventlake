"""Record the public API responses the offline demo replays.

Run from the repository root with network access:

    uv run python scripts/record_demo_data.py

Raw vendor records are saved unmodified under ``src/eventlake/demo_data/``, in the shape each
collector's ``normalize`` consumes. Trimming only removes whole entries (markets, candles, rows
outside the demo window); no value is edited.
"""

import csv
import io
import json
import time
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import requests

from eventlake.calendar import Event, load_events
from eventlake.collectors.kalshi import KalshiCollector
from eventlake.collectors.noaa import BASE_URL as NOAA
from eventlake.collectors.polymarket import CLOB_HISTORY, GAMMA_EVENTS
from eventlake.http import DEFAULT_USER_AGENT

OUT = Path(__file__).resolve().parent.parent / "src" / "eventlake" / "demo_data"
EVENT_IDS = [
    "FOMC:2026-06-17",
    "FOMC:2026-07-29",
    "FOMC:2026-09-16",
    "FOMC:2026-10-28",
    "CPI:2026-08-12",
    "CPI:2026-09-11",
    "CPI:2026-10-14",
    "CPI:2026-11-10",
]
WINDOW_START = datetime(2026, 6, 1, tzinfo=UTC)
RECORDED_END = datetime(2026, 11, 20, tzinfo=UTC)
LEAD_DAYS = 9  # price history kept from this many days before each event
TOLERANCE = timedelta(hours=36)
KALSHI_SERIES = {"KXFEDDECISION": "FOMC", "KXCPI": "CPI"}
MAX_MARKETS_PER_EVENT = 20  # a safety cap; whole markets beyond it would be dropped

SESSION = requests.Session()
SESSION.headers["User-Agent"] = DEFAULT_USER_AGENT


def get(url: str, params: dict[str, Any]) -> requests.Response:
    time.sleep(0.3)
    resp = SESSION.get(url, params=params, timeout=30)
    resp.raise_for_status()
    return resp


def ts(dt: datetime) -> int:
    return int(dt.timestamp())


def parse(s: str) -> datetime:
    return datetime.fromisoformat(s.replace("Z", "+00:00"))


def events() -> dict[str, Event]:
    return {e.event_id: e for e in load_events() if e.event_id in EVENT_IDS}


def record_kalshi(wanted: dict[str, Event]) -> None:
    collector = KalshiCollector()
    markets: dict[str, list[dict[str, Any]]] = {}
    records: list[dict[str, Any]] = []
    for series, kind in KALSHI_SERIES.items():
        listing = [
            (raw, live)
            for raw, live in collector.iter_markets(series, WINDOW_START)
            if parse(raw["close_time"]) <= RECORDED_END
        ]
        kept: list[dict[str, Any]] = []
        for event in (e for e in wanted.values() if e.kind == kind):
            near = sorted(
                (
                    (raw, live)
                    for raw, live in listing
                    if abs(parse(raw["close_time"]) - event.scheduled_time) <= TOLERANCE
                ),
                key=lambda rl: rl[0]["ticker"],
            )[:MAX_MARKETS_PER_EVENT]
            for raw, live in near:
                kept.append(raw)
                start = event.scheduled_time - timedelta(days=LEAD_DAYS)
                for candle in collector.candles(series, raw, live, start, event.scheduled_time):
                    records.append({"series": series, "ticker": raw["ticker"], "candle": candle})
        markets[series] = kept
    dump("kalshi_markets.json", markets)
    dump("kalshi_candles.json", records)


def record_polymarket(wanted: dict[str, Event]) -> None:
    kept: list[dict[str, Any]] = []
    records: list[dict[str, Any]] = []
    page = get(
        GAMMA_EVENTS,
        {
            "tag_slug": "fed",
            "closed": "true",
            "limit": 50,
            "offset": 0,
            "end_date_min": WINDOW_START.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "end_date_max": RECORDED_END.strftime("%Y-%m-%dT%H:%M:%SZ"),
        },
    ).json()
    fomc = [e for e in wanted.values() if e.kind == "FOMC"]
    for event in page:
        if not event["slug"].startswith("fed-decision-in-"):
            continue
        end = parse(event["endDate"])
        match = [e for e in fomc if abs(end - e.scheduled_time) <= TOLERANCE]
        if not match:
            continue
        kept.append(event)
        target = match[0].scheduled_time
        for market in event["markets"]:
            tokens = json.loads(market["clobTokenIds"])
            outcomes = json.loads(market["outcomes"])
            token = next(t for o, t in zip(outcomes, tokens, strict=False) if o.lower() == "yes")
            body = get(CLOB_HISTORY, {"market": token, "interval": "max", "fidelity": 1440}).json()
            for point in body.get("history", []):
                at = datetime.fromtimestamp(point["t"], UTC)
                if target - timedelta(days=LEAD_DAYS) <= at <= target:
                    records.append({"token_id": token, "point": point})
    dump("polymarket_events.json", kept)
    dump("polymarket_history.json", records)


def record_fred() -> None:
    text = get("https://fred.stlouisfed.org/graph/fredgraph.csv", {"id": "SP500"}).text
    rows = [
        {"series": "SP500", "date": r["observation_date"], "value": r["SP500"]}
        for r in csv.DictReader(io.StringIO(text))
        if "2026-05-15" <= r["observation_date"] <= "2026-11-19"
    ]
    dump("fred_sp500.json", rows)


def record_noaa() -> None:
    rows = get(
        NOAA + "/access/services/data/v1",
        {
            "dataset": "daily-summaries",
            "stations": "USW00094728",
            "startDate": "2026-09-01",
            "endDate": "2026-11-18",
            "dataTypes": "TMAX",
            "units": "standard",
            "format": "json",
        },
    ).json()
    dump("noaa_tmax.json", rows)


def dump(name: str, obj: Any) -> None:
    OUT.mkdir(parents=True, exist_ok=True)
    (OUT / name).write_text(json.dumps(obj, indent=None, separators=(",", ":")) + "\n")
    print(f"{name}: {(OUT / name).stat().st_size} bytes")


if __name__ == "__main__":
    wanted = events()
    assert len(wanted) == len(EVENT_IDS), "an event id is missing from the packaged calendar"
    record_kalshi(wanted)
    record_polymarket(wanted)
    record_fred()
    record_noaa()
