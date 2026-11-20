from datetime import UTC, datetime, timedelta

from eventlake.calendar import Event, load_events, upsert_events
from eventlake.entities import (
    LINK_RULES,
    Market,
    link_markets,
    markets_for_event,
    unlinked_markets,
    upsert_markets,
)
from eventlake.store import Store

NOW = datetime(2026, 2, 1, tzinfo=UTC)
JAN_FOMC = datetime(2026, 1, 28, 19, 0, tzinfo=UTC)


def _store_with_events(*events: Event) -> Store:
    store = Store()
    upsert_events(store, events or load_events(["FOMC", "CPI"]))
    return store


def _market(
    ticker: str, close: datetime, series: str = "KXFEDDECISION", venue: str = "kalshi"
) -> Market:
    return Market(
        market_id=f"{venue}:{ticker}",
        venue=venue,  # type: ignore[arg-type]
        venue_ticker=ticker,
        series_ticker=series,
        title=ticker,
        close_time=close,
        attrs={},
    )


def _fomc(day: str, hour: int = 19) -> Event:
    y, m, d = (int(x) for x in day.split("-"))
    return Event(f"FOMC:{day}", "FOMC", datetime(y, m, d, hour, tzinfo=UTC), {})


def test_market_links_to_event_within_tolerance() -> None:
    store = _store_with_events()
    upsert_markets(store, [_market("KXFEDDECISION-26JAN-H0", JAN_FOMC)])
    assert link_markets(store, now=NOW) == 1
    linked = markets_for_event(store, "FOMC:2026-01-28")
    assert [m.market_id for m in linked] == ["kalshi:KXFEDDECISION-26JAN-H0"]
    assert linked[0].close_time == JAN_FOMC
    assert unlinked_markets(store) == []


def test_market_just_inside_tolerance_links() -> None:
    store = _store_with_events()
    upsert_markets(store, [_market("A", JAN_FOMC - timedelta(hours=35, minutes=59))])
    assert link_markets(store, now=NOW) == 1


def test_market_outside_tolerance_is_unlinked() -> None:
    store = _store_with_events()
    upsert_markets(store, [_market("A", JAN_FOMC + timedelta(days=3))])
    assert link_markets(store, now=NOW) == 0
    assert [m.market_id for m in unlinked_markets(store)] == ["kalshi:A"]


def test_unknown_series_is_never_linked() -> None:
    store = _store_with_events()
    upsert_markets(store, [_market("X", JAN_FOMC, series="KXOTHER")])
    assert link_markets(store, now=NOW) == 0
    assert [m.market_id for m in unlinked_markets(store)] == ["kalshi:X"]


def test_same_series_on_other_venue_is_never_linked() -> None:
    store = _store_with_events()
    upsert_markets(store, [_market("X", JAN_FOMC, series="KXFEDDECISION", venue="polymarket")])
    assert link_markets(store, now=NOW) == 0


def test_series_with_other_event_kind_does_not_link_to_fomc() -> None:
    store = _store_with_events(_fomc("2026-01-28"))
    upsert_markets(store, [_market("C", JAN_FOMC, series="KXCPI")])
    assert link_markets(store, now=NOW) == 0


def test_link_picks_closer_event() -> None:
    store = _store_with_events(_fomc("2026-01-28", 19), _fomc("2026-01-29", 19))
    close = datetime(2026, 1, 29, 10, 0, tzinfo=UTC)  # 15h after the 28th, 9h before the 29th
    upsert_markets(store, [_market("A", close)])
    assert link_markets(store, now=NOW) == 1
    assert markets_for_event(store, "FOMC:2026-01-28") == []
    assert [m.venue_ticker for m in markets_for_event(store, "FOMC:2026-01-29")] == ["A"]


def test_link_markets_is_idempotent() -> None:
    store = _store_with_events()
    upsert_markets(store, [_market("A", JAN_FOMC)])
    assert link_markets(store, now=NOW) == 1
    assert link_markets(store, now=NOW + timedelta(days=1)) == 0
    assert store.execute("SELECT count(*) FROM links")[0][0] == 1


def test_existing_link_is_not_moved_when_events_change() -> None:
    store = _store_with_events(_fomc("2026-01-28", 19))
    upsert_markets(store, [_market("A", datetime(2026, 1, 29, 1, 0, tzinfo=UTC))])
    link_markets(store, now=NOW)
    upsert_events(store, [_fomc("2026-01-29", 1)])  # an exact match appears later
    assert link_markets(store, now=NOW) == 0
    assert [m.venue_ticker for m in markets_for_event(store, "FOMC:2026-01-28")] == ["A"]


def test_weather_market_creates_local_day_event() -> None:
    store = Store()
    # A market for Nov 20 (New York) closes at 06:00 UTC on Nov 21.
    upsert_markets(
        store,
        [_market("KXHIGHNY-26NOV20-T69", datetime(2026, 11, 21, 6, 0, tzinfo=UTC), "KXHIGHNY")],
    )
    assert link_markets(store, now=NOW) == 1
    event_id = "WEATHER_HIGH:USW00094728:2026-11-20"
    assert [m.venue_ticker for m in markets_for_event(store, event_id)] == ["KXHIGHNY-26NOV20-T69"]
    assert store.execute("SELECT kind FROM events WHERE event_id = ?", [event_id]) == [
        ("WEATHER_HIGH",)
    ]


def test_weather_market_in_standard_and_daylight_time_maps_to_its_day() -> None:
    store = Store()
    upsert_markets(
        store,
        [
            _market(
                "KXHIGHNY-26JAN15-T40", datetime(2026, 1, 16, 5, 0, tzinfo=UTC), "KXHIGHNY"
            ),  # Jan 15, EST
            _market("KXHIGHNY-26JUL15-T80", datetime(2026, 7, 16, 4, 0, tzinfo=UTC), "KXHIGHNY"),
        ],  # Jul 15, EDT
    )
    link_markets(store, now=NOW)
    assert len(markets_for_event(store, "WEATHER_HIGH:USW00094728:2026-01-15")) == 1
    assert len(markets_for_event(store, "WEATHER_HIGH:USW00094728:2026-07-15")) == 1


def test_polymarket_fed_decision_family_links() -> None:
    store = _store_with_events()
    # Recorded Polymarket decision markets close 4 to 5 hours after the decision time.
    upsert_markets(
        store,
        [_market("p1", datetime(2026, 1, 28, 23, 59, tzinfo=UTC), "fed-decision", "polymarket")],
    )
    assert link_markets(store, now=NOW) == 1
    assert [m.venue_ticker for m in markets_for_event(store, "FOMC:2026-01-28")] == ["p1"]


def test_every_rule_has_positive_tolerance_and_known_kind() -> None:
    assert {r.kind for r in LINK_RULES} == {"FOMC", "CPI", "WEATHER_HIGH"}
    assert all(r.tolerance > timedelta(0) for r in LINK_RULES)


def test_weather_market_whose_ticker_day_disagrees_is_unlinked() -> None:
    store = Store()
    close = datetime(2026, 11, 21, 6, 0, tzinfo=UTC)  # settles Nov 20, but the ticker says Nov 19
    upsert_markets(store, [_market("KXHIGHNY-26NOV19-T69", close, "KXHIGHNY")])
    assert link_markets(store, now=NOW) == 0
    assert len(unlinked_markets(store)) == 1
    assert store.execute("SELECT count(*) FROM events")[0][0] == 0


def test_weather_market_with_unparseable_ticker_is_unlinked() -> None:
    store = Store()
    close = datetime(2026, 11, 21, 6, 0, tzinfo=UTC)
    upsert_markets(
        store,
        [
            _market("KXHIGHNY-T69", close, "KXHIGHNY"),
            _market("KXHIGHNY-26NOV31-T69", close, "KXHIGHNY"),  # not a real date
        ],
    )
    assert link_markets(store, now=NOW) == 0
    assert len(unlinked_markets(store)) == 2


def test_cpi_market_links_to_cpi_event() -> None:
    store = _store_with_events()
    close = datetime(2026, 11, 10, 13, 29, tzinfo=UTC)
    upsert_markets(store, [_market("KXCPI-26OCT-T1.0", close, "KXCPI")])
    assert link_markets(store, now=NOW) == 1
    assert [m.venue_ticker for m in markets_for_event(store, "CPI:2026-11-10")] == [
        "KXCPI-26OCT-T1.0"
    ]
