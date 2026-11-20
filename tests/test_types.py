from datetime import UTC, datetime, timedelta
from typing import Any

import pytest

from eventlake.types import Observation, ValidationError, parse_utc, utc

T0 = datetime(2026, 1, 28, 19, 0, tzinfo=UTC)


def test_valid_observation_roundtrips() -> None:
    o = Observation("kalshi", "KXFED-X", T0, T0, 0.62)
    assert o.kt_quality == "observed" and o.attrs == {}


@pytest.mark.parametrize(
    "kw,msg",
    [
        ({"feed": ""}, "feed"),
        ({"series_id": ""}, "series_id"),
        ({"event_time": T0.replace(tzinfo=None)}, "timezone"),
        ({"knowledge_time": T0.replace(tzinfo=None)}, "timezone"),
        ({"value": float("nan")}, "finite"),
        ({"value": float("inf")}, "finite"),
        ({"knowledge_time": T0 - timedelta(seconds=1)}, "knowledge_time"),
    ],
)
def test_invalid_observation_rejected(kw: dict[str, Any], msg: str) -> None:
    base: dict[str, Any] = {
        "feed": "f",
        "series_id": "s",
        "event_time": T0,
        "knowledge_time": T0,
        "value": 1.0,
    }
    with pytest.raises(ValidationError, match=msg):
        Observation(**{**base, **kw})


def test_utc_rejects_naive() -> None:
    with pytest.raises(ValueError):
        utc(datetime(2026, 1, 1))


def test_utc_converts_offset_to_utc() -> None:
    from datetime import timezone

    dt = datetime(2026, 1, 28, 14, 0, tzinfo=timezone(timedelta(hours=-5)))
    assert utc(dt) == T0
    assert utc(dt).utcoffset() == timedelta(0)


def test_parse_utc_accepts_z() -> None:
    assert parse_utc("2026-01-28T19:00:00Z") == T0


def test_parse_utc_accepts_offset() -> None:
    assert parse_utc("2026-01-28T14:00:00-05:00") == T0


def test_parse_utc_rejects_naive_string() -> None:
    with pytest.raises(ValueError):
        parse_utc("2026-01-28T19:00:00")
