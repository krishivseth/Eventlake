import pytest
import requests
import responses

from eventlake.http import HttpClient, HttpError, RateLimiter

URL = "https://api.example.test/v1/thing"


class FakeClock:
    def __init__(self) -> None:
        self.now = 0.0

    def __call__(self) -> float:
        return self.now


def make_client(sleeps: list[float], **kwargs: object) -> HttpClient:
    return HttpClient("https://api.example.test/v1", sleep=sleeps.append, **kwargs)  # type: ignore[arg-type]


def test_rate_limiter_blocks_when_empty() -> None:
    limiter = RateLimiter(2, 60.0, clock=FakeClock(), sleep=lambda _s: None)
    assert limiter.acquire(timeout_s=0) is True
    assert limiter.acquire(timeout_s=0) is True
    assert limiter.acquire(timeout_s=0) is False


def test_rate_limiter_refills() -> None:
    clock = FakeClock()
    limiter = RateLimiter(2, 60.0, clock=clock, sleep=lambda _s: None)
    assert limiter.acquire(timeout_s=0)
    assert limiter.acquire(timeout_s=0)
    assert not limiter.acquire(timeout_s=0)
    clock.now += 30.0
    assert limiter.acquire(timeout_s=0) is True
    assert limiter.acquire(timeout_s=0) is False


def test_rate_limiter_waits_for_token_within_timeout() -> None:
    clock = FakeClock()

    def sleep(s: float) -> None:
        clock.now += s

    limiter = RateLimiter(1, 10.0, clock=clock, sleep=sleep)
    assert limiter.acquire(timeout_s=0)
    assert limiter.acquire(timeout_s=30.0) is True
    assert clock.now == pytest.approx(10.0)


@responses.activate
def test_retries_429_then_succeeds() -> None:
    responses.add(responses.GET, URL, status=429)
    responses.add(responses.GET, URL, status=429)
    responses.add(responses.GET, URL, json={"ok": True}, status=200)
    sleeps: list[float] = []
    assert make_client(sleeps).get_json("/thing") == {"ok": True}
    assert sleeps == [1.0, 2.0]
    assert len(responses.calls) == 3


@responses.activate
def test_honours_retry_after() -> None:
    responses.add(responses.GET, URL, status=429, headers={"Retry-After": "7"})
    responses.add(responses.GET, URL, json={"ok": 1}, status=200)
    sleeps: list[float] = []
    assert make_client(sleeps).get_json("/thing") == {"ok": 1}
    assert sleeps == [7.0]


@responses.activate
def test_retries_500_then_raises() -> None:
    for _ in range(3):
        responses.add(responses.GET, URL, status=500)
    sleeps: list[float] = []
    with pytest.raises(HttpError) as exc:
        make_client(sleeps).get_json("/thing")
    assert exc.value.status == 500
    assert exc.value.url == URL
    assert len(responses.calls) == 3
    assert sleeps == [1.0, 2.0]


@responses.activate
def test_does_not_retry_404() -> None:
    responses.add(responses.GET, URL, status=404)
    sleeps: list[float] = []
    with pytest.raises(HttpError) as exc:
        make_client(sleeps).get_json("/thing")
    assert exc.value.status == 404
    assert len(responses.calls) == 1
    assert sleeps == []


@responses.activate
def test_timeout_is_retried() -> None:
    responses.add(responses.GET, URL, body=requests.Timeout())
    responses.add(responses.GET, URL, body=requests.Timeout())
    responses.add(responses.GET, URL, json={"ok": True}, status=200)
    sleeps: list[float] = []
    assert make_client(sleeps).get_json("/thing") == {"ok": True}
    assert sleeps == [1.0, 2.0]


@responses.activate
def test_connection_error_exhausts_retries() -> None:
    for _ in range(3):
        responses.add(responses.GET, URL, body=requests.ConnectionError())
    with pytest.raises(HttpError) as exc:
        make_client([]).get_text("/thing")
    assert exc.value.status is None
    assert len(responses.calls) == 3


@responses.activate
def test_get_text_params_and_default_user_agent() -> None:
    responses.add(responses.GET, URL, body="hello", status=200)
    assert make_client([]).get_text("/thing", {"a": "1"}) == "hello"
    req = responses.calls[0].request
    assert req.url == f"{URL}?a=1"
    assert req.headers["User-Agent"] == "eventlake/0.1 (+https://github.com)"


@responses.activate
def test_limiter_is_acquired_per_attempt() -> None:
    responses.add(responses.GET, URL, status=500)
    responses.add(responses.GET, URL, json={}, status=200)
    limiter = RateLimiter(10, 60.0, clock=FakeClock(), sleep=lambda _s: None)
    make_client([], limiter=limiter).get_json("/thing")
    assert limiter.acquire(timeout_s=0)  # 8 of 10 tokens remain
    for _ in range(7):
        assert limiter.acquire(timeout_s=0)
    assert not limiter.acquire(timeout_s=0)
