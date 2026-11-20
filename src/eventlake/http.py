"""Shared HTTP client: token-bucket rate limiting and bounded retry."""

from __future__ import annotations

import threading
import time
from collections.abc import Callable, Mapping
from typing import Any

import requests

DEFAULT_USER_AGENT = "eventlake/0.1 (+https://github.com)"


class RateLimiter:
    """Thread-safe token bucket: `capacity` tokens refilled evenly over `period_s`."""

    def __init__(
        self,
        capacity: int,
        period_s: float,
        clock: Callable[[], float] = time.monotonic,
        sleep: Callable[[float], None] = time.sleep,
    ) -> None:
        if capacity < 1 or period_s <= 0:
            raise ValueError("capacity must be >= 1 and period_s > 0")
        self._capacity = float(capacity)
        self._rate = capacity / period_s
        self._clock = clock
        self._sleep = sleep
        self._tokens = float(capacity)
        self._updated = clock()
        self._lock = threading.Lock()

    def acquire(self, timeout_s: float = 30.0) -> bool:
        """Take one token, waiting up to `timeout_s`. Returns False if none became available."""
        deadline = self._clock() + timeout_s
        while True:
            with self._lock:
                now = self._clock()
                elapsed = max(0.0, now - self._updated)
                self._tokens = min(self._capacity, self._tokens + elapsed * self._rate)
                self._updated = now
                if self._tokens >= 1.0:
                    self._tokens -= 1.0
                    return True
                needed = (1.0 - self._tokens) / self._rate
            remaining = deadline - now
            if needed > remaining:
                return False
            self._sleep(needed)


class HttpError(RuntimeError):
    def __init__(self, message: str, *, status: int | None, url: str) -> None:
        super().__init__(message)
        self.status = status
        self.url = url


def _is_retryable_status(status: int) -> bool:
    return status == 429 or 500 <= status <= 599


class HttpClient:
    def __init__(
        self,
        base_url: str = "",
        *,
        headers: Mapping[str, str] | None = None,
        limiter: RateLimiter | None = None,
        retries: int = 3,
        backoff_s: float = 1.0,
        timeout_s: float = 20.0,
        sleep: Callable[[float], None] = time.sleep,
    ) -> None:
        if retries < 1:
            raise ValueError("retries is the total attempt count and must be >= 1")
        self._base_url = base_url
        self._limiter = limiter
        self._retries = retries
        self._backoff_s = backoff_s
        self._timeout_s = timeout_s
        self._sleep = sleep
        self._session = requests.Session()
        self._session.headers["User-Agent"] = DEFAULT_USER_AGENT
        if headers:
            self._session.headers.update(headers)

    def _url(self, path: str) -> str:
        return f"{self._base_url}{path}"

    def _get(self, path: str, params: Mapping[str, Any] | None) -> requests.Response:
        url = self._url(path)
        last_status: int | None = None
        last_msg = "no attempt made"
        for attempt in range(self._retries):
            wait = self._backoff_s * 2**attempt
            if self._limiter is not None and not self._limiter.acquire():
                raise HttpError("rate limiter timed out", status=None, url=url)
            try:
                resp = self._session.get(url, params=params, timeout=self._timeout_s)
            except (requests.Timeout, requests.ConnectionError) as exc:
                last_status, last_msg = None, f"{type(exc).__name__}: {exc}"
            else:
                if resp.ok:
                    return resp
                last_status, last_msg = resp.status_code, f"HTTP {resp.status_code}"
                if not _is_retryable_status(resp.status_code):
                    raise HttpError(last_msg, status=last_status, url=url)
                if resp.status_code == 429:
                    try:
                        wait = float(resp.headers["Retry-After"])
                    except (KeyError, ValueError):
                        pass
            if attempt < self._retries - 1:
                self._sleep(wait)
        raise HttpError(f"{last_msg} after {self._retries} attempts", status=last_status, url=url)

    def get_json(self, path: str, params: Mapping[str, Any] | None = None) -> Any:
        return self._get(path, params).json()

    def get_text(self, path: str, params: Mapping[str, Any] | None = None) -> str:
        return self._get(path, params).text
