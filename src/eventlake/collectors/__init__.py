"""Registry of collector factories, keyed by feed name."""

from collections.abc import Callable

from eventlake.runner import Collector

REGISTRY: dict[str, Callable[[], Collector]] = {}
