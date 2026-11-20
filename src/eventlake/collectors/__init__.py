"""Registry of collector factories, keyed by feed name."""

from collections.abc import Callable

from eventlake.collectors.kalshi import KalshiCollector
from eventlake.collectors.polymarket import PolymarketCollector
from eventlake.runner import Collector

REGISTRY: dict[str, Callable[[], Collector]] = {
    "kalshi": KalshiCollector,
    "polymarket": PolymarketCollector,
}
