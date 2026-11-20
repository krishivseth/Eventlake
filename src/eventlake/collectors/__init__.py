"""Registry of collector factories, keyed by feed name."""

from collections.abc import Callable

from eventlake.collectors.cftc import CftcCollector
from eventlake.collectors.edgar import EdgarCollector
from eventlake.collectors.fred import FredCollector
from eventlake.collectors.kalshi import KalshiCollector
from eventlake.collectors.noaa import NoaaCollector
from eventlake.collectors.polymarket import PolymarketCollector
from eventlake.collectors.tradier import TradierOptionsCollector
from eventlake.runner import Collector

REGISTRY: dict[str, Callable[[], Collector]] = {
    "cftc": CftcCollector,
    "edgar": EdgarCollector,
    "fred": FredCollector,
    "kalshi": KalshiCollector,
    "noaa": NoaaCollector,
    "polymarket": PolymarketCollector,
    "tradier.options": TradierOptionsCollector,
}
