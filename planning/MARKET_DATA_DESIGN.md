# Market Data Backend — Design

Authoritative, implementation-accurate design for the FinAlly market data subsystem. Covers the unified `MarketDataSource` interface, the thread-safe price cache, the GBM simulator, the Massive (Polygon.io) API client, the SSE streaming endpoint, and FastAPI lifecycle wiring.

**Status:** Implemented and shipped in `backend/app/market/` (8 modules, ~500 lines). 73 tests, 84% coverage. This document supersedes the draft sketches in `planning/archive/` (`MARKET_DATA_DESIGN.md`, `MARKET_INTERFACE.md`, `MASSIVE_API.md`, `MARKET_SIMULATOR.md`) — those captured the initial design; this document reflects the code as built, including the fixes from `planning/archive/MARKET_DATA_REVIEW.md`, and explicitly resolves the market-data-related open questions from `PLAN.md` §13 and `REVIEW.md`.

---

## Table of Contents

1. [Goals & Non-Goals](#1-goals--non-goals)
2. [Architecture](#2-architecture)
3. [File Structure](#3-file-structure)
4. [Data Model — `models.py`](#4-data-model)
5. [Price Cache — `cache.py`](#5-price-cache)
6. [Abstract Interface — `interface.py`](#6-abstract-interface)
7. [Seed Prices & Ticker Parameters — `seed_prices.py`](#7-seed-prices--ticker-parameters)
8. [GBM Simulator — `simulator.py`](#8-gbm-simulator)
9. [Massive API Client — `massive_client.py`](#9-massive-api-client)
10. [Factory — `factory.py`](#10-factory)
11. [SSE Streaming Endpoint — `stream.py`](#11-sse-streaming-endpoint)
12. [FastAPI Lifecycle Integration](#12-fastapi-lifecycle-integration)
13. [Watchlist Coordination](#13-watchlist-coordination)
14. [Testing](#14-testing)
15. [Resolved Design Questions](#15-resolved-design-questions)
16. [Known Limitations](#16-known-limitations)
17. [Configuration Summary](#17-configuration-summary)

---

## 1. Goals & Non-Goals

**Goals**
- One interface, two interchangeable implementations: a zero-dependency GBM simulator (default) and a real-data poller against the Massive (Polygon.io) REST API.
- All downstream code (SSE stream, portfolio valuation, trade execution) reads prices from a single in-memory cache and is completely unaware of which data source is active.
- Sub-second perceived latency for the frontend (~500ms SSE cadence) regardless of the underlying source's actual update rate.

**Non-Goals**
- No order book, no bid/ask spread modeling, no multi-exchange routing — this is a single "last price" per ticker.
- No historical bar storage in this layer (aggregates could be added later for chart backfill, but are not required by `PLAN.md`).
- No WebSocket ingestion from Massive — REST polling only, per `PLAN.md` §6 ("simpler, works on all tiers").

---

## 2. Architecture

```
MarketDataSource (ABC)
├── SimulatorDataSource  →  GBM simulator (default, no API key needed)
└── MassiveDataSource    →  Polygon.io REST poller (when MASSIVE_API_KEY set)
        │
        ▼
   PriceCache (thread-safe, in-memory, versioned)
        │
        ├──→ SSE stream endpoint (GET /api/stream/prices)
        ├──→ Portfolio valuation (GET /api/portfolio)
        └──→ Trade execution (POST /api/portfolio/trade)
```

Both implementations are a **Strategy pattern**: the app picks one at startup via an environment variable and never branches on which one is active again. Both write to the same `PriceCache`; nothing downstream imports `simulator.py` or `massive_client.py` directly.

---

## 3. File Structure

```
backend/
  app/
    market/
      __init__.py             # Re-exports: PriceUpdate, PriceCache, MarketDataSource,
                               #   create_market_data_source, create_stream_router
      models.py                # PriceUpdate dataclass
      cache.py                 # PriceCache (thread-safe in-memory store)
      interface.py              # MarketDataSource ABC
      seed_prices.py            # SEED_PRICES, TICKER_PARAMS, DEFAULT_PARAMS, correlation constants
      simulator.py               # GBMSimulator + SimulatorDataSource
      massive_client.py          # MassiveDataSource
      factory.py                 # create_market_data_source()
      stream.py                  # SSE endpoint (FastAPI router factory)
  tests/
    market/
      test_models.py
      test_cache.py
      test_simulator.py
      test_simulator_source.py
      test_factory.py
      test_massive.py
  market_data_demo.py           # Rich terminal demo (uv run market_data_demo.py)
```

Each file has a single responsibility. `__init__.py` re-exports the public surface so the rest of the backend does `from app.market import ...` and never reaches into submodules.

---

## 4. Data Model

**File: `backend/app/market/models.py`**

`PriceUpdate` is the only object that leaves the market data layer. SSE streaming, portfolio valuation, and trade execution all consume this type exclusively.

```python
"""Data models for market data."""

from __future__ import annotations

import time
from dataclasses import dataclass, field


@dataclass(frozen=True, slots=True)
class PriceUpdate:
    """Immutable snapshot of a single ticker's price at a point in time."""

    ticker: str
    price: float
    previous_price: float
    timestamp: float = field(default_factory=time.time)  # Unix seconds

    @property
    def change(self) -> float:
        """Absolute price change from previous update."""
        return round(self.price - self.previous_price, 4)

    @property
    def change_percent(self) -> float:
        """Percentage change from previous update."""
        if self.previous_price == 0:
            return 0.0
        return round((self.price - self.previous_price) / self.previous_price * 100, 4)

    @property
    def direction(self) -> str:
        """'up', 'down', or 'flat'."""
        if self.price > self.previous_price:
            return "up"
        elif self.price < self.previous_price:
            return "down"
        return "flat"

    def to_dict(self) -> dict:
        """Serialize for JSON / SSE transmission."""
        return {
            "ticker": self.ticker,
            "price": self.price,
            "previous_price": self.previous_price,
            "timestamp": self.timestamp,
            "change": self.change,
            "change_percent": self.change_percent,
            "direction": self.direction,
        }
```

### Design decisions

- **`frozen=True`** — value objects are immutable once created, safe to share across async tasks without copying.
- **`slots=True`** — memory optimization; many instances are created per second at 2 ticks/sec × N tickers.
- **Computed properties** (`change`, `change_percent`, `direction`) — derived from `price`/`previous_price` so they can never be inconsistent with each other.
- **`to_dict()`** — single serialization point shared by the SSE endpoint and any REST route that needs to embed a price.

`change_percent` here is the **tick-over-tick** percentage change (this update vs. the immediately preceding one), not a daily change. See [§15.1](#151-daily-change--change_pct--change_label) for how this composes with `PLAN.md`'s daily/session change requirement.

---

## 5. Price Cache

**File: `backend/app/market/cache.py`**

The central data hub: producers (one data source at a time) write, consumers (SSE, portfolio, trade execution) read. It is not read from and written to only on the same event loop, so it must be safe across threads.

```python
"""Thread-safe in-memory price cache."""

from __future__ import annotations

import time
from threading import Lock

from .models import PriceUpdate


class PriceCache:
    """Thread-safe in-memory cache of the latest price for each ticker.

    Writers: SimulatorDataSource or MassiveDataSource (one at a time).
    Readers: SSE streaming endpoint, portfolio valuation, trade execution.
    """

    def __init__(self) -> None:
        self._prices: dict[str, PriceUpdate] = {}
        self._lock = Lock()
        self._version: int = 0  # Monotonically increasing; bumped on every update

    def update(self, ticker: str, price: float, timestamp: float | None = None) -> PriceUpdate:
        """Record a new price for a ticker. Returns the created PriceUpdate.

        Automatically computes direction and change from the previous price.
        If this is the first update for the ticker, previous_price == price (direction='flat').
        """
        with self._lock:
            ts = timestamp or time.time()
            prev = self._prices.get(ticker)
            previous_price = prev.price if prev else price

            update = PriceUpdate(
                ticker=ticker,
                price=round(price, 2),
                previous_price=round(previous_price, 2),
                timestamp=ts,
            )
            self._prices[ticker] = update
            self._version += 1
            return update

    def get(self, ticker: str) -> PriceUpdate | None:
        """Get the latest price for a single ticker, or None if unknown."""
        with self._lock:
            return self._prices.get(ticker)

    def get_all(self) -> dict[str, PriceUpdate]:
        """Snapshot of all current prices. Returns a shallow copy."""
        with self._lock:
            return dict(self._prices)

    def get_price(self, ticker: str) -> float | None:
        """Convenience: get just the price float, or None."""
        update = self.get(ticker)
        return update.price if update else None

    def remove(self, ticker: str) -> None:
        """Remove a ticker from the cache (e.g., when removed from watchlist)."""
        with self._lock:
            self._prices.pop(ticker, None)

    @property
    def version(self) -> int:
        """Current version counter. Useful for SSE change detection."""
        return self._version

    def __len__(self) -> int:
        with self._lock:
            return len(self._prices)

    def __contains__(self, ticker: str) -> bool:
        with self._lock:
            return ticker in self._prices
```

### Why a version counter

The SSE loop polls the cache every ~500ms. Without a version counter it would re-serialize and resend every ticker on every tick even when nothing changed (which matters a lot for the Massive source, which only updates every 15s). The SSE loop instead does:

```python
last_version = -1
while True:
    if price_cache.version != last_version:
        last_version = price_cache.version
        yield format_sse(price_cache.get_all())
    await asyncio.sleep(0.5)
```

### Thread-safety rationale

`threading.Lock`, not `asyncio.Lock`, because:
- The Massive client's synchronous `get_snapshot_all()` runs via `asyncio.to_thread()` — a real OS thread, which `asyncio.Lock` does not protect against.
- `threading.Lock` is correct from both a worker thread and the async event loop.
- The critical sections are tiny (dict read/write), so contention is negligible at this scale (≤50 tickers, one writer).

---

## 6. Abstract Interface

**File: `backend/app/market/interface.py`**

```python
"""Abstract interface for market data sources."""

from __future__ import annotations

from abc import ABC, abstractmethod


class MarketDataSource(ABC):
    """Contract for market data providers.

    Implementations push price updates into a shared PriceCache on their own
    schedule. Downstream code never calls the data source directly for prices —
    it reads from the cache.

    Lifecycle:
        source = create_market_data_source(cache)
        await source.start(["AAPL", "GOOGL", ...])
        # ... app runs ...
        await source.add_ticker("TSLA")
        await source.remove_ticker("GOOGL")
        # ... app shutting down ...
        await source.stop()
    """

    @abstractmethod
    async def start(self, tickers: list[str]) -> None:
        """Begin producing price updates for the given tickers.

        Starts a background task that periodically writes to the PriceCache.
        Must be called exactly once. Calling start() twice is undefined behavior.
        """

    @abstractmethod
    async def stop(self) -> None:
        """Stop the background task and release resources.

        Safe to call multiple times. After stop(), the source will not write
        to the cache again.
        """

    @abstractmethod
    async def add_ticker(self, ticker: str) -> None:
        """Add a ticker to the active set. No-op if already present.

        The next update cycle will include this ticker.
        """

    @abstractmethod
    async def remove_ticker(self, ticker: str) -> None:
        """Remove a ticker from the active set. No-op if not present.

        Also removes the ticker from the PriceCache.
        """

    @abstractmethod
    def get_tickers(self) -> list[str]:
        """Return the current list of actively tracked tickers."""
```

### Why the source writes to the cache instead of returning prices

Push, not pull. This decouples timing: the simulator ticks at 500ms, Massive polls at 15s, but SSE always reads the cache at its own 500ms cadence regardless of which source produced the last write. Nothing downstream needs to know the active source's update interval.

---

## 7. Seed Prices & Ticker Parameters

**File: `backend/app/market/seed_prices.py`**

Constants only, stdlib-only. Shared by the simulator (initial prices, GBM params, correlation grouping).

```python
"""Seed prices and per-ticker parameters for the market simulator."""

# Realistic starting prices for the default watchlist (as of project creation)
SEED_PRICES: dict[str, float] = {
    "AAPL": 190.00,
    "GOOGL": 175.00,
    "MSFT": 420.00,
    "AMZN": 185.00,
    "TSLA": 250.00,
    "NVDA": 800.00,
    "META": 500.00,
    "JPM": 195.00,
    "V": 280.00,
    "NFLX": 600.00,
}

# Per-ticker GBM parameters
# sigma: annualized volatility (higher = more price movement)
# mu: annualized drift / expected return
TICKER_PARAMS: dict[str, dict[str, float]] = {
    "AAPL": {"sigma": 0.22, "mu": 0.05},
    "GOOGL": {"sigma": 0.25, "mu": 0.05},
    "MSFT": {"sigma": 0.20, "mu": 0.05},
    "AMZN": {"sigma": 0.28, "mu": 0.05},
    "TSLA": {"sigma": 0.50, "mu": 0.03},  # High volatility
    "NVDA": {"sigma": 0.40, "mu": 0.08},  # High volatility, strong drift
    "META": {"sigma": 0.30, "mu": 0.05},
    "JPM": {"sigma": 0.18, "mu": 0.04},  # Low volatility (bank)
    "V": {"sigma": 0.17, "mu": 0.04},  # Low volatility (payments)
    "NFLX": {"sigma": 0.35, "mu": 0.05},
}

# Default parameters for tickers not in the list above (dynamically added)
DEFAULT_PARAMS: dict[str, float] = {"sigma": 0.25, "mu": 0.05}

# Correlation groups for the simulator's Cholesky decomposition
# Tickers in the same group have higher intra-group correlation
CORRELATION_GROUPS: dict[str, set[str]] = {
    "tech": {"AAPL", "GOOGL", "MSFT", "AMZN", "META", "NVDA", "NFLX"},
    "finance": {"JPM", "V"},
}

# Correlation coefficients
INTRA_TECH_CORR = 0.6  # Tech stocks move together
INTRA_FINANCE_CORR = 0.5  # Finance stocks move together
CROSS_GROUP_CORR = 0.3  # Between sectors / unknown tickers
TSLA_CORR = 0.3  # TSLA does its own thing
```

> The shipped file has only one "unmatched" correlation constant (`CROSS_GROUP_CORR`), used both for cross-sector pairs and for pairs involving a ticker outside any named group. The earlier draft in `archive/MARKET_DATA_DESIGN.md` also defined a `DEFAULT_CORR` constant that was numerically identical (0.3) but never referenced — it was removed as dead code during the design review (see `archive/MARKET_DATA_REVIEW.md` §4.3).

---

## 8. GBM Simulator

**File: `backend/app/market/simulator.py`**

Two classes: `GBMSimulator` (pure math engine, stateful) and `SimulatorDataSource` (the `MarketDataSource` implementation wrapping it in an async loop).

### 8.1 The math

```
S(t+dt) = S(t) * exp((mu - sigma^2/2) * dt + sigma * sqrt(dt) * Z)
```

- `S(t)` — current price
- `mu` — annualized drift (expected return)
- `sigma` — annualized volatility
- `dt` — time step as a fraction of a trading year
- `Z` — correlated standard normal draw

For 500ms ticks over a 252-day, 6.5-hour trading year: `dt = 0.5 / (252 * 6.5 * 3600) ≈ 8.48e-8`. This tiny `dt` produces sub-cent per-tick moves that accumulate naturally, and the multiplicative/exponential form guarantees prices stay strictly positive.

### 8.2 Correlated moves

Real stocks don't move independently. A Cholesky decomposition `L` of the ticker correlation matrix `C` (`L = cholesky(C)`) turns independent standard normals into correlated ones: `Z_correlated = L @ Z_independent`. Correlation structure: tech↔tech 0.6, finance↔finance 0.5, TSLA↔anything 0.3, everything else 0.3.

### 8.3 Code

```python
"""GBM-based market simulator."""

from __future__ import annotations

import asyncio
import logging
import math
import random

import numpy as np

from .cache import PriceCache
from .interface import MarketDataSource
from .seed_prices import (
    CORRELATION_GROUPS,
    CROSS_GROUP_CORR,
    DEFAULT_PARAMS,
    INTRA_FINANCE_CORR,
    INTRA_TECH_CORR,
    SEED_PRICES,
    TICKER_PARAMS,
    TSLA_CORR,
)

logger = logging.getLogger(__name__)


class GBMSimulator:
    """Geometric Brownian Motion simulator for correlated stock prices."""

    # 500ms expressed as a fraction of a trading year
    # 252 trading days * 6.5 hours/day * 3600 seconds/hour = 5,896,800 seconds
    TRADING_SECONDS_PER_YEAR = 252 * 6.5 * 3600  # 5,896,800
    DEFAULT_DT = 0.5 / TRADING_SECONDS_PER_YEAR  # ~8.48e-8

    def __init__(
        self,
        tickers: list[str],
        dt: float = DEFAULT_DT,
        event_probability: float = 0.001,
    ) -> None:
        self._dt = dt
        self._event_prob = event_probability

        self._tickers: list[str] = []
        self._prices: dict[str, float] = {}
        self._params: dict[str, dict[str, float]] = {}
        self._cholesky: np.ndarray | None = None

        for ticker in tickers:
            self._add_ticker_internal(ticker)
        self._rebuild_cholesky()

    def step(self) -> dict[str, float]:
        """Advance all tickers by one time step. Returns {ticker: new_price}."""
        n = len(self._tickers)
        if n == 0:
            return {}

        z_independent = np.random.standard_normal(n)
        z_correlated = self._cholesky @ z_independent if self._cholesky is not None else z_independent

        result: dict[str, float] = {}
        for i, ticker in enumerate(self._tickers):
            params = self._params[ticker]
            mu, sigma = params["mu"], params["sigma"]

            drift = (mu - 0.5 * sigma**2) * self._dt
            diffusion = sigma * math.sqrt(self._dt) * z_correlated[i]
            self._prices[ticker] *= math.exp(drift + diffusion)

            # Random event: ~0.1% chance per tick per ticker (~every 50s across 10 tickers)
            if random.random() < self._event_prob:
                shock_magnitude = random.uniform(0.02, 0.05)
                shock_sign = random.choice([-1, 1])
                self._prices[ticker] *= 1 + shock_magnitude * shock_sign
                logger.debug(
                    "Random event on %s: %.1f%% %s",
                    ticker, shock_magnitude * 100, "up" if shock_sign > 0 else "down",
                )

            result[ticker] = round(self._prices[ticker], 2)

        return result

    def add_ticker(self, ticker: str) -> None:
        """Add a ticker to the simulation. Rebuilds the correlation matrix."""
        if ticker in self._prices:
            return
        self._add_ticker_internal(ticker)
        self._rebuild_cholesky()

    def remove_ticker(self, ticker: str) -> None:
        """Remove a ticker from the simulation. Rebuilds the correlation matrix."""
        if ticker not in self._prices:
            return
        self._tickers.remove(ticker)
        del self._prices[ticker]
        del self._params[ticker]
        self._rebuild_cholesky()

    def get_price(self, ticker: str) -> float | None:
        return self._prices.get(ticker)

    def get_tickers(self) -> list[str]:
        """Return the list of currently tracked tickers."""
        return list(self._tickers)

    def _add_ticker_internal(self, ticker: str) -> None:
        if ticker in self._prices:
            return
        self._tickers.append(ticker)
        self._prices[ticker] = SEED_PRICES.get(ticker, random.uniform(50.0, 300.0))
        self._params[ticker] = TICKER_PARAMS.get(ticker, dict(DEFAULT_PARAMS))

    def _rebuild_cholesky(self) -> None:
        n = len(self._tickers)
        if n <= 1:
            self._cholesky = None
            return

        corr = np.eye(n)
        for i in range(n):
            for j in range(i + 1, n):
                rho = self._pairwise_correlation(self._tickers[i], self._tickers[j])
                corr[i, j] = corr[j, i] = rho

        self._cholesky = np.linalg.cholesky(corr)

    @staticmethod
    def _pairwise_correlation(t1: str, t2: str) -> float:
        tech = CORRELATION_GROUPS["tech"]
        finance = CORRELATION_GROUPS["finance"]

        if t1 == "TSLA" or t2 == "TSLA":
            return TSLA_CORR
        if t1 in tech and t2 in tech:
            return INTRA_TECH_CORR
        if t1 in finance and t2 in finance:
            return INTRA_FINANCE_CORR
        return CROSS_GROUP_CORR


class SimulatorDataSource(MarketDataSource):
    """MarketDataSource backed by the GBM simulator."""

    def __init__(
        self,
        price_cache: PriceCache,
        update_interval: float = 0.5,
        event_probability: float = 0.001,
    ) -> None:
        self._cache = price_cache
        self._interval = update_interval
        self._event_prob = event_probability
        self._sim: GBMSimulator | None = None
        self._task: asyncio.Task | None = None

    async def start(self, tickers: list[str]) -> None:
        self._sim = GBMSimulator(tickers=tickers, event_probability=self._event_prob)
        # Seed the cache with initial prices so SSE has data immediately
        for ticker in tickers:
            price = self._sim.get_price(ticker)
            if price is not None:
                self._cache.update(ticker=ticker, price=price)
        self._task = asyncio.create_task(self._run_loop(), name="simulator-loop")
        logger.info("Simulator started with %d tickers", len(tickers))

    async def stop(self) -> None:
        if self._task and not self._task.done():
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        self._task = None
        logger.info("Simulator stopped")

    async def add_ticker(self, ticker: str) -> None:
        if self._sim:
            self._sim.add_ticker(ticker)
            price = self._sim.get_price(ticker)
            if price is not None:
                self._cache.update(ticker=ticker, price=price)
            logger.info("Simulator: added ticker %s", ticker)

    async def remove_ticker(self, ticker: str) -> None:
        if self._sim:
            self._sim.remove_ticker(ticker)
        self._cache.remove(ticker)
        logger.info("Simulator: removed ticker %s", ticker)

    def get_tickers(self) -> list[str]:
        return self._sim.get_tickers() if self._sim else []

    async def _run_loop(self) -> None:
        """Core loop: step the simulation, write to cache, sleep."""
        while True:
            try:
                if self._sim:
                    prices = self._sim.step()
                    for ticker, price in prices.items():
                        self._cache.update(ticker=ticker, price=price)
            except Exception:
                logger.exception("Simulator step failed")
            await asyncio.sleep(self._interval)
```

### Key behaviors

- **Immediate seeding** — `start()` populates the cache with seed prices *before* the loop begins, so the SSE endpoint has data on its very first tick.
- **Graceful cancellation** — `stop()` cancels the task and awaits it, swallowing `CancelledError`, for clean shutdown under FastAPI's lifespan teardown.
- **Exception resilience** — the loop catches exceptions per-step so one bad tick can't kill the feed.
- **Unknown ticker handling** — `_add_ticker_internal` falls back to `random.uniform(50.0, 300.0)` for a seed price and `DEFAULT_PARAMS` (`sigma=0.25, mu=0.05`) for GBM parameters when a ticker isn't in `SEED_PRICES`/`TICKER_PARAMS`. This is what happens today if a user (or the LLM) adds `"XYZ"` to the watchlist — resolves the open question in `PLAN.md` §13 ("New ticker in simulator").

---

## 9. Massive API Client

**File: `backend/app/market/massive_client.py`**

Polls the Massive (formerly Polygon.io) REST snapshot endpoint on a configurable interval. The synchronous `massive` client runs inside `asyncio.to_thread()` so it never blocks the event loop.

```python
"""Massive (Polygon.io) API client for real market data."""

from __future__ import annotations

import asyncio
import logging

from massive import RESTClient
from massive.rest.models import SnapshotMarketType

from .cache import PriceCache
from .interface import MarketDataSource

logger = logging.getLogger(__name__)


class MassiveDataSource(MarketDataSource):
    """MarketDataSource backed by the Massive (Polygon.io) REST API.

    Polls GET /v2/snapshot/locale/us/markets/stocks/tickers for all watched
    tickers in a single API call, then writes results to the PriceCache.

    Rate limits:
      - Free tier: 5 req/min → poll every 15s (default)
      - Paid tiers: higher limits → poll every 2-5s
    """

    def __init__(
        self,
        api_key: str,
        price_cache: PriceCache,
        poll_interval: float = 15.0,
    ) -> None:
        self._api_key = api_key
        self._cache = price_cache
        self._interval = poll_interval
        self._tickers: list[str] = []
        self._task: asyncio.Task | None = None
        self._client: RESTClient | None = None

    async def start(self, tickers: list[str]) -> None:
        self._client = RESTClient(api_key=self._api_key)
        self._tickers = list(tickers)

        # Immediate first poll so the cache has data right away
        await self._poll_once()

        self._task = asyncio.create_task(self._poll_loop(), name="massive-poller")
        logger.info(
            "Massive poller started: %d tickers, %.1fs interval",
            len(tickers), self._interval,
        )

    async def stop(self) -> None:
        if self._task and not self._task.done():
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        self._task = None
        self._client = None
        logger.info("Massive poller stopped")

    async def add_ticker(self, ticker: str) -> None:
        ticker = ticker.upper().strip()
        if ticker not in self._tickers:
            self._tickers.append(ticker)
            logger.info("Massive: added ticker %s (will appear on next poll)", ticker)

    async def remove_ticker(self, ticker: str) -> None:
        ticker = ticker.upper().strip()
        self._tickers = [t for t in self._tickers if t != ticker]
        self._cache.remove(ticker)
        logger.info("Massive: removed ticker %s", ticker)

    def get_tickers(self) -> list[str]:
        return list(self._tickers)

    async def _poll_loop(self) -> None:
        """Poll on interval. First poll already happened in start()."""
        while True:
            await asyncio.sleep(self._interval)
            await self._poll_once()

    async def _poll_once(self) -> None:
        """Execute one poll cycle: fetch snapshots, update cache."""
        if not self._tickers or not self._client:
            return

        try:
            # Synchronous client — run in a thread to avoid blocking the event loop.
            snapshots = await asyncio.to_thread(self._fetch_snapshots)
            processed = 0
            for snap in snapshots:
                try:
                    price = snap.last_trade.price
                    timestamp = snap.last_trade.timestamp / 1000.0  # ms -> seconds
                    self._cache.update(ticker=snap.ticker, price=price, timestamp=timestamp)
                    processed += 1
                except (AttributeError, TypeError) as e:
                    logger.warning("Skipping snapshot for %s: %s", getattr(snap, "ticker", "???"), e)
            logger.debug("Massive poll: updated %d/%d tickers", processed, len(self._tickers))

        except Exception as e:
            logger.error("Massive poll failed: %s", e)
            # Don't re-raise — retried on the next interval.
            # Common failures: 401 (bad key), 429 (rate limit), network errors.

    def _fetch_snapshots(self) -> list:
        """Synchronous call to the Massive REST API. Runs in a thread."""
        return self._client.get_snapshot_all(
            market_type=SnapshotMarketType.STOCKS,
            tickers=self._tickers,
        )
```

> `massive` and `massive.rest.models` are imported at module level (not lazily inside `start()`). The initial draft used a lazy import to keep `massive` optional for simulator-only installs, but `pyproject.toml` declares `massive>=1.0.0` as a core dependency, so the lazy import only added test friction (see `archive/MARKET_DATA_REVIEW.md` §3.2) without buying real optionality. `create_market_data_source()` still only *instantiates* `MassiveDataSource` when `MASSIVE_API_KEY` is set, so the simulator-only path never talks to the network.

### Response shape reference

Each snapshot returned by `get_snapshot_all()`:

```json
{
  "ticker": "AAPL",
  "day": {
    "open": 129.61, "high": 130.15, "low": 125.07, "close": 125.07,
    "volume": 111237700, "previous_close": 129.61,
    "change": -4.54, "change_percent": -3.50
  },
  "last_trade": { "price": 125.07, "size": 100, "exchange": "XNYS", "timestamp": 1675190399000 },
  "last_quote": { "bid_price": 125.06, "ask_price": 125.08, "bid_size": 500, "ask_size": 1000 }
}
```

Only `last_trade.price` and `last_trade.timestamp` feed the `PriceCache` today. `day.previous_close` / `day.change_percent` are available on the same object for the daily-change feature described in [§15.1](#151-daily-change--change_pct--change_label) but are not currently threaded through — they would be read in the same `_poll_once()` loop and passed to a REST-layer daily-change calculation, not into `PriceCache.update()`, since `PriceUpdate` is deliberately source-agnostic and doesn't have a concept of "previous close."

### Error handling philosophy

| Error | Behavior |
|-------|----------|
| **401 Unauthorized** | Logged as error. Poller keeps running (user might fix `.env` and restart). |
| **429 Rate Limited** | Logged as error. Retries after `poll_interval` seconds. |
| **Network timeout** | Logged as error. Retries automatically next cycle. |
| **Malformed snapshot** | That ticker is skipped with a warning; other tickers in the batch still process. |
| **All tickers fail** | Cache retains last-known prices — SSE keeps streaming stale data rather than nothing. |

This resolves the "Massive API failure mode" open question from `PLAN.md` §13: the backend does **not** fall back to the simulator on Massive failure — it retries the same source indefinitely and serves stale cached prices in the meantime. A silent fallback to simulated prices would be worse for a real-money-adjacent feature (even in a paper-trading demo) because it could mask a broken API key behind data that *looks* live.

---

## 10. Factory

**File: `backend/app/market/factory.py`**

```python
"""Factory for creating market data sources."""

from __future__ import annotations

import logging
import os

from .cache import PriceCache
from .interface import MarketDataSource
from .massive_client import MassiveDataSource
from .simulator import SimulatorDataSource

logger = logging.getLogger(__name__)


def create_market_data_source(price_cache: PriceCache) -> MarketDataSource:
    """Create the appropriate market data source based on environment variables.

    - MASSIVE_API_KEY set and non-empty → MassiveDataSource (real market data)
    - Otherwise → SimulatorDataSource (GBM simulation)

    Returns an unstarted source. Caller must await source.start(tickers).
    """
    api_key = os.environ.get("MASSIVE_API_KEY", "").strip()

    if api_key:
        logger.info("Market data source: Massive API (real data)")
        return MassiveDataSource(api_key=api_key, price_cache=price_cache)
    else:
        logger.info("Market data source: GBM Simulator")
        return SimulatorDataSource(price_cache=price_cache)
```

Usage:

```python
price_cache = PriceCache()
source = create_market_data_source(price_cache)
await source.start(initial_tickers)  # e.g. ["AAPL", "GOOGL", ...]
```

---

## 11. SSE Streaming Endpoint

**File: `backend/app/market/stream.py`**

```python
"""SSE streaming endpoint for live price updates."""

from __future__ import annotations

import asyncio
import json
import logging
from collections.abc import AsyncGenerator

from fastapi import APIRouter, Request
from fastapi.responses import StreamingResponse

from .cache import PriceCache

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/stream", tags=["streaming"])


def create_stream_router(price_cache: PriceCache) -> APIRouter:
    """Create the SSE streaming router with a reference to the price cache."""

    @router.get("/prices")
    async def stream_prices(request: Request) -> StreamingResponse:
        """SSE endpoint for live price updates.

        Streams all tracked ticker prices every ~500ms. The client connects
        with EventSource and receives events shaped like:

            data: {"AAPL": {"ticker": "AAPL", "price": 190.50, ...}, ...}
        """
        return StreamingResponse(
            _generate_events(price_cache, request),
            media_type="text/event-stream",
            headers={
                "Cache-Control": "no-cache",
                "Connection": "keep-alive",
                "X-Accel-Buffering": "no",  # Disable nginx buffering if proxied
            },
        )

    return router


async def _generate_events(
    price_cache: PriceCache,
    request: Request,
    interval: float = 0.5,
) -> AsyncGenerator[str, None]:
    """Yield SSE-formatted price events every `interval` seconds until disconnect."""
    yield "retry: 1000\n\n"  # browser auto-reconnect delay if the connection drops

    last_version = -1
    client_ip = request.client.host if request.client else "unknown"
    logger.info("SSE client connected: %s", client_ip)

    try:
        while True:
            if await request.is_disconnected():
                logger.info("SSE client disconnected: %s", client_ip)
                break

            current_version = price_cache.version
            if current_version != last_version:
                last_version = current_version
                prices = price_cache.get_all()
                if prices:
                    data = {ticker: update.to_dict() for ticker, update in prices.items()}
                    yield f"data: {json.dumps(data)}\n\n"

            await asyncio.sleep(interval)
    except asyncio.CancelledError:
        logger.info("SSE stream cancelled for: %s", client_ip)
```

### Wire format

```
data: {"AAPL":{"ticker":"AAPL","price":190.50,"previous_price":190.42,"timestamp":1707580800.5,"change":0.08,"change_percent":0.042,"direction":"up"},"GOOGL":{...}}

```

Client side:

```javascript
const eventSource = new EventSource('/api/stream/prices');
eventSource.onmessage = (event) => {
    const prices = JSON.parse(event.data);
    // { "AAPL": { ticker, price, previous_price, change, change_percent, direction, timestamp }, ... }
};
```

### Why poll-and-push instead of event-driven

Fixed-interval polling of the cache (rather than the data source notifying the SSE layer directly) keeps updates evenly spaced, which matters for the frontend's sparkline accumulation — irregular spacing would distort the visual rate of change. It also means the SSE layer has zero coupling to the data source's own cadence.

---

## 12. FastAPI Lifecycle Integration

The market data system starts and stops with the app via FastAPI's `lifespan` context manager.

**In `backend/app/main.py`:**

```python
from contextlib import asynccontextmanager

from fastapi import FastAPI

from app.market import PriceCache, create_market_data_source, create_stream_router


@asynccontextmanager
async def lifespan(app: FastAPI):
    # --- STARTUP ---
    price_cache = PriceCache()
    app.state.price_cache = price_cache

    source = create_market_data_source(price_cache)
    app.state.market_source = source

    # Tickers to track = watchlist ∪ tickers with an open position (see §13)
    initial_tickers = await load_tracked_tickers()  # reads from SQLite
    await source.start(initial_tickers)

    app.include_router(create_stream_router(price_cache))

    yield  # app is running

    # --- SHUTDOWN ---
    await source.stop()


app = FastAPI(title="FinAlly", lifespan=lifespan)


def get_price_cache() -> PriceCache:
    return app.state.price_cache


def get_market_source():
    return app.state.market_source
```

### Accessing market data from other routes

```python
from fastapi import APIRouter, Depends, HTTPException

router = APIRouter(prefix="/api")


@router.post("/portfolio/trade")
async def execute_trade(trade: TradeRequest, price_cache: PriceCache = Depends(get_price_cache)):
    current_price = price_cache.get_price(trade.ticker)
    if current_price is None:
        raise HTTPException(400, f"Price not yet available for {trade.ticker}. Try again shortly.")
    # ... execute trade at current_price ...


@router.post("/watchlist")
async def add_to_watchlist(payload: WatchlistAdd, source=Depends(get_market_source)):
    # ... insert into watchlist table ...
    await source.add_ticker(payload.ticker)
    # ...
```

---

## 13. Watchlist Coordination

The market data source must always be tracking the union of (a) watchlist tickers and (b) tickers with an open position, even after a ticker is removed from the watchlist. Otherwise a held-but-unwatched position's live P&L silently freezes — this resolves the "SSE scope" open question from `PLAN.md` §13.

### Adding a ticker

```
User (or LLM) → POST /api/watchlist {ticker: "PYPL"}
  → Insert into watchlist table (SQLite)
  → await source.add_ticker("PYPL")
      Simulator: adds to GBMSimulator, rebuilds Cholesky, seeds cache immediately
      Massive:   appends to ticker list, appears on next poll (≤ poll_interval later)
  → Return success (ticker + current price if already cached)
```

### Removing a ticker

```python
@router.delete("/watchlist/{ticker}")
async def remove_from_watchlist(ticker: str, source=Depends(get_market_source)):
    await db.delete_watchlist_entry(ticker)

    # Keep tracking if the user still holds shares — the position table,
    # not the watchlist table, is the source of truth for "must stay in the cache."
    position = await db.get_position(ticker)
    if position is None or position.quantity == 0:
        await source.remove_ticker(ticker)

    return {"status": "ok"}
```

`load_tracked_tickers()` at startup must run the equivalent union query (`SELECT ticker FROM watchlist UNION SELECT ticker FROM positions WHERE quantity != 0`) so a restart doesn't silently stop tracking a held position that was previously removed from the watchlist.

---

## 14. Testing

**Suite:** `backend/tests/market/` — 73 tests, 84% overall coverage.

| Module | Tests | Coverage | Notes |
|--------|-------|----------|-------|
| `test_models.py` | 11 | 100% | `PriceUpdate` properties and serialization |
| `test_cache.py` | 13 | 100% | Update/get/remove, version counter, thread-safety of the API surface |
| `test_simulator.py` | 17 | 98% | GBM math, add/remove ticker, Cholesky rebuild, positivity of prices |
| `test_simulator_source.py` | 10 | — | Integration: start/stop lifecycle, cache seeding, dynamic add/remove |
| `test_factory.py` | 7 | 100% | Env-var branching between simulator and Massive |
| `test_massive.py` | 13 | 56% | Mocked snapshots; real API surface can't run without live credentials |

Representative cases (see the actual files for the full set):

```python
# test_simulator.py
def test_prices_are_positive(self):
    """GBM prices can never go negative (exp() is always positive)."""
    sim = GBMSimulator(tickers=["AAPL"])
    for _ in range(10_000):
        assert sim.step()["AAPL"] > 0

def test_unknown_ticker_gets_random_seed_price(self):
    sim = GBMSimulator(tickers=["ZZZZ"])
    assert 50.0 <= sim.get_price("ZZZZ") <= 300.0
```

```python
# test_cache.py
def test_version_increments(self):
    cache = PriceCache()
    v0 = cache.version
    cache.update("AAPL", 190.00)
    assert cache.version == v0 + 1
```

```python
# test_massive.py — mocks the massive package's response objects directly
def _make_snapshot(ticker: str, price: float, timestamp_ms: int) -> MagicMock:
    snap = MagicMock()
    snap.ticker = ticker
    snap.last_trade.price = price
    snap.last_trade.timestamp = timestamp_ms
    return snap

async def test_malformed_snapshot_skipped(self):
    cache = PriceCache()
    source = MassiveDataSource(api_key="test-key", price_cache=cache, poll_interval=60.0)
    source._tickers = ["AAPL", "BAD"]
    bad_snap = MagicMock(ticker="BAD", last_trade=None)  # triggers AttributeError
    with patch.object(source, "_fetch_snapshots", return_value=[_make_snapshot("AAPL", 190.50, 0), bad_snap]):
        await source._poll_once()
    assert cache.get_price("AAPL") == 190.50
    assert cache.get_price("BAD") is None
```

Run locally:

```bash
cd backend
uv run --extra dev pytest -v
uv run --extra dev pytest --cov=app
uv run --extra dev ruff check app/ tests/
```

Live demo (no test framework, just visual confirmation):

```bash
cd backend
uv run market_data_demo.py   # Rich terminal dashboard, 60s or Ctrl+C
```

---

## 15. Resolved Design Questions

These are the `PLAN.md` §13 / `REVIEW.md` open items that fall inside the market data layer, with the answer this implementation gives.

### 15.1 Daily change: `change_pct` / `change_label`

`PriceUpdate.change_percent` (shipped) is **tick-over-tick**, not daily. `PLAN.md` §13 asks for a `change_pct` (daily/session) and `change_label` ("Daily" vs "Session") field on the SSE payload and `/api/watchlist` response. Decision: **this stays out of `PriceUpdate` and the market data layer entirely.**

- `PriceUpdate` has no concept of "market open price" or "page-load baseline" — those are session/UI concerns, not data-source concerns, and baking them in would break the source-agnostic contract (the simulator has no "yesterday's close"; Massive's `day.previous_close` only exists for that one source).
- **Massive mode**: the REST/watchlist layer reads `day.previous_close` and `day.change_percent` directly off the same snapshot object already being polled in `_poll_once()` (see §9) and exposes them as `change_pct` / `change_label: "Daily"` in the `/api/watchlist` response — not in the SSE payload.
- **Simulator mode**: the frontend already receives every tick via SSE from page load onward, so "session change" is trivially `(current - first_seen) / first_seen`, computed client-side from the first SSE event per ticker. The backend labels this `change_label: "Session"` in `/api/watchlist` so the frontend can render the correct column header without knowing which data source is active.
- The SSE payload itself only ever carries the source-agnostic `change`/`change_percent` (tick-over-tick, used for the flash animation), never `change_pct`/`change_label`.

This keeps `PriceUpdate` and `PriceCache` unchanged and pushes the daily/session distinction to the (not-yet-built) watchlist REST route, which is the right layer since it already needs to be source-aware for other reasons.

### 15.2 New ticker in the simulator

Resolved by shipped code, not a design gap: `GBMSimulator._add_ticker_internal` assigns `random.uniform(50.0, 300.0)` as the seed price and `DEFAULT_PARAMS` (`sigma=0.25, mu=0.05`) when the ticker isn't in `SEED_PRICES`/`TICKER_PARAMS`. No error, no silent no-op — the ticker starts producing prices on the very next `add_ticker()` call. See `test_unknown_ticker_gets_random_seed_price` in §14.

### 15.3 SSE scope (watchlist vs. held positions)

Resolved in §13 above: the set of tracked tickers is `watchlist ∪ {ticker : position.quantity != 0}`, enforced both at startup (`load_tracked_tickers()`) and on watchlist deletion (only call `source.remove_ticker()` if there's no open position).

### 15.4 Massive API failure mode

Resolved in §9: no fallback to the simulator. The poller logs and retries on its own interval indefinitely; the cache serves the last-known price in the meantime. A silent simulator fallback was rejected because it would present fabricated prices as if they were live market data.

---

## 16. Known Limitations

### 16.1 Price continuity across restarts (unresolved)

`PriceCache` is purely in-memory and `SimulatorDataSource.start()` always re-seeds from `SEED_PRICES`. If a user buys AAPL at a simulated $192 and the container restarts, AAPL resets to ~$190 and unresolved P&L will look wrong until the random walk drifts back. This is the one item from `PLAN.md` §13 this document does **not** resolve — it is an accepted limitation for the course-project scope (fake money, single-user, restarts are rare in a demo session), not a design decision.

If this needs fixing later, the shape of the fix is: persist the last known price per ticker (e.g., a `prices` table written on some cadence, or piggybacked onto `portfolio_snapshots`), and have `SimulatorDataSource.start()` prefer that value over `SEED_PRICES` when present. This does not require any interface change — `MarketDataSource.start(tickers)` could remain as-is if the last-known-price lookup happens inside the simulator's own initialization, or `start()` could optionally accept a `seed_overrides: dict[str, float]` argument.

### 16.2 Massive API error surfacing

A bad `MASSIVE_API_KEY` produces logged 401s but no user-visible signal beyond "prices never move." The connection-status indicator (SSE-level) still shows "connected" because the *stream* is fine — only the *data* is stale. This matches `PLAN.md` §13's note that connecting vs. connected are different things worth distinguishing; it is a frontend/observability concern, not something to fix inside `app/market/`.

---

## 17. Configuration Summary

| Parameter | Location | Default | Description |
|-----------|----------|---------|--------------|
| `MASSIVE_API_KEY` | Environment variable | `""` (empty) | If set, use Massive API; otherwise use simulator |
| `update_interval` | `SimulatorDataSource.__init__` | `0.5` sec | Time between simulator ticks |
| `poll_interval` | `MassiveDataSource.__init__` | `15.0` sec | Time between Massive API polls (free tier: 5 req/min) |
| `event_probability` | `GBMSimulator.__init__` | `0.001` | Chance of a random shock event per ticker per tick |
| `dt` | `GBMSimulator.__init__` | `~8.5e-8` | GBM time step (fraction of a trading year) |
| SSE push interval | `_generate_events()` | `0.5` sec | Cadence of cache polling / push to connected clients |
| SSE retry directive | `_generate_events()` | `1000` ms | Browser `EventSource` reconnection delay |
