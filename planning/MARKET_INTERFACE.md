# Market Data Interface

Unified Python interface for market data in FinAlly. The simulator and Massive client both implement the same abstract base class. All downstream code — SSE streaming, portfolio valuation, trade execution — is source-agnostic and works with either implementation.

---

## Core Data Model

```python
# backend/app/market/models.py

from dataclasses import dataclass, field

@dataclass
class PriceUpdate:
    """A single price update for one ticker."""
    ticker: str
    price: float
    previous_price: float
    timestamp: float        # Unix seconds (float)
    change: float           # price - previous_price (absolute)
    change_pct: float       # percentage change from previous_price
    direction: str          # "up", "down", or "flat"
    change_label: str = "Session"   # "Daily" (Massive) or "Session" (simulator)
```

`PriceUpdate` is the only structure that crosses the market data layer boundary. The SSE endpoint serialises it directly to JSON.

---

## Abstract Interface

```python
# backend/app/market/interface.py

from abc import ABC, abstractmethod

class MarketDataSource(ABC):
    """
    Abstract interface for market data providers.

    Implementations write price updates into a shared PriceCache.
    The interface itself does not return prices — it pushes updates
    on its own schedule (poll interval or simulation tick).
    """

    @abstractmethod
    async def start(self, tickers: list[str]) -> None:
        """Begin producing price updates for the given tickers."""

    @abstractmethod
    async def stop(self) -> None:
        """Stop producing price updates and release resources."""

    @abstractmethod
    async def add_ticker(self, ticker: str) -> None:
        """Add a ticker to the active set. No-op if already present."""

    @abstractmethod
    async def remove_ticker(self, ticker: str) -> None:
        """Remove a ticker from the active set. No-op if not present."""

    @abstractmethod
    def get_tickers(self) -> list[str]:
        """Return the current list of active tickers."""
```

---

## Price Cache

Shared in-memory store. Both data sources write to it; the SSE streamer and trade execution logic read from it.

```python
# backend/app/market/cache.py

import time
from threading import Lock
from .models import PriceUpdate


class PriceCache:
    """Thread-safe cache of latest prices per ticker."""

    def __init__(self):
        self._prices: dict[str, PriceUpdate] = {}
        self._lock = Lock()

    def update(
        self,
        ticker: str,
        price: float,
        timestamp: float | None = None,
        prev_close: float | None = None,
        change_pct: float | None = None,
        change_label: str = "Session",
    ) -> PriceUpdate:
        """
        Update price for a ticker. Returns the new PriceUpdate.

        prev_close: if supplied (Massive API), used as the reference price for
                    change_pct. Otherwise, the previous cached price is used
                    (simulator / session-relative change).
        """
        with self._lock:
            ts = timestamp or time.time()
            existing = self._prices.get(ticker)
            previous_price = prev_close if prev_close is not None else (
                existing.price if existing else price
            )

            if price > previous_price:
                direction = "up"
            elif price < previous_price:
                direction = "down"
            else:
                direction = "flat"

            if change_pct is None:
                change_pct = (
                    ((price - previous_price) / previous_price * 100)
                    if previous_price != 0 else 0.0
                )

            update = PriceUpdate(
                ticker=ticker,
                price=price,
                previous_price=previous_price,
                timestamp=ts,
                change=price - previous_price,
                change_pct=change_pct,
                direction=direction,
                change_label=change_label,
            )
            self._prices[ticker] = update
            return update

    def get(self, ticker: str) -> PriceUpdate | None:
        """Get the latest price for a ticker. Returns None if unknown."""
        with self._lock:
            return self._prices.get(ticker)

    def get_all(self) -> dict[str, PriceUpdate]:
        """Snapshot of all current prices."""
        with self._lock:
            return dict(self._prices)

    def remove(self, ticker: str) -> None:
        """Remove a ticker from the cache."""
        with self._lock:
            self._prices.pop(ticker, None)
```

---

## Factory Function

Selects the data source at app startup based on the environment variable.

```python
# backend/app/market/factory.py

import os
from .cache import PriceCache
from .interface import MarketDataSource


def create_market_data_source(price_cache: PriceCache) -> MarketDataSource:
    """
    Return a MassiveDataSource if MASSIVE_API_KEY is set and non-empty,
    otherwise return a SimulatorDataSource.
    """
    api_key = os.environ.get("MASSIVE_API_KEY", "").strip()

    if api_key:
        from .massive_client import MassiveDataSource
        return MassiveDataSource(api_key=api_key, price_cache=price_cache)
    else:
        from .simulator import SimulatorDataSource
        return SimulatorDataSource(price_cache=price_cache)
```

---

## Massive Implementation

```python
# backend/app/market/massive_client.py

import asyncio
from polygon import RESTClient
from polygon.exceptions import AuthError, BadResponse

from .interface import MarketDataSource
from .cache import PriceCache


class MassiveDataSource(MarketDataSource):
    """
    Polls the Massive (Polygon.io) REST API for live stock prices.

    One snapshot call per poll cycle fetches all tickers at once,
    keeping API usage minimal on the free tier.
    """

    def __init__(
        self,
        api_key: str,
        price_cache: PriceCache,
        poll_interval: float = 15.0,
    ):
        self._client = RESTClient(api_key=api_key)
        self._cache = price_cache
        self._interval = poll_interval
        self._tickers: list[str] = []
        self._task: asyncio.Task | None = None

    async def start(self, tickers: list[str]) -> None:
        self._tickers = list(tickers)
        self._task = asyncio.create_task(self._poll_loop())

    async def stop(self) -> None:
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass

    async def add_ticker(self, ticker: str) -> None:
        if ticker not in self._tickers:
            self._tickers.append(ticker)

    async def remove_ticker(self, ticker: str) -> None:
        self._tickers = [t for t in self._tickers if t != ticker]
        self._cache.remove(ticker)

    def get_tickers(self) -> list[str]:
        return list(self._tickers)

    async def _poll_loop(self) -> None:
        while True:
            await self._poll_once()
            await asyncio.sleep(self._interval)

    async def _poll_once(self) -> None:
        if not self._tickers:
            return
        try:
            snapshots = await asyncio.to_thread(
                self._client.get_snapshot_all,
                "stocks",
                tickers=list(self._tickers),
            )
            for snap in snapshots:
                self._cache.update(
                    ticker=snap.ticker,
                    price=snap.last_trade.price,
                    timestamp=snap.last_trade.sip_timestamp / 1e9,
                    prev_close=snap.prev_day.close,
                    change_pct=snap.today_change_percent,
                    change_label="Daily",
                )
        except AuthError:
            raise RuntimeError(
                "MASSIVE_API_KEY is invalid. Check your .env file."
            )
        except BadResponse as e:
            if e.status == 429:
                await asyncio.sleep(60)
            else:
                print(f"[Massive] HTTP {e.status}: {e}")
        except Exception as e:
            print(f"[Massive] Poll error: {e}")
```

---

## Simulator Implementation

```python
# backend/app/market/simulator.py

import asyncio
from .interface import MarketDataSource
from .cache import PriceCache
from .gbm import GBMSimulator   # see MARKET_SIMULATOR.md


class SimulatorDataSource(MarketDataSource):
    """
    Generates synthetic stock prices using GBM.
    Updates every 500ms; writes to PriceCache.
    """

    def __init__(
        self,
        price_cache: PriceCache,
        update_interval: float = 0.5,
    ):
        self._cache = price_cache
        self._interval = update_interval
        self._sim: GBMSimulator | None = None
        self._task: asyncio.Task | None = None

    async def start(self, tickers: list[str]) -> None:
        self._sim = GBMSimulator(tickers=tickers)
        self._task = asyncio.create_task(self._run_loop())

    async def stop(self) -> None:
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass

    async def add_ticker(self, ticker: str) -> None:
        if self._sim:
            self._sim.add_ticker(ticker)

    async def remove_ticker(self, ticker: str) -> None:
        if self._sim:
            self._sim.remove_ticker(ticker)
        self._cache.remove(ticker)

    def get_tickers(self) -> list[str]:
        return self._sim.get_tickers() if self._sim else []

    async def _run_loop(self) -> None:
        while True:
            prices = await asyncio.to_thread(self._sim.step)
            for ticker, price in prices.items():
                self._cache.update(ticker=ticker, price=price, change_label="Session")
            await asyncio.sleep(self._interval)
```

---

## Integration with SSE

The SSE endpoint reads from `PriceCache` on a timer and pushes JSON events to connected clients.

```python
# backend/app/routes/stream.py

import asyncio
import json
from fastapi import Request
from fastapi.responses import StreamingResponse
from ..market.cache import PriceCache


async def price_stream_generator(request: Request, price_cache: PriceCache):
    """Yields SSE events; one event per tick containing all known prices."""
    while True:
        if await request.is_disconnected():
            break

        prices = price_cache.get_all()
        data = {
            ticker: {
                "ticker": p.ticker,
                "price": p.price,
                "previous_price": p.previous_price,
                "change": p.change,
                "change_pct": p.change_pct,
                "change_label": p.change_label,
                "direction": p.direction,
                "timestamp": p.timestamp,
            }
            for ticker, p in prices.items()
        }
        yield f"data: {json.dumps(data)}\n\n"
        await asyncio.sleep(0.5)


def price_stream_endpoint(request: Request, price_cache: PriceCache):
    return StreamingResponse(
        price_stream_generator(request, price_cache),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "X-Accel-Buffering": "no",  # disable nginx buffering
        },
    )
```

---

## SSE Scope: Watchlist + Positions

Per PLAN.md §13, the SSE stream must include **all tickers the user needs live prices for** — not just the watchlist. A user may hold a position in a ticker they've removed from the watchlist; that ticker must still stream so portfolio P&L stays live.

At startup (and whenever positions or the watchlist change), the backend builds the union:

```python
def get_all_relevant_tickers(db, watchlist_tickers: list[str]) -> list[str]:
    position_tickers = db.execute(
        "SELECT DISTINCT ticker FROM positions WHERE user_id = 'default'"
    ).fetchall()
    return list(set(watchlist_tickers) | {row[0] for row in position_tickers})
```

This union is passed to `source.start()` and updated via `add_ticker()` / `remove_ticker()` as needed.

---

## App Lifecycle

```python
# backend/app/main.py  (sketch)

from contextlib import asynccontextmanager
from fastapi import FastAPI
from .market.cache import PriceCache
from .market.factory import create_market_data_source
from .db import get_initial_tickers

price_cache = PriceCache()
market_source = None

@asynccontextmanager
async def lifespan(app: FastAPI):
    global market_source
    initial_tickers = get_initial_tickers()         # watchlist + position tickers from DB
    market_source = create_market_data_source(price_cache)
    await market_source.start(initial_tickers)

    # Take an immediate portfolio snapshot so P&L chart isn't empty on first load
    await record_portfolio_snapshot()

    yield

    await market_source.stop()

app = FastAPI(lifespan=lifespan)
```

### Watchlist Route Integration

```python
@app.post("/api/watchlist")
async def add_to_watchlist(body: WatchlistAdd):
    db.insert_watchlist(body.ticker)
    await market_source.add_ticker(body.ticker)
    return {"ticker": body.ticker}

@app.delete("/api/watchlist/{ticker}")
async def remove_from_watchlist(ticker: str):
    db.delete_watchlist(ticker)
    # Only remove from source if ticker has no open position
    if not db.has_position(ticker):
        await market_source.remove_ticker(ticker)
    return {"ticker": ticker}
```

---

## File Structure

```
backend/
  app/
    market/
      __init__.py         # exports: PriceCache, create_market_data_source
      models.py           # PriceUpdate dataclass
      cache.py            # PriceCache
      interface.py        # MarketDataSource ABC
      factory.py          # create_market_data_source()
      massive_client.py   # MassiveDataSource
      simulator.py        # SimulatorDataSource
      gbm.py              # GBMSimulator (pure math, no async)
      seed_prices.py      # SEED_PRICES, TICKER_PARAMS constants
```

---

## Design Decisions

**Why push-to-cache rather than pull-on-demand?**  
Multiple SSE connections would each need to call the API if prices were fetched per-request. The cache decouples data production from consumption: one poller feeds N connected clients.

**Why `asyncio.to_thread()` for the Massive client?**  
The `polygon-api-client` `RESTClient` is synchronous. Wrapping calls in `to_thread()` runs them in a thread pool without blocking the FastAPI event loop.

**Why rebuild the Cholesky matrix when tickers change?**  
The correlation structure is defined over the set of active tickers. Adding or removing a ticker changes the matrix dimensions. The rebuild is O(n²) but n < 50 in any realistic scenario.

**Unknown tickers in simulator**  
If a user adds a ticker not in `SEED_PRICES` (e.g., `"XYZ"`), the simulator assigns a random starting price between $50–$300 and uses the default GBM parameters (`sigma=0.25, mu=0.05`). No error is raised.
