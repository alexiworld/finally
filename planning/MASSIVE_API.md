# Massive API Reference (formerly Polygon.io)

Reference documentation for the Massive (formerly Polygon.io) REST API as used in FinAlly.

## Overview

- **Base URL**: `https://api.massive.com` (legacy `https://api.polygon.io` also supported)
- **Python package**: `polygon-api-client` (install via `uv add polygon-api-client`)
- **Auth**: API key passed to `RESTClient(api_key=...)` or via `APCA-API-KEY-ID` header
- **Auth header**: `Authorization: Bearer <API_KEY>` (the client handles this automatically)

## Rate Limits

| Tier     | API Calls          | Recommended Poll Interval |
|----------|--------------------|---------------------------|
| Free     | 5 requests/minute  | 15 seconds                |
| Starter  | Unlimited          | 5 seconds                 |
| Developer| Unlimited          | 2 seconds                 |
| Advanced | Unlimited          | 1 second                  |

For FinAlly: use the snapshot endpoint (one call per poll regardless of ticker count) to stay well within limits.

## Client Initialization

```python
from polygon import RESTClient

# Pass API key explicitly
client = RESTClient(api_key="your_key_here")

# Or read from environment variable POLYGON_API_KEY
import os
client = RESTClient(api_key=os.environ["MASSIVE_API_KEY"])
```

---

## Endpoints Used in FinAlly

### 1. Snapshot — All Tickers (Primary Endpoint)

Gets current prices for **multiple tickers in a single API call**. This is the main polling endpoint.

**REST**: `GET /v2/snapshot/locale/us/markets/stocks/tickers?tickers=AAPL,GOOGL,MSFT`

**Python client**:
```python
from polygon import RESTClient

client = RESTClient(api_key="your_key")

snapshots = client.get_snapshot_all(
    "stocks",
    tickers=["AAPL", "GOOGL", "MSFT", "AMZN", "TSLA"],
)

for snap in snapshots:
    ticker  = snap.ticker
    price   = snap.last_trade.price
    prev_close = snap.prev_day.close      # previous session close
    change_pct = snap.today_change_percent  # % change from prev close
    ts      = snap.last_trade.sip_timestamp  # Unix nanoseconds
    print(f"{ticker}: ${price:.2f}  {change_pct:+.2f}%")
```

**Response structure** (per ticker snapshot):
```json
{
  "ticker": "AAPL",
  "todaysChangePerc": -1.23,
  "todaysChange": -2.34,
  "updated": 1714000000000000000,
  "day": {
    "o": 187.50,
    "h": 190.10,
    "l": 185.20,
    "c": 188.00,
    "v": 55234100,
    "vw": 187.93
  },
  "lastTrade": {
    "p": 188.00,
    "s": 100,
    "x": 4,
    "t": 1714000000000000000
  },
  "lastQuote": {
    "P": 188.01,
    "S": 2,
    "p": 187.99,
    "s": 3,
    "t": 1714000000000100000
  },
  "min": {
    "o": 187.80,
    "h": 188.10,
    "l": 187.50,
    "c": 188.00,
    "v": 120000,
    "vw": 187.88,
    "av": 55234100,
    "t": 1714000000000000000,
    "n": 850
  },
  "prevDay": {
    "o": 186.00,
    "h": 191.50,
    "l": 185.00,
    "c": 190.34,
    "v": 62100000,
    "vw": 188.91
  }
}
```

**Key fields extracted for FinAlly**:
| Field | Python attribute | Use |
|-------|-----------------|-----|
| Current price | `snap.last_trade.price` | Display + trade execution |
| Previous close | `snap.prev_day.close` | Daily change % calculation |
| Daily change % | `snap.today_change_percent` | Watchlist display |
| Timestamp | `snap.last_trade.sip_timestamp` | Nanoseconds → divide by 1e9 for seconds |

---

### 2. Single Ticker Snapshot

For detailed data on one ticker (e.g., when user clicks a ticker for the detail chart).

**REST**: `GET /v2/snapshot/locale/us/markets/stocks/tickers/{ticker}`

**Python client**:
```python
snap = client.get_snapshot_ticker("stocks", "AAPL")

print(f"Price:     ${snap.last_trade.price:.2f}")
print(f"Bid/Ask:   ${snap.last_quote.bid_price:.2f} / ${snap.last_quote.ask_price:.2f}")
print(f"Day range: ${snap.day.low:.2f} – ${snap.day.high:.2f}")
print(f"Volume:    {snap.day.volume:,}")
print(f"Change:    {snap.today_change_percent:+.2f}%")
```

---

### 3. Previous Close

Gets the prior trading day's OHLCV bar. Useful for seeding the simulator's starting prices from real data.

**REST**: `GET /v2/aggs/ticker/{ticker}/prev`

**Python client**:
```python
result = client.get_previous_close_agg("AAPL")

for bar in result:
    print(f"Prev close: ${bar.close:.2f}")
    print(f"OHLC: O={bar.open} H={bar.high} L={bar.low} C={bar.close}")
    print(f"Volume: {bar.volume:,}")
    print(f"Date: {bar.timestamp}")   # Unix milliseconds
```

**REST response**:
```json
{
  "ticker": "AAPL",
  "resultsCount": 1,
  "results": [
    {
      "o": 186.00,
      "h": 191.50,
      "l": 185.00,
      "c": 190.34,
      "v": 62100000,
      "vw": 188.91,
      "t": 1713916800000,
      "n": 521234
    }
  ]
}
```

---

### 4. Aggregates (OHLCV Bars)

Historical bars over a date range. Used for populating the main ticker chart with historical data.

**REST**: `GET /v2/aggs/ticker/{ticker}/range/{multiplier}/{timespan}/{from}/{to}`

**Python client**:
```python
bars = []
for bar in client.list_aggs(
    "AAPL",
    multiplier=1,
    timespan="minute",       # "second", "minute", "hour", "day", "week", "month", "quarter", "year"
    from_="2024-01-02",
    to="2024-01-31",
    limit=50000,
    adjusted=True,           # adjust for splits/dividends
):
    bars.append(bar)

# Each bar has: open, high, low, close, volume, vwap, timestamp (ms), transactions
for bar in bars[-5:]:
    print(f"{bar.timestamp}: O={bar.open} H={bar.high} L={bar.low} C={bar.close} V={bar.volume}")
```

**Common timespans for FinAlly**:
- `"minute"` with `multiplier=1` — intraday chart (up to 30 days back on free tier)
- `"day"` with `multiplier=1` — daily chart (1+ year of history)

---

### 5. Last Trade / Last Quote

Individual endpoints for the very latest trade or NBBO quote for a single ticker.

```python
# Last trade
trade = client.get_last_trade("AAPL")
print(f"Last trade: ${trade.price:.2f} x {trade.size} shares")
print(f"Exchange:   {trade.exchange}")
print(f"Timestamp:  {trade.sip_timestamp}")  # nanoseconds

# Last NBBO quote
quote = client.get_last_quote("AAPL")
print(f"Bid: ${quote.bid_price:.2f} x {quote.bid_size}")
print(f"Ask: ${quote.ask_price:.2f} x {quote.ask_size}")
```

---

## How FinAlly Uses the API

The `MassiveDataSource` runs as a background `asyncio` task:

1. Collects all tickers from the current watchlist + any held positions
2. Calls `get_snapshot_all()` with those tickers (**one API call per poll cycle**)
3. Extracts `last_trade.price`, `prev_day.close`, `today_change_percent`, and timestamp
4. Writes results to the shared `PriceCache`
5. Sleeps for the configured poll interval, then repeats

```python
import asyncio
import os
from polygon import RESTClient

async def poll_massive(
    api_key: str,
    get_tickers: callable,
    price_cache,
    interval: float = 15.0,
) -> None:
    """Background task: poll Massive and update price cache."""
    client = RESTClient(api_key=api_key)

    while True:
        tickers = get_tickers()
        if tickers:
            try:
                snapshots = await asyncio.to_thread(
                    client.get_snapshot_all,
                    "stocks",
                    tickers=tickers,
                )
                for snap in snapshots:
                    price_cache.update(
                        ticker=snap.ticker,
                        price=snap.last_trade.price,
                        prev_close=snap.prev_day.close,
                        change_pct=snap.today_change_percent,
                        timestamp=snap.last_trade.sip_timestamp / 1e9,  # ns → seconds
                    )
            except Exception as e:
                # Log and continue — do not crash the polling loop
                print(f"[Massive] Poll error: {e}")

        await asyncio.sleep(interval)
```

---

## Error Handling

The client raises exceptions for HTTP errors. Common ones:

| Status | Meaning | Action |
|--------|---------|--------|
| 401 | Invalid or missing API key | Log + surface config error |
| 403 | Plan doesn't include this endpoint | Log + fall back to simulator |
| 404 | Ticker not found | Skip that ticker, log warning |
| 429 | Rate limit exceeded | Back off; increase poll interval |
| 5xx | Server error | Retry after next interval |

```python
from polygon.exceptions import AuthError, BadResponse

try:
    snapshots = client.get_snapshot_all("stocks", tickers=tickers)
except AuthError:
    raise RuntimeError("MASSIVE_API_KEY is invalid — check your .env file")
except BadResponse as e:
    if e.status == 429:
        await asyncio.sleep(60)   # hard back-off on rate limit
    else:
        print(f"[Massive] HTTP {e.status}: {e}")
```

---

## Market Hours & Data Freshness

- **Real-time**: Paid tiers receive quotes with sub-second latency during market hours (9:30 AM–4:00 PM ET)
- **After-hours / pre-market**: `last_trade.price` reflects the most recent trade, which may be an AH/PM trade
- **Weekends / holidays**: `last_trade.price` is the closing price from the most recent trading day; `today_change_percent` will be stale
- **Timestamps**: All API timestamps are Unix **nanoseconds** (divide by `1e9` for seconds, `1e6` for milliseconds)
- **`day` vs `prevDay`**: The `day` OHLCV object resets at market open; `prevDay` is the completed prior session

---

## change_label for Frontend Display

Per the design spec (PLAN.md §13), the backend should tell the frontend whether the displayed change % is "Daily" or "Session":

```python
# In SSE payload and /api/watchlist response
payload = {
    "ticker": snap.ticker,
    "price": snap.last_trade.price,
    "change_pct": snap.today_change_percent,
    "change_label": "Daily",   # Massive always provides true daily %
}
```

Contrast with the simulator, which uses `"Session"` because it has no anchor for yesterday's close.

---

## Python Package Notes

The package is `polygon-api-client` on PyPI (not `massive` or `polygon`):

```toml
# backend/pyproject.toml
[project]
dependencies = [
    "polygon-api-client>=1.14",
]
```

```bash
uv add polygon-api-client
```

The `RESTClient` is synchronous. Wrap calls in `asyncio.to_thread()` to avoid blocking the FastAPI event loop.
