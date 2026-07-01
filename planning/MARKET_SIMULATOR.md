# Market Simulator

Approach and code structure for generating synthetic stock prices when no `MASSIVE_API_KEY` is set.

---

## Overview

The simulator uses **Geometric Brownian Motion (GBM)** — the stochastic model underlying Black-Scholes option pricing. Key properties:

- Prices evolve continuously with random noise
- Prices can never go negative (GBM is multiplicative)
- Returns follow a lognormal distribution, matching observed market behaviour
- Each ticker can have its own drift (trend) and volatility
- Correlated moves across tickers via Cholesky decomposition
- Occasional random "events" (sudden 2–5% jumps) for visual drama

The `GBMSimulator` class is pure synchronous Python. The `SimulatorDataSource` wraps it in an async loop (see `MARKET_INTERFACE.md`).

---

## GBM Mathematics

At each discrete time step, a stock price evolves as:

```
S(t + dt) = S(t) × exp( (μ - σ²/2) × dt  +  σ × √dt × Z )
```

Where:
- `S(t)` — current price
- `μ` (mu) — annualised drift (expected return), e.g. `0.05` = 5% per year
- `σ` (sigma) — annualised volatility, e.g. `0.25` = 25% per year
- `dt` — time step as a fraction of one trading year
- `Z` — standard normal random variable, drawn from N(0, 1)

### Choosing `dt`

We update every 500 ms. One trading year ≈ 252 days × 6.5 hours × 3600 s = 5,896,800 seconds.

```
dt = 0.5 / 5_896_800 ≈ 8.48e-8
```

This tiny `dt` produces sub-cent moves per tick which accumulate naturally over simulated "trading time", while still producing realistic intraday ranges (e.g. TSLA at σ=0.50 will swing ~1–3% over a simulated day).

### Why the `(μ - σ²/2)` term?

This is the **Itô correction**: without it, the expected geometric return would not equal μ. The `σ²/2` term accounts for the asymmetry between arithmetic and geometric means under a lognormal distribution.

---

## Correlated Moves

Real stocks don't move independently — tech stocks tend to move together, and so on. We apply a **Cholesky decomposition** of a correlation matrix to produce correlated normal draws.

**Algorithm**:
1. Build an n×n correlation matrix `C` where `C[i][j]` = pairwise correlation between tickers `i` and `j`
2. Compute lower-triangular factor `L = cholesky(C)`, such that `L @ L.T = C`
3. At each step, draw independent standard normals `Z_independent ~ N(0, I)`
4. Apply: `Z_correlated = L @ Z_independent`
5. Each `Z_correlated[i]` is now a draw from a distribution with the desired pairwise correlations

**Default correlation groups**:

| Group | Tickers | Within-group ρ |
|-------|---------|----------------|
| Tech | AAPL, GOOGL, MSFT, AMZN, META, NVDA, NFLX | 0.65 |
| Finance | JPM, V | 0.55 |
| TSLA | TSLA | — (loner: ρ=0.25 with everything) |
| Cross-group | Any tech + any finance | 0.30 |
| Unknown tickers | anything added dynamically | 0.20 (weak correlation) |

The matrix must be **positive semi-definite** for Cholesky to succeed. Our hand-picked correlations satisfy this; if you add extreme values (ρ > 0.9 across many tickers), you may need to project to the nearest PSD matrix (e.g. via eigenvalue clipping).

---

## Random Events

Each tick, every ticker independently has a small probability of a sudden price jump, simulating earnings surprises, news, or large orders:

```python
EVENT_PROBABILITY = 0.001   # 0.1% per tick = ~1 event per 500 ticks per ticker
EVENT_SIZE_RANGE  = (0.02, 0.05)   # 2–5% shock
```

With 10 tickers updating at 2 ticks/second, this produces roughly one visible event somewhere every ~25 seconds — frequent enough to look lively, infrequent enough to not feel spammy.

---

## Seed Prices and Parameters

```python
# backend/app/market/seed_prices.py

SEED_PRICES: dict[str, float] = {
    "AAPL":  190.00,
    "GOOGL": 175.00,
    "MSFT":  420.00,
    "AMZN":  185.00,
    "TSLA":  250.00,
    "NVDA":  800.00,
    "META":  500.00,
    "JPM":   195.00,
    "V":     280.00,
    "NFLX":  600.00,
}

TICKER_PARAMS: dict[str, dict[str, float]] = {
    "AAPL":  {"sigma": 0.22, "mu": 0.05},
    "GOOGL": {"sigma": 0.25, "mu": 0.05},
    "MSFT":  {"sigma": 0.20, "mu": 0.05},
    "AMZN":  {"sigma": 0.28, "mu": 0.05},
    "TSLA":  {"sigma": 0.50, "mu": 0.03},   # high vol, muted drift
    "NVDA":  {"sigma": 0.45, "mu": 0.08},   # high vol, strong drift
    "META":  {"sigma": 0.30, "mu": 0.05},
    "JPM":   {"sigma": 0.18, "mu": 0.04},   # low vol (bank)
    "V":     {"sigma": 0.17, "mu": 0.04},   # low vol (payments network)
    "NFLX":  {"sigma": 0.35, "mu": 0.05},
}

DEFAULT_PARAMS: dict[str, float] = {"sigma": 0.25, "mu": 0.05}
```

Tickers not in `SEED_PRICES` start at a random price between $50–$300 and use `DEFAULT_PARAMS`.

---

## Implementation

```python
# backend/app/market/gbm.py

import math
import random
import numpy as np

from .seed_prices import SEED_PRICES, TICKER_PARAMS, DEFAULT_PARAMS


TECH    = frozenset({"AAPL", "GOOGL", "MSFT", "AMZN", "META", "NVDA", "NFLX"})
FINANCE = frozenset({"JPM", "V"})
TSLA    = frozenset({"TSLA"})

# Pairwise correlation look-up
def _pairwise_correlation(t1: str, t2: str) -> float:
    if t1 == t2:
        return 1.0
    in_tech    = (t1 in TECH) and (t2 in TECH)
    in_finance = (t1 in FINANCE) and (t2 in FINANCE)
    either_tsla = (t1 in TSLA) or (t2 in TSLA)

    if in_tech:    return 0.65
    if in_finance: return 0.55
    if either_tsla: return 0.25
    if (t1 in TECH and t2 in FINANCE) or (t1 in FINANCE and t2 in TECH):
        return 0.30
    return 0.20   # unknown / cross-group


class GBMSimulator:
    """
    Generates correlated GBM price paths for a dynamic set of tickers.

    Call step() once per update interval to advance all prices by one tick.
    """

    DT = 8.48e-8                 # 0.5s / (252 days × 6.5 h × 3600 s)
    EVENT_PROB = 0.001
    EVENT_MIN  = 0.02
    EVENT_MAX  = 0.05

    def __init__(self, tickers: list[str]):
        self._tickers: list[str] = []
        self._prices:  dict[str, float] = {}
        self._params:  dict[str, dict[str, float]] = {}
        self._cholesky: np.ndarray | None = None

        for ticker in tickers:
            self.add_ticker(ticker)

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def add_ticker(self, ticker: str) -> None:
        """Add a ticker. No-op if already present."""
        if ticker in self._prices:
            return
        self._tickers.append(ticker)
        self._prices[ticker] = SEED_PRICES.get(ticker, random.uniform(50.0, 300.0))
        self._params[ticker] = dict(TICKER_PARAMS.get(ticker, DEFAULT_PARAMS))
        self._rebuild_cholesky()

    def remove_ticker(self, ticker: str) -> None:
        """Remove a ticker. No-op if not present."""
        if ticker not in self._prices:
            return
        self._tickers.remove(ticker)
        del self._prices[ticker]
        del self._params[ticker]
        self._rebuild_cholesky()

    def get_tickers(self) -> list[str]:
        return list(self._tickers)

    def get_price(self, ticker: str) -> float | None:
        return self._prices.get(ticker)

    def step(self) -> dict[str, float]:
        """
        Advance all prices by one time step (DT).
        Returns {ticker: new_price} for all active tickers.
        """
        n = len(self._tickers)
        if n == 0:
            return {}

        z_independent = np.random.standard_normal(n)
        z = self._cholesky @ z_independent if self._cholesky is not None else z_independent

        result: dict[str, float] = {}
        for i, ticker in enumerate(self._tickers):
            mu    = self._params[ticker]["mu"]
            sigma = self._params[ticker]["sigma"]

            # GBM step: S(t+dt) = S(t) * exp(drift + diffusion)
            drift     = (mu - 0.5 * sigma ** 2) * self.DT
            diffusion = sigma * math.sqrt(self.DT) * float(z[i])
            self._prices[ticker] *= math.exp(drift + diffusion)

            # Random event shock
            if random.random() < self.EVENT_PROB:
                shock = random.uniform(self.EVENT_MIN, self.EVENT_MAX)
                shock *= random.choice((-1, 1))
                self._prices[ticker] *= (1.0 + shock)

            result[ticker] = round(self._prices[ticker], 2)

        return result

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    def _rebuild_cholesky(self) -> None:
        """Recompute the Cholesky factor of the correlation matrix."""
        n = len(self._tickers)
        if n <= 1:
            self._cholesky = None
            return

        corr = np.eye(n)
        for i in range(n):
            for j in range(i + 1, n):
                rho = _pairwise_correlation(self._tickers[i], self._tickers[j])
                corr[i, j] = rho
                corr[j, i] = rho

        self._cholesky = np.linalg.cholesky(corr)
```

---

## Behaviour Notes

### Prices stay positive
GBM is multiplicative (`exp(...)` is always positive), so prices can never reach zero or go negative regardless of how many steps are taken.

### Drift is negligible per tick
With `dt ≈ 8.5e-8`, the drift term `mu × dt ≈ 4.2e-9` per tick. Over a simulated trading day (~47,000 ticks at 2 ticks/s × 6.5 h), drift contributes only `~0.02%` directional bias — invisible to the user but mathematically correct. The random volatility term completely dominates.

### Cholesky rebuild cost
`_rebuild_cholesky()` is O(n³) in general (Cholesky of an n×n matrix) but n ≤ ~50 in any realistic watchlist, making it trivially fast. It is called only when the ticker set changes, not on every tick.

### Adding tickers at runtime
When a new ticker is added mid-session, it receives a starting price from `SEED_PRICES` (or a random $50–$300 if unknown) and immediately participates in the correlated step. The Cholesky matrix is rebuilt to include it.

### Session vs. daily change %
The simulator has no concept of "yesterday's close." The `change_label` is therefore always `"Session"` — the `previous_price` in each `PriceUpdate` is simply the price from the prior tick, and `change_pct` reflects the tick-to-tick move. The frontend should display "Session" (not "Daily") in this mode.

### Reproducibility
For deterministic test runs, seed NumPy before calling `step()`:

```python
import numpy as np
np.random.seed(42)
sim = GBMSimulator(["AAPL", "TSLA"])
prices = sim.step()
```

---

## File Structure

```
backend/
  app/
    market/
      gbm.py            # GBMSimulator class (pure math, no async)
      seed_prices.py    # SEED_PRICES, TICKER_PARAMS, DEFAULT_PARAMS
      simulator.py      # SimulatorDataSource (wraps GBMSimulator in async loop)
```

`gbm.py` and `seed_prices.py` have zero async code — they are pure Python and easily unit-tested without an event loop.

---

## Unit Test Sketch

```python
# backend/tests/market/test_simulator.py

import numpy as np
from app.market.gbm import GBMSimulator
from app.market.seed_prices import SEED_PRICES


def test_prices_stay_positive():
    sim = GBMSimulator(["AAPL", "TSLA", "JPM"])
    for _ in range(10_000):
        prices = sim.step()
        for price in prices.values():
            assert price > 0


def test_seed_prices_used():
    sim = GBMSimulator(["AAPL"])
    assert sim.get_price("AAPL") == SEED_PRICES["AAPL"]


def test_unknown_ticker_gets_random_price():
    sim = GBMSimulator(["XYZ"])
    price = sim.get_price("XYZ")
    assert 50.0 <= price <= 300.0


def test_add_remove_ticker():
    sim = GBMSimulator(["AAPL"])
    sim.add_ticker("GOOGL")
    assert "GOOGL" in sim.get_tickers()
    sim.remove_ticker("GOOGL")
    assert "GOOGL" not in sim.get_tickers()
    prices = sim.step()
    assert "GOOGL" not in prices


def test_correlated_moves_exist():
    """Tech stocks should show positive correlation over many steps."""
    np.random.seed(0)
    sim = GBMSimulator(["AAPL", "MSFT"])
    aapl_returns, msft_returns = [], []
    prev_aapl = sim.get_price("AAPL")
    prev_msft = sim.get_price("MSFT")

    for _ in range(1000):
        prices = sim.step()
        aapl_returns.append(prices["AAPL"] / prev_aapl - 1)
        msft_returns.append(prices["MSFT"] / prev_msft - 1)
        prev_aapl, prev_msft = prices["AAPL"], prices["MSFT"]

    correlation = np.corrcoef(aapl_returns, msft_returns)[0, 1]
    assert correlation > 0.3   # should be close to the configured 0.65
```
