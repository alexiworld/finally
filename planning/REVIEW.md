# PLAN.md Review

Reviewer pass over `planning/PLAN.md` as of 2026-06-28. The market-data subsystem is already built (see `MARKET_DATA_SUMMARY.md`); everything else — portfolio, watchlist, chat, frontend, Docker, tests — is still to be developed. This review focuses on what an implementing agent still needs decided, and deliberately does **not** repeat the existing Section 13 "Review Notes" except where the shipped code now changes the answer.

## Overall Assessment

The plan is strong: clear vision, sensible architecture, good rationale tables, and an unusually honest self-review in Section 13. The single-container / single-port / SSE / SQLite choices are well-matched to a no-auth single-user course project. The main gaps are not in the big decisions but in the **contracts between components** — exact JSON payload shapes, error semantics, and a few numeric constants — that multiple agents must agree on to build in parallel without integration friction.

## Status of Section 13 items after the market-data build

The market-data implementation appears to have already resolved several Section 13 questions. The plan text should be updated so it stops asking questions the code has answered:

- **SSE payload shape** — `PriceUpdate` (ticker, price, previous_price, timestamp, change, direction) is now concrete. But the plan's Section 13 promises `change_pct` and `change_label` fields on the SSE payload and `/api/watchlist`, and the shipped `PriceUpdate` dataclass lists `change`/`direction`, not `change_pct`/`change_label`. **Confirm whether the daily/session change % and its label live on the SSE event or are computed in the `/api/watchlist` REST response.** Downstream frontend work is blocked on this.
- **New ticker in simulator** — `seed_prices.py` defines seed prices and GBM params per known ticker. The behavior for an *unknown* ticker added via watchlist (e.g. "XYZ") still needs to be stated as observed behavior, not left as an open question. What does `add_ticker("XYZ")` actually do today — assign a default price/vol, or no-op?
- **Price continuity across restarts** — the summary says `PriceCache` is in-memory only, so the Section 13 "Potential Issue" still stands unresolved. Decide explicitly: accept the reset for the demo, or seed from last-known DB price.

## Open Contracts That Still Block Parallel Work

These are the highest-value things to nail down because two or more agents depend on them.

1. **`GET /api/portfolio` response shape is undefined.** The endpoint promises "positions, cash balance, total value, unrealized P&L" but no field names, nesting, or P&L sign convention. The positions table, heatmap, header, and chat context builder all consume this. Specify the JSON exactly (per-position: ticker, quantity, avg_cost, current_price, market_value, unrealized_pnl, unrealized_pnl_pct, weight; top-level: cash_balance, total_value, total_unrealized_pnl).

2. **`POST /api/portfolio/trade` request/response and error model.** Request is `{ticker, quantity, side}` but: is `quantity` shares or dollars? What HTTP status and body on insufficient cash / insufficient shares / unknown ticker? The plan says manual trades and LLM trades share validation, so this error contract is reused by the chat flow — define it once here (e.g. 422 with `{error_code, message}`).

3. **`actions` JSON shape in `chat_messages`.** Section 13 already flags this; it is worth promoting to a hard spec because the frontend renders it inline and E2E tests assert on it. Recommend persisting *execution results*, not raw LLM intent: `{trades: [{ticker, side, quantity, price, status, error?}], watchlist_changes: [{ticker, action, status}]}`.

4. **Mock LLM response content (`LLM_MOCK=true`).** Still unspecified. The backend author and the E2E author must agree on the exact `message`/`trades`/`watchlist_changes` the mock returns, ideally keyed off the user input (e.g. input containing "buy" triggers a deterministic AAPL buy) so tests can assert the inline trade confirmation path end-to-end.

5. **`GET /api/portfolio/history` shape and bootstrap.** Field names for the P&L chart, plus the Section 13 "initial snapshot" question — take one snapshot at startup/first request so the chart is never empty.

## New Issues Not Covered in Section 13

- **Total portfolio value needs live prices, but `/api/portfolio` is a REST snapshot.** The header shows "portfolio total value (updating live)". That means the frontend must recompute total value as SSE prices arrive, using quantities from the last `/api/portfolio` fetch — the REST endpoint alone won't update live. The plan should state that the header/heatmap/positions P&L are recomputed client-side from the SSE stream, and that `/api/portfolio` is the source of quantities/avg_cost only. This also resolves the SSE-scope question (Section 13): the stream **must** include all held tickers even if removed from the watchlist, or live P&L silently freezes for those positions.

- **Concurrency on trade execution.** SQLite + a background snapshot task + trade writes can collide. Specify the concurrency approach (single writer / WAL mode / serialized DB access). Low risk single-user, but worth one sentence so an agent doesn't discover `database is locked` at integration time.

- **Sell-all / position deletion semantics.** When quantity sold equals quantity held, is the `positions` row deleted or kept at quantity 0? The E2E scenario "position updates or disappears" is ambiguous; the heatmap and positions table need a definite rule.

- **`avg_cost` recomputation on buys vs. sells.** The schema has `avg_cost` but no formula. State that buys update weighted-average cost and sells leave avg_cost unchanged (realized P&L is not tracked, only unrealized) — otherwise different agents may implement different cost-basis math.

- **Chat history persistence vs. window.** Section 13 caps the LLM context window (good), but doesn't say whether `chat_messages` is reloaded into the UI on page refresh. Confirm the frontend fetches history on load (no `GET /api/chat` endpoint exists — either add one or document that history is ephemeral on the client).

- **`LLM_MODEL` not actually parameterized.** Section 13 recommends making the model an env var; Section 5's env table still hardcodes nothing for it. If you accept the recommendation, add `LLM_MODEL` (default `openrouter/openai/gpt-oss-120b`) to Section 5 so it's a real contract, not just a note.

## Simplifications

(Section 13 already covers UUID PKs, the snapshot background task, Recharts-vs-Lightweight-Charts, containerized Playwright, and the `users_profile` name — all still agree.)

- **Drop `GET /api/portfolio/history` polling cadence ambiguity by snapshotting only on trade + startup.** This is the same as Section 13's snapshot-task simplification but stated as an API consequence: with no 30s task, `portfolio_snapshots` rows map 1:1 to meaningful events, and the background-task moving part disappears.

- **One charting library, not two.** Pulling in both Lightweight Charts (main/sparkline) and Recharts (P&L/heatmap) doubles the bundle and the learning surface. Lightweight Charts can do the line charts; a treemap can be a small custom flex/CSS-grid component. Consider committing to one to keep the frontend lean.

## Documentation Consistency Nits

- **CLAUDE.md vs. skill name.** Section 9 and CLAUDE.md reference a `cerebras-inference` skill; the available skill in this workspace is named `cerebras`. Align the name in the plan so the LLM-integration agent invokes the right skill (Section 13 flags the skill as undefined — it now exists, just under a different name).
- **`MASSIVE_API_KEY` = Polygon.io** is stated in the summary but only implied in the plan. Add the one-line clarification in Section 5 so the env var's meaning is self-contained.
- **Port/volume mapping wording.** Section 11 says "The `db/` directory in the project root maps to `/app/db`" but the `docker run` example uses a **named volume** `finally-data:/app/db`, not a bind mount of `./db`. These are different behaviors (named volume won't surface `finally.db` in the repo's `db/` folder). Pick one and make the prose match the command.

## What's Good and Should Not Change

- SSE-over-WebSockets and the single-container model — correct for the use case.
- The PriceCache-as-single-source-of-truth strategy pattern, already validated by the shipped market-data code with 84% coverage.
- Auto-execution of LLM trades without a confirmation dialog — the right call for a fake-money demo and the course's agentic theme.
- The explicit "Why These Choices" rationale table — keep this pattern for the portfolio/chat sections as they're specified.
