# Live Reset Report

## Current judgment

- The system is not yet in a state where it should be trusted for continuous learning-by-loss.
- The main problem is no longer "we need more indicators."
- The main problem is that execution timing, stale candidate reuse, emergency behavior, and validation process are mixed together.

## What the live data actually shows

- Cumulative closed trades: 28
- Win rate: 39.29%
- Realized PnL: -2.3163 USDT
- Current state at review time: emergency stop active
- Emergency reason: abnormal slippage on `SAHARA/USDT:USDT`

## Entry timing problem

Recent live entries show typical lag from prior signal/review to actual entry:

- `SAHARA`: about 6.65 minutes
- `NEAR`: about 6.30 minutes
- `ETH`: about 6.87 minutes
- `HYPER`: about 7.98 minutes
- `ENJ`: about 8.08 minutes
- `ARB`: about 7.65 minutes
- `DOT`: about 8.32 minutes
- `ETC`: about 8.64 minutes

Outliers:

- `UNI`: about 15.60 minutes
- `DEEP`: about 224.50 minutes
- `IMX`: about 217.13 minutes

## Interpretation

- A 6 to 8 minute lag is consistent with the current `BOT_LOOP_SECONDS=60` structure plus extra confirmation stages.
- This means the system is not entering at first valid detection. It is often entering after AI review or later-loop promotion.
- The 200+ minute cases are not normal confirmation delay. They strongly suggest stale candidate reuse.

## Why the user is right to be frustrated

- The system now has many features, but the features are not governed by a strict validation gate.
- Because of that, a feature can exist in code while still producing bad operational behavior.
- This creates a gap between "capability exists" and "capability is trustworthy."

## Core operational failures

### 1. Entry timing is too slow for aggressive entries

- Aggressive entries should not be arriving 6 to 8 minutes after their last meaningful validation.
- If the system wants fast entries, the signal must expire quickly.

### 2. Stale signal reuse is likely happening

- `DEEP` and `IMX` show entry after more than 200 minutes from prior AI scan stage.
- This should never happen in aggressive or exploratory mode.

### 3. Emergency stop currently blocks new analysis loops too broadly

- When emergency stop is active, the loop returns early.
- That means new entry is blocked, but open-position management may also be skipped depending on where the stop triggers.
- This is dangerous for unattended operation.

### 4. Opportunity review is stale

- `opportunity_reviews` latest timestamp is still around `2026-03-20`.
- That means the "learn from missed moves" loop is not currently up to date.

### 5. Logging is not strong enough for validation

- Recent entry payload inspection showed missing `setup_type` in entry logs.
- If the engine/source is not logged cleanly, then later analysis becomes guesswork.

## What is working

- The system does filter some bad trades.
- `take_profit` exits are positive overall.
- AI post-entry management is active and is actually issuing reductions.
- The service and supervisor layer are much more structured than before.

## Why profitability still looks weak

- The system is not mainly failing because "all signals are fake."
- It is failing because:
  - some entries are late,
  - some candidates appear stale,
  - AI post-management is reducing too early,
  - emergency handling interrupts normal operation,
  - missed-move learning is not current.

## Reset method from now on

No more feature-first development.

From now on every change must pass 4 gates:

### Gate 1. Operational safety

- No stale entry candidate older than 2 loops for aggressive mode
- No stale entry candidate older than 4 loops for exploratory mode
- Emergency stop must not abandon open-position management

### Gate 2. Logging completeness

- Every entry must log:
  - engine family
  - setup type
  - signal timestamp
  - entry timestamp
  - lag in seconds
  - AI role and confidence

### Gate 3. Validation before live

- Before enabling live logic:
  - local compile pass
  - deterministic dry run
  - report of expected side effects
  - "what can go wrong" note

### Gate 4. Post-change report

- Every important strategy change must produce a report with:
  - signal count
  - entry count
  - average entry lag
  - blocker changes
  - PnL delta
  - missed-move delta

## Immediate fix priorities

1. Kill stale candidate reuse
2. Keep position management alive during emergency stop
3. Restart opportunity review syncing
4. Record entry lag and engine family in every entry payload
5. Split fast-entry engines from slow-confirmation engines more clearly

## Live mode recommendation

- Current state should be treated as validation-live, not production-live.
- If capital preservation is the priority, do not treat current behavior as acceptable autonomous operation yet.
- The next goal is not "add more signals."
- The next goal is "make every live entry explainable, fresh, and measurable."
