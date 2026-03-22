# Engine Split Report

## Goal

Separate signal generation into distinct engine families so live trading and review can tell which logic produced each candidate.

## Implemented Engine Families

- `continuation`
  - trend-following continuation
  - pullback recovery
  - breakout / breakdown confirmation
- `reversal`
  - early reversal
  - SMC-style reversal
- `scout`
  - AI-assisted exploratory and context-recovery candidates
- `hot_mover`
  - dynamic movers promoted by the hot-mover scout

## Runtime Behavior

- core signal creation still happens in `strategy.py`
- `strategy_engines/` classifies each promoted signal into an engine family
- the selected engine is written into `signal.strategy_data`
  - `engine_key`
  - `engine_family`
  - `engine_priority`
  - `engine_confidence_hint`
  - `engine_exploratory_preferred`
  - `engine_reason`

## Why This Helps

- makes continuation and reversal performance easier to compare later
- lets exploratory and hot-mover logic stay aggressive without polluting core trend logic
- prepares the codebase for a future `portfolio allocator` that can choose between engines, not only symbols

## Next Extension

- route each engine into its own performance bucket in live/backtest reports
- add a dedicated `portfolio allocator` that ranks candidates across engine families
- move more engine-specific thresholds out of `strategy.py` into per-engine configs
