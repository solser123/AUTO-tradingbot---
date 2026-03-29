# External Standards Gap Checklist

Date: 2026-03-30  
Scope: compare the current trading bot architecture against widely used external operating standards

## Reference Standards

This checklist is based on the following external references:

- QuantConnect Algorithm Framework:
  - Alpha / Portfolio Construction / Risk Management / Execution separation
- Freqtrade:
  - strategy callbacks, stoploss management, protections
- SEC Rule 15c3-5:
  - documented market access risk controls and supervisory review
- Trading Technologies risk controls:
  - account/user position limits, credit limits, pre-trade price controls

These are not copied into our system one-to-one. They are used as structural benchmarks.

## Summary

Current overall assessment:

- Technical signal generation: medium to strong
- Risk control layer: strong
- Capital allocation layer: medium
- Execution control layer: medium
- Role ownership clarity: medium
- AI usage discipline: medium
- Auditability by decision owner: low to medium

Main conclusion:

The bot is no longer missing the major building blocks.  
The biggest remaining gap is not “missing indicators”.  
It is that ownership boundaries, allocator logic, and role-based reporting are still not explicit enough.

## Checklist

### 1. Signal Generation Separated From Allocation

External expectation:
- signal creation should be separate from capital sizing and portfolio weighting

Current state:
- partially aligned
- signal generation lives in:
  - `binance_bot/strategy.py`
  - `binance_bot/strategy_engines/*`
- sizing/allocation lives in:
  - `binance_bot/sizing.py`
  - parts of `binance_bot/risk.py`
  - parts of `binance_bot/engine.py`

Gap:
- signal engines are now split better than before
- but allocator behavior is still partly mixed into execution flow

Priority:
- high

Needed next:
- create an explicit allocator summary layer
- separate “candidate accepted” from “capital assigned”

### 2. Dedicated Portfolio Construction Layer

External expectation:
- systems like QuantConnect use a dedicated portfolio construction stage after signal generation

Current state:
- partially aligned
- the bot has portfolio gate logic and dynamic open cap
- but it does not yet have a formal portfolio-construction module

Gap:
- no single allocator object owns final basket selection
- correlation, urgency, and sector exposure are handled in scattered gates

Priority:
- very high

Needed next:
- create a real portfolio allocator owned by CFO role
- inputs:
  - engine family
  - correlation bucket
  - sector crowding
  - score / RR
  - remaining risk budget
- outputs:
  - selected symbols
  - selected size
  - rejected by allocation reason

### 3. Dedicated Risk Layer With Hard Veto

External expectation:
- pre-trade risk controls must be explicit, documented, and capable of veto

Current state:
- aligned
- `binance_bot/risk.py` is already a true veto layer

Strengths:
- emergency stop
- daily/weekly loss limits
- stop distance checks
- sector and correlation checks
- open risk caps

Gap:
- risk decisions are strong, but role ownership is still not surfaced clearly in reports

Priority:
- medium

Needed next:
- role-tag risk blocks as CRO decisions in reporting

### 4. Execution Readiness Layer

External expectation:
- execution quality should be checked separately from signal logic
- stale signals, spread, slippage, price reasonability, and order validity should be handled before order placement

Current state:
- partially aligned
- current execution checks exist in:
  - `binance_bot/engine.py`
  - `binance_bot/exchange.py`

Strengths:
- signal freshness
- invalid symbol filtering
- microstructure gating
- slippage emergency handling

Gap:
- execution readiness is scattered and not formally summarized
- there is no single “COO decision object”

Priority:
- high

Needed next:
- create explicit execution readiness output:
  - executable_now
  - stale
  - micro_reject
  - session_reject
  - symbol_invalid

### 5. Post-Entry Management Separate From Entry

External expectation:
- open-trade management should be independent from entry logic
- Freqtrade-style custom stoploss / custom exit / protections are examples of this separation

Current state:
- aligned in direction
- `binance_bot/ai_position_manager.py` exists

Strengths:
- AI is already limited more to post-entry management than before
- entry AI is no longer the main gate

Gap:
- position management still needs stronger reporting and measurement
- not enough breakdown by action quality:
  - hold quality
  - premature reduce
  - target raise effectiveness

Priority:
- high

Needed next:
- report PnL by AI action family
- evaluate whether AI management is additive or destructive

### 6. Documented Supervisory Review

External expectation:
- SEC-style controls require regular review of effectiveness and prompt issue handling

Current state:
- partially aligned
- we do have:
  - live reports
  - ops reports
  - runtime failure reports
  - opportunity reviews

Gap:
- review exists, but not yet tied to fixed role owners
- no formal “who blocked it?” dashboard by role

Priority:
- medium

Needed next:
- add reporting sections:
  - blocked by CRO
  - blocked by COO
  - rejected by allocator
  - candidate never produced by CIO/CTO

### 7. Explicit Regime Control

External expectation:
- many strong systems operate differently depending on regime instead of applying one static rule set everywhere

Current state:
- partially aligned
- the bot has adaptive profile behavior and engine-family awareness
- but it does not yet have an explicit regime owner

Gap:
- no clean regime state like:
  - continuation-led
  - reversal-led
  - defensive
  - hot-mover opportunistic

Priority:
- high

Needed next:
- create explicit regime state
- this should be the CEO layer output

### 8. Audit Trail Per Decision Owner

External expectation:
- each important trade decision should be attributable to a control owner or module

Current state:
- weak to medium
- logs are rich
- but ownership is still more stage-based than role-based

Gap:
- we know the stage
- we do not always know the owner identity in a governance sense

Priority:
- high

Needed next:
- tag decisions with role owner:
  - CIO/CTO
  - CMO
  - CRO
  - CFO
  - COO
  - AI PM

### 9. AI Governance

External expectation:
- AI should not have unconstrained control over capital or safety rules

Current state:
- improved
- entry AI has been reduced
- AI is mainly used for position management now

Strengths:
- AI is no longer the primary entry bottleneck
- hard risk remains outside AI

Gap:
- AI management still needs stricter scorecards
- no formal “AI may do X, may not do Y” runtime policy object yet

Priority:
- medium

Needed next:
- formalize AI action policy
- report compliance with that policy

### 10. Portfolio Basket Selection Quality

External expectation:
- portfolio engines should decide not just whether a trade is valid, but whether it deserves one of the limited available slots

Current state:
- partial
- current portfolio gate exists

Gap:
- current basket decision is still closer to a gate than a true optimizer
- there is no ranked allocation committee output

Priority:
- very high

Needed next:
- explicit basket scoring
- candidate ordering by:
  - engine family
  - expected quality
  - correlation load
  - sector overlap
  - execution readiness
  - urgency

## Biggest Current Gaps

If reduced to only the most important structural gaps, they are:

1. No explicit CFO allocator module
2. No explicit CEO regime state
3. No explicit COO execution-readiness object
4. Role-based reporting is still weak
5. Portfolio gate is not yet a full portfolio construction layer

## What Is Already Strong

These are not weak points anymore:

- hard pre-trade risk blocking
- emergency handling
- invalid symbol filtering
- signal freshness control
- technical engine split starting to exist
- position-management AI separated from entry

## Recommended Order

The best order to close the remaining standard gaps is:

1. Build CEO regime state
2. Build CFO allocator module
3. Build COO execution-readiness summary
4. Tag every block/open/manage event with role owner
5. Add daily report by role and by engine family

## Bottom Line

Compared to external standards, the system is no longer missing the major control categories.  
It is now at the stage where the next quality leap comes from:

- explicit ownership
- explicit regime control
- explicit portfolio allocation
- explicit execution-readiness reporting

That is the difference between a complex bot and a small systematic trading firm.
