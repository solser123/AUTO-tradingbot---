# Internal AI Review

## Why this review exists

The user is correct.

The current problem is not mainly "we need more indicators" or "we need another API."

The current problem is that the internal operating model is not aligned with the user's real goal:

- small capital,
- high learning value,
- low waste,
- explainable entries,
- AI used to bridge judgment gaps,
- not AI used as an expensive decorative filter.

## Current internal mismatch

### What the user wants

- AI should reduce the gap between rigid technical rules and actual market timing.
- AI should help with ambiguous cases and trade management.
- AI should justify its cost by improving execution quality and learning quality.

### What the system is currently doing

- AI is used too often before the trade.
- AI is being asked to scan many low-value situations.
- The system still suffers from stale signals, delayed entries, and weak post-trade payoff management.

That means the system is spending credit before it proves operational value.

## Hard evidence from the current live data

Recent 48h decision counts:

- `ai_scan_assist`: 15,107
- `ai_review`: 1,456
- `ai_position_manage`: 20
- `entry opened`: 24

Interpretation:

- The system asked AI to participate more than fifteen thousand times at the scan layer.
- That produced only twenty-four actual entries.
- This is a very poor value-to-credit ratio.

This is the opposite of what an early-stage survival system should do.

## Core internal problems

### 1. AI is used too early and too often

Current pattern:

- scan
- AI scan assist
- more gates
- AI review
- maybe entry

This means many credits are spent before freshness and executability are proven.

### 2. Deterministic safety is not strict enough

AI should not be asked to think about candidates that are already operationally bad.

Examples:

- stale candidates
- session-invalid candidates
- too-thin microstructure
- slippage-risk candidates

These should be filtered by rules first.

### 3. AI is not yet being used where it matters most

The user wants AI to close the judgment gap.

That means AI is most valuable in:

- B-grade entry acceleration,
- post-entry management,
- live explanation and review.

It is less valuable in brute-force scan pass/fail on thousands of low-quality loops.

### 4. Cost awareness is missing in the design

The user is carrying the early burden of:

- OpenAI usage,
- API usage,
- low capital drawdown risk.

That means every AI call must have a reason.

Right now the system does not enforce that strongly enough.

## Correct design principle from now on

If a situation is too irrational or too obvious for AI, do not use AI there.

Use AI only when it creates real leverage.

## New AI role split

### Deterministic only

These should be non-AI:

- stale signal rejection
- slippage hard stop
- max open positions
- session windows
- signal freshness expiry
- emergency stop rules
- hard stop-loss
- minimum liquidity floor

### AI-assisted

These are appropriate for AI:

- B-grade entry upgrade
- exploratory entry approval
- post-entry partial reduction / hold / target raise
- human-readable explanation
- review report generation

### AI should not be primary here

- scanning every symbol every loop
- repeating the same rejection on already bad candidates
- reviewing old candidates after timing decay

## Practical direction to match the user's real goal

The user does not need a "smart-looking" bot.

The user needs a system that:

1. does not waste money,
2. learns from real fills,
3. explains itself,
4. improves steadily.

That means:

### Phase 1. Survival-first validation live

- only fresh signals
- only explainable entries
- AI budgeted
- strict cost discipline

### Phase 2. Useful AI

- AI only on ambiguity and live management
- every AI action must produce either:
  - a trade quality improvement,
  - a risk reduction,
  - or a reportable learning outcome

### Phase 3. Scalable operation

- when the above is proven, widen symbol universe and AI coverage

## Immediate internal redesign

### A. Freshness-first entry model

Every entry must include:

- signal_time
- entry_time
- lag_seconds
- freshness_bucket

Rules:

- aggressive entry expires fast
- exploratory entry expires slightly slower
- old candidates must never be promoted later

### B. AI budget model

Introduce hard limits:

- max AI scan assist calls per loop
- max AI scan assist calls per symbol per hour
- max AI review calls per symbol per setup window
- max AI position management calls per position per interval
- daily AI call budget and budget usage reporting

### C. Value-first AI invocation

AI should only run when:

- candidate is fresh,
- candidate passed deterministic safety,
- candidate is near executable,
- or position is already open.

### D. Mandatory explanation output

Every live action must explain:

- why now,
- why this side,
- why this size,
- what invalidates the trade,
- what changed if AI manages it later.

## What should happen next

No new "smart" feature should go live until these are in place:

1. freshness expiry
2. stale candidate kill switch
3. AI budget counters
4. AI invocation gating
5. post-change cost/value report

## Bottom line

The user is not asking for something irrational.

The user is asking for AI to be used where AI is actually worth paying for.

That is reasonable.

The system should therefore move from:

- "AI everywhere"

to:

- "AI only where deterministic logic is not enough and where the cost is justified."
