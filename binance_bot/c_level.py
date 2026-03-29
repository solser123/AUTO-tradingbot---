from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime


@dataclass(frozen=True)
class RegimeState:
    regime: str
    continuation_bias: float
    reversal_bias: float
    hot_mover_bias: float
    defensive_bias: float
    owner: str
    reason: str

    def as_payload(self) -> dict[str, float | str]:
        payload = asdict(self)
        for key in ("continuation_bias", "reversal_bias", "hot_mover_bias", "defensive_bias"):
            payload[key] = round(float(payload[key]), 4)
        return payload


@dataclass(frozen=True)
class AllocationDecision:
    allowed: bool
    score: float
    threshold: float
    reason: str
    owner: str
    payload: dict[str, float | int | str | bool]


@dataclass(frozen=True)
class ExecutionReadiness:
    ready: bool
    reason: str
    owner: str
    freshness_ok: bool
    freshness_limit_seconds: int
    signal_age_seconds: float
    session_ok: bool
    session_override: bool
    micro_ok: bool
    micro_soft_pass: bool
    symbol_valid: bool
    payload: dict[str, float | int | str | bool]


ROLE_OWNER_BY_STAGE: dict[str, str] = {
    "scan": "CIO_CTO",
    "regime_state": "CEO",
    "context_recovery": "CIO_CTO",
    "entry_policy": "CIO_CTO",
    "ai_scan_assist": "CIO_CTO",
    "ai_scan_budget": "CIO_CTO",
    "ai_scan_gate": "CIO_CTO",
    "ai_review": "CIO_CTO",
    "ai_review_budget": "CIO_CTO",
    "overflow_review": "CIO_CTO",
    "overflow_budget": "CIO_CTO",
    "overflow_committee": "CIO_CTO",
    "horizon_gate": "CEO",
    "hot_mover_scout": "CMO",
    "sector_gate": "CMO",
    "sector_flow_sync": "CMO",
    "external_gate": "CMO",
    "external_sync": "CMO",
    "sizing_model": "CFO",
    "portfolio_gate": "CFO",
    "risk_gate": "CRO",
    "emergency_stop": "CRO",
    "balance_check": "CRO",
    "signal_freshness": "COO",
    "execution_readiness": "COO",
    "micro_gate": "COO",
    "runtime_exception": "COO",
    "runtime_recovery": "COO",
    "position_reconcile": "COO",
    "telegram": "COO",
    "opportunity_sync": "COO",
    "entry": "COO",
    "ai_position_manage": "AI_PM",
    "position_rebalance": "AI_PM",
    "emergency_position_manage": "AI_PM",
}


def role_owner_for_stage(stage: str, payload: dict | None = None) -> str:
    if payload and payload.get("role_owner"):
        return str(payload["role_owner"])
    normalized = str(stage or "").strip().lower()
    if normalized in ROLE_OWNER_BY_STAGE:
        return ROLE_OWNER_BY_STAGE[normalized]
    if normalized.startswith("ai_position"):
        return "AI_PM"
    if normalized.endswith("_sync"):
        return "COO"
    return "SYSTEM"


def build_regime_state(
    *,
    reference_time: datetime,
    trade_metrics: dict[str, float | int],
    sector_flows: list[dict[str, object]],
    open_positions: list,
    hot_mover_count: int,
    max_open_positions: int,
) -> RegimeState:
    avg_abs_flow = 0.0
    positive_flows = 0
    negative_flows = 0
    if sector_flows:
        abs_flows = [abs(float(item.get("flow_score", 0.0) or 0.0)) for item in sector_flows]
        avg_abs_flow = sum(abs_flows) / len(abs_flows)
        positive_flows = sum(1 for item in sector_flows if float(item.get("flow_score", 0.0) or 0.0) >= 0.18)
        negative_flows = sum(1 for item in sector_flows if float(item.get("flow_score", 0.0) or 0.0) <= -0.18)

    continuation_open = sum(1 for position in open_positions if str(getattr(position, "engine_family", "") or "").lower() == "continuation")
    reversal_open = sum(1 for position in open_positions if str(getattr(position, "engine_family", "") or "").lower() == "reversal")
    hot_open = sum(1 for position in open_positions if str(getattr(position, "engine_family", "") or "").lower() == "hot_mover")
    realized_pnl = float(trade_metrics.get("realized_pnl", 0.0) or 0.0)
    profit_factor = float(trade_metrics.get("profit_factor", 0.0) or 0.0)
    max_drawdown_abs = float(trade_metrics.get("max_drawdown_abs", 0.0) or 0.0)
    open_load = min(len(open_positions) / max(max_open_positions, 1), 2.0)

    continuation_bias = min(1.0, 0.35 + (avg_abs_flow * 0.55) + (0.08 if positive_flows >= 2 or negative_flows >= 2 else 0.0) + min(continuation_open * 0.05, 0.10))
    reversal_bias = min(1.0, 0.30 + ((1.0 - min(avg_abs_flow, 1.0)) * 0.28) + (0.12 if positive_flows >= 1 and negative_flows >= 1 else 0.0) + min(reversal_open * 0.05, 0.10))
    hot_mover_bias = min(1.0, 0.20 + min(hot_mover_count, 4) * 0.12 + min(hot_open * 0.06, 0.12))
    defensive_bias = min(
        1.0,
        0.18
        + (0.18 if realized_pnl < 0 else 0.0)
        + (0.14 if profit_factor < 1.0 and float(trade_metrics.get("trades", 0) or 0) >= 8 else 0.0)
        + (0.10 if max_drawdown_abs > abs(realized_pnl) and max_drawdown_abs > 0 else 0.0)
        + (0.10 if open_load >= 1.0 else 0.0),
    )

    regime = "reversal_led"
    reason = "Mixed breadth and transition conditions favor reversal monitoring."
    if defensive_bias >= 0.52 and defensive_bias >= max(continuation_bias, reversal_bias):
        regime = "defensive"
        reason = "Drawdown pressure and open-load conditions favor defensive capital preservation."
    elif hot_mover_bias >= max(continuation_bias, reversal_bias) and hot_mover_bias >= 0.56:
        regime = "hot_mover_opportunistic"
        reason = "Attention expansion and hot-mover breadth favor opportunistic high-volatility routing."
    elif continuation_bias >= reversal_bias + 0.06:
        regime = "continuation_led"
        reason = "Sector breadth and directional coherence favor continuation-led operation."

    return RegimeState(
        regime=regime,
        continuation_bias=continuation_bias,
        reversal_bias=reversal_bias,
        hot_mover_bias=hot_mover_bias,
        defensive_bias=defensive_bias,
        owner="CEO",
        reason=reason,
    )


def build_allocation_decision(
    *,
    score: float,
    threshold: float,
    components: dict[str, float | int | str | bool],
    regime_state: RegimeState | None,
) -> AllocationDecision:
    payload = dict(components)
    payload["score"] = round(score, 4)
    payload["threshold"] = round(threshold, 4)
    if regime_state is not None:
        payload["regime"] = regime_state.regime
        payload["regime_owner"] = regime_state.owner
    allowed = score >= threshold
    if allowed:
        reason = f"Portfolio allocator accepted candidate: {score:.2f} >= {threshold:.2f}."
    else:
        reason = f"Portfolio allocator rejected candidate: {score:.2f} < {threshold:.2f}."
    return AllocationDecision(
        allowed=allowed,
        score=score,
        threshold=threshold,
        reason=reason,
        owner="CFO",
        payload=payload,
    )


def build_execution_readiness(
    *,
    signal_age_seconds: float,
    freshness_limit_seconds: int,
    session_ok: bool,
    session_override: bool,
    symbol_valid: bool,
    micro_rejection: str | None,
    micro_soft_pass: bool,
) -> ExecutionReadiness:
    freshness_ok = signal_age_seconds <= freshness_limit_seconds
    micro_ok = micro_rejection is None or micro_soft_pass
    ready = freshness_ok and (session_ok or session_override) and symbol_valid and micro_ok
    reason = "Execution path is ready."
    if not freshness_ok:
        reason = "Execution readiness failed because the signal is stale."
    elif not (session_ok or session_override):
        reason = "Execution readiness failed because entry window rules block this trade."
    elif not symbol_valid:
        reason = "Execution readiness failed because the symbol is not tradeable."
    elif not micro_ok:
        reason = "Execution readiness failed because microstructure quality is too weak."
    payload = {
        "freshness_ok": freshness_ok,
        "freshness_limit_seconds": int(freshness_limit_seconds),
        "signal_age_seconds": round(signal_age_seconds, 3),
        "session_ok": session_ok,
        "session_override": session_override,
        "symbol_valid": symbol_valid,
        "micro_ok": micro_ok,
        "micro_soft_pass": micro_soft_pass,
        "micro_rejection": micro_rejection or "",
    }
    return ExecutionReadiness(
        ready=ready,
        reason=reason,
        owner="COO",
        freshness_ok=freshness_ok,
        freshness_limit_seconds=int(freshness_limit_seconds),
        signal_age_seconds=signal_age_seconds,
        session_ok=session_ok,
        session_override=session_override,
        micro_ok=micro_ok,
        micro_soft_pass=micro_soft_pass,
        symbol_valid=symbol_valid,
        payload=payload,
    )
