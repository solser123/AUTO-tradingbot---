from __future__ import annotations

import json
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path

from .storage import StateStore


@dataclass(frozen=True)
class LiveReport:
    generated_at: datetime
    lookback_hours: int
    summary: dict[str, float | int]
    by_side: dict[str, dict[str, float]]
    by_setup: dict[str, dict[str, float]]
    by_symbol: dict[str, dict[str, float]]
    blocker_counts: dict[str, int]
    stage_counts: dict[str, int]
    opportunity: dict[str, float | int]
    weaknesses: list[str]


def _bucket_stats(rows: list[dict]) -> dict[str, float]:
    trade_count = len(rows)
    realized = sum(float(row["realized_pnl"] or 0.0) for row in rows)
    wins = sum(1 for row in rows if float(row["realized_pnl"] or 0.0) > 0)
    return {
        "trades": float(trade_count),
        "win_rate": (wins / trade_count) * 100 if trade_count else 0.0,
        "realized_pnl": realized,
        "avg_pnl": (realized / trade_count) if trade_count else 0.0,
    }


def build_live_report(store: StateStore, *, lookback_hours: int = 48) -> LiveReport:
    cutoff = datetime.now(timezone.utc) - timedelta(hours=max(lookback_hours, 1))
    with store._connect() as conn:
        closed_rows = conn.execute(
            """
            SELECT symbol, side, realized_pnl, exit_reason
            FROM positions
            WHERE status = 'CLOSED'
              AND closed_at IS NOT NULL
              AND closed_at >= ?
            ORDER BY closed_at DESC
            """,
            (cutoff.isoformat(),),
        ).fetchall()
        signal_rows = conn.execute(
            """
            SELECT setup_type, side, approved, ai_confidence, created_at
            FROM signals
            WHERE created_at >= ?
            ORDER BY id DESC
            """,
            (cutoff.isoformat(),),
        ).fetchall()
        decision_rows = conn.execute(
            """
            SELECT stage, outcome, detail, payload_json, symbol, created_at
            FROM decision_log
            WHERE created_at >= ?
            ORDER BY id DESC
            """,
            (cutoff.isoformat(),),
        ).fetchall()
        opp_rows = conn.execute(
            """
            SELECT dominant_side, dominant_move_pct, missed_notional_pnl, is_material
            FROM opportunity_reviews
            WHERE reviewed_at >= ?
            ORDER BY id DESC
            """,
            (cutoff.isoformat(),),
        ).fetchall()

    closed = [dict(row) for row in closed_rows]
    signals = [dict(row) for row in signal_rows]
    decisions = [dict(row) for row in decision_rows]
    opps = [dict(row) for row in opp_rows]

    by_side_source: dict[str, list[dict]] = defaultdict(list)
    by_symbol_source: dict[str, list[dict]] = defaultdict(list)
    for row in closed:
        by_side_source[str(row["side"])].append(row)
        by_symbol_source[str(row["symbol"])].append(row)

    by_setup_counter: dict[str, list[float]] = defaultdict(list)
    for row in signals:
        by_setup_counter[str(row["setup_type"])].append(float(row["ai_confidence"] or 0.0))

    blocker_counts: Counter[str] = Counter()
    stage_counts: Counter[str] = Counter()
    for row in decisions:
        stage = str(row["stage"])
        stage_counts[stage] += 1
        if row["outcome"] != "rejected":
            continue
        detail = str(row["detail"] or "")
        for part in [chunk.strip() for chunk in detail.split("|") if chunk.strip()]:
            blocker_counts[part] += 1

    total_closed = len(closed)
    total_realized = sum(float(row["realized_pnl"] or 0.0) for row in closed)
    wins = sum(1 for row in closed if float(row["realized_pnl"] or 0.0) > 0)
    longs = sum(1 for row in closed if str(row["side"]) == "long")
    shorts = sum(1 for row in closed if str(row["side"]) == "short")
    approved_signals = sum(1 for row in signals if int(row["approved"] or 0) == 1)
    avg_ai_conf = sum(float(row["ai_confidence"] or 0.0) for row in signals) / len(signals) if signals else 0.0

    by_setup = {
        key: {
            "signals": float(len(values)),
            "avg_ai_confidence": (sum(values) / len(values)) if values else 0.0,
        }
        for key, values in sorted(by_setup_counter.items(), key=lambda item: len(item[1]), reverse=True)
    }
    by_side = {side: _bucket_stats(rows) for side, rows in by_side_source.items()}
    by_symbol = {
        symbol: _bucket_stats(rows)
        for symbol, rows in sorted(by_symbol_source.items(), key=lambda item: _bucket_stats(item[1])["realized_pnl"], reverse=True)
    }

    opportunity = {
        "reviews": len(opps),
        "material_reviews": sum(1 for row in opps if int(row["is_material"] or 0) == 1),
        "missed_notional_pnl": sum(float(row["missed_notional_pnl"] or 0.0) for row in opps),
        "avg_move_pct": (
            sum(float(row["dominant_move_pct"] or 0.0) for row in opps) / len(opps)
            if opps
            else 0.0
        ),
    }

    weaknesses: list[str] = []
    top_blockers = blocker_counts.most_common(5)
    if top_blockers:
        weaknesses.append("Top blockers are still dominating live participation.")
    if by_side.get("long", {}).get("trades", 0.0) == 0 and by_side.get("short", {}).get("trades", 0.0) > 0:
        weaknesses.append("Long-side live execution is underrepresented versus short-side execution.")
    if by_side.get("short", {}).get("trades", 0.0) == 0 and by_side.get("long", {}).get("trades", 0.0) > 0:
        weaknesses.append("Short-side live execution is underrepresented versus long-side execution.")
    if total_closed == 0:
        weaknesses.append("No closed live trades in the lookback window; the system is still learning more from decisions than executions.")
    if opportunity["missed_notional_pnl"] > max(total_realized, 0.0) * 2:
        weaknesses.append("Missed opportunity cost remains materially larger than realized pnl.")
    if blocker_counts.get("Long rejected: higher timeframe bias is still too weak.", 0) > 10:
        weaknesses.append("Higher-timeframe bias remains the dominant long-side blocker.")
    if blocker_counts.get("Short rejected: higher timeframe bias is still too strong for a short.", 0) > 10:
        weaknesses.append("Higher-timeframe bias remains the dominant short-side blocker.")

    return LiveReport(
        generated_at=datetime.now(timezone.utc),
        lookback_hours=lookback_hours,
        summary={
            "closed_trades": total_closed,
            "win_rate": (wins / total_closed) * 100 if total_closed else 0.0,
            "realized_pnl": total_realized,
            "signals": len(signals),
            "approved_signals": approved_signals,
            "avg_ai_confidence": avg_ai_conf,
            "long_closed": longs,
            "short_closed": shorts,
        },
        by_side=by_side,
        by_setup=by_setup,
        by_symbol=by_symbol,
        blocker_counts=dict(top_blockers),
        stage_counts=dict(stage_counts.most_common(10)),
        opportunity=opportunity,
        weaknesses=weaknesses,
    )


def render_live_report(report: LiveReport) -> str:
    lines = [
        "# Live Trading Report",
        "",
        f"- generated_at: {report.generated_at.isoformat()}",
        f"- lookback_hours: {report.lookback_hours}",
        "",
        "## Summary",
        f"- closed_trades: {int(report.summary['closed_trades'])}",
        f"- win_rate: {float(report.summary['win_rate']):.2f}%",
        f"- realized_pnl: {float(report.summary['realized_pnl']):.4f}",
        f"- signals: {int(report.summary['signals'])}",
        f"- approved_signals: {int(report.summary['approved_signals'])}",
        f"- avg_ai_confidence: {float(report.summary['avg_ai_confidence']):.4f}",
        f"- long_closed: {int(report.summary['long_closed'])}",
        f"- short_closed: {int(report.summary['short_closed'])}",
        "",
        "## By Side",
    ]
    for side, stats in sorted(report.by_side.items()):
        lines.append(
            f"- {side}: trades={int(stats['trades'])} win_rate={stats['win_rate']:.2f}% pnl={stats['realized_pnl']:.4f}"
        )

    lines.extend(["", "## Top Setups"])
    for setup, stats in list(report.by_setup.items())[:8]:
        lines.append(
            f"- {setup}: signals={int(stats['signals'])} avg_ai_confidence={stats['avg_ai_confidence']:.4f}"
        )

    lines.extend(["", "## Top Symbols"])
    for symbol, stats in list(report.by_symbol.items())[:8]:
        lines.append(
            f"- {symbol}: trades={int(stats['trades'])} win_rate={stats['win_rate']:.2f}% pnl={stats['realized_pnl']:.4f}"
        )

    lines.extend(["", "## Top Blockers"])
    for blocker, count in report.blocker_counts.items():
        lines.append(f"- {blocker}: {count}")

    lines.extend(["", "## Opportunity Cost"])
    lines.append(f"- reviews: {int(report.opportunity['reviews'])}")
    lines.append(f"- material_reviews: {int(report.opportunity['material_reviews'])}")
    lines.append(f"- missed_notional_pnl: {float(report.opportunity['missed_notional_pnl']):.4f}")
    lines.append(f"- avg_move_pct: {float(report.opportunity['avg_move_pct']):.2f}%")

    lines.extend(["", "## Weaknesses"])
    if report.weaknesses:
        for item in report.weaknesses:
            lines.append(f"- {item}")
    else:
        lines.append("- No major weakness flags identified in this window.")

    return "\n".join(lines)


def write_live_report(report: LiveReport, output_dir: Path) -> Path:
    output_dir.mkdir(parents=True, exist_ok=True)
    path = output_dir / f"live_report_{report.generated_at.strftime('%Y%m%d_%H%M%S')}.md"
    path.write_text(render_live_report(report), encoding="utf-8")
    return path
