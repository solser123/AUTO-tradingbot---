from __future__ import annotations

from collections import Counter
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path

from .storage import StateStore


@dataclass(frozen=True)
class OpsReport:
    generated_at: datetime
    lookback_days: int
    top_blockers: list[tuple[str, int]]
    top_stage_outcomes: list[tuple[str, str, int]]
    engine_entries: list[tuple[str, int]]
    emergency_events: list[dict[str, str]]
    summary: dict[str, int]


def build_ops_report(store: StateStore, *, lookback_days: int = 7) -> OpsReport:
    cutoff = datetime.now(timezone.utc) - timedelta(days=max(lookback_days, 1))
    blocker_counts: Counter[str] = Counter()
    stage_counts: Counter[tuple[str, str]] = Counter()
    engine_counts: Counter[str] = Counter()

    with store._connect() as conn:
        decisions = conn.execute(
            """
            SELECT created_at, stage, outcome, detail, payload_json
            FROM decision_log
            WHERE created_at >= ?
            ORDER BY id DESC
            """,
            (cutoff.isoformat(),),
        ).fetchall()
        emergency_rows = conn.execute(
            """
            SELECT created_at, stage, outcome, detail
            FROM decision_log
            WHERE created_at >= ?
              AND stage IN ('emergency_stop', 'position_reconcile', 'runtime_recovery', 'manual_reconcile')
            ORDER BY id DESC
            LIMIT 40
            """,
            (cutoff.isoformat(),),
        ).fetchall()

    for row in decisions:
        stage = str(row["stage"])
        outcome = str(row["outcome"])
        detail = str(row["detail"] or "")
        stage_counts[(stage, outcome)] += 1
        if outcome == "rejected":
            blocker_counts[detail] += 1
        if stage == "entry" and outcome == "opened":
            payload = str(row["payload_json"] or "")
            if '"engine_key"' in payload:
                for engine_key in ("continuation", "reversal", "hot_mover", "scout"):
                    if f'"engine_key": "{engine_key}"' in payload:
                        engine_counts[engine_key] += 1
                        break
            else:
                engine_counts["unknown"] += 1

    return OpsReport(
        generated_at=datetime.now(timezone.utc),
        lookback_days=lookback_days,
        top_blockers=blocker_counts.most_common(12),
        top_stage_outcomes=[(stage, outcome, count) for (stage, outcome), count in stage_counts.most_common(20)],
        engine_entries=engine_counts.most_common(),
        emergency_events=[dict(row) for row in emergency_rows],
        summary={
            "decision_rows": len(decisions),
            "emergency_rows": len(emergency_rows),
            "distinct_blockers": len(blocker_counts),
            "distinct_engine_entries": len(engine_counts),
        },
    )


def render_ops_report(report: OpsReport) -> str:
    lines = [
        "# Ops Report",
        "",
        f"- generated_at: {report.generated_at.isoformat()}",
        f"- lookback_days: {report.lookback_days}",
        "",
        "## Summary",
    ]
    for key, value in report.summary.items():
        lines.append(f"- {key}: {value}")
    lines.extend(["", "## Top Blockers"])
    for detail, count in report.top_blockers:
        lines.append(f"- {detail}: {count}")
    lines.extend(["", "## Top Stage Outcomes"])
    for stage, outcome, count in report.top_stage_outcomes:
        lines.append(f"- {stage}/{outcome}: {count}")
    lines.extend(["", "## Engine Entries"])
    if report.engine_entries:
        for engine_key, count in report.engine_entries:
            lines.append(f"- {engine_key}: {count}")
    else:
        lines.append("- none")
    lines.extend(["", "## Emergency And Reconcile Events"])
    for row in report.emergency_events:
        lines.append(f"- {row['created_at']} | {row['stage']} | {row['outcome']} | {row['detail']}")
    return "\n".join(lines)


def write_ops_report(report: OpsReport, output_dir: Path) -> Path:
    output_dir.mkdir(parents=True, exist_ok=True)
    path = output_dir / f"ops_report_{report.generated_at.strftime('%Y%m%d_%H%M%S')}.md"
    path.write_text(render_ops_report(report), encoding="utf-8")
    return path
