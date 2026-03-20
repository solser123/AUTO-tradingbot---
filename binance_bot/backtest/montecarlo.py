from __future__ import annotations

import random


def monte_carlo_equity_paths(pnls: list[float], trials: int = 100, seed: int = 42) -> list[float]:
    if not pnls:
        return []
    random.seed(seed)
    outcomes: list[float] = []
    for _ in range(max(trials, 1)):
        sample = [random.choice(pnls) for _ in range(len(pnls))]
        outcomes.append(sum(sample))
    return outcomes
