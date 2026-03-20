from __future__ import annotations


def split_walkforward(total_bars: int, train_bars: int, test_bars: int) -> list[tuple[int, int, int, int]]:
    windows: list[tuple[int, int, int, int]] = []
    start = 0
    while start + train_bars + test_bars <= total_bars:
        windows.append((start, start + train_bars, start + train_bars, start + train_bars + test_bars))
        start += test_bars
    return windows
