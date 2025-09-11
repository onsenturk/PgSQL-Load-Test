from __future__ import annotations

import time
import math
from dataclasses import dataclass
from typing import Iterable, List
import threading


@dataclass
class MetricsSnapshot:
    elapsed: float
    operations: int
    errors: int
    throughput: float
    percentiles: dict[str, float]


class LatencyHistogram:
    """Fixed bucket latency histogram (microseconds)."""

    def __init__(self, max_us: int = 5_000_000, significant_figures: int = 2):  # up to 5s
        # exponential-ish buckets
        self._bounds: List[int] = []
        value = 1
        base = 1 + 10 ** (-significant_figures)
        while value < max_us:
            self._bounds.append(value)
            value = int(value * base * 1.15) + 1
        self._counts = [0] * (len(self._bounds) + 1)
        self._lock = threading.Lock()
        self._total = 0

    def record(self, latency_seconds: float) -> None:
        us = int(latency_seconds * 1_000_000)
        # binary search
        lo, hi = 0, len(self._bounds) - 1
        idx = len(self._bounds)
        while lo <= hi:
            mid = (lo + hi) // 2
            if us <= self._bounds[mid]:
                idx = mid
                hi = mid - 1
            else:
                lo = mid + 1
        with self._lock:
            self._counts[idx] += 1
            self._total += 1

    def percentile(self, pct: float) -> float:
        if self._total == 0:
            return 0.0
        target = math.ceil(self._total * pct / 100.0)
        cumulative = 0
        for b, count in enumerate(self._counts):
            cumulative += count
            if cumulative >= target:
                if b >= len(self._bounds):
                    return self._bounds[-1] / 1_000_000.0
                return self._bounds[b] / 1_000_000.0
        return self._bounds[-1] / 1_000_000.0

    def percentiles(self, pcts: Iterable[float]) -> dict[str, float]:
        return {f"p{int(p)}": self.percentile(p) for p in pcts}


class MetricsRecorder:
    def __init__(self) -> None:
        self._hist = LatencyHistogram()
        self._operations = 0
        self._errors = 0
        self._start = time.perf_counter()
        self._lock = threading.Lock()

    def record(self, latency_seconds: float) -> None:
        self._hist.record(latency_seconds)
        with self._lock:
            self._operations += 1

    def record_error(self) -> None:
        with self._lock:
            self._errors += 1

    def snapshot(self) -> MetricsSnapshot:
        now = time.perf_counter()
        elapsed = now - self._start
        with self._lock:
            ops = self._operations
            errs = self._errors
        throughput = ops / elapsed if elapsed > 0 else 0.0
        percentiles = self._hist.percentiles([50, 90, 95, 99])
        return MetricsSnapshot(
            elapsed=elapsed,
            operations=ops,
            errors=errs,
            throughput=throughput,
            percentiles=percentiles,
        )

    @property
    def operations(self) -> int:
        with self._lock:
            return self._operations
