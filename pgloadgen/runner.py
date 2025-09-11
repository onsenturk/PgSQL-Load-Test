from __future__ import annotations

import asyncio
import time
import asyncpg
from .config import LoadGenConfig
from .metrics import MetricsRecorder
from .workloads import get_workload
from rich.console import Console

console = Console()


async def worker_task(pool: asyncpg.Pool, config: LoadGenConfig, metrics: MetricsRecorder, stop_time: float, target_rate: float):
    workload_cls = get_workload(config.workload)
    try:
        workload = workload_cls(
            table=config.table,
            payload_size=config.payload_size,
            start_date=config.start_date,
            partition_span_days=config.partition_span_days,
            second_table=config.second_table,
        )
    except TypeError:
        workload = workload_cls(table=config.table, payload_size=config.payload_size)
    async with pool.acquire() as conn:
        await workload.setup(conn)
    op_index = 0
    while time.perf_counter() < stop_time:
        if config.operations and metrics.operations >= config.operations:
            break
        if target_rate > 0:
            scheduled = op_index / target_rate
            actual = time.perf_counter() - metrics._start  # noqa: SLF001 (internal timing ok)
            delay = scheduled - actual
            if delay > 0:
                await asyncio.sleep(delay)
        op_index += 1
        start = time.perf_counter()
        try:
            async with pool.acquire() as conn:
                await workload.execute(conn)
            end = time.perf_counter()
            metrics.record(end - start)
        except Exception:  # pragma: no cover - broad catch for load test robustness
            metrics.record_error()


async def run_async(config: LoadGenConfig):
    metrics = MetricsRecorder()
    stop_time = time.perf_counter() + config.duration_seconds
    pool = await asyncpg.create_pool(dsn=config.dsn, min_size=min(2, config.concurrency), max_size=config.concurrency)
    target_rate = config.target_rate
    tasks = [asyncio.create_task(worker_task(pool, config, metrics, stop_time, target_rate)) for _ in range(config.concurrency)]

    try:
        last_report = 0.0
        while any(not t.done() for t in tasks):
            await asyncio.sleep(0.5)
            snap = metrics.snapshot()
            if snap.elapsed - last_report >= config.report_interval:
                last_report = snap.elapsed
                console.print(
                    f"[blue]{snap.elapsed:6.1f}s[/] ops={snap.operations} err={snap.errors} thr={snap.throughput:,.1f}/s "
                    + " ".join(f"{k}={v*1000:.2f}ms" for k, v in snap.percentiles.items())
                )
            if time.perf_counter() >= stop_time or (config.operations and metrics.operations >= config.operations):
                break
    finally:
        for t in tasks:
            t.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        await pool.close()
    snap = metrics.snapshot()
    console.print("\n[bold green]Summary[/bold green]")
    console.print(
        f"Elapsed {snap.elapsed:.2f}s | Ops {snap.operations} | Errors {snap.errors} | Throughput {snap.throughput:,.1f}/s"
    )
    console.print(
        "Percentiles: " + ", ".join(f"{k}={v*1000:.2f}ms" for k, v in snap.percentiles.items())
    )


def run_load_test(config: LoadGenConfig):
    asyncio.run(run_async(config))
