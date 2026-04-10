from __future__ import annotations

import click
from .config import LoadGenConfig
from .runner import run_load_test


def common_options(func):
    func = click.option("--dsn", type=str, help="PostgreSQL DSN (overrides config)")(func)
    func = click.option("--workload", type=str, help="Workload name")(func)
    func = click.option("--concurrency", type=int, help="Number of concurrent workers")(func)
    func = click.option("--duration", type=int, help="Duration seconds")(func)
    func = click.option("--target-rate", type=float, help="Target operations per second (0=unlimited)")(func)
    func = click.option("--table", type=str, help="Target table for sample workloads")(func)
    func = click.option("--payload-size", type=int, help="Payload size bytes")(func)
    func = click.option("--operations", type=int, help="Stop after N operations (overrides duration)")(func)
    func = click.option("--start-date", type=str, help="Start date (YYYY-MM-DD) for partition workload")(func)
    func = click.option("--partition-span-days", type=int, help="Span of days to cover with partitions")(func)
    func = click.option("--second-table", is_flag=True, help="Create and use a second partitioned table")(func)
    func = click.option(
        "--fk-topology",
        type=click.Choice(["chain", "star"], case_sensitive=False),
        help="FK topology for fk_chain_insert: chain (default) or star",
    )(func)
    func = click.option(
        "--fk-reset",
        is_flag=True,
        help="For fk_chain_insert: drop/recreate the generated tables before running",
    )(func)
    func = click.option("--think-time", type=float, help="Seconds to sleep between operations (0=none)")(func)
    func = click.option("--ramp-up", type=float, help="Seconds to stagger worker start-up (0=instant)")(func)
    func = click.option("--output-file", type=str, help="Path for JSON results export")(func)
    func = click.option("--read-pct", type=float, help="Read %% for mixed workload (default 50)")(func)
    func = click.option("--update-pct", type=float, help="Update %% for mixed workload (default 10)")(func)
    func = click.option("--delete-pct", type=float, help="Delete %% for mixed workload (default 5)")(func)
    return func


@click.group()
def main():
    """pgloadgen CLI"""


@main.command()
@click.option("--config", "config_path", type=click.Path(exists=True, dir_okay=False), help="YAML/JSON config file")
@common_options
def run(config_path, **overrides):  # type: ignore[override]
    """Run a load test."""
    if config_path:
        cfg = LoadGenConfig.load(config_path)
    else:
        missing = [k for k in ["dsn", "workload"] if not overrides.get(k)]
        if missing:
            raise click.UsageError(f"Missing required: {', '.join(missing)} (or supply --config)")
        cfg = LoadGenConfig(
            dsn=overrides["dsn"],
            workload=overrides["workload"],
            concurrency=overrides.get("concurrency") or 4,
            duration_seconds=overrides.get("duration") or 30,
            target_rate=overrides.get("target_rate") or 0.0,
            table=overrides.get("table") or "loadgen_events",
            payload_size=overrides.get("payload_size") or 128,
            operations=overrides.get("operations") or 0,
            start_date=overrides.get("start_date") or "2024-01-01",
            partition_span_days=overrides.get("partition_span_days") or 180,
            second_table=overrides.get("second_table") or False,
            fk_topology=(overrides.get("fk_topology") or "chain"),
            fk_reset=overrides.get("fk_reset") or False,
            think_time=overrides.get("think_time") or 0.0,
            ramp_up_seconds=overrides.get("ramp_up") or 0.0,
            output_file=overrides.get("output_file") or "",
            read_pct=overrides.get("read_pct") or 50.0,
            update_pct=overrides.get("update_pct") or 10.0,
            delete_pct=overrides.get("delete_pct") or 5.0,
        )

    # Apply overrides if provided
    override_map = {"duration": "duration_seconds", "ramp_up": "ramp_up_seconds"}
    for field, value in overrides.items():
        if value is not None:
            attr = override_map.get(field, field)
            if hasattr(cfg, attr):
                setattr(cfg, attr, value)
    run_load_test(cfg)


@main.command("partition-test")
@click.option("--dsn", required=True, type=str, help="PostgreSQL DSN")
@click.option("--table", required=True, type=str, help="Partitioned table (parent)")
@click.option("--from", "from_date", required=True, type=str, help="Start date (YYYY-MM-DD)")
@click.option("--to", "to_date", required=True, type=str, help="End date (YYYY-MM-DD, exclusive)")
@click.option("--filter-category", type=int, default=None, help="Optional category filter for selectivity")
def partition_test(dsn, table, from_date, to_date, filter_category):  # type: ignore[override]
    """EXPLAIN (FORMAT JSON) a filtered count to view partition pruning."""
    import asyncio
    import asyncpg
    import json as _json
    import datetime as _dt

    async def _run():
        conn = await asyncpg.connect(dsn=dsn)
        try:
            where = "event_date >= $1 AND event_date < $2"
            params = [_dt.date.fromisoformat(from_date), _dt.date.fromisoformat(to_date)]
            if filter_category is not None:
                where += " AND category = $3"
                params.append(filter_category)
            sql = f"SELECT count(*) FROM {table} WHERE {where}"  # noqa: S608 controlled identifier
            plan_rows = await conn.fetch(f"EXPLAIN (FORMAT JSON) {sql}", *params)
            plan_json = plan_rows[0]["QUERY PLAN"]  # type: ignore[index]
            def _count(nodes):
                if isinstance(nodes, dict):
                    c = 0
                    if nodes.get("Relation Name") and nodes.get("Parent Relationship") == "Member":
                        c += 1
                    for v in nodes.values():
                        c += _count(v)
                    return c
                if isinstance(nodes, list):
                    return sum(_count(n) for n in nodes)
                return 0
            partitions = _count(plan_json)
            click.echo(f"Estimated partitions scanned: {partitions}")
            s = _json.dumps(plan_json)
            click.echo((s[:2000] + ("..." if len(s) > 2000 else "")))
        finally:
            await conn.close()
    asyncio.run(_run())


if __name__ == "__main__":  # pragma: no cover
    main()
