# pgloadgen

Asyncio-based PostgreSQL load generator supporting pluggable workloads, YAML/JSON configuration, and real-time metrics (throughput & latency percentiles).

## Features
* Async execution using `asyncpg`
* Concurrency & (optional) rate limiting
* Pluggable workload modules
* Latency histogram & percentile calculation (p50, p90, p95, p99, max)
* Graceful shutdown on CTRL+C
* CLI with config file or flags
* Partitioned table workload (`partition_insert`) & pruning inspection command

## Quick Start
Create & activate a virtual environment, then install:

```bash
pip install -e .[dev]
```

Example config in `config/example.yaml`:
```yaml
dsn: postgresql://user:password@localhost:5432/postgres
workload: sample_insert
concurrency: 8
duration_seconds: 30
target_rate: 0          # 0 means unlimited
report_interval: 2
table: loadgen_events
payload_size: 128
```

Run:
```bash
pgloadgen run --config config/example.yaml
```

Or override via flags:
```bash
pgloadgen run --dsn postgresql://user:pass@localhost/db --workload sample_insert --concurrency 4 --duration 10
```

### Small Trial (Sanity Check)
Before launching a large multi-million row run, execute a brief trial to verify connectivity, table creation, and metrics output:
```bash
pgloadgen run --dsn postgresql://user:password@localhost:5432/postgres \
	--workload sample_insert --concurrency 2 --duration 5
```
You should see periodic lines with ops, throughput, and latency percentiles, then a summary.

## Workloads
Workloads live under `pgloadgen/workloads/`. Implement `BaseWorkload` and register with `@register_workload("name")`.

### FK Chain Insert Workload
`fk_chain_insert` creates 50 tables named `{table}_00` .. `{table}_49`.

Each table has:
* a `BIGSERIAL` primary key (`id`)
* a `DATE` field (`event_date`)
* 5-10 columns (this workload uses 8)
* a deferrable FK using either a straight chain or a hub-and-spoke (star)

Each operation inserts one row into each table inside a single transaction.

Topologies:
* `chain` (default): `{table}_01` references `{table}_00`, `{table}_02` references `{table}_01`, ...
* `star`: ~10% of tables are hubs; remaining tables reference a hub (hub sizes vary randomly)

Example:
```bash
pgloadgen run --dsn postgresql://user:password@host:5432/postgres \
	--workload fk_chain_insert --concurrency 4 --duration 10 \
	--table loadgen_fk --payload-size 256
```

Star topology example:
```bash
pgloadgen run --dsn postgresql://user:password@host:5432/postgres \
	--workload fk_chain_insert --fk-topology star --concurrency 4 --duration 10 \
	--table loadgen_fk --payload-size 256
```

If you change topology after tables already exist, Postgres will keep the old FK constraints. Use a new `--table` prefix or pass `--fk-reset` to drop/recreate the 50 tables.

### Partition Insert Workload
`partition_insert` creates monthly RANGE partitions on `event_date` over a configurable span and inserts wide rows (10 columns) with random distribution. Optionally create a second table (`--second-table`) to split load across two parents.

Columns: id, event_date, created_at, category, region, user_id, amount, status, payload, extra.

Key options:
* `--operations N` – precise total rows (use for 65,000,000 target)
* `--start-date YYYY-MM-DD` & `--partition-span-days D` – define partition range
* `--second-table` – also produce `{table}_b`

Example (65M rows, two tables, 6 months, 16 workers):
```bash
pgloadgen run --dsn postgresql://user:pass@localhost/db \
	--workload partition_insert --concurrency 16 \
	--operations 65000000 --table events_range --payload-size 64 \
	--start-date 2025-01-01 --partition-span-days 180 --second-table
```

### Partition Pruning Test
Use the helper command:
```bash
pgloadgen partition-test --dsn postgresql://user:pass@localhost/db \
	--table events_range --from 2025-02-01 --to 2025-03-01 --filter-category 3
```
Prints truncated JSON plan and an estimate of partitions scanned.
operations: 0            # set >0 to stop after N operations instead of duration

## Metrics
Latency recorded in microseconds into a fixed histogram. Percentiles derived on demand; overhead is low for high throughput.

## Tests
Run tests:
```bash
pytest
```

## Next Ideas
* CSV/JSON export of summary
* Prometheus metrics endpoint
* More workload templates (read/update/mixed)

## License
MIT

