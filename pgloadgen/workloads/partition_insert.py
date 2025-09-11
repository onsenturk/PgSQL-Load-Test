from __future__ import annotations

import datetime as _dt
import random
import secrets
import json
from .base import BaseWorkload, register_workload


@register_workload("partition_insert")
class PartitionInsertWorkload(BaseWorkload):
    """Insert rows into one or two partitioned tables (monthly RANGE on event_date)."""

    def __init__(self, *, table: str, payload_size: int, start_date: str = "2024-01-01", partition_span_days: int = 180, second_table: bool = False):
        super().__init__(table=table, payload_size=payload_size)
        self.start_date = _dt.date.fromisoformat(start_date)
        self.partition_span_days = partition_span_days
        self.second_table = second_table
        self.table2 = f"{table}_b" if second_table else None

    async def setup(self, conn):  # type: ignore[override]
        await self._ensure_partitioned_table(conn, self.table)
        if self.table2:
            await self._ensure_partitioned_table(conn, self.table2)

    async def _ensure_partitioned_table(self, conn, table_name: str):
        await conn.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                id BIGSERIAL PRIMARY KEY,
                event_date DATE NOT NULL,
                created_at TIMESTAMPTZ DEFAULT now(),
                category INT NOT NULL,
                region INT NOT NULL,
                user_id BIGINT NOT NULL,
                amount NUMERIC(12,2) NOT NULL,
                status SMALLINT NOT NULL,
                payload BYTEA NOT NULL,
                extra JSONB
            ) PARTITION BY RANGE (event_date);
            """
        )
        end_date = self.start_date + _dt.timedelta(days=self.partition_span_days)
        cur = _dt.date(self.start_date.year, self.start_date.month, 1)
        while cur < end_date:
            if cur.month == 12:
                nxt = _dt.date(cur.year + 1, 1, 1)
            else:
                nxt = _dt.date(cur.year, cur.month + 1, 1)
            part = f"{table_name}_{cur:%Y%m}"
            await conn.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {part} PARTITION OF {table_name}
                FOR VALUES FROM ('{cur:%Y-%m-%d}') TO ('{nxt:%Y-%m-%d}');
                """
            )
            cur = nxt

    async def execute(self, conn):  # type: ignore[override]
        span = self.partition_span_days
        event_date = self.start_date + _dt.timedelta(days=random.randrange(span))
        table = self.table2 if self.table2 and random.random() < 0.5 else self.table
        category = random.randint(0, 9)
        region = random.randint(0, 49)
        user_id = random.randint(1, 5_000_000)
        amount = round(random.random() * 1000, 2)
        status = random.randint(0, 3)
        payload = secrets.token_bytes(self.payload_size)
        extra = json.dumps({"k": random.randint(0, 1000)})
        await conn.execute(
            f"INSERT INTO {table} (event_date, category, region, user_id, amount, status, payload, extra) VALUES ($1,$2,$3,$4,$5,$6,$7,$8)",
            event_date, category, region, user_id, amount, status, payload, extra
        )