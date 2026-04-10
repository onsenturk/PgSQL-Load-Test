from __future__ import annotations

import random
import secrets
from .base import BaseWorkload, register_workload


@register_workload("read_query")
class ReadQueryWorkload(BaseWorkload):
    """SELECT-only workload: point lookups, range scans, and aggregates.

    Seeds 10,000 rows on first run, then executes a mix of:
      - 50% point lookups by PK
      - 30% range scans (BETWEEN ... LIMIT 50)
      - 20% aggregate queries (GROUP BY category)
    """

    def __init__(self, *, table: str, payload_size: int):
        super().__init__(table=table, payload_size=payload_size)
        self._max_id: int = 0

    async def setup(self, conn) -> None:
        await conn.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {self.table} (
                id BIGSERIAL PRIMARY KEY,
                created_at TIMESTAMPTZ DEFAULT now(),
                category INT NOT NULL DEFAULT 0,
                payload BYTEA NOT NULL
            )
            """
        )
        await conn.execute(
            f"CREATE INDEX IF NOT EXISTS {self.table}_category_idx ON {self.table} (category)"
        )
        count = await conn.fetchval(f"SELECT count(*) FROM {self.table}")
        if count < 10_000:
            needed = 10_000 - count
            batch_size = 500
            for i in range(0, needed, batch_size):
                batch = min(batch_size, needed - i)
                rows = [
                    (random.randint(0, 9), secrets.token_bytes(self.payload_size))
                    for _ in range(batch)
                ]
                await conn.executemany(
                    f"INSERT INTO {self.table} (category, payload) VALUES ($1, $2)", rows
                )
        self._max_id = await conn.fetchval(
            f"SELECT COALESCE(MAX(id), 0) FROM {self.table}"
        )

    async def execute(self, conn) -> None:
        roll = random.random()
        if roll < 0.5:
            # Point lookup by PK
            pk = random.randint(1, max(1, self._max_id))
            await conn.fetchrow(f"SELECT * FROM {self.table} WHERE id = $1", pk)
        elif roll < 0.8:
            # Range scan
            start = random.randint(1, max(1, self._max_id - 100))
            await conn.fetch(
                f"SELECT * FROM {self.table} WHERE id BETWEEN $1 AND $2 LIMIT 50",
                start,
                start + 100,
            )
        else:
            # Aggregate by category
            cat = random.randint(0, 9)
            await conn.fetchrow(
                f"SELECT category, count(*), avg(length(payload)) FROM {self.table}"
                f" WHERE category = $1 GROUP BY category",
                cat,
            )
