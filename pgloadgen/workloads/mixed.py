from __future__ import annotations

import random
import secrets
from .base import BaseWorkload, register_workload


@register_workload("mixed")
class MixedWorkload(BaseWorkload):
    """Configurable mix of INSERT / SELECT / UPDATE / DELETE operations.

    Percentages are configured via read_pct, update_pct, delete_pct.
    The remaining percentage becomes inserts.
    Seeds 1,000 rows on first run to ensure reads/updates/deletes have data.
    """

    def __init__(
        self,
        *,
        table: str,
        payload_size: int,
        read_pct: float = 50.0,
        update_pct: float = 10.0,
        delete_pct: float = 5.0,
    ):
        super().__init__(table=table, payload_size=payload_size)
        total = read_pct + update_pct + delete_pct
        if total > 100.0:
            raise ValueError(
                f"read_pct + update_pct + delete_pct = {total}, must be <= 100"
            )
        self.read_pct = read_pct
        self.update_pct = update_pct
        self.delete_pct = delete_pct
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
        if count < 1_000:
            needed = 1_000 - count
            rows = [
                (random.randint(0, 9), secrets.token_bytes(self.payload_size))
                for _ in range(needed)
            ]
            await conn.executemany(
                f"INSERT INTO {self.table} (category, payload) VALUES ($1, $2)", rows
            )
        self._max_id = await conn.fetchval(
            f"SELECT COALESCE(MAX(id), 0) FROM {self.table}"
        )

    async def execute(self, conn) -> None:
        roll = random.random() * 100.0
        if roll < self.read_pct:
            await self._do_read(conn)
        elif roll < self.read_pct + self.update_pct:
            await self._do_update(conn)
        elif roll < self.read_pct + self.update_pct + self.delete_pct:
            await self._do_delete(conn)
        else:
            await self._do_insert(conn)

    async def _do_read(self, conn) -> None:
        if random.random() < 0.7:
            pk = random.randint(1, max(1, self._max_id))
            await conn.fetchrow(f"SELECT * FROM {self.table} WHERE id = $1", pk)
        else:
            cat = random.randint(0, 9)
            await conn.fetch(
                f"SELECT * FROM {self.table} WHERE category = $1 LIMIT 50", cat
            )

    async def _do_update(self, conn) -> None:
        pk = random.randint(1, max(1, self._max_id))
        await conn.execute(
            f"UPDATE {self.table} SET category = $1 WHERE id = $2",
            random.randint(0, 9),
            pk,
        )

    async def _do_delete(self, conn) -> None:
        pk = random.randint(1, max(1, self._max_id))
        await conn.execute(f"DELETE FROM {self.table} WHERE id = $1", pk)

    async def _do_insert(self, conn) -> None:
        payload = secrets.token_bytes(self.payload_size)
        await conn.execute(
            f"INSERT INTO {self.table} (category, payload) VALUES ($1, $2)",
            random.randint(0, 9),
            payload,
        )
        self._max_id += 1  # approximate tracking
