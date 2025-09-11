from __future__ import annotations

import secrets
from .base import BaseWorkload, register_workload


@register_workload("sample_insert")
class SampleInsertWorkload(BaseWorkload):
    async def setup(self, conn):  # type: ignore[override]
        await conn.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {self.table} (
                id BIGSERIAL PRIMARY KEY,
                created_at TIMESTAMPTZ DEFAULT now(),
                payload BYTEA NOT NULL
            )
            """
        )

    async def execute(self, conn):  # type: ignore[override]
        payload = secrets.token_bytes(self.payload_size)
        await conn.execute(
            f"INSERT INTO {self.table} (payload) VALUES ($1)", payload
        )
