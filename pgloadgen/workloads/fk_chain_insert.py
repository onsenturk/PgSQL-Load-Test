from __future__ import annotations

import datetime as _dt
import hashlib
import random
import re
import secrets
from dataclasses import dataclass
from typing import Optional

from .base import BaseWorkload, register_workload


_IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def _validate_identifier(value: str) -> str:
    if not _IDENTIFIER_RE.match(value):
        raise ValueError(
            "Invalid identifier. Use only letters, digits, and underscore; must not start with a digit."
        )
    return value


def _parse_table_target(value: str) -> tuple[Optional[str], str]:
        """Parse optional schema-qualified prefix.

        Accepts either:
            - prefix
            - schema.prefix

        Schema/prefix are validated as identifiers.
        """

        if "." in value:
                schema, prefix = value.split(".", 1)
                return _validate_identifier(schema), _validate_identifier(prefix)
        return None, _validate_identifier(value)


def _qident(value: str) -> str:
    # Identifiers are validated; quoting keeps us safe across search_path and keywords.
    return '"' + value.replace('"', '""') + '"'


def _qname(schema: str, name: str) -> str:
    return f"{_qident(schema)}.{_qident(name)}"


def _stable_rng(*parts: str) -> random.Random:
    seed_bytes = hashlib.sha256(":".join(parts).encode("utf-8")).digest()[:8]
    seed = int.from_bytes(seed_bytes, "big", signed=False)
    return random.Random(seed)


@dataclass(frozen=True, slots=True)
class _RowData:
    event_date: _dt.date
    val_int: int
    val_text: str
    amount: float
    payload: bytes


@register_workload("fk_chain_insert")
class FkChainInsertWorkload(BaseWorkload):
    """Insert a linked chain across 50 tables with foreign keys.

    Each operation creates one row in each table within a single transaction.
        Tables are named: {table}_00 .. {table}_49

        Topologies:
            - chain: straight chain: {table}_01 -> {table}_00 -> ... -> {table}_49 -> {table}_48
            - star:  multi-hub star: ~10% of tables are hubs; each remaining table references a hub

    Every table has:
      - primary key (id)
      - a date field (event_date)
      - 5-10 columns (we use 8)
    """

    table_count: int = 50

    def __init__(
        self,
        *,
        table: str,
        payload_size: int,
        fk_topology: str = "chain",
        fk_reset: bool = False,
    ):
        super().__init__(table=table, payload_size=payload_size)
        if fk_topology not in {"chain", "star"}:
            raise ValueError("fk_topology must be 'chain' or 'star'")
        self.fk_topology = fk_topology
        self.fk_reset = fk_reset
        schema, prefix = _parse_table_target(table)
        self._requested_schema = schema
        self._prefix = prefix
        self._table_names = [f"{self._prefix}_{i:02d}" for i in range(self.table_count)]

        # Parent mapping (table_name -> parent table_name). Root tables have no parent.
        self._parent_of: dict[str, Optional[str]] = {}
        self._roots: list[str] = []
        self._non_roots: list[str] = []

        self._build_parent_mapping()

        self._schema: Optional[str] = None
        self._insert_sql: dict[str, str] = {}

    def _build_parent_mapping(self) -> None:
        if self.fk_topology == "chain":
            self._parent_of = {name: (self._table_names[i - 1] if i > 0 else None) for i, name in enumerate(self._table_names)}
        else:
            # Multi-hub star with ~10% hubs and a random number of satellites per hub.
            rng = _stable_rng(self._prefix, "fk_chain_insert", self.fk_topology)
            hub_count = max(1, int(round(self.table_count * 0.10)))
            hubs = sorted(rng.sample(self._table_names, k=hub_count))
            satellites = [n for n in self._table_names if n not in set(hubs)]
            rng.shuffle(satellites)

            # Randomly partition satellites across hubs.
            remaining = len(satellites)
            groups: dict[str, list[str]] = {h: [] for h in hubs}
            start = 0
            for idx, hub in enumerate(hubs):
                if idx == len(hubs) - 1:
                    size = remaining
                else:
                    size = rng.randint(0, remaining)
                groups[hub] = satellites[start : start + size]
                start += size
                remaining -= size

            self._parent_of = {h: None for h in hubs}
            for hub, children in groups.items():
                for child in children:
                    self._parent_of[child] = hub

            # Safety: any unassigned satellite (shouldn't happen) gets assigned randomly.
            for name in self._table_names:
                if name not in self._parent_of:
                    self._parent_of[name] = rng.choice(hubs)

        self._roots = [n for n in self._table_names if self._parent_of.get(n) is None]
        self._non_roots = [n for n in self._table_names if self._parent_of.get(n) is not None]

    def _qtbl(self, name: str) -> str:
        if not self._schema:
            raise RuntimeError("Schema not initialized; setup() must run first")
        return _qname(self._schema, name)

    async def _existing_fk_parent_table(self, conn, *, schema: str, table_name: str) -> Optional[str]:
        # Find any FK constraint that uses fk_parent_id and return referenced table name.
        return await conn.fetchval(
            """
            SELECT n2.nspname || '.' || c2.relname
            FROM pg_constraint con
            JOIN pg_class c1 ON c1.oid = con.conrelid
            JOIN pg_class c2 ON c2.oid = con.confrelid
            JOIN pg_namespace n1 ON n1.oid = c1.relnamespace
            JOIN pg_namespace n2 ON n2.oid = c2.relnamespace
            JOIN pg_attribute a ON a.attrelid = c1.oid
            WHERE con.contype = 'f'
                AND n1.nspname = $1
                AND c1.relname = $2
                AND a.attname = 'fk_parent_id'
                AND a.attnum = ANY(con.conkey)
            LIMIT 1
            """,
            schema,
            table_name,
        )

    async def setup(self, conn):  # type: ignore[override]
        # Serialize schema creation/validation across workers.
        lock_key = f"{self._prefix}:fk_chain_insert_schema"
        await conn.execute("SELECT pg_advisory_lock(hashtext($1), 4242)", lock_key)
        try:
            if self._schema is None:
                if self._requested_schema is not None:
                    self._schema = self._requested_schema
                else:
                    # Prefer public if it's writable; otherwise fall back to current_schema().
                    can_create_public = await conn.fetchval(
                        "SELECT has_schema_privilege(current_user, 'public', 'CREATE')"
                    )
                    self._schema = "public" if can_create_public else await conn.fetchval("SELECT current_schema()")

            # Prebuild statement templates once schema is known.
            if not self._insert_sql:
                for name in self._table_names:
                    self._insert_sql[name] = (
                        f"INSERT INTO {self._qtbl(name)} (event_date, val_int, val_text, amount, payload, fk_parent_id) "
                        f"VALUES ($1, $2, $3, $4, $5, $6) RETURNING id"
                    )

            if self.fk_reset:
                # Drop in reverse to minimize dependency conflicts.
                for name in reversed(self._table_names):
                    await conn.execute(f"DROP TABLE IF EXISTS {self._qtbl(name)} CASCADE")

            # Pass 1: ensure all tables exist (no FK constraint yet).
            for name in self._table_names:
                await conn.execute(
                    f"""
                    CREATE TABLE IF NOT EXISTS {self._qtbl(name)} (
                        id BIGSERIAL PRIMARY KEY,
                        event_date DATE NOT NULL,
                        created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                        val_int INTEGER NOT NULL,
                        val_text TEXT NOT NULL,
                        amount NUMERIC(12, 2) NOT NULL,
                        payload BYTEA NOT NULL,
                        fk_parent_id BIGINT
                    )
                    """
                )
                await conn.execute(
                    f"CREATE INDEX IF NOT EXISTS {name}_fk_parent_idx ON {self._qtbl(name)}(fk_parent_id)"
                )
                await conn.execute(
                    f"CREATE INDEX IF NOT EXISTS {name}_event_date_idx ON {self._qtbl(name)}(event_date)"
                )

            # Pass 2: validate/add FK constraints once all parent tables exist.
            for name in self._table_names:
                parent = self._parent_of.get(name)
                expected_parent = None if parent is None else _qname(self._schema, parent)

                existing_parent = await self._existing_fk_parent_table(
                    conn,
                    schema=self._schema,
                    table_name=name,
                )

                if expected_parent is None:
                    # Root table should not have an FK on fk_parent_id.
                    if existing_parent is not None:
                        raise RuntimeError(
                            "Existing schema has an FK on the root table. "
                            "Use a new --table prefix or run with --fk-reset to recreate tables."
                        )
                    continue

                if existing_parent is None:
                    await conn.execute(
                        f"""
                        ALTER TABLE {self._qtbl(name)}
                        	ADD CONSTRAINT {name}_fk_parent
                        	FOREIGN KEY (fk_parent_id)
                        	REFERENCES {expected_parent}(id)
                        	DEFERRABLE INITIALLY DEFERRED
                        """
                    )
                elif existing_parent != expected_parent:
                    raise RuntimeError(
                        f"Existing schema for {name} references {existing_parent}, "
                        f"but topology '{self.fk_topology}' expects {expected_parent}. "
                        "Use a new --table prefix or run with --fk-reset to recreate tables."
                    )
        finally:
            await conn.execute("SELECT pg_advisory_unlock(hashtext($1), 4242)", lock_key)

    def _make_row(self) -> _RowData:
        # Small random distribution around "today"; keeps the date column meaningful.
        day_offset = random.randint(0, 30)
        event_date = _dt.date.today() - _dt.timedelta(days=day_offset)
        val_int = random.randint(1, 1_000_000)
        val_text = secrets.token_hex(8)
        amount = random.random() * 10_000
        payload = secrets.token_bytes(self.payload_size)
        return _RowData(
            event_date=event_date,
            val_int=val_int,
            val_text=val_text,
            amount=amount,
            payload=payload,
        )

    async def execute(self, conn):  # type: ignore[override]
        row = self._make_row()

        async with conn.transaction():
            ids: dict[str, int] = {}

            # Insert roots first.
            for name in self._roots:
                new_id = await conn.fetchval(
                    self._insert_sql[name],
                    row.event_date,
                    row.val_int,
                    row.val_text,
                    row.amount,
                    row.payload,
                    None,
                )
                ids[name] = new_id

            # Insert non-roots; in chain topology, order matters but our list preserves it.
            for name in self._non_roots:
                parent = self._parent_of.get(name)
                if parent is None:
                    parent_id = None
                else:
                    parent_id = ids[parent]

                new_id = await conn.fetchval(
                    self._insert_sql[name],
                    row.event_date,
                    row.val_int,
                    row.val_text,
                    row.amount,
                    row.payload,
                    parent_id,
                )
                ids[name] = new_id
