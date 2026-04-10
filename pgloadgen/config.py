from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any
import json
import yaml


@dataclass(slots=True)
class LoadGenConfig:
    dsn: str
    workload: str
    concurrency: int = 4
    duration_seconds: int = 30
    target_rate: float = 0.0  # 0 => unlimited
    report_interval: int = 5
    table: str = "loadgen_events"
    payload_size: int = 128
    operations: int = 0  # if >0 stop after N operations instead of duration
    start_date: str = "2024-01-01"  # for partition workloads
    partition_span_days: int = 180
    second_table: bool = False
    fk_topology: str = "chain"  # for fk_chain_insert workload: 'chain' or 'star'
    fk_reset: bool = False  # drop/recreate FK tables for fk_chain_insert
    think_time: float = 0.0  # seconds to sleep between operations
    ramp_up_seconds: float = 0.0  # gradual worker start-up period
    output_file: str = ""  # path for JSON results export (empty = no export)
    read_pct: float = 50.0  # read percentage for mixed workload
    update_pct: float = 10.0  # update percentage for mixed workload
    delete_pct: float = 5.0  # delete percentage for mixed workload

    @staticmethod
    def from_mapping(data: dict[str, Any]) -> "LoadGenConfig":
        return LoadGenConfig(**data)

    @staticmethod
    def load(path: str | Path) -> "LoadGenConfig":
        p = Path(path)
        text = p.read_text(encoding="utf-8")
        if p.suffix.lower() in {".yaml", ".yml"}:
            data = yaml.safe_load(text) or {}
        elif p.suffix.lower() == ".json":
            data = json.loads(text)
        else:
            raise ValueError(f"Unsupported config extension: {p.suffix}")
        return LoadGenConfig.from_mapping(data)
