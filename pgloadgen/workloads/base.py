from __future__ import annotations

import abc
from typing import Dict, Type

_WORKLOADS: Dict[str, Type["BaseWorkload"]] = {}


def register_workload(name: str):
    def deco(cls):
        _WORKLOADS[name] = cls
        cls.workload_name = name  # type: ignore[attr-defined]
        return cls
    return deco


def get_workload(name: str) -> Type["BaseWorkload"]:
    try:
        return _WORKLOADS[name]
    except KeyError:  # pragma: no cover - defensive
        raise ValueError(f"Unknown workload '{name}'. Available: {', '.join(_WORKLOADS)}")


class BaseWorkload(abc.ABC):
    """Abstract workload contract."""

    def __init__(self, *, table: str, payload_size: int):
        self.table = table
        self.payload_size = payload_size

    async def setup(self, conn):  # type: ignore[override]
        """Optional DB setup (e.g., ensure tables)."""
        return None

    @abc.abstractmethod
    async def execute(self, conn):  # type: ignore[override]
        """Perform a single logical operation."""
        raise NotImplementedError
