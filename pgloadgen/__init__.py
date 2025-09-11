"""pgloadgen package."""

from .config import LoadGenConfig
from .runner import run_load_test

__all__ = ["LoadGenConfig", "run_load_test"]
