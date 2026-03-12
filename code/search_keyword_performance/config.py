"""Centralized configuration for all execution modes.

A single ``EngineConfig`` dataclass is the source of truth for CLI flags,
Lambda env vars, and Batch env vars.  This prevents drift between
execution modes and makes validation explicit.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Optional


MEMORY_WARN_VISITORS_DEFAULT = 500_000
CHECKPOINT_INTERVAL_DEFAULT = 100_000

REQUIRED_COLUMNS = frozenset({
    "hit_time_gmt",
    "ip",
    "user_agent",
    "event_list",
    "product_list",
    "referrer",
})

OUTPUT_SCHEMA_VERSION = "v1"


@dataclass(frozen=True)
class EngineConfig:
    """Validated, immutable configuration for the attribution engine.

    Construct via :meth:`from_cli_args` or :meth:`from_env` to guarantee
    validation runs exactly once.
    """

    session_timeout: Optional[int] = None
    sort_by_time: bool = False
    checkpoint_dir: Optional[str] = None
    checkpoint_interval: int = CHECKPOINT_INTERVAL_DEFAULT
    memory_warn_threshold: int = MEMORY_WARN_VISITORS_DEFAULT
    validate_schema: bool = True

    def __post_init__(self) -> None:
        if self.session_timeout is not None and self.session_timeout < 0:
            raise ValueError(
                f"session_timeout must be >= 0 or None, got {self.session_timeout}"
            )
        if self.checkpoint_interval < 1:
            raise ValueError(
                f"checkpoint_interval must be >= 1, got {self.checkpoint_interval}"
            )
        if self.memory_warn_threshold < 1:
            raise ValueError(
                f"memory_warn_threshold must be >= 1, got {self.memory_warn_threshold}"
            )

    @classmethod
    def from_env(cls) -> EngineConfig:
        """Build config from environment variables (Lambda / Batch)."""
        timeout_raw = os.environ.get("SESSION_TIMEOUT", "0")
        session_timeout = None if timeout_raw == "0" else int(timeout_raw)

        return cls(
            session_timeout=session_timeout,
            sort_by_time=os.environ.get("SORT_BY_TIME", "0") == "1",
            checkpoint_dir=os.environ.get("CHECKPOINT_DIR") or None,
            checkpoint_interval=int(
                os.environ.get("CHECKPOINT_INTERVAL", str(CHECKPOINT_INTERVAL_DEFAULT))
            ),
            memory_warn_threshold=int(
                os.environ.get("MEMORY_WARN_THRESHOLD", str(MEMORY_WARN_VISITORS_DEFAULT))
            ),
        )
