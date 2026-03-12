"""Attribution engine that credits search-engine revenue across visitor sessions.

The core class :class:`SearchKeywordAttributor` streams hit-level records,
tracks each visitor's most recent external search referrer, and attributes
purchase revenue to that referrer (last-touch attribution).

Production features:
- Schema validation (fail fast on wrong file shape)
- Data quality counters (malformed referrers, unattributed purchases, etc.)
- Metadata / manifest output for auditability
- Session timeout (configurable, disabled by default)
- Compressed input (.gz, .zst)
- Optional pre-sorting by hit_time_gmt
- Periodic checkpointing for crash recovery
- Memory guardrails (warn when visitor map grows large)
- Atomic output writes (temp-then-rename)
"""

import csv
import gzip
import io
import json
import logging
import os
import tempfile
import time
from collections import defaultdict
from dataclasses import asdict, dataclass, field
from decimal import Decimal
from typing import Optional, TextIO
from urllib.parse import urlparse as _urlparse

from .config import (
    CHECKPOINT_INTERVAL_DEFAULT,
    MEMORY_WARN_VISITORS_DEFAULT,
    OUTPUT_SCHEMA_VERSION,
    REQUIRED_COLUMNS,
    EngineConfig,
)
from .exceptions import (
    CheckpointRestoreError,
    CheckpointWriteError,
    InputSchemaError,
    OutputWriteError,
)
from .parsers import (
    extract_search_referrer,
    parse_event_list,
    parse_product_list_revenue,
)

logger = logging.getLogger(__name__)

DEFAULT_CHECKPOINT_INTERVAL = CHECKPOINT_INTERVAL_DEFAULT


@dataclass
class DataQualityMetrics:
    """Counters for data quality issues encountered during processing."""

    malformed_referrers: int = 0
    purchases_without_revenue: int = 0
    unattributed_purchases: int = 0
    blank_timestamps: int = 0
    unknown_referrer_domains: int = 0
    total_purchase_events: int = 0
    attributed_purchase_events: int = 0
    total_revenue_attributed: Decimal = field(default_factory=lambda: Decimal("0"))
    total_revenue_unattributed: Decimal = field(default_factory=lambda: Decimal("0"))
    rows_with_search_referrer: int = 0

    def to_dict(self) -> dict:
        d = {}
        for k, v in asdict(self).items():
            d[k] = str(v) if isinstance(v, Decimal) else v
        return d


def open_input(filepath: str) -> TextIO:
    """Return a text-mode file handle, transparently decompressing if needed."""
    if filepath.endswith(".gz"):
        return gzip.open(filepath, "rt", encoding="utf-8")
    if filepath.endswith(".zst"):
        try:
            import zstandard
        except ImportError:
            raise ImportError(
                "Install the 'zstandard' package to read .zst files: "
                "pip install zstandard"
            )
        fh = open(filepath, "rb")
        dctx = zstandard.ZstdDecompressor()
        reader = dctx.stream_reader(fh)
        return io.TextIOWrapper(reader, encoding="utf-8")
    return open(filepath, "r", encoding="utf-8")


class SearchKeywordAttributor:
    """Streaming last-touch attribution engine for search keyword revenue.

    Parameters:
        session_timeout:
            Seconds of inactivity before a visitor's session resets and
            their tracked search referrer is cleared.  Set to ``None``
            to disable session timeout (original behaviour).
        checkpoint_dir:
            Directory for checkpoint files.  ``None`` disables
            checkpointing.
        checkpoint_interval:
            Number of hits between checkpoint writes.
        validate_schema:
            If ``True`` (default), verify required columns exist in the
            first row and raise :class:`InputSchemaError` if not.
        memory_warn_threshold:
            Log a warning when the visitor map exceeds this many entries.
    """

    def __init__(
        self,
        session_timeout: Optional[int] = None,
        checkpoint_dir: Optional[str] = None,
        checkpoint_interval: int = DEFAULT_CHECKPOINT_INTERVAL,
        validate_schema: bool = True,
        memory_warn_threshold: int = MEMORY_WARN_VISITORS_DEFAULT,
    ) -> None:
        self._session_timeout = session_timeout
        self._checkpoint_dir = checkpoint_dir
        self._checkpoint_interval = checkpoint_interval
        self._validate_schema = validate_schema
        self._memory_warn_threshold = memory_warn_threshold
        self._memory_warned = False

        self._visitor_search: dict[tuple[str, str], tuple[str, str]] = {}
        self._visitor_last_time: dict[tuple[str, str], int] = {}
        self._revenue: defaultdict[tuple[str, str], Decimal] = defaultdict(
            lambda: Decimal("0")
        )
        self._keyword_display: dict[tuple[str, str], str] = {}
        self._hits_processed: int = 0

        self._quality = DataQualityMetrics()
        self._start_time: Optional[float] = None
        self._input_filepath: Optional[str] = None

    @classmethod
    def from_config(cls, config: EngineConfig) -> "SearchKeywordAttributor":
        """Construct from a validated :class:`EngineConfig`."""
        return cls(
            session_timeout=config.session_timeout,
            checkpoint_dir=config.checkpoint_dir,
            checkpoint_interval=config.checkpoint_interval,
            validate_schema=config.validate_schema,
            memory_warn_threshold=config.memory_warn_threshold,
        )

    @property
    def hits_processed(self) -> int:
        """Number of hit-level records processed so far."""
        return self._hits_processed

    @property
    def quality(self) -> DataQualityMetrics:
        """Data quality counters accumulated during processing."""
        return self._quality

    # ------------------------------------------------------------------
    # Core processing
    # ------------------------------------------------------------------

    def process_hit(self, row: dict[str, str]) -> None:
        """Ingest a single hit-level record (one row from the TSV)."""
        ip = row.get("ip", "").strip()
        user_agent = row.get("user_agent", "").strip()
        referrer = row.get("referrer", "").strip()
        event_list = row.get("event_list", "").strip()
        product_list = row.get("product_list", "").strip()

        visitor_key = (ip, user_agent)
        self._hits_processed += 1

        # --- session timeout check ---
        if self._session_timeout is not None:
            raw_time = row.get("hit_time_gmt", "").strip()
            if not raw_time:
                self._quality.blank_timestamps += 1
            hit_time = int(raw_time) if raw_time else 0
            if visitor_key in self._visitor_last_time:
                gap = hit_time - self._visitor_last_time[visitor_key]
                if gap > self._session_timeout:
                    self._visitor_search.pop(visitor_key, None)
                    logger.debug(
                        "Session expired for visitor %s (gap=%ds)", visitor_key, gap
                    )
            self._visitor_last_time[visitor_key] = hit_time

        # --- update last-touch search referrer ---
        search_info = extract_search_referrer(referrer)
        if search_info:
            self._visitor_search[visitor_key] = search_info
            self._quality.rows_with_search_referrer += 1
            domain_s, keyword_s = search_info
            display_key = (domain_s, keyword_s.lower())
            if display_key not in self._keyword_display:
                self._keyword_display[display_key] = keyword_s
        elif referrer:
            try:
                parsed = _urlparse(referrer)
                hostname = (parsed.hostname or "").lower()
                if hostname and not hostname.endswith("esshopzilla.com"):
                    self._quality.unknown_referrer_domains += 1
            except Exception:
                self._quality.malformed_referrers += 1

        # --- attribute purchase revenue ---
        if parse_event_list(event_list):
            self._quality.total_purchase_events += 1
            revenue = parse_product_list_revenue(product_list)

            if revenue > 0 and visitor_key in self._visitor_search:
                domain, keyword = self._visitor_search[visitor_key]
                agg_key = (domain, keyword.lower())
                self._revenue[agg_key] += revenue
                if agg_key not in self._keyword_display:
                    self._keyword_display[agg_key] = keyword
                self._quality.attributed_purchase_events += 1
                self._quality.total_revenue_attributed += revenue
            elif revenue > 0:
                self._quality.unattributed_purchases += 1
                self._quality.total_revenue_unattributed += revenue
            elif revenue == 0:
                self._quality.purchases_without_revenue += 1

        # --- memory guardrail ---
        if (
            not self._memory_warned
            and len(self._visitor_search) > self._memory_warn_threshold
        ):
            self._memory_warned = True
            logger.warning(
                "Visitor map has grown to %d entries (threshold: %d). "
                "Consider using Batch/Fargate or Glue for this workload.",
                len(self._visitor_search),
                self._memory_warn_threshold,
            )

        # --- periodic checkpoint ---
        if (
            self._checkpoint_dir
            and self._hits_processed % self._checkpoint_interval == 0
        ):
            self._write_checkpoint()

    def process_file(self, filepath: str, *, sort_by_time: bool = False) -> None:
        """Stream-process a hit-level data file.

        Supports plain TSV, ``.gz``, and ``.zst`` compressed files.

        Args:
            filepath: Path to the input file.
            sort_by_time: If ``True``, read all rows into memory and sort
                by ``hit_time_gmt`` before processing.  Use only when the
                input is not guaranteed to be chronological **and** fits
                in memory.
        """
        self._start_time = time.monotonic()
        self._input_filepath = filepath
        logger.info("Processing input file: %s", filepath)
        self.restore_checkpoint()

        fh = open_input(filepath)
        try:
            if sort_by_time:
                self._process_sorted(fh)
            else:
                self._process_stream(fh)
        finally:
            fh.close()

        if self._checkpoint_dir:
            self._write_checkpoint()

        elapsed = time.monotonic() - self._start_time
        logger.info(
            "Finished: %d hits, %d visitors, %d keyword groups in %.2fs",
            self._hits_processed,
            len(self._visitor_search),
            len(self._revenue),
            elapsed,
        )
        self._log_business_metrics()

    # ------------------------------------------------------------------
    # Results
    # ------------------------------------------------------------------

    def get_results(self) -> list[tuple[str, str, Decimal]]:
        """Return aggregated results sorted by revenue descending.

        Returns:
            List of ``(search_engine_domain, keyword, revenue)`` tuples.
            Revenue is rounded to 2 decimal places.  Ties are broken
            alphabetically by domain then keyword.
        """
        results: list[tuple[str, str, Decimal]] = []
        for agg_key, revenue in self._revenue.items():
            domain = agg_key[0]
            keyword = self._keyword_display[agg_key]
            results.append((domain, keyword, revenue.quantize(Decimal("0.01"))))
        results.sort(key=lambda r: (-r[2], r[0], r[1].lower()))
        return results

    def write_output(self, output_path: str) -> None:
        """Write results to a tab-delimited file with a header row.

        Uses atomic write: writes to a temp file first, then renames.
        This avoids partially written outputs if the process crashes.
        """
        results = self.get_results()
        output_dir = os.path.dirname(output_path) or "."

        try:
            fd, tmp_path = tempfile.mkstemp(
                suffix=".tab.tmp", dir=output_dir
            )
            with os.fdopen(fd, "w", encoding="utf-8", newline="") as fh:
                writer = csv.writer(fh, delimiter="\t")
                writer.writerow(["Search Engine Domain", "Search Keyword", "Revenue"])
                for domain, keyword, revenue in results:
                    writer.writerow([domain, keyword, str(revenue)])

            os.replace(tmp_path, output_path)
            logger.info("Output written to %s (%d rows)", output_path, len(results))
        except Exception as e:
            if "tmp_path" in locals() and os.path.exists(tmp_path):
                os.unlink(tmp_path)
            raise OutputWriteError(
                f"Failed to write output to {output_path}: {e}"
            ) from e

    def write_metadata(self, metadata_path: str) -> None:
        """Write a JSON metadata/manifest file for the current run.

        Captures input file, execution context, counts, quality metrics,
        and active configuration for auditability.
        """
        elapsed = None
        if self._start_time is not None:
            elapsed = round(time.monotonic() - self._start_time, 3)

        results = self.get_results()
        metadata = {
            "schema_version": OUTPUT_SCHEMA_VERSION,
            "input_file": self._input_filepath,
            "execution_timestamp_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            "elapsed_seconds": elapsed,
            "rows_processed": self._hits_processed,
            "unique_visitors": len(self._visitor_search),
            "keyword_groups": len(self._revenue),
            "total_attributed_revenue": str(
                sum(r[2] for r in results)
            ) if results else "0",
            "config": {
                "session_timeout": self._session_timeout,
                "validate_schema": self._validate_schema,
                "checkpoint_dir": self._checkpoint_dir,
                "memory_warn_threshold": self._memory_warn_threshold,
            },
            "data_quality": self._quality.to_dict(),
            "results_summary": [
                {"domain": r[0], "keyword": r[1], "revenue": str(r[2])}
                for r in results
            ],
        }

        try:
            fd, tmp_path = tempfile.mkstemp(
                suffix=".json.tmp",
                dir=os.path.dirname(metadata_path) or ".",
            )
            with os.fdopen(fd, "w", encoding="utf-8") as fh:
                json.dump(metadata, fh, indent=2, default=str)

            os.replace(tmp_path, metadata_path)
            logger.info("Metadata written to %s", metadata_path)
        except Exception as e:
            if "tmp_path" in locals() and os.path.exists(tmp_path):
                os.unlink(tmp_path)
            raise OutputWriteError(
                f"Failed to write metadata to {metadata_path}: {e}"
            ) from e

    def get_metadata(self) -> dict:
        """Return run metadata as a dictionary (for embedding in logs/responses)."""
        results = self.get_results()
        return {
            "rows_processed": self._hits_processed,
            "unique_visitors": len(self._visitor_search),
            "keyword_groups": len(self._revenue),
            "total_attributed_revenue": str(
                sum(r[2] for r in results)
            ) if results else "0",
            "data_quality": self._quality.to_dict(),
        }

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    def _validate_header(self, fieldnames: list[str] | None) -> None:
        """Raise InputSchemaError if required columns are missing."""
        if not fieldnames:
            raise InputSchemaError("Input file has no header row")
        present = {f.strip().lower() for f in fieldnames}
        missing = {c for c in REQUIRED_COLUMNS if c.lower() not in present}
        if missing:
            raise InputSchemaError(
                f"Input file is missing required columns: {sorted(missing)}. "
                f"Found columns: {sorted(fieldnames)}"
            )

    def _process_stream(self, fh: TextIO) -> None:
        """Process rows from an already-open file handle."""
        reader = csv.DictReader(fh, delimiter="\t")
        if self._validate_schema:
            self._validate_header(reader.fieldnames)
        for row in reader:
            self.process_hit(row)

    def _process_sorted(self, fh: TextIO) -> None:
        """Read all rows, sort by hit_time_gmt, then process."""
        reader = csv.DictReader(fh, delimiter="\t")
        if self._validate_schema:
            self._validate_header(reader.fieldnames)
        rows = list(reader)
        logger.info("Sorting %d rows by hit_time_gmt...", len(rows))
        rows.sort(key=lambda r: int(r.get("hit_time_gmt", "0") or "0"))
        for row in rows:
            self.process_hit(row)

    def _log_business_metrics(self) -> None:
        """Emit business-level observability metrics to logs."""
        q = self._quality
        results = self.get_results()
        total_revenue = sum(r[2] for r in results) if results else Decimal("0")

        pct_search = (
            (q.rows_with_search_referrer / self._hits_processed * 100)
            if self._hits_processed > 0
            else 0
        )

        logger.info(
            "Business metrics: attributed_revenue=%s keyword_groups=%d "
            "attributed_purchases=%d unattributed_purchases=%d "
            "pct_rows_with_search_referrer=%.1f%%",
            total_revenue,
            len(results),
            q.attributed_purchase_events,
            q.unattributed_purchases,
            pct_search,
        )

        if q.malformed_referrers > 0:
            logger.warning(
                "Data quality: %d malformed referrer URLs", q.malformed_referrers
            )
        if q.blank_timestamps > 0:
            logger.warning(
                "Data quality: %d rows with blank hit_time_gmt", q.blank_timestamps
            )
        if q.purchases_without_revenue > 0:
            logger.warning(
                "Data quality: %d purchase events with zero revenue",
                q.purchases_without_revenue,
            )

    # ------------------------------------------------------------------
    # Checkpointing
    # ------------------------------------------------------------------

    def _checkpoint_path(self) -> str:
        return os.path.join(self._checkpoint_dir, "skp_checkpoint.json")  # type: ignore[arg-type]

    def _write_checkpoint(self) -> None:
        """Serialize current state to a JSON checkpoint file."""
        try:
            os.makedirs(self._checkpoint_dir, exist_ok=True)  # type: ignore[arg-type]
            state = {
                "hits_processed": self._hits_processed,
                "visitor_search": {
                    f"{k[0]}|{k[1]}": list(v) for k, v in self._visitor_search.items()
                },
                "visitor_last_time": {
                    f"{k[0]}|{k[1]}": v for k, v in self._visitor_last_time.items()
                },
                "revenue": {
                    f"{k[0]}|{k[1]}": str(v) for k, v in self._revenue.items()
                },
                "keyword_display": {
                    f"{k[0]}|{k[1]}": v for k, v in self._keyword_display.items()
                },
            }
            path = self._checkpoint_path()
            tmp = path + ".tmp"
            with open(tmp, "w", encoding="utf-8") as fh:
                json.dump(state, fh)
            os.replace(tmp, path)
            logger.info("Checkpoint written at %d hits: %s", self._hits_processed, path)
        except Exception as e:
            raise CheckpointWriteError(
                f"Failed to write checkpoint: {e}"
            ) from e

    def restore_checkpoint(self) -> None:
        """Restore state from a checkpoint if one exists.

        This is a no-op when checkpointing is disabled or no checkpoint
        file is found.  Safe to call unconditionally before processing.
        """
        if not self._checkpoint_dir:
            return
        path = self._checkpoint_path()
        if not os.path.exists(path):
            return

        try:
            logger.info("Restoring from checkpoint: %s", path)
            with open(path, "r", encoding="utf-8") as fh:
                state = json.load(fh)

            self._hits_processed = state["hits_processed"]

            self._visitor_search = {}
            for k, v in state["visitor_search"].items():
                parts = k.split("|", 1)
                self._visitor_search[(parts[0], parts[1])] = tuple(v)  # type: ignore[assignment]

            self._visitor_last_time = {}
            for k, v in state["visitor_last_time"].items():
                parts = k.split("|", 1)
                self._visitor_last_time[(parts[0], parts[1])] = v

            self._revenue = defaultdict(lambda: Decimal("0"))
            for k, v in state["revenue"].items():
                parts = k.split("|", 1)
                self._revenue[(parts[0], parts[1])] = Decimal(v)

            self._keyword_display = {}
            for k, v in state["keyword_display"].items():
                parts = k.split("|", 1)
                self._keyword_display[(parts[0], parts[1])] = v

            logger.info("Restored state: %d hits already processed", self._hits_processed)
        except Exception as e:
            raise CheckpointRestoreError(
                f"Failed to restore checkpoint from {path}: {e}"
            ) from e
