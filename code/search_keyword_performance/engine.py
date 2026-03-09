"""Attribution engine that credits search-engine revenue across visitor sessions.

The core class :class:`SearchKeywordAttributor` streams hit-level records,
tracks each visitor's most recent external search referrer, and attributes
purchase revenue to that referrer (last-touch attribution).

Production features:
- 30-minute session timeout (configurable)
- Compressed input (.gz, .zst)
- Optional pre-sorting by hit_time_gmt
- Periodic checkpointing for crash recovery
"""

import csv
import gzip
import io
import json
import logging
import os
from collections import defaultdict
from decimal import Decimal
from typing import Optional, TextIO

from .parsers import (
    extract_search_referrer,
    parse_event_list,
    parse_product_list_revenue,
)

logger = logging.getLogger(__name__)

DEFAULT_SESSION_TIMEOUT = 1800  # 30 minutes in seconds
DEFAULT_CHECKPOINT_INTERVAL = 100_000  # rows between checkpoints


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
    """

    def __init__(
        self,
        session_timeout: Optional[int] = None,
        checkpoint_dir: Optional[str] = None,
        checkpoint_interval: int = DEFAULT_CHECKPOINT_INTERVAL,
    ) -> None:
        self._session_timeout = session_timeout
        self._checkpoint_dir = checkpoint_dir
        self._checkpoint_interval = checkpoint_interval

        self._visitor_search: dict[tuple[str, str], tuple[str, str]] = {}
        self._visitor_last_time: dict[tuple[str, str], int] = {}
        self._revenue: defaultdict[tuple[str, str], Decimal] = defaultdict(
            lambda: Decimal("0")
        )
        self._keyword_display: dict[tuple[str, str], str] = {}
        self._hits_processed: int = 0

    @property
    def hits_processed(self) -> int:
        """Number of hit-level records processed so far."""
        return self._hits_processed

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

        # --- attribute purchase revenue ---
        if parse_event_list(event_list):
            revenue = parse_product_list_revenue(product_list)
            if revenue > 0 and visitor_key in self._visitor_search:
                domain, keyword = self._visitor_search[visitor_key]
                agg_key = (domain, keyword.lower())
                self._revenue[agg_key] += revenue
                if agg_key not in self._keyword_display:
                    self._keyword_display[agg_key] = keyword

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
        logger.info("Processing input file: %s", filepath)
        self._try_restore_checkpoint(filepath)

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

        logger.info(
            "Finished: %d hits processed, %d unique visitors, %d keyword groups",
            self._hits_processed,
            len(self._visitor_search),
            len(self._revenue),
        )

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
        """Write results to a tab-delimited file with a header row."""
        results = self.get_results()
        with open(output_path, "w", encoding="utf-8", newline="") as fh:
            writer = csv.writer(fh, delimiter="\t")
            writer.writerow(["Search Engine Domain", "Search Keyword", "Revenue"])
            for domain, keyword, revenue in results:
                writer.writerow([domain, keyword, str(revenue)])
        logger.info("Output written to %s (%d rows)", output_path, len(results))

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    def _process_stream(self, fh: TextIO) -> None:
        """Process rows from an already-open file handle."""
        reader = csv.DictReader(fh, delimiter="\t")
        for row in reader:
            self.process_hit(row)

    def _process_sorted(self, fh: TextIO) -> None:
        """Read all rows, sort by hit_time_gmt, then process."""
        reader = csv.DictReader(fh, delimiter="\t")
        rows = list(reader)
        logger.info("Sorting %d rows by hit_time_gmt...", len(rows))
        rows.sort(key=lambda r: int(r.get("hit_time_gmt", "0") or "0"))
        for row in rows:
            self.process_hit(row)

    # ------------------------------------------------------------------
    # Checkpointing
    # ------------------------------------------------------------------

    def _checkpoint_path(self) -> str:
        return os.path.join(self._checkpoint_dir, "skp_checkpoint.json")  # type: ignore[arg-type]

    def _write_checkpoint(self) -> None:
        """Serialize current state to a JSON checkpoint file."""
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

    def _try_restore_checkpoint(self, filepath: str) -> None:
        """Restore state from a checkpoint if one exists."""
        if not self._checkpoint_dir:
            return
        path = self._checkpoint_path()
        if not os.path.exists(path):
            return
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
