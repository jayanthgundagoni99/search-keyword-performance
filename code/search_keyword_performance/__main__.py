"""CLI entry point: ``python -m search_keyword_performance <input_file>``

By default, the engine uses pure file-order last-touch attribution with
no session timeout -- matching the exercise requirements exactly.

Production flags:
    --session-timeout N   Enable session timeout (seconds, e.g. 1800 for 30 min)
    --sort                Pre-sort input by hit_time_gmt
    --checkpoint-dir DIR  Enable checkpointing to DIR
"""

import argparse
import logging
import sys
from datetime import date

from .engine import SearchKeywordAttributor

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(name)s  %(message)s",
)
logger = logging.getLogger(__name__)


def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="python -m search_keyword_performance",
        description="Search Keyword Performance Attribution Engine",
    )
    p.add_argument("input_file", help="Path to the hit-level data file (.tsv, .gz, .zst)")
    p.add_argument(
        "-o", "--output",
        help="Output file path (default: YYYY-mm-dd_SearchKeywordPerformance.tab)",
    )
    p.add_argument(
        "--sort", action="store_true",
        help="Pre-sort rows by hit_time_gmt (use when input is not chronological)",
    )
    p.add_argument(
        "--session-timeout", type=int, default=None, metavar="SECONDS",
        help=(
            "Enable session timeout: clear tracked referrer after N seconds "
            "of visitor inactivity (e.g. 1800 for 30 min). "
            "Disabled by default to match the exercise spec."
        ),
    )
    p.add_argument(
        "--checkpoint-dir",
        help="Directory for checkpoint files (enables crash recovery)",
    )
    return p


def main() -> None:
    args = _build_parser().parse_args()

    output_file = args.output
    if not output_file:
        today = date.today().strftime("%Y-%m-%d")
        output_file = f"{today}_SearchKeywordPerformance.tab"

    logger.info("Starting Search Keyword Performance Attribution")
    logger.info("Input : %s", args.input_file)
    logger.info("Output: %s", output_file)
    if args.session_timeout is not None:
        logger.info("Session timeout: %ds", args.session_timeout)
    else:
        logger.info("Session timeout: disabled (pure file-order last-touch)")
    if args.sort:
        logger.info("Pre-sorting: enabled")
    if args.checkpoint_dir:
        logger.info("Checkpointing: %s", args.checkpoint_dir)

    attributor = SearchKeywordAttributor(
        session_timeout=args.session_timeout,
        checkpoint_dir=args.checkpoint_dir,
    )
    attributor.process_file(args.input_file, sort_by_time=args.sort)
    attributor.write_output(output_file)

    for domain, keyword, revenue in attributor.get_results():
        logger.info("  %s\t%s\t%s", domain, keyword, revenue)

    logger.info("Done.")


if __name__ == "__main__":
    main()
