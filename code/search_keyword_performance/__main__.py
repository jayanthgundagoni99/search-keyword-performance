"""CLI entry point: ``python -m search_keyword_performance <input_file>``

By default, the engine uses pure file-order last-touch attribution with
no session timeout -- matching the exercise requirements exactly.

Production flags:
    --session-timeout N   Enable session timeout (seconds, e.g. 1800 for 30 min)
    --sort                Pre-sort input by hit_time_gmt
    --checkpoint-dir DIR  Enable checkpointing to DIR
    --no-validate         Skip schema validation
    --metadata            Write a JSON metadata file alongside the output
"""

import argparse
import logging
import sys
from datetime import date

from .config import EngineConfig
from .engine import SearchKeywordAttributor
from .exceptions import InputSchemaError, SearchKeywordError

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
    p.add_argument(
        "--no-validate", action="store_true",
        help="Skip input schema validation",
    )
    p.add_argument(
        "--metadata", action="store_true",
        help="Write a JSON metadata/manifest file alongside the output",
    )
    return p


def main() -> None:
    args = _build_parser().parse_args()

    output_file = args.output
    if not output_file:
        today = date.today().strftime("%Y-%m-%d")
        output_file = f"{today}_SearchKeywordPerformance.tab"

    config = EngineConfig(
        session_timeout=args.session_timeout,
        sort_by_time=args.sort,
        checkpoint_dir=args.checkpoint_dir,
        validate_schema=not args.no_validate,
    )

    logger.info("Starting Search Keyword Performance Attribution")
    logger.info("Input : %s", args.input_file)
    logger.info("Output: %s", output_file)
    if config.session_timeout is not None:
        logger.info("Session timeout: %ds", config.session_timeout)
    else:
        logger.info("Session timeout: disabled (pure file-order last-touch)")
    if config.sort_by_time:
        logger.info("Pre-sorting: enabled")
    if config.checkpoint_dir:
        logger.info("Checkpointing: %s", config.checkpoint_dir)

    try:
        attributor = SearchKeywordAttributor.from_config(config)
        attributor.process_file(args.input_file, sort_by_time=config.sort_by_time)
        attributor.write_output(output_file)

        if args.metadata:
            metadata_file = output_file.rsplit(".", 1)[0] + "_metadata.json"
            attributor.write_metadata(metadata_file)

        for domain, keyword, revenue in attributor.get_results():
            logger.info("  %s\t%s\t%s", domain, keyword, revenue)

        logger.info("Done.")
    except InputSchemaError as e:
        logger.error("Input schema error: %s", e)
        sys.exit(2)
    except SearchKeywordError as e:
        logger.error("Processing error: %s", e)
        sys.exit(1)


if __name__ == "__main__":
    main()
