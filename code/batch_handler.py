"""AWS Batch handler for Search Keyword Performance Attribution.

Designed to run as a Fargate container task.  Reads configuration from
environment variables via the centralized ``EngineConfig``, downloads the
input file from S3, runs the attribution engine, and uploads the result
along with a metadata manifest.

Environment variables:
    INPUT_BUCKET   -- S3 bucket containing the input file
    INPUT_KEY      -- S3 object key of the input file
    OUTPUT_PREFIX  -- S3 key prefix for the output file (default: "output/")
    SESSION_TIMEOUT -- Session timeout in seconds (default: 0 = disabled)
    SORT_BY_TIME   -- "1" to pre-sort input by hit_time_gmt (default: "0")
    CHECKPOINT_DIR -- Local directory for checkpoint files (default: /tmp/checkpoints)
"""

import logging
import os
import sys
import tempfile
import traceback
from datetime import date

import boto3

from search_keyword_performance.config import EngineConfig
from search_keyword_performance.engine import SearchKeywordAttributor
from search_keyword_performance.exceptions import SearchKeywordError

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(name)s  %(message)s",
)
logger = logging.getLogger(__name__)


def main() -> None:
    bucket = os.environ.get("INPUT_BUCKET")
    key = os.environ.get("INPUT_KEY")
    if not bucket or not key:
        logger.error("INPUT_BUCKET and INPUT_KEY environment variables are required")
        sys.exit(1)

    output_prefix = os.environ.get("OUTPUT_PREFIX", "output/")

    config = EngineConfig.from_env()
    if not config.checkpoint_dir:
        config = EngineConfig(
            session_timeout=config.session_timeout,
            sort_by_time=config.sort_by_time,
            checkpoint_dir=os.environ.get("CHECKPOINT_DIR", "/tmp/checkpoints"),
            checkpoint_interval=config.checkpoint_interval,
            memory_warn_threshold=config.memory_warn_threshold,
        )

    logger.info(
        "Config: bucket=%s key=%s output_prefix=%s session_timeout=%s sort=%s",
        bucket, key, output_prefix, config.session_timeout, config.sort_by_time,
    )

    s3 = boto3.client("s3")

    try:
        with tempfile.TemporaryDirectory() as tmpdir:
            filename = os.path.basename(key)
            local_input = os.path.join(tmpdir, filename)

            logger.info("Downloading s3://%s/%s -> %s", bucket, key, local_input)
            s3.download_file(bucket, key, local_input)

            attributor = SearchKeywordAttributor.from_config(config)
            attributor.process_file(local_input, sort_by_time=config.sort_by_time)

            today = date.today().strftime("%Y-%m-%d")
            output_filename = f"{today}_SearchKeywordPerformance.tab"
            local_output = os.path.join(tmpdir, output_filename)
            attributor.write_output(local_output)

            output_key = f"{output_prefix}{output_filename}"
            logger.info("Uploading %s -> s3://%s/%s", local_output, bucket, output_key)
            s3.upload_file(local_output, bucket, output_key)

            metadata_filename = f"{today}_SearchKeywordPerformance_metadata.json"
            local_metadata = os.path.join(tmpdir, metadata_filename)
            attributor.write_metadata(local_metadata)
            metadata_key = f"{output_prefix}{metadata_filename}"
            s3.upload_file(local_metadata, bucket, metadata_key)

        logger.info(
            "Batch job complete: %d hits processed, %d keyword groups",
            attributor.hits_processed, len(attributor.get_results()),
        )
        logger.info("Data quality: %s", attributor.quality.to_dict())

    except SearchKeywordError as e:
        logger.error("Processing error [%s]: %s", type(e).__name__, e)
        sys.exit(1)
    except Exception:
        logger.error("Batch job failed:\n%s", traceback.format_exc())
        sys.exit(1)


if __name__ == "__main__":
    main()
