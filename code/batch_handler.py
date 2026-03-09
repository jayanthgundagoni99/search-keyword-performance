"""AWS Batch handler for Search Keyword Performance Attribution.

Designed to run as a Fargate container task.  Reads configuration from
environment variables, downloads the input file from S3, runs the
attribution engine, and uploads the result.

Environment variables:
    INPUT_BUCKET   -- S3 bucket containing the input file
    INPUT_KEY      -- S3 object key of the input file
    OUTPUT_PREFIX  -- S3 key prefix for the output file (default: "output/")
    SESSION_TIMEOUT -- Session timeout in seconds (default: 1800, "0" to disable)
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

from search_keyword_performance.engine import SearchKeywordAttributor

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
    checkpoint_dir = os.environ.get("CHECKPOINT_DIR", "/tmp/checkpoints")
    sort_by_time = os.environ.get("SORT_BY_TIME", "0") == "1"

    timeout_raw = os.environ.get("SESSION_TIMEOUT", "1800")
    session_timeout = None if timeout_raw == "0" else int(timeout_raw)

    logger.info(
        "Config: bucket=%s key=%s output_prefix=%s session_timeout=%s sort=%s",
        bucket, key, output_prefix, session_timeout, sort_by_time,
    )

    s3 = boto3.client("s3")

    try:
        with tempfile.TemporaryDirectory() as tmpdir:
            filename = os.path.basename(key)
            local_input = os.path.join(tmpdir, filename)

            logger.info("Downloading s3://%s/%s -> %s", bucket, key, local_input)
            s3.download_file(bucket, key, local_input)

            attributor = SearchKeywordAttributor(
                session_timeout=session_timeout,
                checkpoint_dir=checkpoint_dir,
            )
            attributor.process_file(local_input, sort_by_time=sort_by_time)

            today = date.today().strftime("%Y-%m-%d")
            output_filename = f"{today}_SearchKeywordPerformance.tab"
            local_output = os.path.join(tmpdir, output_filename)
            attributor.write_output(local_output)

            output_key = f"{output_prefix}{output_filename}"
            logger.info("Uploading %s -> s3://%s/%s", local_output, bucket, output_key)
            s3.upload_file(local_output, bucket, output_key)

        logger.info(
            "Batch job complete: %d hits processed, %d keyword groups",
            attributor.hits_processed, len(attributor.get_results()),
        )

    except Exception:
        logger.error("Batch job failed:\n%s", traceback.format_exc())
        sys.exit(1)


if __name__ == "__main__":
    main()
