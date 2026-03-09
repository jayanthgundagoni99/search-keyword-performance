"""AWS Lambda handler for Search Keyword Performance Attribution.

Triggered by S3 ``s3:ObjectCreated:*`` events on the ``input/`` prefix.
Downloads the hit-level data file, runs the attribution engine, and
uploads the resulting ``.tab`` file to the ``output/`` prefix in the
same bucket.

Production best practices applied:
- Structured JSON logging with request context
- SDK client initialized outside handler (cold start optimization)
- Error handling that does not leak internal details
- Processing metrics (duration, file size, hit count)
"""

import json
import logging
import os
import tempfile
import time
from datetime import date
from typing import Any
from urllib.parse import unquote_plus

import boto3

from search_keyword_performance.engine import SearchKeywordAttributor

logger = logging.getLogger()
logger.setLevel(os.environ.get("LOG_LEVEL", "INFO").upper())

# SDK client initialized at module level -- reused across warm invocations
s3 = boto3.client("s3")
OUTPUT_PREFIX = os.environ.get("OUTPUT_PREFIX", "output/")


def _structured_log(level: str, message: str, **kwargs) -> None:
    """Emit a structured JSON log line for CloudWatch Insights queries."""
    entry = {
        "level": level,
        "message": message,
        "function": os.environ.get("AWS_LAMBDA_FUNCTION_NAME", "local"),
        "version": os.environ.get("AWS_LAMBDA_FUNCTION_VERSION", "$LATEST"),
        **kwargs,
    }
    log_fn = getattr(logger, level.lower(), logger.info)
    log_fn(json.dumps(entry, default=str))


def handler(event: dict[str, Any], context: Any) -> dict[str, Any]:
    """Lambda entry point for S3 event triggers."""
    request_id = getattr(context, "aws_request_id", "local")
    remaining_ms = getattr(context, "get_remaining_time_in_millis", lambda: 0)()

    _structured_log("info", "Handler invoked", request_id=request_id,
                    remaining_ms=remaining_ms)

    processed = 0
    errors = 0

    for record in event.get("Records", []):
        s3_event = record.get("s3", {})
        bucket = s3_event.get("bucket", {}).get("name")
        raw_key = s3_event.get("object", {}).get("key", "")
        key = unquote_plus(raw_key)
        file_size = s3_event.get("object", {}).get("size", 0)

        if not bucket or not key:
            _structured_log("warning", "Missing bucket/key in S3 event record",
                            request_id=request_id)
            errors += 1
            continue

        _structured_log("info", "Processing file",
                        request_id=request_id,
                        bucket=bucket, key=key, file_size_bytes=file_size)

        start = time.monotonic()

        try:
            with tempfile.TemporaryDirectory() as tmpdir:
                local_input = os.path.join(tmpdir, os.path.basename(key))
                s3.download_file(bucket, key, local_input)

                attributor = SearchKeywordAttributor()
                attributor.process_file(local_input)

                today = date.today().strftime("%Y-%m-%d")
                output_filename = f"{today}_SearchKeywordPerformance.tab"
                local_output = os.path.join(tmpdir, output_filename)
                attributor.write_output(local_output)

                output_key = f"{OUTPUT_PREFIX}{output_filename}"
                s3.upload_file(local_output, bucket, output_key)

                duration_ms = int((time.monotonic() - start) * 1000)
                results = attributor.get_results()

                _structured_log("info", "File processed successfully",
                                request_id=request_id,
                                bucket=bucket,
                                input_key=key,
                                output_key=output_key,
                                hits_processed=attributor.hits_processed,
                                keyword_groups=len(results),
                                duration_ms=duration_ms)
                processed += 1

        except Exception as e:
            duration_ms = int((time.monotonic() - start) * 1000)
            _structured_log("error", "Failed to process file",
                            request_id=request_id,
                            bucket=bucket, key=key,
                            error_type=type(e).__name__,
                            error_message=str(e),
                            duration_ms=duration_ms)
            errors += 1
            raise

    _structured_log("info", "Handler complete",
                    request_id=request_id,
                    records_processed=processed, records_failed=errors)

    return {"statusCode": 200, "body": "OK"}
