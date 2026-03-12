"""AWS Lambda handler for Search Keyword Performance Attribution.

Triggered by S3 ``s3:ObjectCreated:*`` events on the ``input/`` prefix.
Downloads the hit-level data file, runs the attribution engine, and
uploads the resulting ``.tab`` file to the ``output/`` prefix in the
same bucket.

Production best practices applied:
- Idempotency: checks if output already exists for this input (ETag-based)
- Structured JSON logging with request context
- Metadata/manifest JSON alongside output
- Business-level metrics in logs
- SDK client initialized outside handler (cold start optimization)
- Error handling with structured error taxonomy
- Atomic output writes
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
from botocore.exceptions import ClientError

from search_keyword_performance.engine import SearchKeywordAttributor
from search_keyword_performance.exceptions import (
    AWSIOError,
    DuplicateRunError,
    SearchKeywordError,
)

logger = logging.getLogger()
logger.setLevel(os.environ.get("LOG_LEVEL", "INFO").upper())

s3 = boto3.client("s3")
OUTPUT_PREFIX = os.environ.get("OUTPUT_PREFIX", "output/")
ENABLE_IDEMPOTENCY = os.environ.get("ENABLE_IDEMPOTENCY", "1") == "1"


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


def _check_already_processed(bucket: str, key: str, etag: str) -> bool:
    """Check if a manifest already exists for this input (idempotency guard).

    The manifest key encodes the input key and ETag so re-uploads of the
    same file with different content are still processed.
    """
    if not ENABLE_IDEMPOTENCY:
        return False
    manifest_key = f"{OUTPUT_PREFIX}_manifests/{key}.{etag}.json"
    try:
        s3.head_object(Bucket=bucket, Key=manifest_key)
        return True
    except ClientError:
        return False


def _write_manifest(bucket: str, key: str, etag: str, metadata: dict) -> str:
    """Write a processing manifest to S3 for idempotency tracking."""
    manifest_key = f"{OUTPUT_PREFIX}_manifests/{key}.{etag}.json"
    s3.put_object(
        Bucket=bucket,
        Key=manifest_key,
        Body=json.dumps(metadata, indent=2, default=str).encode("utf-8"),
        ContentType="application/json",
    )
    return manifest_key


def handler(event: dict[str, Any], context: Any) -> dict[str, Any]:
    """Lambda entry point for S3 event triggers."""
    request_id = getattr(context, "aws_request_id", "local")
    remaining_ms = getattr(context, "get_remaining_time_in_millis", lambda: 0)()

    _structured_log("info", "Handler invoked", request_id=request_id,
                    remaining_ms=remaining_ms)

    processed = 0
    errors = 0
    skipped = 0

    for record in event.get("Records", []):
        s3_event = record.get("s3", {})
        bucket = s3_event.get("bucket", {}).get("name")
        raw_key = s3_event.get("object", {}).get("key", "")
        key = unquote_plus(raw_key)
        file_size = s3_event.get("object", {}).get("size", 0)
        etag = s3_event.get("object", {}).get("eTag", "unknown")

        if not bucket or not key:
            _structured_log("warning", "Missing bucket/key in S3 event record",
                            request_id=request_id)
            errors += 1
            continue

        if _check_already_processed(bucket, key, etag):
            _structured_log("info", "Skipping already-processed file (idempotency)",
                            request_id=request_id, bucket=bucket, key=key, etag=etag)
            skipped += 1
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

                metadata_filename = f"{today}_SearchKeywordPerformance_metadata.json"
                local_metadata = os.path.join(tmpdir, metadata_filename)
                attributor.write_metadata(local_metadata)
                metadata_key = f"{OUTPUT_PREFIX}{metadata_filename}"
                s3.upload_file(local_metadata, bucket, metadata_key)

                duration_ms = int((time.monotonic() - start) * 1000)
                run_metadata = attributor.get_metadata()
                run_metadata["request_id"] = request_id
                run_metadata["input_key"] = key
                run_metadata["output_key"] = output_key
                run_metadata["duration_ms"] = duration_ms

                _write_manifest(bucket, key, etag, run_metadata)

                _structured_log("info", "File processed successfully",
                                request_id=request_id,
                                bucket=bucket,
                                input_key=key,
                                output_key=output_key,
                                metadata_key=metadata_key,
                                hits_processed=attributor.hits_processed,
                                keyword_groups=len(attributor.get_results()),
                                duration_ms=duration_ms,
                                data_quality=attributor.quality.to_dict())
                processed += 1

        except SearchKeywordError as e:
            duration_ms = int((time.monotonic() - start) * 1000)
            _structured_log("error", "Processing error",
                            request_id=request_id,
                            bucket=bucket, key=key,
                            error_type=type(e).__name__,
                            error_message=str(e),
                            duration_ms=duration_ms)
            errors += 1
            raise
        except Exception as e:
            duration_ms = int((time.monotonic() - start) * 1000)
            _structured_log("error", "Unexpected error",
                            request_id=request_id,
                            bucket=bucket, key=key,
                            error_type=type(e).__name__,
                            error_message=str(e),
                            duration_ms=duration_ms)
            errors += 1
            raise

    _structured_log("info", "Handler complete",
                    request_id=request_id,
                    records_processed=processed,
                    records_skipped=skipped,
                    records_failed=errors)

    return {"statusCode": 200, "body": "OK"}
