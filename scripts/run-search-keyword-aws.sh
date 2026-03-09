#!/usr/bin/env bash
# Upload an input file to S3 and wait for the Lambda output.
#
# Usage:
#   ./scripts/run-search-keyword-aws.sh <bucket-name> <local-input-file>
#
# Example:
#   ./scripts/run-search-keyword-aws.sh my-skp-bucket data/data.sql
set -euo pipefail

BUCKET="${1:?Usage: $0 <bucket-name> <local-input-file>}"
INPUT_FILE="${2:?Usage: $0 <bucket-name> <local-input-file>}"
INPUT_KEY="input/$(basename "$INPUT_FILE")"
TODAY=$(date +%Y-%m-%d)
OUTPUT_KEY="output/${TODAY}_SearchKeywordPerformance.tab"

echo "Uploading $INPUT_FILE -> s3://$BUCKET/$INPUT_KEY"
aws s3 cp "$INPUT_FILE" "s3://$BUCKET/$INPUT_KEY"

echo "Waiting for Lambda to produce output..."
for i in $(seq 1 30); do
    sleep 5
    if aws s3 ls "s3://$BUCKET/$OUTPUT_KEY" &>/dev/null; then
        echo "Output ready. Downloading..."
        aws s3 cp "s3://$BUCKET/$OUTPUT_KEY" "./${TODAY}_SearchKeywordPerformance.tab"
        echo "Done.  Output saved to ${TODAY}_SearchKeywordPerformance.tab"
        exit 0
    fi
    echo "  ... waiting ($((i * 5))s)"
done

echo "ERROR: Timed out waiting for output after 150s."
exit 1
