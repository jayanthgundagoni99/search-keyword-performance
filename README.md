# Search Keyword Performance Attribution Engine

[![CI](https://github.com/jayanthgundagoni99/search-keyword-performance/actions/workflows/ci.yml/badge.svg)](https://github.com/jayanthgundagoni99/search-keyword-performance/actions/workflows/ci.yml)

A Python application that answers the question: **"How much revenue is the client getting from external search engines (Google, Yahoo, MSN/Bing), and which keywords are performing the best based on revenue?"**

> **This submission's default execution path solves the exercise with a simple Python streaming application in AWS.**

---

## How to Review This Repo

| What to look at | Where |
|---|---|
| Core solution (one class, streaming, last-touch attribution) | `code/search_keyword_performance/engine.py` |
| Parsing logic (pure functions, no state) | `code/search_keyword_performance/parsers.py` |
| CLI entry point (single file argument) | `code/search_keyword_performance/__main__.py` |
| Centralized config + error taxonomy | `code/search_keyword_performance/config.py`, `exceptions.py` |
| AWS deployment (CDK) | `infra/lib/infra-stack.ts` |
| Tests (78 tests: unit + integration + production features) | `tests/` |
| Business problem walkthrough | `docs/search-keyword-walkthrough.md` |
| Expected output | `output/2026-03-05_SearchKeywordPerformance.tab` |
| CI pipeline | `.github/workflows/ci.yml` |

---

## The Solution

The application processes hit-level web analytics data using **last-touch attribution**: it streams through every hit, tracks each visitor's most recent external search engine referrer, and credits purchase revenue to that keyword when a purchase occurs. Revenue uses `Decimal` arithmetic to avoid float drift.

### How It Works

1. **Stream** the tab-separated hit file row by row (`csv.DictReader`).
2. **Track** each visitor's most recent external search referrer (`ip + user_agent` as the visitor key).
3. When a **purchase event** (`event_list` contains `1`) occurs, attribute the revenue from `product_list` to the visitor's last search referrer.
4. **Aggregate** revenue by `(search_engine_domain, keyword)`, case-insensitive on keyword.
5. **Write** a tab-delimited output file sorted by revenue descending.

### Expected Output (`output/2026-03-05_SearchKeywordPerformance.tab`)

```
Search Engine Domain	Search Keyword	Revenue
google.com	Ipod	480.00
bing.com	Zune	250.00
```

See the [Business Problem Walkthrough](docs/search-keyword-walkthrough.md) for a detailed explanation of the attribution challenge, assumptions, sample data trace, and scaling discussion.

---

## Quick Start

### Prerequisites

- Python 3.10+

### Run Locally (single file argument)

```bash
pip install -r requirements.txt

cd code && python -m search_keyword_performance ../data/data.sql
```

That single positional argument is the only required input. The output file is written to the current directory as `YYYY-mm-dd_SearchKeywordPerformance.tab`.

### Run Tests

```bash
python -m pytest tests/ -v
```

---

## Exercise Requirements vs Assumptions vs Production Enhancements

### Required by the exercise (implemented)

- Python application with at least one class (`SearchKeywordAttributor`)
- Accepts a single input file path as argument
- Tab-delimited output: `Search Engine Domain`, `Search Keyword`, `Revenue`
- Sorted by revenue descending, with header row
- Output filename: `[Date]_SearchKeywordPerformance.tab`
- AWS deployment path (Lambda via CDK)
- Scaling discussion for 10+ GB files

### Assumptions made (not specified in the PDF)

| Assumption | Rationale |
|---|---|
| Last-touch attribution | PDF doesn't specify a model; last-touch is simple and standard |
| Visitor = `(ip, user_agent)` | No visitor/session ID in dataset |
| Case-insensitive keywords, first-seen display casing | Parser extracts raw keyword text; engine aggregates case-insensitively while preserving the first-seen casing for display (e.g. `"Ipod"` and `"ipod"` combine, displayed as `"Ipod"`) |
| `search.yahoo.com` displayed as `yahoo.com` | Standardized display domain |
| Input is chronologically sorted | Sample data is sorted; `--sort` flag available as safety net |

### Production enhancements (beyond the exercise)

These are available in the codebase but **disabled by default**. The core solution works without any of them.

| Feature | How to Enable | Why It Exists |
|---|---|---|
| Schema validation | On by default; `--no-validate` to skip | Fail fast on wrong file shape |
| Data quality metrics | Automatic; visible in logs and metadata | Track malformed referrers, unattributed purchases, blank timestamps |
| Metadata manifest | `--metadata` flag or automatic in Lambda | JSON audit trail per run (counts, config, quality) |
| Idempotency guard | Automatic in Lambda (`ENABLE_IDEMPOTENCY=1`) | Prevents duplicate processing on re-uploads |
| Structured errors | `exceptions.py` taxonomy | Actionable alerts by failure category |
| Centralized config | `config.py` / `EngineConfig` dataclass | Prevents drift between CLI, Lambda, and Batch modes |
| Atomic output writes | Automatic (temp-then-rename) | No partial outputs on crash |
| Memory guardrails | Automatic warning at 500K visitors | Early warning when workload exceeds Lambda tier |
| Session timeout | `--session-timeout 1800` | Guards against stale attribution across long gaps |
| Compressed input | Automatic by `.gz`/`.zst` extension | Reduces storage and transfer costs |
| Pre-sorting | `--sort` | Correctness guarantee for non-chronological input |
| Checkpointing | `--checkpoint-dir /tmp/ckpt` | Crash recovery for multi-hour runs |
| Batch + Fargate | CDK-deployed, submit via AWS CLI | No time limit for medium-large files |
| Glue (PySpark) | `code/glue/search_keyword_glue.py` | Partitioned processing for 100+ GB |

---

## AWS Deployment (CDK)

The primary AWS path is **Lambda** -- auto-triggered when a file is uploaded to the S3 bucket's `input/` prefix.

### Deploy

```bash
make deploy
```

This deploys:
- **S3 Bucket** -- `input/` for data files, `output/` for results
- **Lambda Function** -- Python 3.12, 512 MB, 15-min timeout, triggered by S3 upload
- **Monitoring** -- CloudWatch alarms, SNS notifications, dead letter queue

### Upload Data and Get Results

```bash
./scripts/run-search-keyword-aws.sh <bucket-name> data/data.sql
```

### Tear Down

```bash
make destroy
```

---

## Scaling to 10+ GB Files

The streaming engine processes rows one at a time. Memory scales with **unique visitor count**, not file size -- a 10 GB file with 1M unique visitors uses ~200 MB of visitor state. For files that exceed Lambda's limits, the repo includes a documented scaling path:

| Scale | Approach | Details |
|---|---|---|
| Small (< 2 GB) | **Lambda** | Primary path. Auto-triggered, no ops. |
| Medium (2-50 GB) | **Batch + Fargate** | Containerized CLI, no time limit, checkpointing. |
| Large (50+ GB) | **Glue (PySpark)** | Partitioned by visitor key, ordered by `hit_time_gmt` within each partition. |

The CDK stack deploys Lambda and Batch+Fargate. The Glue PySpark script is provided as a reference implementation.

---

## CI/CD

[![CI](https://github.com/jayanthgundagoni99/search-keyword-performance/actions/workflows/ci.yml/badge.svg)](https://github.com/jayanthgundagoni99/search-keyword-performance/actions/workflows/ci.yml)

GitHub Actions runs on every push and PR to `main`:

- **Python tests** across 3.10, 3.11, 3.12 (with pip caching)
- **CDK stack tests** (TypeScript assertions)
- **Docker build** verification

---

## Project Structure

```
code/
  search_keyword_performance/     # Python package (the core solution)
    __init__.py
    __main__.py                   # CLI: python -m search_keyword_performance <file>
    engine.py                     # SearchKeywordAttributor class + DataQualityMetrics
    parsers.py                    # Pure parsing functions
    config.py                     # Centralized EngineConfig (CLI, Lambda, Batch)
    exceptions.py                 # Structured error taxonomy
  search_keyword_handler.py       # Lambda entry point (idempotency, metadata)
  batch_handler.py                # Batch/Fargate entry point
  glue/
    search_keyword_glue.py        # PySpark Glue job
tests/
  test_parsers.py                 # Unit tests for parsing (38 tests)
  test_engine.py                  # Integration + golden data + production features (40 tests)
infra/
  lib/infra-stack.ts              # CDK stack: S3 + Lambda + Batch + Glue + monitoring
  test/infra.test.ts              # CDK stack assertions
scripts/
  deploy-search-keyword.sh        # CDK deploy wrapper
  run-search-keyword-aws.sh       # Upload data, wait for Lambda output
data/data.sql                     # Sample hit-level input (21 hits, 4 visitors)
output/                           # Expected output artifact
.github/workflows/ci.yml          # CI pipeline
Dockerfile                        # Container for Batch/Fargate
Makefile                          # Developer shortcuts
```

---

## Key Design Decisions

| Decision | Rationale |
|---|---|
| `Decimal` for revenue | Avoids IEEE-754 float drift on monetary values |
| Streaming `csv.DictReader` | Processes rows without loading entire file; memory scales with visitor cardinality |
| Exact event-code matching | `"1"` in set, not substring; prevents `10`/`11` false positives |
| Case-insensitive keyword aggregation | `"Ipod"` and `"ipod"` are the same keyword |
| `search.yahoo.com` displayed as `yahoo.com` | Standardized well-known search domains |
| Schema validation on by default | Fail fast with clear error on wrong delimiter or missing columns |
| Atomic output writes | Temp-then-rename prevents partially written files on crash |
| Structured error taxonomy | Categorized exceptions make alerts actionable |
| Centralized `EngineConfig` | Single source of truth for CLI, Lambda, and Batch modes |
| Idempotency via ETag manifest | Same input file uploaded twice does not produce duplicate processing |
| Session timeout disabled by default | Matches exercise requirements; available as opt-in production enhancement |
| CDK for infrastructure | Type-safe, composable, single `cdk deploy` command |

---

## Output Versioning

Output artifacts follow schema version `v1`:

| Artifact | Format | Naming |
|---|---|---|
| Results | Tab-delimited, 3 columns, revenue-descending | `YYYY-mm-dd_SearchKeywordPerformance.tab` |
| Metadata | JSON manifest (counts, config, quality metrics) | `YYYY-mm-dd_SearchKeywordPerformance_metadata.json` |

Schema changes increment the version in `config.py:OUTPUT_SCHEMA_VERSION`.

---

## Operational Runbook

### Rerunning failed jobs

**Lambda**: Re-upload the input file to `s3://<bucket>/input/`. Lambda auto-triggers on `s3:ObjectCreated:*`. The idempotency guard checks the ETag; if the file content differs, it will reprocess.

**Batch**: Resubmit with `aws batch submit-job`. Checkpointing resumes from last checkpoint if the checkpoint directory persists.

**Glue**: `aws glue start-job-run --job-name search-keyword-performance`.

### Inspecting the dead letter queue

```bash
aws sqs receive-message --queue-url <DLQ_URL> --max-number-of-messages 10
```

Messages land here after Lambda fails twice (configured retry count). Each message contains the original S3 event record.

### Restoring from checkpoint

Checkpoints are JSON files in `--checkpoint-dir`. The engine automatically restores on `process_file()` if a checkpoint exists. To force a fresh run, delete `skp_checkpoint.json` from the checkpoint directory.

### Common failure modes

| Symptom | Likely Cause | Fix |
|---|---|---|
| `InputSchemaError` | Wrong delimiter or missing columns | Verify input is tab-separated with expected headers |
| Lambda timeout (15 min) | File too large for Lambda | Use Batch/Fargate or Glue |
| Memory warning in logs | > 500K unique visitors in-memory | Switch to Batch (more RAM) or Glue (distributed) |
| DLQ messages | Lambda crashed or hit resource limits | Check CloudWatch logs, reprocess via re-upload |
| Duplicate output | File re-uploaded without content change | Idempotency guard skips; check `_manifests/` prefix |

### CloudWatch alarms

| Alarm | Trigger | Action |
|---|---|---|
| `search-keyword-lambda-errors` | Any Lambda error in 5-min window | Check CloudWatch logs |
| `search-keyword-dlq-messages` | Messages in DLQ | Inspect DLQ, fix root cause, re-upload |

---

## Backfill and Replay Strategy

To reprocess historical files safely:

1. **Same-day reruns** overwrite the output file (same `YYYY-mm-dd` filename). This is safe because the output is deterministic for a given input.
2. **Multi-day backfill**: Upload files with the `-o` flag to write date-specific outputs, or upload each file to the S3 `input/` prefix sequentially (Lambda processes them serially per invocation).
3. **Version-safe replay**: The idempotency manifest in `output/_manifests/` tracks which input ETag has been processed. To force reprocessing, delete the manifest or upload the file with new content.

---

## Contract Tests for AWS Paths

Full end-to-end cloud integration tests are documented but not automated in CI to avoid AWS costs:

```bash
# 1. Deploy the stack
cd infra && npx cdk deploy

# 2. Upload sample input
aws s3 cp data/data.sql s3://<bucket>/input/data.sql

# 3. Wait for Lambda to process (~10s)
sleep 15

# 4. Verify output exists and content matches
aws s3 cp s3://<bucket>/output/$(date +%Y-%m-%d)_SearchKeywordPerformance.tab -

# 5. Verify metadata manifest exists
aws s3 ls s3://<bucket>/output/$(date +%Y-%m-%d)_SearchKeywordPerformance_metadata.json

# 6. Verify Glue job runs
aws glue start-job-run --job-name search-keyword-performance
```

The CI pipeline validates Python tests, CDK synthesis, and Docker build. Cloud integration is verified manually before release.

---

## Secrets and Configuration Hygiene

This project does not use secrets directly, but in a production deployment:

- **Bucket names, SNS topic ARNs, and account IDs** are outputs of the CDK stack, not hardcoded. Environment-specific values come from CDK context or parameter store.
- **No secrets in code or env vars**. Notification email addresses would be managed via AWS SSM Parameter Store or Secrets Manager.
- **IAM roles** follow least-privilege: Lambda gets `s3:GetObject`/`s3:PutObject` on the data bucket only; Glue gets the same plus `AWSGlueServiceRole`.
- **Encryption**: S3 buckets use SSE-S3 encryption at rest. All bucket policies enforce `aws:SecureTransport` (TLS in transit).

---

## PII and Data Governance

The input data contains IP addresses and user-agent strings, which are considered personally identifiable or quasi-identifiable information:

| Control | Implementation |
|---|---|
| Least-privilege access | IAM roles scoped to specific S3 prefixes |
| Encryption at rest | S3 SSE-S3 on all buckets |
| Encryption in transit | `enforceSSL: true` on all S3 buckets |
| Retention policy | S3 lifecycle rules expire output after 90 days, non-current versions after 30 days |
| No PII in logs | Lambda handler logs bucket/key and metrics, not raw IP or user-agent values |
| Access logging | S3 server access logs stored in a separate access-logs bucket |

In a full production deployment, IP addresses would ideally be hashed or anonymized before processing, and the data bucket would be registered in a data catalog with classification tags.

---

## What I Would Add with More Time

1. Multi-touch attribution (proportional credit across all search touchpoints)
2. Session-based analysis (duration, page depth, conversion funnel per keyword)
3. Dashboard (QuickSight or Streamlit for interactive exploration)
4. Lake Formation data catalog registration with column-level access control
5. Step Functions orchestration for multi-file batch workflows
