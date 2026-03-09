# Search Keyword Performance Attribution Engine

[![CI](https://github.com/jayanthgundagoni99/search-keyword-performance/actions/workflows/ci.yml/badge.svg)](https://github.com/jayanthgundagoni99/search-keyword-performance/actions/workflows/ci.yml)

A Python application that answers the question: **"How much revenue is the client getting from external search engines (Google, Yahoo, MSN/Bing), and which keywords are performing the best based on revenue?"**

> **This submission's default execution path solves the exercise with a simple Python streaming application in AWS. Additional options such as session timeout, checkpointing, Batch/Fargate, and Glue are documented as production-oriented extensions for larger client workloads.**

---

## How to Review This Repo

| What to look at | Where |
|---|---|
| Core solution (one class, streaming, last-touch attribution) | `code/search_keyword_performance/engine.py` |
| Parsing logic (pure functions, no state) | `code/search_keyword_performance/parsers.py` |
| CLI entry point (single file argument) | `code/search_keyword_performance/__main__.py` |
| AWS deployment (CDK) | `infra/lib/infra-stack.ts` |
| Tests (unit + integration + golden data) | `tests/` |
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
| Case-insensitive keywords | `"Ipod"` and `"ipod"` combined for cleaner reporting |
| `search.yahoo.com` displayed as `yahoo.com` | Standardized display domain |
| Input is chronologically sorted | Sample data is sorted; `--sort` flag available as safety net |

### Production enhancements (beyond the exercise)

These are available in the codebase but **disabled by default**. The core solution works without any of them.

| Feature | How to Enable | Why It Exists |
|---|---|---|
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
    engine.py                     # SearchKeywordAttributor class
    parsers.py                    # Pure parsing functions
  search_keyword_handler.py       # Lambda entry point
  batch_handler.py                # Batch/Fargate entry point
  glue/
    search_keyword_glue.py        # PySpark Glue job (reference)
tests/
  test_parsers.py                 # Unit tests for parsing
  test_engine.py                  # Integration tests + golden data assertions
infra/
  lib/infra-stack.ts              # CDK stack: S3 + Lambda + ECR + Batch/Fargate
  test/infra.test.ts              # CDK stack assertions
scripts/
  deploy-search-keyword.sh        # CDK deploy wrapper
  run-search-keyword-aws.sh       # Upload data, wait for output
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
| Session timeout disabled by default | Matches exercise requirements; available as opt-in production enhancement |
| CDK for infrastructure | Type-safe, composable, single `cdk deploy` command |

---

## What I Would Add with More Time

1. Multi-touch attribution (proportional credit across all search touchpoints)
2. Session-based analysis (duration, page depth, conversion funnel per keyword)
3. Automated data quality checks (column validation, anomaly detection)
4. Dashboard (QuickSight or Streamlit for interactive exploration)
