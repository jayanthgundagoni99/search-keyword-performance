# Search Keyword Performance -- Business Problem Walkthrough

*Prepared for the 60-minute code review session.*

---

## 1. The Business Question

> **How much revenue is the client getting from external search engines (Google, Yahoo, MSN/Bing), and which keywords are performing the best based on revenue?**

The client's website (esshopzilla.com) receives traffic from multiple sources. They want to understand which external search engine keywords are actually driving purchases -- not just visits, but **revenue**.

---

## 2. Why This Is Harder Than It Looks

### The Naive Approach (and Why It Fails)

A first instinct might be: "Find rows where a purchase happened, read the referrer, and sum the revenue."

This produces **zero results from search engines**. Here is why:

When a visitor from Google actually completes a purchase, their hit looks like this:

```
event_list: 1  (Purchase)
product_list: Electronics;Ipod - Touch - 32GB;1;290;
referrer: https://www.esshopzilla.com/checkout/?a=confirm   <- INTERNAL
```

The referrer on the purchase hit is the **checkout confirmation page**, not Google. The Google referrer appeared much earlier, when the visitor first arrived at the site:

```
referrer: http://www.google.com/search?...&q=Ipod   <- THE REAL SOURCE
```

### The Real Problem: Attribution Across a Visitor Journey

The visitor's journey looks like this:

```
Hit 1:  Google search -> lands on Home page
Hit 2:  browses -> Search Results page
Hit 3:  browses -> Product page (Ipod Nano)
Hit 4:  browses -> Search Results again
Hit 5:  browses -> Product page (Ipod Touch)
Hit 6:  adds to cart
Hit 7:  checkout
Hit 8:  order confirmation
Hit 9:  PURCHASE  revenue = $290  (referrer = internal checkout)
```

The $290 must be **attributed back** to the Google search in Hit 1 -- that is the external search engine that brought this visitor to the site.

---

## 3. Assumptions and Trade-offs

Before describing the approach, here are the assumptions I made -- what the PDF specifies, what it leaves open, and the judgment calls I made.

### Supported by the PDF (defend confidently)

| Assumption | Source |
|---|---|
| Revenue only counts when `event_list` contains purchase event `1` | Appendix B: "Revenue is only actualized when the purchase event is set in the events_list" |
| Revenue is at semicolon-index 3 in `product_list` | Appendix B format: `Category;Product Name;Number of Items;Total Revenue;...` |
| Exact event-code matching (`"1"` in a set, not substring) | Event codes 10, 11, 12 are not purchases -- Appendix A lists them separately |
| Bing supported alongside MSN | PDF says "Google, Yahoo and MSN" but the sample data uses `bing.com`. Supporting both covers the prompt and the data. |

### Reasonable but not specified (call out explicitly)

| Assumption | Rationale | What I would confirm with the client |
|---|---|---|
| **Last-touch attribution** | The PDF asks "how much revenue from search engines" but never specifies an attribution model. I chose last-touch because purchase hits have internal referrers (checkout page), so we must look backwards. Last-touch is simple, common, and explainable. | Whether they want first-touch, last-touch, or proportional multi-touch. |
| **Visitor identity = `(ip, user_agent)`** | The dataset has no visitor or session ID (Appendix A). This composite key is a practical engineering approximation, not true user identity. Shared IPs (offices/NATs) merge visitors; different browsers split them. | Whether a user ID or cookie-based session ID is available in the full dataset. |
| **Case-insensitive keyword aggregation** | `"Ipod"` and `"ipod"` are combined to produce $480. This is a reporting normalization decision -- business-friendly, but the PDF does not mandate it. | Whether the client prefers case-preserved or normalized keyword grouping. |
| **`search.yahoo.com` displayed as `yahoo.com`** | The PDF's output example shows `google.com` style, not subdomain-qualified. I standardized well-known search domains for cleaner reporting. Internally the raw hostname is parsed; normalization happens at display. | Whether they want the raw hostname or a standardized display domain. |
| **Input is chronologically sorted** | The sample data is in ascending `hit_time_gmt` order, but the PDF never guarantees this. The engine streams in file order by default. A `--sort` flag is available if the source system cannot guarantee order. At Glue scale, Spark handles ordering via window functions. | Whether upstream ETL guarantees chronological ordering. |

### Production enhancement (not in the PDF)

| Feature | Why it exists | How it is scoped |
|---|---|---|
| **Session timeout** | In real analytics, a visitor's morning search should not be credited for an afternoon purchase. Configurable session expiration guards against stale attribution. | **Disabled by default** to preserve pure file-order last-touch behavior matching the exercise. Enable with `--session-timeout 1800`. |

---

## 4. Approach

### Attribution Model: Last-Touch

The application uses **last-touch attribution**: for each visitor, it tracks the most recent external search engine referrer they arrived from. When a purchase occurs later in their session, revenue is credited to that search source.

"Last-touch" means that if a visitor arrives from Google, leaves, comes back from Bing, and then purchases -- **Bing gets the credit** (most recent touch). I chose this model because it is simple, common, and the most defensible for this exercise. With a real client, I would confirm the desired attribution methodology.

### Visitor Identity

The dataset has no explicit visitor or session ID. A visitor is defined as a unique `(IP address, User Agent)` pair -- a standard approximation for analytics data when no better identifier is available. This is a pragmatic fallback, not true user identity.

### Streaming Architecture

The application reads the TSV file row by row (streaming), maintaining two core data structures in memory:

1. **Visitor state**: maps each `(ip, user_agent)` to their most recent `(search_domain, keyword)`
2. **Revenue aggregation**: maps each `(domain, keyword)` pair to its accumulated revenue

Memory usage scales with the number of **unique visitors**, not with the file size.

---

## 5. Results from the Sample Dataset

### Input Summary

- 22 hits from 4 unique visitors
- 3 visitors arrived from external search engines:
  - `67.98.123.1` from Google (`q=Ipod`)
  - `23.8.61.21` from Bing (`q=Zune`)
  - `44.12.96.2` from Google (`q=ipod`)
- 1 visitor arrived from Yahoo (`112.33.98.231`, `p=cd player`) but made no purchase

### Output

| Search Engine Domain | Search Keyword | Revenue |
|---|---|---|
| google.com | Ipod | 480.00 |
| bing.com | Zune | 250.00 |

### Revenue Attribution Detail

**google.com / Ipod -- $480.00**

| Visitor | Arrived via | Purchased | Revenue |
|---|---|---|---|
| `67.98.123.1` | `google.com?q=Ipod` | Ipod - Touch - 32GB | $290.00 |
| `44.12.96.2` | `google.com?q=ipod` | Ipod - Nano - 8GB | $190.00 |
| | | **Total** | **$480.00** |

Note: `"Ipod"` and `"ipod"` are aggregated together (case-insensitive). Display form uses the first occurrence: `"Ipod"`.

**bing.com / Zune -- $250.00**

| Visitor | Arrived via | Purchased | Revenue |
|---|---|---|---|
| `23.8.61.21` | `bing.com?q=Zune` | Zune - 32GB | $250.00 |

**yahoo.com / cd player -- no revenue**

Visitor `112.33.98.231` arrived from Yahoo searching for "cd player" but never made a purchase.

---

## 6. How to Run

### Locally

```bash
cd code && python -m search_keyword_performance ../data/data.sql
# Or using the Makefile from repo root:
make run
```

### Production CLI Flags

```bash
# Pre-sort non-chronological input
python -m search_keyword_performance --sort data.tsv

# Custom session timeout (seconds)
python -m search_keyword_performance --session-timeout 3600 data.tsv

# Enable checkpointing for crash recovery
python -m search_keyword_performance --checkpoint-dir /tmp/ckpt data.tsv

# Compressed input (detected automatically)
python -m search_keyword_performance data.tsv.gz
```

### In AWS (CDK)

The CDK stack deploys a two-tier architecture:

```bash
# Deploy S3 + Lambda + ECR + Batch/Fargate
make deploy

# Small files -- upload to S3 and Lambda processes automatically
./scripts/run-search-keyword-aws.sh <bucket-name> data/data.sql

# Large files -- submit a Batch job
aws batch submit-job \
  --job-name skp-run \
  --job-queue search-keyword-performance \
  --job-definition search-keyword-performance \
  --container-overrides '{
    "environment": [
      {"name": "INPUT_BUCKET", "value": "<bucket-name>"},
      {"name": "INPUT_KEY", "value": "input/large_data.tsv.gz"}
    ]
  }'

# Tear down
make destroy
```

---

## 7. Key Design Decisions

| Decision | Rationale |
|---|---|
| `Decimal` for revenue | Avoids IEEE-754 float drift on monetary values |
| Streaming `csv.DictReader` | Processes rows without loading entire file; memory scales with visitor cardinality |
| Exact event-code matching | `"1"` in set, not substring; prevents `10`/`11` false positives |
| Case-insensitive keyword aggregation | `"Ipod"` and `"ipod"` are the same keyword |
| `search.yahoo.com` displayed as `yahoo.com` | Standardized well-known search domains for cleaner reporting |
| Session timeout (optional) | Production guard against stale attribution; disabled by default |
| Checkpointing every 100K rows | Crash recovery for multi-hour processing runs |
| Compressed input (.gz, .zst) | Reduces storage/transfer costs for large datasets |
| Pre-sorting option | Correctness guarantee when input is not chronological |

---

## 8. Scaling to 10+ GB Files

### Current Design (Submitted)

The streaming engine processes rows one at a time. Memory is O(unique visitors), not O(file size).

| Aspect | Behavior |
|---|---|
| File I/O | Streaming, one row at a time |
| Memory | O(unique visitors + keyword pairs) |
| CPU | Single-threaded, O(N) |

For a 10 GB file (~100M rows), estimated processing time is 15-45 minutes. Memory ~1 GB for visitor state.

### AWS Deployment Path

The **primary delivery path** is Lambda -- auto-triggered by S3 upload, no ops.

For files that exceed Lambda's 15-minute timeout or 10 GB `/tmp` limit, the repo includes a **documented scaling path**:

| Scale | Service | Details |
|---|---|---|
| Small (< 2 GB) | **Lambda** | Primary path. Auto-triggered by S3 upload. |
| Medium (2-50 GB) | **Batch + Fargate** | Containerized CLI, no time limit, checkpointing. |
| Large (50+ GB) | **Glue (PySpark)** | Partitioned by visitor key, ordered by `hit_time_gmt` within each partition, autoscale. |

The CDK stack deploys Lambda and Batch+Fargate. The Glue PySpark script (`code/glue/search_keyword_glue.py`) is provided as a reference implementation.

### Production Extensions (Beyond the Exercise)

These are available in the codebase but are not required for the exercise:

1. **Session timeout** -- configurable inactivity gap, disabled by default
2. **Checkpointing** -- crash recovery for multi-hour runs
3. **Pre-sorting** -- correctness for non-chronological input
4. **Compressed input** -- `.gz` and `.zst` transparency
5. **Dockerfile** -- containerized execution on Batch/Fargate
6. **PySpark Glue job** -- horizontal scaling via Spark partitioning

---

## 9. Code Organization

```
code/search_keyword_performance/
  parsers.py       3 pure functions (no state, no I/O, easy to unit-test)
  engine.py        SearchKeywordAttributor class (streaming, session timeout,
                     compressed input, pre-sorting, checkpointing)
  __main__.py      CLI with argparse (--sort, --session-timeout, --checkpoint-dir)

code/
  search_keyword_handler.py   Lambda entry point (S3 event -> process -> S3 output)
  batch_handler.py            Batch/Fargate entry point (env vars -> S3 I/O)
  glue/
    search_keyword_glue.py    PySpark Glue job (partitioned attribution)

tests/
  test_parsers.py    13 unit tests for parsing edge cases
  test_engine.py     20+ tests: core attribution, session timeout, compressed input,
                       pre-sorting, checkpointing, golden data

infra/
  lib/infra-stack.ts   CDK stack: S3 + Lambda + ECR + Batch/Fargate
  test/infra.test.ts   CDK stack assertions (S3, Lambda, ECR, Batch)
```

---

## 10. What I Would Add with More Time

1. Multi-touch attribution (proportional credit across all search touchpoints)
2. Session-based analysis (duration, page depth, conversion funnel per keyword)
3. Automated data quality checks (column validation, anomaly detection)
4. Dashboard (QuickSight or Streamlit for interactive exploration)
