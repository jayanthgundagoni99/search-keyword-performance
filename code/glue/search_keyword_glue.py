"""AWS Glue PySpark job for Search Keyword Performance Attribution.

Implements the same last-touch attribution logic as the Python CLI, but
uses Spark to partition by visitor key and process partitions in parallel.

Usage (Glue job parameters):
    --input_path   s3://bucket/input/data.tsv
    --output_path  s3://bucket/output/
    --session_timeout  1800   (seconds, 0 to disable)

Architecture:
    1. Read the tab-separated input file as a Spark DataFrame.
    2. Partition by (ip, user_agent) so each visitor's hits are co-located.
    3. Within each partition, sort by hit_time_gmt and apply last-touch
       attribution with session timeout.
    4. Aggregate revenue by (search_engine_domain, keyword).
    5. Write a single tab-separated output file sorted by revenue desc.

Ordering guarantee:
    Last-touch attribution requires chronological processing per visitor.
    The partitionBy(ip, user_agent).orderBy(hit_time_gmt) window ensures
    each visitor's hits are processed in time order regardless of the
    physical layout of the source data across Spark partitions.
"""

import sys
from decimal import Decimal, InvalidOperation
from urllib.parse import parse_qs, urlparse

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import (
    ArrayType,
    DecimalType,
    StringType,
    StructField,
    StructType,
)

SEARCH_ENGINES = [
    ("google.com", "q", "google.com"),
    ("bing.com", "q", "bing.com"),
    ("search.yahoo.com", "p", "yahoo.com"),
    ("yahoo.com", "p", "yahoo.com"),
    ("msn.com", "q", "msn.com"),
]


# ------------------------------------------------------------------
# UDFs
# ------------------------------------------------------------------

def _extract_search_referrer(referrer: str):
    """Return (domain, keyword) or None."""
    if not referrer:
        return None
    try:
        parsed = urlparse(referrer)
        hostname = (parsed.hostname or "").lower().removeprefix("www.")
    except Exception:
        return None
    for engine_host, param, display_domain in SEARCH_ENGINES:
        if hostname == engine_host or hostname.endswith("." + engine_host):
            keywords = parse_qs(parsed.query).get(param, [])
            if keywords and keywords[0].strip():
                return (display_domain, keywords[0].strip())
            return None
    return None


def _parse_product_list_revenue(product_list: str) -> Decimal:
    if not product_list:
        return Decimal("0")
    total = Decimal("0")
    for product in product_list.split(","):
        fields = product.split(";")
        if len(fields) >= 4 and fields[3].strip():
            try:
                total += Decimal(fields[3].strip())
            except InvalidOperation:
                continue
    return total


def _is_purchase(event_list: str) -> bool:
    if not event_list:
        return False
    return "1" in {e.strip() for e in event_list.split(",")}


def main():
    args = getResolvedOptions(
        sys.argv, ["JOB_NAME", "input_path", "output_path", "session_timeout"]
    )
    input_path = args["input_path"]
    output_path = args["output_path"]
    session_timeout = int(args.get("session_timeout", "1800"))

    sc = SparkContext()
    glue_context = GlueContext(sc)
    spark = glue_context.spark_session
    job = Job(glue_context)
    job.init(args["JOB_NAME"], args)

    df = spark.read.csv(input_path, sep="\t", header=True, inferSchema=False)

    # Register UDFs
    search_schema = StructType([
        StructField("domain", StringType(), True),
        StructField("keyword", StringType(), True),
    ])

    udf_extract = F.udf(_extract_search_referrer, search_schema)
    udf_revenue = F.udf(_parse_product_list_revenue, DecimalType(18, 2))
    udf_purchase = F.udf(_is_purchase, "boolean")

    df = df.withColumn("hit_time", F.col("hit_time_gmt").cast("long"))
    df = df.withColumn("search_info", udf_extract(F.col("referrer")))
    df = df.withColumn("is_purchase", udf_purchase(F.col("event_list")))
    df = df.withColumn("revenue", udf_revenue(F.col("product_list")))

    # Within each visitor partition, sort by time and find last search referrer
    visitor_window = (
        Window.partitionBy("ip", "user_agent")
        .orderBy("hit_time")
        .rowsBetween(Window.unboundedPreceding, Window.currentRow)
    )

    df = df.withColumn("search_domain", F.col("search_info.domain"))
    df = df.withColumn("search_keyword", F.col("search_info.keyword"))

    # Forward-fill the last non-null search referrer within each visitor
    df = df.withColumn(
        "last_search_domain",
        F.last(F.col("search_domain"), ignorenulls=True).over(visitor_window),
    )
    df = df.withColumn(
        "last_search_keyword",
        F.last(F.col("search_keyword"), ignorenulls=True).over(visitor_window),
    )

    # Session timeout: check gap from previous hit
    if session_timeout > 0:
        df = df.withColumn(
            "prev_hit_time",
            F.lag("hit_time", 1).over(
                Window.partitionBy("ip", "user_agent").orderBy("hit_time")
            ),
        )
        df = df.withColumn(
            "session_gap",
            F.when(
                F.col("prev_hit_time").isNotNull(),
                F.col("hit_time") - F.col("prev_hit_time"),
            ).otherwise(F.lit(0)),
        )
        # Mark session boundaries
        df = df.withColumn(
            "new_session",
            F.when(F.col("session_gap") > session_timeout, F.lit(1)).otherwise(
                F.lit(0)
            ),
        )
        session_window = (
            Window.partitionBy("ip", "user_agent")
            .orderBy("hit_time")
            .rowsBetween(Window.unboundedPreceding, Window.currentRow)
        )
        df = df.withColumn("session_id", F.sum("new_session").over(session_window))

        # Re-compute last search referrer within each session
        session_visitor_window = (
            Window.partitionBy("ip", "user_agent", "session_id")
            .orderBy("hit_time")
            .rowsBetween(Window.unboundedPreceding, Window.currentRow)
        )
        df = df.withColumn(
            "last_search_domain",
            F.last(F.col("search_domain"), ignorenulls=True).over(
                session_visitor_window
            ),
        )
        df = df.withColumn(
            "last_search_keyword",
            F.last(F.col("search_keyword"), ignorenulls=True).over(
                session_visitor_window
            ),
        )

    # Filter to purchase hits with attributed search referrer
    purchases = df.filter(
        (F.col("is_purchase") == True)
        & (F.col("revenue") > 0)
        & (F.col("last_search_domain").isNotNull())
    )

    # Aggregate by domain + keyword (case-insensitive)
    purchases = purchases.withColumn(
        "keyword_lower", F.lower(F.col("last_search_keyword"))
    )

    agg = purchases.groupBy("last_search_domain", "keyword_lower").agg(
        F.sum("revenue").alias("total_revenue"),
        F.first("last_search_keyword").alias("display_keyword"),
    )

    result = agg.select(
        F.col("last_search_domain").alias("Search Engine Domain"),
        F.col("display_keyword").alias("Search Keyword"),
        F.col("total_revenue").alias("Revenue"),
    ).orderBy(F.col("Revenue").desc())

    result.coalesce(1).write.csv(
        output_path, sep="\t", header=True, mode="overwrite"
    )

    job.commit()


if __name__ == "__main__":
    main()
