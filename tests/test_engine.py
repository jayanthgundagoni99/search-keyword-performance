"""Integration tests for SearchKeywordAttributor including production features."""

import csv
import gzip
import json
import os
import tempfile
from decimal import Decimal

import pytest

from search_keyword_performance.engine import SearchKeywordAttributor, open_input
from search_keyword_performance.config import EngineConfig
from search_keyword_performance.exceptions import (
    InputSchemaError,
    CheckpointRestoreError,
    OutputWriteError,
)

SAMPLE_DATA = os.path.join(os.path.dirname(__file__), "..", "data", "data.sql")
REFERENCE_ARTIFACT = "2026-03-05_SearchKeywordPerformance.tab"

TSV_FIELDS = [
    "hit_time_gmt", "date_time", "user_agent", "ip", "event_list",
    "geo_city", "geo_region", "geo_country", "pagename", "page_url",
    "product_list", "referrer",
]


def _make_tsv(rows: list[dict[str, str]], tmpdir: str, filename: str = "test_input.tsv") -> str:
    """Write *rows* to a temp TSV file and return the path."""
    path = os.path.join(tmpdir, filename)
    with open(path, "w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=TSV_FIELDS, delimiter="\t")
        writer.writeheader()
        for row in rows:
            full = {k: "" for k in TSV_FIELDS}
            full.update(row)
            writer.writerow(full)
    return path


def _make_gzip_tsv(rows: list[dict[str, str]], tmpdir: str) -> str:
    """Write *rows* to a gzipped TSV file and return the path."""
    path = os.path.join(tmpdir, "test_input.tsv.gz")
    with gzip.open(path, "wt", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=TSV_FIELDS, delimiter="\t")
        writer.writeheader()
        for row in rows:
            full = {k: "" for k in TSV_FIELDS}
            full.update(row)
            writer.writerow(full)
    return path


# ===================================================================
# Core attribution tests
# ===================================================================


class TestSearchKeywordAttributor:
    def test_single_visitor_purchase(self):
        rows = [
            {"hit_time_gmt": "100", "ip": "1.2.3.4", "user_agent": "UA1",
             "referrer": "http://www.google.com/search?q=shoes"},
            {"hit_time_gmt": "200", "ip": "1.2.3.4", "user_agent": "UA1",
             "event_list": "1", "product_list": "Shoes;Red Shoes;1;49.99;"},
        ]
        with tempfile.TemporaryDirectory() as tmpdir:
            path = _make_tsv(rows, tmpdir)
            attr = SearchKeywordAttributor(session_timeout=None)
            attr.process_file(path)
            results = attr.get_results()

        assert len(results) == 1
        assert results[0] == ("google.com", "shoes", Decimal("49.99"))

    def test_revenue_ignored_without_purchase_event(self):
        """Revenue in product_list is not counted unless event_list contains 1."""
        rows = [
            {"hit_time_gmt": "100", "ip": "1.2.3.4", "user_agent": "UA1",
             "referrer": "http://www.google.com/search?q=shoes",
             "event_list": "2", "product_list": "Shoes;Red Shoes;1;49.99;"},
        ]
        with tempfile.TemporaryDirectory() as tmpdir:
            path = _make_tsv(rows, tmpdir)
            attr = SearchKeywordAttributor(session_timeout=None)
            attr.process_file(path)

        assert attr.get_results() == []

    def test_purchase_without_prior_search_not_attributed(self):
        rows = [
            {"hit_time_gmt": "100", "ip": "1.2.3.4", "user_agent": "UA1",
             "referrer": "http://www.esshopzilla.com/"},
            {"hit_time_gmt": "200", "ip": "1.2.3.4", "user_agent": "UA1",
             "event_list": "1", "product_list": "Shoes;Red Shoes;1;49.99;"},
        ]
        with tempfile.TemporaryDirectory() as tmpdir:
            path = _make_tsv(rows, tmpdir)
            attr = SearchKeywordAttributor(session_timeout=None)
            attr.process_file(path)

        assert attr.get_results() == []

    def test_case_insensitive_aggregation_preserves_first_seen_casing(self):
        """Keywords are grouped case-insensitively; display form is the
        first-seen casing (from search referrer, not purchase time)."""
        rows = [
            {"hit_time_gmt": "100", "ip": "1.1.1.1", "user_agent": "UA1",
             "referrer": "http://www.google.com/search?q=Ipod"},
            {"hit_time_gmt": "200", "ip": "1.1.1.1", "user_agent": "UA1",
             "event_list": "1", "product_list": "E;Ipod;1;100;"},
            {"hit_time_gmt": "300", "ip": "2.2.2.2", "user_agent": "UA2",
             "referrer": "http://www.google.com/search?q=ipod"},
            {"hit_time_gmt": "400", "ip": "2.2.2.2", "user_agent": "UA2",
             "event_list": "1", "product_list": "E;Ipod;1;50;"},
        ]
        with tempfile.TemporaryDirectory() as tmpdir:
            path = _make_tsv(rows, tmpdir)
            attr = SearchKeywordAttributor(session_timeout=None)
            attr.process_file(path)
            results = attr.get_results()

        assert len(results) == 1
        assert results[0] == ("google.com", "Ipod", Decimal("150.00"))

    def test_multiple_engines_sorted_by_revenue(self):
        rows = [
            {"hit_time_gmt": "100", "ip": "1.1.1.1", "user_agent": "UA1",
             "referrer": "http://www.bing.com/search?q=tablet"},
            {"hit_time_gmt": "200", "ip": "1.1.1.1", "user_agent": "UA1",
             "event_list": "1", "product_list": "E;Tablet;1;500;"},
            {"hit_time_gmt": "100", "ip": "2.2.2.2", "user_agent": "UA2",
             "referrer": "http://www.google.com/search?q=laptop"},
            {"hit_time_gmt": "200", "ip": "2.2.2.2", "user_agent": "UA2",
             "event_list": "1", "product_list": "E;Laptop;1;200;"},
        ]
        with tempfile.TemporaryDirectory() as tmpdir:
            path = _make_tsv(rows, tmpdir)
            attr = SearchKeywordAttributor(session_timeout=None)
            attr.process_file(path)
            results = attr.get_results()

        assert results[0] == ("bing.com", "tablet", Decimal("500.00"))
        assert results[1] == ("google.com", "laptop", Decimal("200.00"))

    def test_last_touch_attribution(self):
        """Revenue goes to Bing (last-touch), not Google (first-touch)."""
        rows = [
            {"hit_time_gmt": "100", "ip": "1.1.1.1", "user_agent": "UA1",
             "referrer": "http://www.google.com/search?q=phone"},
            {"hit_time_gmt": "200", "ip": "1.1.1.1", "user_agent": "UA1",
             "referrer": "http://www.bing.com/search?q=smartphone"},
            {"hit_time_gmt": "300", "ip": "1.1.1.1", "user_agent": "UA1",
             "event_list": "1", "product_list": "E;Phone;1;300;"},
        ]
        with tempfile.TemporaryDirectory() as tmpdir:
            path = _make_tsv(rows, tmpdir)
            attr = SearchKeywordAttributor(session_timeout=None)
            attr.process_file(path)
            results = attr.get_results()

        assert len(results) == 1
        assert results[0] == ("bing.com", "smartphone", Decimal("300.00"))

    def test_repeated_purchases_accumulate_under_same_keyword(self):
        """Two purchases after one search both attribute to the same keyword."""
        rows = [
            {"hit_time_gmt": "100", "ip": "1.1.1.1", "user_agent": "UA1",
             "referrer": "http://www.google.com/search?q=shoes"},
            {"hit_time_gmt": "200", "ip": "1.1.1.1", "user_agent": "UA1",
             "event_list": "1", "product_list": "S;Sneakers;1;80;"},
            {"hit_time_gmt": "300", "ip": "1.1.1.1", "user_agent": "UA1",
             "event_list": "1", "product_list": "S;Boots;1;120;"},
        ]
        with tempfile.TemporaryDirectory() as tmpdir:
            path = _make_tsv(rows, tmpdir)
            attr = SearchKeywordAttributor(session_timeout=None)
            attr.process_file(path)
            results = attr.get_results()

        assert len(results) == 1
        assert results[0] == ("google.com", "shoes", Decimal("200.00"))

    def test_hits_processed_counts_all_rows(self):
        """The public hits_processed property reflects every row ingested."""
        rows = [
            {"hit_time_gmt": "100", "ip": "1.1.1.1", "user_agent": "UA1",
             "referrer": "http://www.google.com/search?q=x"},
            {"hit_time_gmt": "200", "ip": "1.1.1.1", "user_agent": "UA1"},
            {"hit_time_gmt": "300", "ip": "1.1.1.1", "user_agent": "UA1",
             "event_list": "1", "product_list": "E;X;1;10;"},
        ]
        with tempfile.TemporaryDirectory() as tmpdir:
            path = _make_tsv(rows, tmpdir)
            attr = SearchKeywordAttributor(session_timeout=None)
            attr.process_file(path)

        assert attr.hits_processed == 3

    def test_internal_referrer_does_not_clear_attribution(self):
        """Browsing internal pages between search and purchase preserves attribution."""
        rows = [
            {"hit_time_gmt": "100", "ip": "1.1.1.1", "user_agent": "UA1",
             "referrer": "http://www.google.com/search?q=camera"},
            {"hit_time_gmt": "200", "ip": "1.1.1.1", "user_agent": "UA1",
             "referrer": "http://www.esshopzilla.com/products/"},
            {"hit_time_gmt": "300", "ip": "1.1.1.1", "user_agent": "UA1",
             "referrer": "http://www.esshopzilla.com/cart/"},
            {"hit_time_gmt": "400", "ip": "1.1.1.1", "user_agent": "UA1",
             "event_list": "1", "product_list": "E;Camera;1;450;",
             "referrer": "https://www.esshopzilla.com/checkout/"},
        ]
        with tempfile.TemporaryDirectory() as tmpdir:
            path = _make_tsv(rows, tmpdir)
            attr = SearchKeywordAttributor(session_timeout=None)
            attr.process_file(path)
            results = attr.get_results()

        assert len(results) == 1
        assert results[0] == ("google.com", "camera", Decimal("450.00"))

    def test_write_output_creates_valid_tab_file(self):
        rows = [
            {"hit_time_gmt": "100", "ip": "1.1.1.1", "user_agent": "UA1",
             "referrer": "http://www.google.com/search?q=phone"},
            {"hit_time_gmt": "200", "ip": "1.1.1.1", "user_agent": "UA1",
             "event_list": "1", "product_list": "E;Phone;1;99.95;"},
        ]
        with tempfile.TemporaryDirectory() as tmpdir:
            input_path = _make_tsv(rows, tmpdir)
            output_path = os.path.join(tmpdir, "output.tab")
            attr = SearchKeywordAttributor(session_timeout=None)
            attr.process_file(input_path)
            attr.write_output(output_path)

            with open(output_path, "r", encoding="utf-8") as fh:
                reader = csv.reader(fh, delimiter="\t")
                header = next(reader)
                data_rows = list(reader)

        assert header == ["Search Engine Domain", "Search Keyword", "Revenue"]
        assert len(data_rows) == 1
        assert data_rows[0] == ["google.com", "phone", "99.95"]


# ===================================================================
# Session timeout tests
# ===================================================================


class TestSessionTimeout:
    def test_session_timeout_resets_referrer(self):
        """A 30-minute gap clears the tracked search referrer."""
        rows = [
            {"hit_time_gmt": "1000", "ip": "1.1.1.1", "user_agent": "UA1",
             "referrer": "http://www.google.com/search?q=shoes"},
            {"hit_time_gmt": "8200", "ip": "1.1.1.1", "user_agent": "UA1",
             "event_list": "1", "product_list": "S;Shoes;1;100;"},
        ]
        with tempfile.TemporaryDirectory() as tmpdir:
            path = _make_tsv(rows, tmpdir)
            attr = SearchKeywordAttributor(session_timeout=1800)
            attr.process_file(path)

        assert attr.get_results() == []

    def test_within_session_window_attributes_correctly(self):
        """Hits within 30 minutes keep the attribution."""
        rows = [
            {"hit_time_gmt": "1000", "ip": "1.1.1.1", "user_agent": "UA1",
             "referrer": "http://www.google.com/search?q=shoes"},
            {"hit_time_gmt": "1600", "ip": "1.1.1.1", "user_agent": "UA1",
             "event_list": "1", "product_list": "S;Shoes;1;100;"},
        ]
        with tempfile.TemporaryDirectory() as tmpdir:
            path = _make_tsv(rows, tmpdir)
            attr = SearchKeywordAttributor(session_timeout=1800)
            attr.process_file(path)
            results = attr.get_results()

        assert len(results) == 1
        assert results[0] == ("google.com", "shoes", Decimal("100.00"))

    def test_new_search_after_session_timeout(self):
        """After session expires, a new search referrer takes over."""
        rows = [
            {"hit_time_gmt": "1000", "ip": "1.1.1.1", "user_agent": "UA1",
             "referrer": "http://www.google.com/search?q=old"},
            {"hit_time_gmt": "8200", "ip": "1.1.1.1", "user_agent": "UA1",
             "referrer": "http://www.bing.com/search?q=new"},
            {"hit_time_gmt": "8300", "ip": "1.1.1.1", "user_agent": "UA1",
             "event_list": "1", "product_list": "E;Item;1;50;"},
        ]
        with tempfile.TemporaryDirectory() as tmpdir:
            path = _make_tsv(rows, tmpdir)
            attr = SearchKeywordAttributor(session_timeout=1800)
            attr.process_file(path)
            results = attr.get_results()

        assert len(results) == 1
        assert results[0] == ("bing.com", "new", Decimal("50.00"))

    def test_disabled_session_timeout(self):
        """With session_timeout=None, the old behaviour applies (no expiry)."""
        rows = [
            {"hit_time_gmt": "1000", "ip": "1.1.1.1", "user_agent": "UA1",
             "referrer": "http://www.google.com/search?q=shoes"},
            {"hit_time_gmt": "999999", "ip": "1.1.1.1", "user_agent": "UA1",
             "event_list": "1", "product_list": "S;Shoes;1;100;"},
        ]
        with tempfile.TemporaryDirectory() as tmpdir:
            path = _make_tsv(rows, tmpdir)
            attr = SearchKeywordAttributor(session_timeout=None)
            attr.process_file(path)
            results = attr.get_results()

        assert len(results) == 1
        assert results[0] == ("google.com", "shoes", Decimal("100.00"))


# ===================================================================
# Compressed input tests
# ===================================================================


class TestCompressedInput:
    def test_gzip_input(self):
        rows = [
            {"hit_time_gmt": "100", "ip": "1.1.1.1", "user_agent": "UA1",
             "referrer": "http://www.google.com/search?q=laptop"},
            {"hit_time_gmt": "200", "ip": "1.1.1.1", "user_agent": "UA1",
             "event_list": "1", "product_list": "E;Laptop;1;999;"},
        ]
        with tempfile.TemporaryDirectory() as tmpdir:
            path = _make_gzip_tsv(rows, tmpdir)
            attr = SearchKeywordAttributor(session_timeout=None)
            attr.process_file(path)
            results = attr.get_results()

        assert len(results) == 1
        assert results[0] == ("google.com", "laptop", Decimal("999.00"))

    def test_open_input_reads_plain_tsv(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            path = os.path.join(tmpdir, "test.tsv")
            with open(path, "w") as f:
                f.write("hello\n")
            fh = open_input(path)
            assert fh.read() == "hello\n"
            fh.close()

    def test_open_input_reads_gzip_tsv(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            path = os.path.join(tmpdir, "test.tsv.gz")
            with gzip.open(path, "wt") as f:
                f.write("hello\n")
            fh = open_input(path)
            assert fh.read() == "hello\n"
            fh.close()


# ===================================================================
# Pre-sorting tests
# ===================================================================


class TestPreSorting:
    def test_unsorted_input_with_sort_flag(self):
        """Out-of-order hits are correctly processed when sort_by_time=True."""
        rows = [
            {"hit_time_gmt": "200", "ip": "1.1.1.1", "user_agent": "UA1",
             "event_list": "1", "product_list": "E;Phone;1;300;"},
            {"hit_time_gmt": "100", "ip": "1.1.1.1", "user_agent": "UA1",
             "referrer": "http://www.google.com/search?q=phone"},
        ]
        with tempfile.TemporaryDirectory() as tmpdir:
            path = _make_tsv(rows, tmpdir)
            attr = SearchKeywordAttributor(session_timeout=None)
            attr.process_file(path, sort_by_time=True)
            results = attr.get_results()

        assert len(results) == 1
        assert results[0] == ("google.com", "phone", Decimal("300.00"))

    def test_unsorted_input_without_sort_flag_misses_attribution(self):
        """Without sorting, out-of-order data yields no attribution."""
        rows = [
            {"hit_time_gmt": "200", "ip": "1.1.1.1", "user_agent": "UA1",
             "event_list": "1", "product_list": "E;Phone;1;300;"},
            {"hit_time_gmt": "100", "ip": "1.1.1.1", "user_agent": "UA1",
             "referrer": "http://www.google.com/search?q=phone"},
        ]
        with tempfile.TemporaryDirectory() as tmpdir:
            path = _make_tsv(rows, tmpdir)
            attr = SearchKeywordAttributor(session_timeout=None)
            attr.process_file(path, sort_by_time=False)

        assert attr.get_results() == []

    def test_sort_treats_blank_hit_time_gmt_as_zero(self):
        """Rows with blank hit_time_gmt are treated as timestamp 0 and sort first."""
        rows = [
            {"hit_time_gmt": "", "ip": "1.1.1.1", "user_agent": "UA1",
             "referrer": "http://www.google.com/search?q=widget"},
            {"hit_time_gmt": "200", "ip": "1.1.1.1", "user_agent": "UA1",
             "event_list": "1", "product_list": "E;Widget;1;75;"},
        ]
        with tempfile.TemporaryDirectory() as tmpdir:
            path = _make_tsv(rows, tmpdir)
            attr = SearchKeywordAttributor(session_timeout=None)
            attr.process_file(path, sort_by_time=True)
            results = attr.get_results()

        assert len(results) == 1
        assert results[0] == ("google.com", "widget", Decimal("75.00"))
        assert attr.hits_processed == 2


# ===================================================================
# Checkpointing tests
# ===================================================================


class TestCheckpointing:
    def test_checkpoint_written(self):
        rows = [
            {"hit_time_gmt": "100", "ip": "1.1.1.1", "user_agent": "UA1",
             "referrer": "http://www.google.com/search?q=test"},
            {"hit_time_gmt": "200", "ip": "1.1.1.1", "user_agent": "UA1",
             "event_list": "1", "product_list": "E;Item;1;50;"},
        ]
        with tempfile.TemporaryDirectory() as tmpdir:
            ckpt_dir = os.path.join(tmpdir, "checkpoints")
            path = _make_tsv(rows, tmpdir)
            attr = SearchKeywordAttributor(
                session_timeout=None,
                checkpoint_dir=ckpt_dir,
                checkpoint_interval=1,
            )
            attr.process_file(path)

            ckpt_path = os.path.join(ckpt_dir, "skp_checkpoint.json")
            assert os.path.exists(ckpt_path)

            with open(ckpt_path) as f:
                state = json.load(f)
            assert state["hits_processed"] == 2

    def test_checkpoint_restore_preserves_results(self):
        """Verify that a checkpoint can be restored and results are preserved."""
        rows = [
            {"hit_time_gmt": "100", "ip": "1.1.1.1", "user_agent": "UA1",
             "referrer": "http://www.google.com/search?q=test"},
            {"hit_time_gmt": "200", "ip": "1.1.1.1", "user_agent": "UA1",
             "event_list": "1", "product_list": "E;Item;1;50;"},
        ]
        with tempfile.TemporaryDirectory() as tmpdir:
            ckpt_dir = os.path.join(tmpdir, "checkpoints")
            path = _make_tsv(rows, tmpdir)

            attr1 = SearchKeywordAttributor(
                session_timeout=None,
                checkpoint_dir=ckpt_dir,
                checkpoint_interval=1,
            )
            attr1.process_file(path)
            results1 = attr1.get_results()

            attr2 = SearchKeywordAttributor(
                session_timeout=None,
                checkpoint_dir=ckpt_dir,
            )
            attr2.restore_checkpoint()
            assert attr2.hits_processed == 2
            assert attr2.get_results() == results1


# ===================================================================
# Schema validation tests
# ===================================================================


class TestSchemaValidation:
    def test_missing_required_column_raises_error(self):
        """Schema validation fails fast when a required column is missing."""
        with tempfile.TemporaryDirectory() as tmpdir:
            path = os.path.join(tmpdir, "bad.tsv")
            with open(path, "w", encoding="utf-8") as fh:
                fh.write("ip\tuser_agent\n")
                fh.write("1.2.3.4\tUA1\n")

            attr = SearchKeywordAttributor(session_timeout=None, validate_schema=True)
            with pytest.raises(InputSchemaError, match="missing required columns"):
                attr.process_file(path)

    def test_valid_schema_passes(self):
        """All required columns present passes validation."""
        rows = [
            {"hit_time_gmt": "100", "ip": "1.1.1.1", "user_agent": "UA1",
             "referrer": "http://www.google.com/search?q=test"},
        ]
        with tempfile.TemporaryDirectory() as tmpdir:
            path = _make_tsv(rows, tmpdir)
            attr = SearchKeywordAttributor(session_timeout=None, validate_schema=True)
            attr.process_file(path)
            assert attr.hits_processed == 1

    def test_validation_disabled_skips_check(self):
        """With validate_schema=False, missing columns do not raise."""
        with tempfile.TemporaryDirectory() as tmpdir:
            path = os.path.join(tmpdir, "minimal.tsv")
            with open(path, "w", encoding="utf-8") as fh:
                fh.write("ip\tuser_agent\n")
                fh.write("1.2.3.4\tUA1\n")

            attr = SearchKeywordAttributor(session_timeout=None, validate_schema=False)
            attr.process_file(path)
            assert attr.hits_processed == 1


# ===================================================================
# Data quality metrics tests
# ===================================================================


class TestDataQualityMetrics:
    def test_unattributed_purchase_counted(self):
        """Purchase without prior search is counted as unattributed."""
        rows = [
            {"hit_time_gmt": "100", "ip": "1.1.1.1", "user_agent": "UA1",
             "referrer": "http://www.esshopzilla.com/home"},
            {"hit_time_gmt": "200", "ip": "1.1.1.1", "user_agent": "UA1",
             "event_list": "1", "product_list": "E;Item;1;100;"},
        ]
        with tempfile.TemporaryDirectory() as tmpdir:
            path = _make_tsv(rows, tmpdir)
            attr = SearchKeywordAttributor(session_timeout=None)
            attr.process_file(path)

        assert attr.quality.unattributed_purchases == 1
        assert attr.quality.total_revenue_unattributed == Decimal("100")

    def test_attributed_purchase_counted(self):
        rows = [
            {"hit_time_gmt": "100", "ip": "1.1.1.1", "user_agent": "UA1",
             "referrer": "http://www.google.com/search?q=test"},
            {"hit_time_gmt": "200", "ip": "1.1.1.1", "user_agent": "UA1",
             "event_list": "1", "product_list": "E;Item;1;50;"},
        ]
        with tempfile.TemporaryDirectory() as tmpdir:
            path = _make_tsv(rows, tmpdir)
            attr = SearchKeywordAttributor(session_timeout=None)
            attr.process_file(path)

        assert attr.quality.attributed_purchase_events == 1
        assert attr.quality.total_revenue_attributed == Decimal("50")
        assert attr.quality.total_purchase_events == 1

    def test_purchase_without_revenue_counted(self):
        rows = [
            {"hit_time_gmt": "100", "ip": "1.1.1.1", "user_agent": "UA1",
             "referrer": "http://www.google.com/search?q=test"},
            {"hit_time_gmt": "200", "ip": "1.1.1.1", "user_agent": "UA1",
             "event_list": "1", "product_list": "E;Item;1;;"},
        ]
        with tempfile.TemporaryDirectory() as tmpdir:
            path = _make_tsv(rows, tmpdir)
            attr = SearchKeywordAttributor(session_timeout=None)
            attr.process_file(path)

        assert attr.quality.purchases_without_revenue == 1

    def test_search_referrer_rows_counted(self):
        rows = [
            {"hit_time_gmt": "100", "ip": "1.1.1.1", "user_agent": "UA1",
             "referrer": "http://www.google.com/search?q=shoes"},
            {"hit_time_gmt": "200", "ip": "1.1.1.1", "user_agent": "UA1",
             "referrer": "http://www.esshopzilla.com/cart/"},
        ]
        with tempfile.TemporaryDirectory() as tmpdir:
            path = _make_tsv(rows, tmpdir)
            attr = SearchKeywordAttributor(session_timeout=None)
            attr.process_file(path)

        assert attr.quality.rows_with_search_referrer == 1


# ===================================================================
# Metadata / manifest tests
# ===================================================================


class TestMetadataOutput:
    def test_write_metadata_creates_json(self):
        rows = [
            {"hit_time_gmt": "100", "ip": "1.1.1.1", "user_agent": "UA1",
             "referrer": "http://www.google.com/search?q=test"},
            {"hit_time_gmt": "200", "ip": "1.1.1.1", "user_agent": "UA1",
             "event_list": "1", "product_list": "E;Item;1;50;"},
        ]
        with tempfile.TemporaryDirectory() as tmpdir:
            path = _make_tsv(rows, tmpdir)
            attr = SearchKeywordAttributor(session_timeout=None)
            attr.process_file(path)

            meta_path = os.path.join(tmpdir, "metadata.json")
            attr.write_metadata(meta_path)

            assert os.path.exists(meta_path)
            with open(meta_path) as f:
                metadata = json.load(f)

        assert metadata["rows_processed"] == 2
        assert metadata["keyword_groups"] == 1
        assert metadata["schema_version"] == "v1"
        assert "data_quality" in metadata
        assert "results_summary" in metadata
        assert len(metadata["results_summary"]) == 1

    def test_get_metadata_returns_dict(self):
        rows = [
            {"hit_time_gmt": "100", "ip": "1.1.1.1", "user_agent": "UA1",
             "referrer": "http://www.google.com/search?q=test"},
            {"hit_time_gmt": "200", "ip": "1.1.1.1", "user_agent": "UA1",
             "event_list": "1", "product_list": "E;Item;1;50;"},
        ]
        with tempfile.TemporaryDirectory() as tmpdir:
            path = _make_tsv(rows, tmpdir)
            attr = SearchKeywordAttributor(session_timeout=None)
            attr.process_file(path)

        meta = attr.get_metadata()
        assert meta["rows_processed"] == 2
        assert meta["keyword_groups"] == 1
        assert "data_quality" in meta


# ===================================================================
# Config tests
# ===================================================================


class TestEngineConfig:
    def test_default_config(self):
        config = EngineConfig()
        assert config.session_timeout is None
        assert config.sort_by_time is False
        assert config.validate_schema is True

    def test_negative_session_timeout_raises(self):
        with pytest.raises(ValueError, match="session_timeout"):
            EngineConfig(session_timeout=-1)

    def test_from_config_constructor(self):
        config = EngineConfig(session_timeout=1800, sort_by_time=True)
        attr = SearchKeywordAttributor.from_config(config)
        assert attr._session_timeout == 1800


# ===================================================================
# Golden sample data tests
# ===================================================================


class TestGoldenSampleData:
    @pytest.fixture(autouse=True)
    def _skip_if_missing(self):
        if not os.path.exists(SAMPLE_DATA):
            pytest.skip("Sample data file (data.sql) not found")

    def test_sample_data_matches_expected_attribution_output(self):
        """Default constructor (no session timeout) matches expected output."""
        attr = SearchKeywordAttributor()
        attr.process_file(SAMPLE_DATA)
        results = attr.get_results()

        assert len(results) == 2
        assert results[0] == ("google.com", "Ipod", Decimal("480.00"))
        assert results[1] == ("bing.com", "Zune", Decimal("250.00"))

    def test_sample_data_with_session_timeout_matches(self):
        """With 30-min session timeout enabled, sample data results are
        the same (all hits per visitor are within minutes of each other)."""
        attr = SearchKeywordAttributor(session_timeout=1800)
        attr.process_file(SAMPLE_DATA)
        results = attr.get_results()

        assert len(results) == 2
        assert results[0] == ("google.com", "Ipod", Decimal("480.00"))
        assert results[1] == ("bing.com", "Zune", Decimal("250.00"))

    def test_sample_data_processes_all_hits(self):
        attr = SearchKeywordAttributor(session_timeout=None)
        attr.process_file(SAMPLE_DATA)
        assert attr.hits_processed == 21

    def test_output_file_format(self):
        attr = SearchKeywordAttributor(session_timeout=None)
        attr.process_file(SAMPLE_DATA)

        with tempfile.TemporaryDirectory() as tmpdir:
            output_path = os.path.join(tmpdir, "output.tab")
            attr.write_output(output_path)

            with open(output_path, "r", encoding="utf-8") as fh:
                lines = fh.read().splitlines()

        assert lines[0] == "Search Engine Domain\tSearch Keyword\tRevenue"
        assert lines[1] == "google.com\tIpod\t480.00"
        assert lines[2] == "bing.com\tZune\t250.00"
        assert len(lines) == 3

    def test_output_matches_reference_artifact(self):
        """Generated output from sample data matches the committed artifact exactly."""
        reference_path = os.path.join(
            os.path.dirname(__file__), "..", "output", REFERENCE_ARTIFACT
        )
        assert os.path.exists(reference_path), (
            f"Reference artifact not found: {REFERENCE_ARTIFACT}"
        )

        attr = SearchKeywordAttributor(session_timeout=None)
        attr.process_file(SAMPLE_DATA)

        with tempfile.TemporaryDirectory() as tmpdir:
            generated_path = os.path.join(tmpdir, "generated.tab")
            attr.write_output(generated_path)

            with open(generated_path, "r", encoding="utf-8") as fh:
                generated = fh.read()
            with open(reference_path, "r", encoding="utf-8") as fh:
                reference = fh.read()

        assert generated == reference, (
            f"Generated output does not match reference artifact {REFERENCE_ARTIFACT}"
        )

    def test_sample_data_quality_metrics(self):
        """Verify data quality metrics for the sample dataset."""
        attr = SearchKeywordAttributor(session_timeout=None)
        attr.process_file(SAMPLE_DATA)

        assert attr.quality.total_purchase_events >= 2
        assert attr.quality.attributed_purchase_events >= 2
        assert attr.quality.total_revenue_attributed == Decimal("730")
