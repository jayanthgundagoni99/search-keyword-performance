"""Unit tests for the pure parsing functions in search_keyword_performance.parsers."""

from decimal import Decimal

import pytest

from search_keyword_performance.parsers import (
    extract_search_referrer,
    parse_event_list,
    parse_product_list_revenue,
)


class TestExtractSearchReferrer:
    def test_google_with_www(self):
        url = "http://www.google.com/search?hl=en&q=Ipod&aq=f"
        assert extract_search_referrer(url) == ("google.com", "Ipod")

    def test_google_without_www(self):
        url = "http://google.com/search?q=laptop"
        assert extract_search_referrer(url) == ("google.com", "laptop")

    def test_bing(self):
        url = "http://www.bing.com/search?q=Zune&go=&form=QBLH"
        assert extract_search_referrer(url) == ("bing.com", "Zune")

    def test_yahoo_search_subdomain(self):
        url = "http://search.yahoo.com/search?p=cd+player&toggle=1"
        assert extract_search_referrer(url) == ("yahoo.com", "cd player")

    def test_yahoo_plus_decoded_as_space(self):
        url = "http://search.yahoo.com/search?p=red+shoes"
        assert extract_search_referrer(url) == ("yahoo.com", "red shoes")

    def test_percent_encoded_keyword(self):
        url = "http://www.google.com/search?q=blue%20widget"
        assert extract_search_referrer(url) == ("google.com", "blue widget")

    def test_internal_referrer_returns_none(self):
        url = "http://www.esshopzilla.com/product/?pid=as32213"
        assert extract_search_referrer(url) is None

    def test_empty_string_returns_none(self):
        assert extract_search_referrer("") is None

    def test_missing_keyword_param_returns_none(self):
        url = "http://www.google.com/search?hl=en"
        assert extract_search_referrer(url) is None

    def test_blank_keyword_returns_none(self):
        url = "http://www.google.com/search?q=&hl=en"
        assert extract_search_referrer(url) is None

    def test_msn(self):
        url = "http://www.msn.com/search?q=headphones"
        assert extract_search_referrer(url) == ("msn.com", "headphones")

    def test_google_complex_url_from_sample_data(self):
        url = (
            "http://www.google.com/search?hl=en&client=firefox-a"
            "&rls=org.mozilla%3Aen-US%3Aofficial&hs=ZzP"
            "&q=Ipod&aq=f&oq=&aqi="
        )
        assert extract_search_referrer(url) == ("google.com", "Ipod")

    def test_malformed_url_returns_none(self):
        assert extract_search_referrer("not-a-url") is None


class TestParseEventList:
    def test_purchase_only(self):
        assert parse_event_list("1") is True

    def test_purchase_with_others(self):
        assert parse_event_list("1,200,201") is True

    def test_no_purchase(self):
        assert parse_event_list("2") is False

    def test_cart_events_not_purchase(self):
        assert parse_event_list("12,11") is False

    def test_event_10_not_confused_with_1(self):
        assert parse_event_list("10") is False

    def test_event_11_not_confused_with_1(self):
        assert parse_event_list("11,12") is False

    def test_event_13_14_not_purchase(self):
        assert parse_event_list("13,14") is False

    def test_empty_string(self):
        assert parse_event_list("") is False

    def test_whitespace_around_codes(self):
        assert parse_event_list(" 1 , 2 ") is True

    def test_purchase_buried_in_many_events(self):
        assert parse_event_list("2,10,11,12,1,200") is True


class TestParseProductListRevenue:
    def test_single_product(self):
        assert parse_product_list_revenue("Electronics;Zune - 32GB;1;250;") == Decimal("250")

    def test_single_product_decimal_value(self):
        assert parse_product_list_revenue("Shoes;Red Shoes;1;49.99;") == Decimal("49.99")

    def test_multiple_products(self):
        pl = "Computers;HP Pavillion;1;1000;200|201,Office Supplies;Red Folders;4;4.00;205"
        assert parse_product_list_revenue(pl) == Decimal("1004.00")

    def test_empty_revenue_field(self):
        assert parse_product_list_revenue("Electronics;Zune - 32GB;1;;") == Decimal("0")

    def test_empty_string(self):
        assert parse_product_list_revenue("") == Decimal("0")

    def test_too_few_fields_skipped(self):
        assert parse_product_list_revenue("A;B") == Decimal("0")

    def test_non_numeric_revenue_skipped(self):
        assert parse_product_list_revenue("A;B;1;abc;") == Decimal("0")

    def test_mixed_valid_and_invalid_products(self):
        pl = "A;B;1;abc;,C;D;1;25.50;"
        assert parse_product_list_revenue(pl) == Decimal("25.50")

    def test_no_float_drift(self):
        pl = "A;B;1;0.1;,C;D;1;0.2;"
        assert parse_product_list_revenue(pl) == Decimal("0.3")
