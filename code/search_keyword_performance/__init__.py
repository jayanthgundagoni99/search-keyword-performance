"""Search Keyword Performance Attribution Engine.

Processes hit-level web analytics data to attribute purchase revenue to
external search engine keywords using last-touch session attribution.
"""

from .engine import SearchKeywordAttributor, open_input
from .parsers import (
    extract_search_referrer,
    parse_event_list,
    parse_product_list_revenue,
)

__all__ = [
    "SearchKeywordAttributor",
    "open_input",
    "extract_search_referrer",
    "parse_event_list",
    "parse_product_list_revenue",
]
