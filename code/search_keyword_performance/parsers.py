"""Pure parsing functions for hit-level web analytics data.

Each function is stateless and side-effect-free, making them easy to test
and reuse independently of the attribution engine.
"""

from decimal import Decimal, InvalidOperation
from typing import Optional
from urllib.parse import parse_qs, urlparse

SEARCH_ENGINES: list[tuple[str, str, str]] = [
    # (hostname_suffix, query_param, display_domain)
    # More-specific entries first so search.yahoo.com matches before yahoo.com.
    ("google.com", "q", "google.com"),
    ("bing.com", "q", "bing.com"),
    ("search.yahoo.com", "p", "yahoo.com"),
    ("yahoo.com", "p", "yahoo.com"),
    ("msn.com", "q", "msn.com"),
]


def extract_search_referrer(referrer: str) -> Optional[tuple[str, str]]:
    """Extract the search engine domain and keyword from a referrer URL.

    Recognizes Google, Bing, Yahoo, and MSN.  Strips ``www.`` prefix before
    matching.  URL-decodes the keyword (``+`` -> space, ``%xx`` sequences).

    Returns:
        ``(display_domain, keyword)`` if the referrer is a recognized external
        search engine with a non-empty keyword, otherwise ``None``.
    """
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


def parse_event_list(event_list: str) -> bool:
    """Return ``True`` if *event_list* contains the purchase event (code 1).

    Event codes are comma-separated integers.  Exact-match comparison
    prevents false positives from codes like ``10``, ``11``, ``12``.
    """
    if not event_list:
        return False
    return "1" in {e.strip() for e in event_list.split(",")}


def parse_product_list_revenue(product_list: str) -> Decimal:
    """Sum the revenue across all products in a ``product_list`` string.

    Format per product (semicolon-delimited):
        ``Category;Product Name;Quantity;Revenue;Custom Events;eVars``

    Revenue is at semicolon-index 3.  Products are comma-delimited.
    Non-numeric or missing revenue fields are silently skipped.

    Returns:
        Total revenue as a :class:`~decimal.Decimal`.  Returns
        ``Decimal("0")`` when *product_list* is empty or contains no
        parseable revenue.
    """
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
