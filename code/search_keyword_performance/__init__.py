"""Search Keyword Performance Attribution Engine.

Processes hit-level web analytics data to attribute purchase revenue to
external search engine keywords using last-touch session attribution.
"""

from .config import EngineConfig
from .engine import DataQualityMetrics, SearchKeywordAttributor, open_input
from .exceptions import (
    AWSIOError,
    CheckpointError,
    CheckpointRestoreError,
    CheckpointWriteError,
    DuplicateRunError,
    InputSchemaError,
    OutputWriteError,
    ParsingError,
    SearchKeywordError,
)
from .parsers import (
    extract_search_referrer,
    parse_event_list,
    parse_product_list_revenue,
)

__all__ = [
    "EngineConfig",
    "DataQualityMetrics",
    "SearchKeywordAttributor",
    "open_input",
    "extract_search_referrer",
    "parse_event_list",
    "parse_product_list_revenue",
    "SearchKeywordError",
    "InputSchemaError",
    "ParsingError",
    "CheckpointError",
    "CheckpointRestoreError",
    "CheckpointWriteError",
    "OutputWriteError",
    "AWSIOError",
    "DuplicateRunError",
]
