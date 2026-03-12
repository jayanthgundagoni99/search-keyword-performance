"""Structured error taxonomy for Search Keyword Performance.

Categorized exceptions make logs, alerts, and debugging much easier
in production pipelines.  Each category maps to a distinct failure mode
so operators can route and triage automatically.
"""


class SearchKeywordError(Exception):
    """Base exception for all search keyword performance errors."""


class InputSchemaError(SearchKeywordError):
    """Raised when required columns are missing or the file shape is wrong.

    Typical causes: wrong delimiter, missing header row, truncated upload.
    """


class ParsingError(SearchKeywordError):
    """Raised when an individual record cannot be parsed.

    Logged and counted rather than raised in normal operation (the engine
    skips malformed rows), but available for strict-mode callers.
    """


class CheckpointError(SearchKeywordError):
    """Raised when checkpoint save or restore fails.

    Subtypes distinguish direction so alerts can differentiate
    a crash-recovery failure from a progress-save failure.
    """


class CheckpointRestoreError(CheckpointError):
    """Checkpoint file exists but cannot be read or deserialized."""


class CheckpointWriteError(CheckpointError):
    """Checkpoint cannot be written (permissions, disk full, etc.)."""


class OutputWriteError(SearchKeywordError):
    """Raised when the final output file cannot be written or uploaded."""


class AWSIOError(SearchKeywordError):
    """Raised when an AWS SDK call (S3 download/upload, etc.) fails."""


class DuplicateRunError(SearchKeywordError):
    """Raised when idempotency check detects an already-processed input."""
