"""Extract package — shared utilities for source data extraction.

B-7: ConnectorX Rust panic recovery wrapper for all extractors.
"""

from __future__ import annotations

import logging
import time

import connectorx as cx
import polars as pl

logger = logging.getLogger(__name__)

# B-7: Default retry configuration for ConnectorX calls.
_CX_MAX_RETRIES = 3
_CX_RETRY_BASE_DELAY = 2.0  # seconds, doubles each retry (exponential backoff)


def cx_read_sql_safe(
    *,
    conn: str,
    query: str,
    return_type: str = "polars",
    partition_on: str | None = None,
    partition_num: int | None = None,
    context: str = "",
    max_retries: int = _CX_MAX_RETRIES,
) -> pl.DataFrame:
    """B-7: Wrapper around cx.read_sql with Rust panic recovery and retry.

    ConnectorX errors manifest as Rust thread panics (PanicException) rather
    than standard Python exceptions. These may not inherit from Exception —
    they can inherit directly from BaseException. This wrapper catches
    BaseException to handle both regular errors and Rust panics.

    Retries with exponential backoff for transient failures (connection
    timeouts, Oracle listener issues, SQL Server busy). Non-retryable
    errors (syntax errors, permission denied) fail immediately.

    Args:
        conn: ConnectorX connection URI.
        query: SQL query to execute.
        return_type: ConnectorX return type (default "polars").
        partition_on: Column for partitioned extraction.
        partition_num: Number of partitions.
        context: Description for log messages (e.g. "Oracle extract ACCT").
        max_retries: Maximum retry attempts (default 3).

    Returns:
        Polars DataFrame with extraction results.

    Raises:
        BaseException: After all retries exhausted, re-raises the last error.
    """
    cx_kwargs: dict = {
        "conn": conn,
        "query": query,
        "return_type": return_type,
    }
    if partition_on is not None:
        cx_kwargs["partition_on"] = partition_on
    if partition_num is not None:
        cx_kwargs["partition_num"] = partition_num

    last_error: BaseException | None = None

    for attempt in range(1, max_retries + 1):
        try:
            return cx.read_sql(**cx_kwargs)
        except BaseException as e:
            last_error = e
            error_type = type(e).__name__

            # O-3: Categorized retry logic — distinguish transient from permanent errors.
            # Transient: connection reset, timeout, Oracle listener, SQL Server busy.
            # Permanent: syntax, permissions, missing objects — fail immediately.
            error_str = str(e).lower()
            non_retryable = _is_non_retryable_error(error_str, error_type)

            if non_retryable or attempt == max_retries:
                if attempt > 1:
                    logger.error(
                        "B-7: ConnectorX %s failed after %d attempts (%s: %s)%s",
                        context, attempt, error_type, e,
                        " [non-retryable]" if non_retryable else "",
                    )
                else:
                    logger.error(
                        "B-7: ConnectorX %s failed (%s: %s)%s",
                        context, error_type, e,
                        " [non-retryable]" if non_retryable else "",
                    )
                raise

            delay = _CX_RETRY_BASE_DELAY * (2 ** (attempt - 1))
            logger.warning(
                "B-7: ConnectorX %s attempt %d/%d failed (%s: %s). "
                "Retrying in %.1fs...",
                context, attempt, max_retries, error_type, e, delay,
            )
            time.sleep(delay)

    # Should not reach here, but satisfy type checker
    raise last_error  # type: ignore[misc]


# O-3: Non-retryable error patterns for ConnectorX.
# SQL syntax, permissions, and object-not-found errors are permanent —
# retrying only wastes time and obscures the root cause.
_NON_RETRYABLE_PATTERNS = (
    # SQL Server permanent errors
    "syntax",
    "permission",
    "does not exist",
    "invalid column",
    "invalid object",
    "login failed",
    "access denied",
    # Oracle permanent errors (ORA- codes)
    "ora-00942",      # table or view does not exist
    "ora-00904",      # invalid identifier
    "ora-01017",      # invalid username/password
    "ora-01031",      # insufficient privileges
    "ora-00933",      # SQL command not properly ended (syntax)
    "ora-06550",      # PL/SQL compilation error
)

# O-3: Transient error patterns — these SHOULD be retried.
# Listed for documentation; if an error matches a transient pattern,
# it is explicitly NOT classified as non-retryable.
_TRANSIENT_PATTERNS = (
    "ora-12541",      # TNS:no listener
    "ora-12170",      # TNS:connect timeout
    "ora-03135",      # connection lost contact
    "ora-03113",      # end-of-file on communication channel
    "ora-12537",      # TNS:connection closed
    "connection reset",
    "connection refused",
    "timeout",
    "timed out",
    "broken pipe",
    "network",
    "server is not available",
)


def _is_non_retryable_error(error_str: str, error_type: str) -> bool:
    """O-3: Classify ConnectorX errors as retryable or permanent.

    Returns True if the error should NOT be retried (syntax, permissions,
    missing objects). Returns False for transient errors that may succeed
    on retry (connection issues, timeouts).
    """
    # Check transient patterns first — if it matches a known transient
    # pattern, always retry even if it also matches a non-retryable pattern
    for pattern in _TRANSIENT_PATTERNS:
        if pattern in error_str:
            return False

    # Check non-retryable patterns
    for pattern in _NON_RETRYABLE_PATTERNS:
        if pattern in error_str:
            return True

    # KeyboardInterrupt and SystemExit should not be retried
    if error_type in ("KeyboardInterrupt", "SystemExit"):
        return True

    # Default: assume retryable (conservative — prefer retrying over failing)
    return False
