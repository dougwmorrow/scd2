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

            # Check if error is non-retryable (syntax errors, permission issues)
            error_str = str(e).lower()
            non_retryable = (
                "syntax" in error_str
                or "permission" in error_str
                or "does not exist" in error_str
                or "invalid column" in error_str
                or "invalid object" in error_str
            )

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
