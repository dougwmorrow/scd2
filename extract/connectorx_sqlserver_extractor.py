"""ConnectorX SQL Server extraction -> Polars DataFrame -> BCP CSV.

Used for SQL Server sources (CCM, EPICOR, etc.).
Supports partition_on for parallel reads when PartitionOn column is set.

Provides two modes:
  - extract_sqlserver_connectorx(): Full table scan (small tables).
  - extract_sqlserver_connectorx_windowed(): Date-windowed extraction (large tables).
"""

from __future__ import annotations

import logging
from datetime import date
from pathlib import Path
from typing import TYPE_CHECKING

import connectorx as cx
import polars as pl

from data_load.bcp_csv import prepare_dataframe_for_bcp, validate_schema_before_concat, write_bcp_csv
from extract import cx_read_sql_safe
from sources import get_source

if TYPE_CHECKING:
    from orchestration.table_config import TableConfig

logger = logging.getLogger(__name__)


def extract_sqlserver_connectorx(
    table_config: TableConfig,
    output_dir: str | Path,
    partition_on: str | None = None,
    partition_num: int = 4,
) -> tuple[pl.DataFrame, Path]:
    """Extract from SQL Server via ConnectorX into a Polars DataFrame and write BCP CSV.

    Args:
        table_config: Table configuration with source details.
        output_dir: Directory for output CSV file.
        partition_on: Column to partition on for parallel extraction.
        partition_num: Number of partitions for parallel extraction.

    Returns:
        Tuple of (prepared DataFrame, CSV file path).
    """
    source = get_source(table_config.source_name)
    uri = source.connectorx_uri()

    query = f"SELECT * FROM {table_config.source_schema_name}.{table_config.source_object_name}"
    logger.info("ConnectorX SQL Server extract: %s", query)

    cx_kwargs = {
        "conn": uri,
        "query": query,
        "return_type": "polars",
    }

    if partition_on:
        # E-1: Force partition_num=1 for CDC-sensitive extraction.
        # partition_num > 1 opens N independent SQL Server connections under
        # READ COMMITTED, each seeing committed data at slightly different times.
        # This produces torn reads causing phantom CDC changes.
        if partition_num > 1:
            logger.warning(
                "E-1: Overriding partition_num from %d to 1 for %s — "
                "CDC-sensitive extraction requires a single consistent snapshot. "
                "partition_num > 1 causes torn reads on actively-written tables.",
                partition_num, table_config.source_object_name,
            )
            partition_num = 1
        cx_kwargs["partition_on"] = partition_on
        cx_kwargs["partition_num"] = partition_num
        logger.info("Using partition_on=%s with %d partitions", partition_on, partition_num)

    # B-7: Use safe wrapper with Rust panic recovery and retry.
    df = cx_read_sql_safe(
        conn=cx_kwargs["conn"],
        query=cx_kwargs["query"],
        return_type=cx_kwargs["return_type"],
        partition_on=cx_kwargs.get("partition_on"),
        partition_num=cx_kwargs.get("partition_num"),
        context=f"SQL Server extract {table_config.source_object_name}",
    )
    logger.info("Extracted %d rows, %d columns from %s", len(df), len(df.columns), table_config.source_full_table_name)

    # E-2: Supplement partitioned extraction with NULL partition column rows.
    if partition_on:
        df = _supplement_null_partition_rows(
            df, uri, query, partition_on, table_config,
        )

    # P2-3: Log partition skew warning if partition_on was used
    if partition_on and len(df) > 0:
        _log_partition_skew(df, partition_on, partition_num, table_config)

    # P3-5: Warn about large full-scan extractions
    if not partition_on and len(df) > 5_000_000:
        logger.warning(
            "P3-5: Full-scan extraction returned %d rows from %s. "
            "Consider using PartitionOn or date-windowed extraction for better performance.",
            len(df), table_config.source_full_table_name,
        )

    df = prepare_dataframe_for_bcp(df, fix_oracle_dates=False)

    csv_path = write_bcp_csv(
        df,
        Path(output_dir) / f"{table_config.source_name}_{table_config.source_object_name}.csv",
    )

    return df, csv_path


def _supplement_null_partition_rows(
    df: pl.DataFrame,
    uri: str,
    base_query: str,
    partition_on: str,
    table_config,
) -> pl.DataFrame:
    """E-2: Fetch rows where partition column IS NULL and concat with main result.

    ConnectorX's partition logic generates WHERE clauses using > and < operators,
    which never match NULL. Any row with a NULL partition column is silently excluded.
    """
    try:
        # Append WHERE partition_col IS NULL to the base query
        null_query = f"{base_query} WHERE [{partition_on}] IS NULL"
        # If base query already has WHERE, use AND instead
        if " WHERE " in base_query.upper():
            null_query = f"{base_query} AND [{partition_on}] IS NULL"

        df_nulls = cx.read_sql(uri, null_query, return_type="polars")

        if len(df_nulls) > 0:
            logger.warning(
                "E-2: Found %d rows with NULL [%s] in %s — these would be silently "
                "excluded by ConnectorX partitioned extraction. Supplementing result.",
                len(df_nulls), partition_on, table_config.source_object_name,
            )
            # W-7: Validate schemas before diagonal_relaxed concat.
            try:
                validate_schema_before_concat(
                    [df, df_nulls],
                    f"SQL Server NULL partition supplement for {table_config.source_object_name}",
                )
            except Exception as e:
                logger.warning("W-7: %s — proceeding with diagonal_relaxed concat", e)
            df = pl.concat([df, df_nulls], how="diagonal_relaxed")
        else:
            logger.debug("E-2: No NULL [%s] rows in %s", partition_on, table_config.source_object_name)

    except Exception:
        logger.warning(
            "E-2: Failed to check NULL partition column rows for %s — "
            "continuing with partitioned result only",
            table_config.source_object_name, exc_info=True,
        )

    return df


def _log_partition_skew(
    df: pl.DataFrame,
    partition_on: str,
    partition_num: int,
    table_config,
) -> None:
    """P2-3: Log partition column statistics and warn about potential skew."""
    if partition_on not in df.columns:
        return

    try:
        col = df[partition_on]
        col_min = col.min()
        col_max = col.max()
        col_null = col.null_count()
        avg_per_partition = len(df) / partition_num if partition_num > 0 else len(df)

        logger.info(
            "P2-3: Partition stats for %s.%s on [%s]: min=%s, max=%s, "
            "nulls=%d, total=%d, partitions=%d, avg_per_partition=%.0f",
            table_config.source_name, table_config.source_object_name,
            partition_on, col_min, col_max,
            col_null, len(df), partition_num, avg_per_partition,
        )

        if col_null > len(df) * 0.1:
            logger.warning(
                "P2-3: Partition column [%s] has %d NULL values (%.1f%%) in %s. "
                "ConnectorX excludes NULL rows from partitioned reads. "
                "Consider a different partition_on column.",
                partition_on, col_null, col_null / len(df) * 100,
                table_config.source_object_name,
            )
    except Exception:
        logger.debug("Could not compute partition skew stats for %s", partition_on)


def extract_sqlserver_connectorx_windowed(
    table_config: TableConfig,
    output_dir: str | Path,
    start_date: date,
    end_date: date,
    partition_on: str | None = None,
    partition_num: int = 4,
) -> tuple[pl.DataFrame, Path]:
    """Extract a date window from SQL Server via ConnectorX.

    For large tables. Uses SourceAggregateColumnName for the WHERE clause.

    Args:
        table_config: Table configuration with source details.
        output_dir: Directory for output CSV file.
        start_date: Start of date range (inclusive).
        end_date: End of date range (exclusive).
        partition_on: Column to partition on for parallel extraction.
        partition_num: Number of partitions for parallel extraction.

    Returns:
        Tuple of (prepared DataFrame, CSV file path).
    """
    source = get_source(table_config.source_name)
    uri = source.connectorx_uri()

    date_col = table_config.source_aggregate_column_name
    schema = table_config.source_schema_name
    table = table_config.source_object_name

    query = (
        f"SELECT * FROM {schema}.{table} "
        f"WHERE {date_col} >= '{start_date}' AND {date_col} < '{end_date}'"
    )
    logger.info("ConnectorX SQL Server windowed extract: %s [%s, %s)", table, start_date, end_date)

    cx_kwargs = {
        "conn": uri,
        "query": query,
        "return_type": "polars",
    }

    if partition_on:
        # E-1: Force single-partition for windowed CDC-sensitive extraction.
        if partition_num > 1:
            logger.warning(
                "E-1: Overriding partition_num from %d to 1 for windowed %s — "
                "CDC-sensitive extraction requires a single consistent snapshot.",
                partition_num, table,
            )
            partition_num = 1
        cx_kwargs["partition_on"] = partition_on
        cx_kwargs["partition_num"] = partition_num

    # B-7: Use safe wrapper with Rust panic recovery and retry.
    df = cx_read_sql_safe(
        conn=cx_kwargs["conn"],
        query=cx_kwargs["query"],
        return_type=cx_kwargs["return_type"],
        partition_on=cx_kwargs.get("partition_on"),
        partition_num=cx_kwargs.get("partition_num"),
        context=f"SQL Server windowed extract {table} [{start_date}, {end_date})",
    )
    logger.info(
        "Extracted %d rows from %s.%s for [%s, %s)",
        len(df), schema, table, start_date, end_date,
    )

    if len(df) == 0:
        csv_path = Path(output_dir) / f"{table_config.source_name}_{table}_{start_date}.csv"
        csv_path.parent.mkdir(parents=True, exist_ok=True)
        csv_path.write_text("")
        return df, csv_path

    df = prepare_dataframe_for_bcp(df, fix_oracle_dates=False)

    csv_path = write_bcp_csv(
        df,
        Path(output_dir) / f"{table_config.source_name}_{table}_{start_date}.csv",
    )

    return df, csv_path
