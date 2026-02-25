"""CLI common boilerplate — shared setup for small and large table pipelines.

L-1: Centralizes environment setup, logging, startup checks, RSS monitoring,
and TableConfig serialization that was duplicated between main_small_tables.py
and main_large_tables.py.

Import this module BEFORE any other project imports in main_*.py files.
Module-level code sets MALLOC_ARENA_MAX, POLARS_MAX_THREADS, and sys.path.
"""

from __future__ import annotations

import logging
import os
import sys
from pathlib import Path

# M-1: Capture BEFORE setdefault — was MALLOC_ARENA_MAX set externally?
# glibc arena configuration is locked at process start — os.environ.setdefault()
# only covers child processes (ProcessPoolExecutor workers).
MALLOC_ARENA_EXTERNALLY_SET = "MALLOC_ARENA_MAX" in os.environ

# M-1: Set MALLOC_ARENA_MAX=2 for child processes. Reduces glibc arenas from
# 8x cores (48 on 6-core) to 2, preventing 10x memory bloat (Polars #23128).
os.environ.setdefault("MALLOC_ARENA_MAX", "2")
# M-2: POLARS_MAX_THREADS=1 prevents thread oversubscription when using
# ProcessPoolExecutor — parallelism is achieved via multiprocessing.
os.environ.setdefault("POLARS_MAX_THREADS", "1")

# Ensure project root is on sys.path for imports
sys.path.insert(0, str(Path(__file__).parent))

import config
from observability.log_handler import SqlServerLogHandler

logger = logging.getLogger(__name__)


def setup_logging(batch_id: int | None = None) -> SqlServerLogHandler:
    """Configure logging: StreamHandler + SqlServerLogHandler.

    Args:
        batch_id: Pipeline batch ID for log context.

    Returns:
        The SqlServerLogHandler instance (for flush/context updates).
    """
    root = logging.getLogger()
    root.setLevel(getattr(logging, config.LOG_LEVEL, logging.INFO))

    # Console handler
    console = logging.StreamHandler(sys.stdout)
    console.setLevel(logging.INFO)
    console.setFormatter(
        logging.Formatter("%(asctime)s [%(levelname)s] %(name)s - %(message)s")
    )
    root.addHandler(console)

    # SQL Server log handler
    sql_handler = SqlServerLogHandler(level=getattr(logging, config.LOG_LEVEL, logging.INFO))
    if batch_id is not None:
        sql_handler.set_context(batch_id=batch_id)
    root.addHandler(sql_handler)

    return sql_handler


def startup_checks() -> None:
    """Run startup checks: staging cleanup + RCSI verification.

    P3-3: Clean up orphaned staging tables from previous crashed runs.
    E-9: Verify RCSI enabled on Bronze for non-blocking SCD2 updates.
    """
    from schema.staging_cleanup import cleanup_orphaned_staging_tables
    cleanup_orphaned_staging_tables()

    from connections import verify_rcsi_enabled
    verify_rcsi_enabled()


def warn_malloc_arena() -> None:
    """W-4: Warn if MALLOC_ARENA_MAX was not set in the external environment.

    glibc arena configuration is locked at process start. os.environ.setdefault()
    only covers child processes. The variable must be set before the Python
    interpreter starts (systemd unit file, shell wrapper, .bashrc).
    """
    if not MALLOC_ARENA_EXTERNALLY_SET:
        logger.warning(
            "W-4: MALLOC_ARENA_MAX was not set in the external environment "
            "(current: %s, set by os.environ.setdefault). glibc arena "
            "configuration is locked at process start. Set MALLOC_ARENA_MAX=2 "
            "in the systemd unit file or shell wrapper to prevent glibc arena "
            "fragmentation causing 10x memory bloat (Polars issue #23128).",
            os.environ.get("MALLOC_ARENA_MAX"),
        )


def warn_workers(workers: int) -> None:
    """M-2: Warn if workers exceed recommended maximum for 64 GB system."""
    if workers > 4:
        logger.warning(
            "M-2: Running with %d workers. On a 64 GB system, >4 workers risks OOM "
            "during CDC/SCD2 join operations (peak memory = 2.5-3x DataFrame size). "
            "Recommended: --workers 4 or fewer.",
            workers,
        )


def check_rss_memory(source_name: str, table_name: str) -> None:
    """B-8: Check RSS memory between table iterations.

    Polars + glibc arena fragmentation can cause RSS to grow monotonically
    during multi-table runs (Polars issue #23128). Logs WARNING at 85% of
    MAX_RSS_GB and ERROR at the limit.
    """
    try:
        import psutil
        rss_gb = psutil.Process().memory_info().rss / (1024 ** 3)
        if rss_gb > config.MAX_RSS_GB:
            logger.error(
                "B-8: RSS memory %.1f GB exceeds MAX_RSS_GB (%.1f GB) before %s.%s. "
                "Polars allocator may not be releasing memory to OS (issue #23128). "
                "Consider: (a) set MALLOC_ARENA_MAX=2 (W-4), (b) reduce --workers, "
                "(c) restart pipeline to reclaim RSS.",
                rss_gb, config.MAX_RSS_GB, source_name, table_name,
            )
        elif rss_gb > config.MAX_RSS_GB * 0.85:
            logger.warning(
                "B-8: RSS memory %.1f GB approaching MAX_RSS_GB (%.1f GB) before %s.%s.",
                rss_gb, config.MAX_RSS_GB, source_name, table_name,
            )
    except ImportError:
        pass  # psutil not installed — skip RSS check


def validate_cli_filters(
    source_name: str | None,
    table_name: str | None,
) -> None:
    """H-4: Validate --source and --table CLI arguments against UdmTablesList.

    Belt-and-suspenders guard: even though H-3 parameterizes queries, this
    catches typos early and prevents unexpected empty result sets.

    Raises:
        SystemExit: If the value is not found in UdmTablesList.
    """
    if source_name is None and table_name is None:
        return

    from orchestration.table_config import TableConfigLoader
    loader = TableConfigLoader()

    if source_name:
        known_sources = loader.get_known_sources()
        if source_name not in known_sources:
            logger.error(
                "H-4: --source '%s' not found in UdmTablesList. "
                "Known sources: %s",
                source_name, sorted(known_sources),
            )
            sys.exit(1)

    if table_name:
        known_tables = loader.get_known_tables()
        if table_name not in known_tables:
            logger.error(
                "H-4: --table '%s' not found in UdmTablesList. "
                "Known tables (first 20): %s",
                table_name, sorted(known_tables)[:20],
            )
            sys.exit(1)


def log_connection_overhead() -> None:
    """P-3: Log cumulative connection overhead at pipeline end."""
    from connections import get_connection_overhead
    total_ms, count = get_connection_overhead()
    if count > 0:
        logger.info(
            "P-3: Connection overhead: %.1f ms total across %d connections (%.1f ms avg)",
            total_ms, count, total_ms / count,
        )


def shutdown_connections() -> None:
    """Item-18: Close pooled connections at pipeline shutdown."""
    from connections import close_connection_pool
    close_connection_pool()


def table_config_to_dict(tc, batch_id: int) -> dict:
    """Serialize a TableConfig for cross-process transfer via ProcessPoolExecutor.

    Args:
        tc: TableConfig instance.
        batch_id: Current batch ID.

    Returns:
        Dict suitable for pickling across process boundaries.
    """
    return {
        "source_object_name": tc.source_object_name,
        "source_server": tc.source_server,
        "source_database": tc.source_database,
        "source_schema_name": tc.source_schema_name,
        "source_name": tc.source_name,
        "stage_table_name": tc.stage_table_name,
        "bronze_table_name": tc.bronze_table_name,
        "source_aggregate_column_name": tc.source_aggregate_column_name,
        "source_aggregate_column_type": tc.source_aggregate_column_type,
        "source_index_hint": tc.source_index_hint,
        "partition_on": tc.partition_on,
        "first_load_date": tc.first_load_date,
        "lookback_days": tc.lookback_days,
        "stage_load_tool": tc.stage_load_tool,
        "columns": [
            {
                "source_name": c.source_name,
                "table_name": c.table_name,
                "column_name": c.column_name,
                "ordinal_position": c.ordinal_position,
                "is_primary_key": c.is_primary_key,
                "layer": c.layer,
                "is_index": c.is_index,
                "index_name": c.index_name,
                "index_type": c.index_type,
            }
            for c in tc.columns
        ],
        "batch_id": batch_id,
        "force": False,
    }
