"""CLI entry point for small tables pipeline.

Usage:
    python3 main_small_tables.py --workers 4
    python3 main_small_tables.py --table ACCT --source DNA
    python3 main_small_tables.py --list-tables
    python3 main_small_tables.py --workers 4 --source DNA
"""

from __future__ import annotations

# L-1: cli_common sets MALLOC_ARENA_MAX (M-1), POLARS_MAX_THREADS (M-2),
# and sys.path — must be imported before any other project modules.
import cli_common  # noqa: F401

import argparse
import logging
import sys
from concurrent.futures import ProcessPoolExecutor, as_completed

from observability.event_tracker import PipelineEventTracker
from orchestration.table_config import TableConfigLoader


def _process_table_worker(table_config_dict: dict) -> tuple[str, str, bool]:
    """Worker function for ProcessPoolExecutor.

    Takes a serialized table config dict (for pickling across processes),
    reconstructs the config, and processes the table.
    """
    from orchestration.table_config import TableConfig, ColumnConfig
    from orchestration.small_tables import process_small_table

    tc = TableConfig(
        source_object_name=table_config_dict["source_object_name"],
        source_server=table_config_dict["source_server"],
        source_database=table_config_dict["source_database"],
        source_schema_name=table_config_dict["source_schema_name"],
        source_name=table_config_dict["source_name"],
        stage_table_name=table_config_dict.get("stage_table_name"),
        bronze_table_name=table_config_dict.get("bronze_table_name"),
        source_aggregate_column_name=table_config_dict.get("source_aggregate_column_name"),
        source_aggregate_column_type=table_config_dict.get("source_aggregate_column_type"),
        source_index_hint=table_config_dict.get("source_index_hint"),
        partition_on=table_config_dict.get("partition_on"),
        first_load_date=table_config_dict.get("first_load_date"),
        lookback_days=table_config_dict.get("lookback_days"),
        stage_load_tool=table_config_dict.get("stage_load_tool"),
        columns=[
            ColumnConfig(**col) for col in table_config_dict.get("columns", [])
        ],
        _resolved_stage_schema=table_config_dict.get("_resolved_stage_schema"),
        _resolved_bronze_schema=table_config_dict.get("_resolved_bronze_schema"),
    )

    tracker = PipelineEventTracker()
    tracker._batch_id = table_config_dict["batch_id"]

    force = table_config_dict.get("force", False)
    refresh_pks = table_config_dict.get("refresh_pks", False)
    success = process_small_table(tc, tracker, force=force, refresh_pks=refresh_pks)
    return tc.source_name, tc.source_object_name, success


def main() -> None:
    parser = argparse.ArgumentParser(description="UDM Small Tables Pipeline")
    # M-2: Default to 4 workers (not 6). On 64 GB with OS overhead (2-4 GB),
    # 4 workers get ~15 GB each — enough for 3M x 100-col CDC joins.
    # 6 workers at ~10 GB each OOM during anti-join peak memory (2.5-3x DataFrame).
    parser.add_argument("--workers", type=int, default=4, help="Number of parallel workers (default: 4, max recommended: 4 on 64 GB)")
    parser.add_argument("--table", type=str, help="Process a single table by name")
    parser.add_argument("--source", type=str, help="Filter by source name (DNA, CCM, EPICOR)")
    parser.add_argument("--list-tables", action="store_true", help="List available tables and exit")
    parser.add_argument("--force", action="store_true", help="Skip extraction row-count guard (P1-1). Use for intentional reloads.")
    parser.add_argument("--refresh-pks", action="store_true", help="P1-10: Re-discover PKs from source and update UdmTablesColumnsList.")
    args = parser.parse_args()

    logger = logging.getLogger(__name__)

    # H-4: Validate CLI arguments against known values before querying
    cli_common.validate_cli_filters(args.source, args.table)

    # Load table configs
    loader = TableConfigLoader()
    configs = loader.load_small_tables(
        source_name=args.source,
        table_name=args.table,
    )

    if args.list_tables:
        print(f"\n{'Source':<12} {'Table':<30} {'Stage Name':<30} {'Bronze Name':<30}")
        print("-" * 102)
        for tc in sorted(configs, key=lambda x: (x.source_name, x.source_object_name)):
            print(
                f"{tc.source_name:<12} {tc.source_object_name:<30} "
                f"{tc.effective_stage_name:<30} {tc.effective_bronze_name:<30}"
            )
        print(f"\nTotal: {len(configs)} small tables")
        return

    if not configs:
        print("No tables found matching the specified filters.")
        return

    # Set up tracking
    tracker = PipelineEventTracker()
    batch_id = tracker.batch_id
    sql_handler = cli_common.setup_logging(batch_id)

    # L-1: Startup checks (staging cleanup + RCSI verification)
    cli_common.startup_checks()

    # W-4: Warn if MALLOC_ARENA_MAX was not set in the external environment.
    cli_common.warn_malloc_arena()

    # M-2: Warn if workers exceed recommended maximum for 64 GB system
    cli_common.warn_workers(args.workers)

    logger.info("Starting small tables pipeline: batch_id=%d, tables=%d, workers=%d",
                batch_id, len(configs), args.workers)

    succeeded = 0
    failed = 0

    if args.workers <= 1:
        # Sequential execution
        for tc in configs:
            # B-8: RSS monitoring between table iterations.
            cli_common.check_rss_memory(tc.source_name, tc.source_object_name)
            sql_handler.set_context(batch_id=batch_id, table_name=tc.source_object_name, source_name=tc.source_name)
            from orchestration.small_tables import process_small_table
            success = process_small_table(tc, tracker, force=args.force, refresh_pks=args.refresh_pks)
            if success:
                succeeded += 1
            else:
                failed += 1
    else:
        # Parallel execution
        table_dicts = [cli_common.table_config_to_dict(tc, batch_id) for tc in configs]
        if args.force:
            for td in table_dicts:
                td["force"] = True
        if args.refresh_pks:
            for td in table_dicts:
                td["refresh_pks"] = True
        with ProcessPoolExecutor(max_workers=args.workers) as executor:
            futures = {
                executor.submit(_process_table_worker, td): td
                for td in table_dicts
            }
            for future in as_completed(futures):
                td = futures[future]
                try:
                    source, table, success = future.result()
                    if success:
                        succeeded += 1
                        logger.info("Completed: %s.%s", source, table)
                    else:
                        failed += 1
                        logger.error("Failed: %s.%s", source, table)
                except Exception:
                    failed += 1
                    logger.exception("Worker exception for %s.%s",
                                     td.get("source_name"), td.get("source_object_name"))

    # P-3: Log connection overhead before flushing.
    cli_common.log_connection_overhead()

    # Item-18: Close pooled connections at shutdown.
    cli_common.shutdown_connections()

    # Flush logs
    sql_handler.flush()

    logger.info(
        "Pipeline complete: batch_id=%d, succeeded=%d, failed=%d",
        batch_id, succeeded, failed,
    )

    sys.exit(1 if failed > 0 else 0)


if __name__ == "__main__":
    main()
