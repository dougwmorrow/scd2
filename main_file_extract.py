"""CLI entry point for file-based extraction pipeline.

Usage:
    python3 main_file_extract.py --workers 4
    python3 main_file_extract.py --table RATES --source VENDOR_A
    python3 main_file_extract.py --list-tables
    python3 main_file_extract.py --workers 4 --source VENDOR_A
    python3 main_file_extract.py --table RATES --source VENDOR_A --force
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
from orchestration.file_config import FileConfigLoader


def _file_config_to_dict(fc, batch_id: int) -> dict:
    """Serialize a FileConfig for cross-process transfer via ProcessPoolExecutor.

    Args:
        fc: FileConfig instance.
        batch_id: Current batch ID.

    Returns:
        Dict suitable for pickling across process boundaries.
    """
    return {
        "source_name": fc.source_name,
        "table_name": fc.table_name,
        "base_path": fc.base_path,
        "file_pattern": fc.file_pattern,
        "file_type": fc.file_type,
        "sheet_name": fc.sheet_name,
        "header_row": fc.header_row,
        "skip_rows": fc.skip_rows,
        "delimiter": fc.delimiter,
        "encoding": fc.encoding,
        "column_mapping": fc.column_mapping,
        "columns_to_extract": fc.columns_to_extract,
        "stage_table_name": fc.stage_table_name,
        "bronze_table_name": fc.bronze_table_name,
        "pk_column_names": fc.pk_column_names,
        "change_mode": fc.change_mode,
        "expected_frequency": fc.expected_frequency,
        "expected_min_rows": fc.expected_min_rows,
        "expected_columns": fc.expected_columns,
        "_resolved_stage_schema": fc._resolved_stage_schema,
        "_resolved_bronze_schema": fc._resolved_bronze_schema,
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
            for c in fc.columns
        ],
        "batch_id": batch_id,
        "force": False,
    }


def _process_file_worker(file_config_dict: dict) -> tuple[str, str, bool]:
    """Worker function for ProcessPoolExecutor.

    Takes a serialized file config dict (for pickling across processes),
    reconstructs the FileConfig, and processes the file table.
    """
    from orchestration.file_config import FileConfig
    from orchestration.table_config import ColumnConfig
    from orchestration.file_tables import process_file_table

    fc = FileConfig(
        source_name=file_config_dict["source_name"],
        table_name=file_config_dict["table_name"],
        base_path=file_config_dict["base_path"],
        file_pattern=file_config_dict["file_pattern"],
        file_type=file_config_dict["file_type"],
        sheet_name=file_config_dict.get("sheet_name"),
        header_row=file_config_dict.get("header_row", 0),
        skip_rows=file_config_dict.get("skip_rows", 0),
        delimiter=file_config_dict.get("delimiter"),
        encoding=file_config_dict.get("encoding", "utf-8"),
        column_mapping=file_config_dict.get("column_mapping"),
        columns_to_extract=file_config_dict.get("columns_to_extract"),
        stage_table_name=file_config_dict.get("stage_table_name"),
        bronze_table_name=file_config_dict.get("bronze_table_name"),
        pk_column_names=file_config_dict.get("pk_column_names", []),
        change_mode=file_config_dict.get("change_mode", "full_replace"),
        expected_frequency=file_config_dict.get("expected_frequency"),
        expected_min_rows=file_config_dict.get("expected_min_rows", 1),
        expected_columns=file_config_dict.get("expected_columns"),
        columns=[
            ColumnConfig(**col) for col in file_config_dict.get("columns", [])
        ],
        _resolved_stage_schema=file_config_dict.get("_resolved_stage_schema"),
        _resolved_bronze_schema=file_config_dict.get("_resolved_bronze_schema"),
    )

    tracker = PipelineEventTracker()
    tracker._batch_id = file_config_dict["batch_id"]

    force = file_config_dict.get("force", False)
    success = process_file_table(fc, tracker, force=force)
    return fc.source_name, fc.table_name, success


def _validate_file_cli_filters(
    source_name: str | None,
    table_name: str | None,
) -> None:
    """H-4: Validate --source and --table CLI arguments against FileExtract."""
    if source_name is None and table_name is None:
        return

    logger = logging.getLogger(__name__)
    loader = FileConfigLoader()

    if source_name:
        known_sources = loader.get_known_sources()
        if source_name not in known_sources:
            logger.error(
                "H-4: --source '%s' not found in FileExtract. "
                "Known sources: %s",
                source_name, sorted(known_sources),
            )
            sys.exit(1)

    if table_name:
        known_tables = loader.get_known_tables()
        if table_name not in known_tables:
            logger.error(
                "H-4: --table '%s' not found in FileExtract. "
                "Known tables: %s",
                table_name, sorted(known_tables),
            )
            sys.exit(1)


def main() -> None:
    parser = argparse.ArgumentParser(description="UDM File Extraction Pipeline")
    parser.add_argument("--workers", type=int, default=4, help="Number of parallel workers (default: 4)")
    parser.add_argument("--table", type=str, help="Process a single table by name")
    parser.add_argument("--source", type=str, help="Filter by source name")
    parser.add_argument("--list-tables", action="store_true", help="List available file tables and exit")
    parser.add_argument("--force", action="store_true", help="Skip extraction row-count guard (P1-1)")
    args = parser.parse_args()

    logger = logging.getLogger(__name__)

    # H-4: Validate CLI arguments
    _validate_file_cli_filters(args.source, args.table)

    # Load file configs
    loader = FileConfigLoader()
    configs = loader.load_file_configs(
        source_name=args.source,
        table_name=args.table,
    )

    if args.list_tables:
        print(f"\n{'Source':<15} {'Table':<25} {'Type':<6} {'Pattern':<30} {'Path'}")
        print("-" * 110)
        for fc in sorted(configs, key=lambda x: (x.source_name, x.table_name)):
            print(
                f"{fc.source_name:<15} {fc.table_name:<25} "
                f"{fc.file_type:<6} {fc.file_pattern:<30} {fc.base_path}"
            )
        print(f"\nTotal: {len(configs)} file tables")
        return

    if not configs:
        print("No file tables found matching the specified filters.")
        return

    # Set up tracking
    tracker = PipelineEventTracker()
    batch_id = tracker.batch_id
    sql_handler = cli_common.setup_logging(batch_id)

    # L-1: Startup checks
    cli_common.startup_checks()

    # W-4: Warn if MALLOC_ARENA_MAX was not set externally
    cli_common.warn_malloc_arena()

    # M-2: Warn if workers exceed recommended maximum
    cli_common.warn_workers(args.workers)

    logger.info(
        "Starting file extraction pipeline: batch_id=%d, tables=%d, workers=%d",
        batch_id, len(configs), args.workers,
    )

    succeeded = 0
    failed = 0

    if args.workers <= 1:
        # Sequential execution
        for fc in configs:
            cli_common.check_rss_memory(fc.source_name, fc.table_name)
            sql_handler.set_context(
                batch_id=batch_id,
                table_name=fc.table_name,
                source_name=fc.source_name,
            )
            from orchestration.file_tables import process_file_table
            success = process_file_table(fc, tracker, force=args.force)
            if success:
                succeeded += 1
            else:
                failed += 1
    else:
        # Parallel execution
        file_dicts = [_file_config_to_dict(fc, batch_id) for fc in configs]
        if args.force:
            for fd in file_dicts:
                fd["force"] = True
        with ProcessPoolExecutor(max_workers=args.workers) as executor:
            futures = {
                executor.submit(_process_file_worker, fd): fd
                for fd in file_dicts
            }
            for future in as_completed(futures):
                fd = futures[future]
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
                    logger.exception(
                        "Worker exception for %s.%s",
                        fd.get("source_name"), fd.get("table_name"),
                    )

    # P-3: Log connection overhead
    cli_common.log_connection_overhead()

    # Item-18: Close pooled connections
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
