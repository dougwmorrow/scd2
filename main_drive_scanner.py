"""CLI entry point for network drive metadata scanner pipeline.

Usage:
    python3 main_drive_scanner.py --workers 7
    python3 main_drive_scanner.py --drive FINANCE_SHARE
    python3 main_drive_scanner.py --list-drives
    python3 main_drive_scanner.py --drive FINANCE_SHARE --force
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
from orchestration.drive_config import DriveConfigLoader


def _drive_config_to_dict(dc, batch_id: int) -> dict:
    """Serialize a DriveConfig for cross-process transfer via ProcessPoolExecutor."""
    return {
        "source_name": dc.source_name,
        "table_name": dc.table_name,
        "drive_name": dc.drive_name,
        "mount_path": dc.mount_path,
        "unc_path": dc.unc_path,
        "exclude_patterns": dc.exclude_patterns,
        "include_patterns": dc.include_patterns,
        "max_depth": dc.max_depth,
        "follow_symlinks": dc.follow_symlinks,
        "stage_table_name": dc.stage_table_name,
        "bronze_table_name": dc.bronze_table_name,
        "pk_column_names": dc.pk_column_names,
        "_resolved_stage_schema": dc._resolved_stage_schema,
        "_resolved_bronze_schema": dc._resolved_bronze_schema,
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
            for c in dc.columns
        ],
        "batch_id": batch_id,
        "force": False,
    }


def _process_drive_worker(drive_config_dict: dict) -> tuple[str, str, bool]:
    """Worker function for ProcessPoolExecutor."""
    from orchestration.drive_config import DriveConfig
    from orchestration.table_config import ColumnConfig
    from orchestration.drive_tables import process_drive_table

    dc = DriveConfig(
        source_name=drive_config_dict["source_name"],
        table_name=drive_config_dict["table_name"],
        drive_name=drive_config_dict["drive_name"],
        mount_path=drive_config_dict["mount_path"],
        unc_path=drive_config_dict.get("unc_path"),
        exclude_patterns=drive_config_dict.get("exclude_patterns"),
        include_patterns=drive_config_dict.get("include_patterns"),
        max_depth=drive_config_dict.get("max_depth"),
        follow_symlinks=drive_config_dict.get("follow_symlinks", False),
        stage_table_name=drive_config_dict.get("stage_table_name"),
        bronze_table_name=drive_config_dict.get("bronze_table_name"),
        pk_column_names=drive_config_dict.get("pk_column_names", ["drive_name", "full_file_path"]),
        columns=[
            ColumnConfig(**col) for col in drive_config_dict.get("columns", [])
        ],
        _resolved_stage_schema=drive_config_dict.get("_resolved_stage_schema"),
        _resolved_bronze_schema=drive_config_dict.get("_resolved_bronze_schema"),
    )

    tracker = PipelineEventTracker()
    tracker._batch_id = drive_config_dict["batch_id"]

    force = drive_config_dict.get("force", False)
    success = process_drive_table(dc, tracker, force=force)
    return dc.source_name, dc.drive_name, success


def _validate_drive_cli_filters(drive_name: str | None) -> None:
    """H-4: Validate --drive CLI argument against NetworkDriveConfig."""
    if drive_name is None:
        return

    logger = logging.getLogger(__name__)
    loader = DriveConfigLoader()
    known_drives = loader.get_known_drives()

    if drive_name not in known_drives:
        logger.error(
            "H-4: --drive '%s' not found in NetworkDriveConfig. "
            "Known drives: %s",
            drive_name, sorted(known_drives),
        )
        sys.exit(1)


def main() -> None:
    parser = argparse.ArgumentParser(description="UDM Network Drive Metadata Scanner")
    parser.add_argument(
        "--workers", type=int, default=7,
        help="Number of parallel workers — one per drive (default: 7)",
    )
    parser.add_argument("--drive", type=str, help="Scan a single drive by name")
    parser.add_argument("--list-drives", action="store_true", help="List configured drives and exit")
    parser.add_argument("--force", action="store_true", help="Skip extraction guard (P1-1)")
    args = parser.parse_args()

    logger = logging.getLogger(__name__)

    # H-4: Validate CLI arguments
    _validate_drive_cli_filters(args.drive)

    # Load drive configs
    loader = DriveConfigLoader()
    configs = loader.load_drive_configs(drive_name=args.drive)

    if args.list_drives:
        print(f"\n{'Drive':<20} {'Table':<25} {'Source':<18} {'Mount Path'}")
        print("-" * 100)
        for dc in sorted(configs, key=lambda x: x.drive_name):
            print(
                f"{dc.drive_name:<20} {dc.table_name:<25} "
                f"{dc.source_name:<18} {dc.mount_path}"
            )
        print(f"\nTotal: {len(configs)} drives configured")
        return

    if not configs:
        print("No drives found matching the specified filters.")
        return

    # Set up tracking
    tracker = PipelineEventTracker()
    batch_id = tracker.batch_id
    sql_handler = cli_common.setup_logging(batch_id)

    cli_common.startup_checks()
    cli_common.warn_malloc_arena()

    logger.info(
        "Starting drive scanner: batch_id=%d, drives=%d, workers=%d",
        batch_id, len(configs), args.workers,
    )

    succeeded = 0
    failed = 0

    if args.workers <= 1:
        for dc in configs:
            cli_common.check_rss_memory(dc.source_name, dc.drive_name)
            sql_handler.set_context(
                batch_id=batch_id,
                table_name=dc.table_name,
                source_name=dc.source_name,
            )
            from orchestration.drive_tables import process_drive_table
            success = process_drive_table(dc, tracker, force=args.force)
            if success:
                succeeded += 1
            else:
                failed += 1
    else:
        drive_dicts = [_drive_config_to_dict(dc, batch_id) for dc in configs]
        if args.force:
            for dd in drive_dicts:
                dd["force"] = True
        with ProcessPoolExecutor(max_workers=args.workers) as executor:
            futures = {
                executor.submit(_process_drive_worker, dd): dd
                for dd in drive_dicts
            }
            for future in as_completed(futures):
                dd = futures[future]
                try:
                    source, drive, success = future.result()
                    if success:
                        succeeded += 1
                        logger.info("Completed: %s", drive)
                    else:
                        failed += 1
                        logger.error("Failed: %s", drive)
                except Exception:
                    failed += 1
                    logger.exception(
                        "Worker exception for %s", dd.get("drive_name"),
                    )

    cli_common.log_connection_overhead()
    cli_common.shutdown_connections()
    sql_handler.flush()

    logger.info(
        "Scanner complete: batch_id=%d, succeeded=%d, failed=%d",
        batch_id, succeeded, failed,
    )

    sys.exit(1 if failed > 0 else 0)


if __name__ == "__main__":
    main()
