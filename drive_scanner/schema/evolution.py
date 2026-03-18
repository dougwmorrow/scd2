"""Schema evolution: detect and handle source column changes (P0-2).

Compares DataFrame columns against existing target table columns on every run.
Handles:
  - New columns: ALTER TABLE ADD (both Stage and Bronze)
  - Removed columns: WARNING log, do NOT drop (data preservation)
  - Type changes: ERROR log, raises SchemaEvolutionError to skip the table

Runs after ensure_stage_table / ensure_bronze_table but before CDC.
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

import polars as pl

import connections
from connections import quote_identifier, quote_table
from data_load.schema_utils import ColumnMetadata, get_column_metadata
from extract.udm_connectorx_extractor import table_exists
from schema.table_creator import _polars_dtype_to_sql

if TYPE_CHECKING:
    from orchestration.table_config import TableConfig

logger = logging.getLogger(__name__)


class SchemaEvolutionError(Exception):
    """Raised when schema drift cannot be auto-resolved (e.g. type change)."""


@dataclass
class SchemaEvolutionResult:
    """B-3: Result of schema evolution, propagated to orchestrators.

    When columns_added is non-empty, all row hashes change on the next run
    (new column included in hash). CDC sees every row as "updated." The
    orchestrator uses hash_affecting_change to suppress the E-12 phantom
    change rate warning and log the mass-update as expected behavior.
    """

    columns_added: list[str] = field(default_factory=list)
    columns_removed: list[str] = field(default_factory=list)
    hash_affecting_change: bool = False


def validate_source_schema(table_config: TableConfig, df: pl.DataFrame) -> list[str]:
    """E-11: Pre-extraction schema validation against expected column contract.

    Compares the extracted DataFrame's columns against the expected columns
    from UdmTablesColumnsList. Catches column renames, drops, and unexpected
    additions before CDC/SCD2 processing begins.

    Only runs if UdmTablesColumnsList has been populated for this table
    (i.e., not on the very first run before column_sync).

    Args:
        table_config: Table configuration with column metadata.
        df: Freshly extracted DataFrame.

    Returns:
        List of warning/error messages. Empty if validation passes.
    """
    # Skip if no columns loaded yet (first run — column_sync hasn't run)
    stage_cols = [
        c.column_name for c in table_config.columns
        if c.layer == "Stage"
    ]
    if not stage_cols:
        return []

    # Only compare source columns (exclude internal _ prefixed columns)
    df_cols = {c for c in df.columns if not c.startswith("_")}
    expected_cols = {c for c in stage_cols if not c.startswith("_")}

    if not expected_cols:
        return []

    warnings = []
    missing = expected_cols - df_cols
    unexpected = df_cols - expected_cols

    if missing:
        msg = (
            f"E-11: Source schema validation for {table_config.source_name}."
            f"{table_config.source_object_name}: {len(missing)} expected "
            f"column(s) MISSING from extraction: {sorted(missing)}. "
            f"Possible column rename or drop in source."
        )
        logger.error(msg)
        warnings.append(msg)

    if unexpected:
        msg = (
            f"E-11: Source schema validation for {table_config.source_name}."
            f"{table_config.source_object_name}: {len(unexpected)} UNEXPECTED "
            f"column(s) in extraction: {sorted(unexpected)}. "
            f"New columns will be handled by schema evolution."
        )
        logger.warning(msg)
        warnings.append(msg)

    if not missing and not unexpected:
        logger.debug(
            "E-11: Schema validation passed for %s.%s (%d columns match)",
            table_config.source_name, table_config.source_object_name,
            len(expected_cols),
        )

    return warnings


def evolve_schema(table_config: TableConfig, df: pl.DataFrame) -> SchemaEvolutionResult:
    """Compare DataFrame columns against Stage/Bronze tables and apply schema evolution.

    B-3: Returns SchemaEvolutionResult so orchestrators can condition E-12 warnings
    during schema migration runs (new columns change all hashes → mass CDC updates).

    Args:
        table_config: Table configuration.
        df: Freshly extracted DataFrame (with source columns, before CDC/SCD2 columns).

    Returns:
        SchemaEvolutionResult with columns_added, columns_removed, hash_affecting_change.

    Raises:
        SchemaEvolutionError: If a column type changed in the source (cannot auto-resolve).
    """
    result = SchemaEvolutionResult()

    # Only source columns (no _ prefixed internal columns)
    source_cols = {
        col: dtype for col, dtype in zip(df.columns, df.dtypes)
        if not col.startswith("_")
    }

    if table_exists(table_config.stage_full_table_name):
        added, removed = _evolve_table(
            table_config.stage_full_table_name,
            source_cols,
            table_config,
            "Stage",
        )
        result.columns_added.extend(added)
        result.columns_removed.extend(removed)

    if table_exists(table_config.bronze_full_table_name):
        added, removed = _evolve_table(
            table_config.bronze_full_table_name,
            source_cols,
            table_config,
            "Bronze",
        )
        # Only count unique columns (may overlap between Stage and Bronze)
        for col in added:
            if col not in result.columns_added:
                result.columns_added.append(col)
        for col in removed:
            if col not in result.columns_removed:
                result.columns_removed.append(col)

    # B-3: New columns change hash output (new column included in hash computation)
    result.hash_affecting_change = len(result.columns_added) > 0

    return result


def _evolve_table(
    full_table_name: str,
    source_cols: dict[str, pl.DataType],
    table_config: TableConfig,
    layer: str,
) -> tuple[list[str], list[str]]:
    """Compare and evolve a single target table's schema.

    Args:
        full_table_name: e.g. 'UDM_Stage.DNA.ACCT_cdc'
        source_cols: Dict of {column_name: polars_dtype} from the extraction DataFrame.
        table_config: Table config for context.
        layer: 'Stage' or 'Bronze' for logging.

    Returns:
        Tuple of (added_columns, removed_columns) lists.
    """
    existing_meta = get_column_metadata(full_table_name)

    # Build sets for comparison — only look at source columns, not CDC/SCD2 internal ones
    internal_cols = {
        "_row_hash", "_extracted_at",
        "_cdc_operation", "_cdc_valid_from", "_cdc_valid_to",
        "_cdc_is_current", "_cdc_batch_id",
        "_scd2_key",
        "UdmHash", "UdmEffectiveDateTime", "UdmEndDateTime",
        "UdmActiveFlag", "UdmScd2Operation",
    }

    existing_source_cols = {
        m.column_name: m
        for m in existing_meta
        if m.column_name not in internal_cols
    }

    existing_names = set(existing_source_cols.keys())
    new_names = set(source_cols.keys())

    # --- New columns: ALTER TABLE ADD ---
    added = new_names - existing_names
    if added:
        logger.info(
            "%s schema evolution for %s: adding %d new column(s): %s",
            layer, full_table_name, len(added), sorted(added),
        )
        _add_columns(full_table_name, {col: source_cols[col] for col in added})

        # P1-15: Warn that next run will see mass-update cascade.
        # Adding a column changes the hash (new column included in hash computation).
        # All stored hashes were computed WITHOUT this column, so every row
        # will appear "updated" on the next run.
        logger.warning(
            "P1-15: %d column(s) added to %s %s: %s. "
            "Next run will rehash all rows — expect a mass-update cascade "
            "(every row will appear changed due to new column in hash). "
            "This is correct but may be slow for large tables.",
            len(added), layer, full_table_name, sorted(added),
        )

    # --- Removed columns: WARNING only, never drop ---
    removed = existing_names - new_names
    if removed:
        logger.warning(
            "%s schema evolution for %s: source no longer has %d column(s): %s. "
            "Columns will NOT be dropped — existing data preserved.",
            layer, full_table_name, len(removed), sorted(removed),
        )

    # V-12: Column rename detection heuristic.
    # When columns are simultaneously added and removed, check if any pair
    # shares the same SQL type and a similar name (Levenshtein distance <= 3).
    if added and removed:
        _detect_potential_renames(added, removed, source_cols, existing_source_cols,
                                 full_table_name, layer)

    # --- Type changes: ERROR, raise to skip table ---
    # P1-9: Compare resolved SQL Server types rather than raw Polars dtypes.
    # ConnectorX may return different Polars dtypes for the same column across runs
    # (e.g. Int32 vs Int64 depending on data range). Comparing SQL types avoids
    # false positives from ConnectorX dtype instability.
    common = existing_names & new_names
    type_mismatches = []
    for col_name in common:
        existing_type = existing_source_cols[col_name].data_type.upper()
        new_sql_type = _polars_dtype_to_sql(source_cols[col_name]).upper()

        # Normalize for comparison (INFORMATION_SCHEMA returns base types)
        if not _types_compatible(existing_type, new_sql_type):
            type_mismatches.append(
                f"{col_name}: {existing_type} -> {new_sql_type} "
                f"(Polars dtype: {source_cols[col_name]})"
            )

    if type_mismatches:
        msg = (
            f"{layer} schema evolution for {full_table_name}: "
            f"column type changes detected (cannot auto-resolve): "
            f"{type_mismatches}"
        )
        logger.error(msg)
        # W-17: Record the type change in the quarantine table for investigation.
        quarantine_record(
            table_name=table_config.source_object_name,
            source_name=table_config.source_name,
            rejection_reason=f"Schema type change: {type_mismatches}",
        )
        raise SchemaEvolutionError(msg)

    # O-2: Structured JSON for schema evolution signals.
    if added or removed or type_mismatches:
        logger.info(
            "O-2_SCHEMA: %s",
            json.dumps({
                "signal": "schema_evolution",
                "source": table_config.source_name,
                "table": table_config.source_object_name,
                "layer": layer,
                "columns_added": sorted(added),
                "columns_removed": sorted(removed),
                "type_mismatches": type_mismatches,
                "hash_affecting_change": len(added) > 0,
            }),
        )

    return sorted(added), sorted(removed)


def _types_compatible(existing: str, new: str) -> bool:
    """Check if two SQL Server types are compatible (allowing safe widening)."""
    # Normalize — strip parens for base type comparison
    existing_base = existing.split("(")[0].strip()
    new_base = new.split("(")[0].strip()

    if existing_base == new_base:
        return True

    # Allow common safe widenings (both directions for ConnectorX dtype instability)
    # P1-9: ConnectorX may return Int32 one run and Int64 the next for the same column.
    # Both map to different SQL types but the SQL Server column hasn't changed.
    compatible_pairs = {
        ("INT", "BIGINT"),
        ("BIGINT", "INT"),  # ConnectorX returned wider type first, narrower later
        ("SMALLINT", "INT"),
        ("SMALLINT", "BIGINT"),
        ("INT", "SMALLINT"),  # ConnectorX dtype drift
        ("TINYINT", "SMALLINT"),
        ("TINYINT", "INT"),
        ("TINYINT", "BIGINT"),
        ("FLOAT", "FLOAT"),
        ("REAL", "FLOAT"),
        ("FLOAT", "REAL"),
        ("DATE", "DATETIME2"),
        ("DATETIME", "DATETIME2"),
        ("DATETIME2", "DATETIME"),
        ("NVARCHAR", "NVARCHAR"),
        ("VARCHAR", "NVARCHAR"),
        ("VARCHAR", "VARCHAR"),
        ("NTEXT", "NVARCHAR"),
        ("TEXT", "VARCHAR"),
    }

    return (existing_base, new_base) in compatible_pairs


def _detect_potential_renames(
    added: set[str],
    removed: set[str],
    source_cols: dict[str, pl.DataType],
    existing_source_cols: dict[str, ColumnMetadata],
    full_table_name: str,
    layer: str,
) -> None:
    """V-12: Detect potential column renames (simultaneous add + remove with same type).

    Uses Levenshtein distance <= 3 as a heuristic for similar names.
    Logs WARNING if potential renames are found — no automated action taken.
    """
    potential_renames = []

    for new_col in added:
        new_sql_type = _polars_dtype_to_sql(source_cols[new_col]).upper().split("(")[0]
        for old_col in removed:
            old_sql_type = existing_source_cols[old_col].data_type.upper().split("(")[0]

            if new_sql_type != old_sql_type:
                continue

            distance = _levenshtein(new_col.lower(), old_col.lower())
            if distance <= 3:
                potential_renames.append(
                    f"[{old_col}] -> [{new_col}] (type={new_sql_type}, distance={distance})"
                )

    if potential_renames:
        logger.warning(
            "V-12: %s schema evolution for %s: potential column RENAME(s) detected: %s. "
            "A rename appears as simultaneous drop + add. If confirmed, this will "
            "trigger a full CDC update wave (all row hashes change). Coordinate with "
            "the pipeline team to minimize impact on large tables.",
            layer, full_table_name, potential_renames,
        )


def _levenshtein(s1: str, s2: str) -> int:
    """Compute Levenshtein edit distance between two strings."""
    if len(s1) < len(s2):
        return _levenshtein(s2, s1)
    if len(s2) == 0:
        return len(s1)

    prev_row = list(range(len(s2) + 1))
    for i, c1 in enumerate(s1):
        curr_row = [i + 1]
        for j, c2 in enumerate(s2):
            insertions = prev_row[j + 1] + 1
            deletions = curr_row[j] + 1
            substitutions = prev_row[j] + (c1 != c2)
            curr_row.append(min(insertions, deletions, substitutions))
        prev_row = curr_row

    return prev_row[-1]


# ---------------------------------------------------------------------------
# W-17: Schema contract quarantine table
# ---------------------------------------------------------------------------

_QUARANTINE_TABLE = "General.ops.Quarantine"


def ensure_quarantine_table() -> None:
    """W-17: Create the quarantine table if it doesn't exist.

    Quarantine table stores records that fail schema contracts, allowing
    the pipeline to continue processing valid records while isolating
    problematic ones for investigation.

    Schema:
        QuarantineId    BIGINT IDENTITY(1,1) PRIMARY KEY
        TableName       NVARCHAR(255)
        SourceName      NVARCHAR(255)
        RejectionReason NVARCHAR(MAX)
        RecordData      NVARCHAR(MAX)   -- JSON representation
        BatchId         BIGINT
        QuarantinedAt   DATETIME2 DEFAULT GETUTCDATE()
    """
    from extract.udm_connectorx_extractor import table_exists

    if table_exists(_QUARANTINE_TABLE):
        return

    conn = connections.get_connection("General")
    try:
        cursor = conn.cursor()
        cursor.execute("""
            IF NOT EXISTS (
                SELECT 1 FROM General.INFORMATION_SCHEMA.TABLES
                WHERE TABLE_SCHEMA = 'ops' AND TABLE_NAME = 'Quarantine'
            )
            BEGIN
                IF NOT EXISTS (SELECT 1 FROM General.sys.schemas WHERE name = 'ops')
                    EXEC('CREATE SCHEMA [ops]')

                CREATE TABLE General.ops.Quarantine (
                    QuarantineId BIGINT IDENTITY(1,1) PRIMARY KEY,
                    TableName NVARCHAR(255) NOT NULL,
                    SourceName NVARCHAR(255) NOT NULL,
                    RejectionReason NVARCHAR(MAX) NOT NULL,
                    RecordData NVARCHAR(MAX) NULL,
                    BatchId BIGINT NULL,
                    QuarantinedAt DATETIME2 NOT NULL DEFAULT GETUTCDATE()
                )
            END
        """)
        cursor.close()
        logger.info("W-17: Created quarantine table %s", _QUARANTINE_TABLE)
    except Exception:
        logger.warning(
            "W-17: Could not create quarantine table %s — quarantine disabled",
            _QUARANTINE_TABLE, exc_info=True,
        )
    finally:
        conn.close()


def quarantine_record(
    table_name: str,
    source_name: str,
    rejection_reason: str,
    record_data: str | None = None,
    batch_id: int | None = None,
) -> None:
    """W-17: Write a single record to the quarantine table.

    Args:
        table_name: The table that failed the schema contract.
        source_name: The source system name.
        rejection_reason: Why the record was quarantined.
        record_data: JSON string of the record data (optional).
        batch_id: Pipeline batch ID (optional).
    """
    try:
        conn = connections.get_connection("General")
        try:
            cursor = conn.cursor()
            cursor.execute(
                f"INSERT INTO {_QUARANTINE_TABLE} "
                "(TableName, SourceName, RejectionReason, RecordData, BatchId) "
                "VALUES (?, ?, ?, ?, ?)",
                table_name, source_name, rejection_reason, record_data, batch_id,
            )
            cursor.close()
        finally:
            conn.close()
    except Exception:
        logger.debug(
            "W-17: Could not write quarantine record for %s.%s — continuing",
            source_name, table_name, exc_info=True,
        )


def quarantine_batch(
    table_name: str,
    source_name: str,
    rejection_reason: str,
    records: list[dict],
    batch_id: int | None = None,
    max_records: int = 100,
) -> int:
    """W-17: Write multiple records to the quarantine table.

    Limits to max_records to prevent quarantine table bloat from
    systematic failures (e.g., every row failing a type check).

    Args:
        table_name: The table that failed the schema contract.
        source_name: The source system name.
        rejection_reason: Why the records were quarantined.
        records: List of record dicts to quarantine.
        batch_id: Pipeline batch ID.
        max_records: Maximum records to quarantine per call.

    Returns:
        Number of records actually quarantined.
    """
    import json

    to_write = records[:max_records]
    written = 0

    try:
        conn = connections.get_connection("General")
        try:
            cursor = conn.cursor()
            for record in to_write:
                try:
                    record_json = json.dumps(record, default=str)
                except (TypeError, ValueError):
                    record_json = str(record)

                cursor.execute(
                    f"INSERT INTO {_QUARANTINE_TABLE} "
                    "(TableName, SourceName, RejectionReason, RecordData, BatchId) "
                    "VALUES (?, ?, ?, ?, ?)",
                    table_name, source_name, rejection_reason, record_json, batch_id,
                )
                written += 1
            cursor.close()
        finally:
            conn.close()
    except Exception:
        logger.debug(
            "W-17: Could not write quarantine batch for %s.%s — continuing",
            source_name, table_name, exc_info=True,
        )

    if written > 0:
        truncated = len(records) - written
        logger.info(
            "W-17: Quarantined %d records for %s.%s (reason: %s%s)",
            written, source_name, table_name, rejection_reason[:100],
            f", {truncated} more truncated" if truncated > 0 else "",
        )

    return written


def _add_columns(
    full_table_name: str,
    columns: dict[str, pl.DataType],
) -> None:
    """ALTER TABLE ADD for new source columns."""
    db = full_table_name.split(".")[0]

    conn = connections.get_connection(db)
    try:
        cursor = conn.cursor()
        for col_name, dtype in columns.items():
            sql_type = _polars_dtype_to_sql(dtype)
            alter_sql = f"ALTER TABLE {quote_table(full_table_name)} ADD {quote_identifier(col_name)} {sql_type} NULL"
            try:
                cursor.execute(alter_sql)
                logger.info("Added column [%s] %s to %s", col_name, sql_type, full_table_name)
            except Exception:
                logger.exception(
                    "Failed to add column [%s] to %s", col_name, full_table_name
                )
        cursor.close()
    finally:
        conn.close()
