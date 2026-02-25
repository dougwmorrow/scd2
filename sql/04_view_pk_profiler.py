"""
04_view_pk_profiler.py

View Primary Key Candidate Profiler

Purpose:
    Views (Oracle and SQL Server) don't have PK constraints or indexes.
    This script connects to source systems and profiles view columns to
    identify which columns (or combinations) are unique and NOT NULL —
    i.e., valid PK candidates for UdmTablesColumnsList.IsPrimaryKey.

Approach:
    1. Read UdmTablesList to find all pipeline tables
    2. For each source, identify which objects are views
    3. For views, run COUNT(*) vs COUNT(DISTINCT col) queries on
       candidate columns (NOT NULL + naming heuristics)
    4. Report uniqueness ratios and recommend PK candidates
    5. Test composite key uniqueness for multi-column PKs

Requirements:
    - Same environment as the pipeline (config.py, sources.py, connections.py)
    - Oracle Instant Client (for DNA) and ODBC Driver 18 (for CCM/EPICOR)

Usage:
    python 04_view_pk_profiler.py                        # All sources
    python 04_view_pk_profiler.py --source DNA           # Oracle only
    python 04_view_pk_profiler.py --source CCM           # SQL Server only
    python 04_view_pk_profiler.py --table MY_VIEW        # Specific object
    python 04_view_pk_profiler.py --source DNA --max-rows 100000  # Sample
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
from dataclasses import dataclass, field

# Add parent directory to path for pipeline imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import config
import connections
from sources import SourceType, get_source

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)


# ============================================================================
# Data Models
# ============================================================================

@dataclass
class ColumnProfile:
    """Profiling results for a single column."""
    column_name: str
    data_type: str
    is_nullable: bool
    total_rows: int = 0
    distinct_count: int = 0
    null_count: int = 0
    uniqueness_ratio: float = 0.0  # distinct / total
    is_unique: bool = False
    pk_candidate_score: str = "LOW"
    naming_match: bool = False


@dataclass
class CompositeKeyTest:
    """Result of testing a composite key combination."""
    columns: list[str]
    total_rows: int = 0
    distinct_count: int = 0
    is_unique: bool = False


@dataclass
class ViewProfile:
    """Complete profiling results for a view."""
    source_name: str
    schema_name: str
    view_name: str
    object_type: str  # VIEW, MATERIALIZED VIEW, TABLE (if no PK)
    total_rows: int = 0
    column_profiles: list[ColumnProfile] = field(default_factory=list)
    composite_tests: list[CompositeKeyTest] = field(default_factory=list)
    recommended_pk: list[str] = field(default_factory=list)
    recommendation_reason: str = ""


# ============================================================================
# Column Name Heuristics
# ============================================================================

PK_NAME_PATTERNS = [
    "_ID", "_KEY", "_CODE", "_NUM", "_NO", "_NBR",
    "_SEQ", "_NUMBER", "ID_",
]

PK_EXACT_NAMES = {"ID", "ROWID", "KEY", "CODE"}


def _is_pk_name_pattern(column_name: str) -> bool:
    """Check if column name matches common PK naming conventions."""
    upper = column_name.upper()
    if upper in PK_EXACT_NAMES:
        return True
    return any(upper.endswith(p) or upper.startswith(p) for p in PK_NAME_PATTERNS)


# ============================================================================
# Oracle Profiling
# ============================================================================

def _profile_oracle_views(
    source_name: str,
    schema: str,
    target_table: str | None = None,
    max_rows: int | None = None,
) -> list[ViewProfile]:
    """Profile Oracle views to find PK candidates."""
    import oracledb

    source = get_source(source_name)
    if source.source_type != SourceType.ORACLE:
        return []

    # Initialize thick mode
    oracledb.init_oracle_client(lib_dir=config.ORACLE_CLIENT_DIR)
    connect_params = source.oracledb_connect_params()
    schema_upper = schema.upper()

    conn = oracledb.connect(**connect_params)
    profiles = []

    try:
        # Step 1: Identify views (and tables without PKs)
        cursor = conn.cursor()

        if target_table:
            # Check specific object
            cursor.execute(
                """
                SELECT OBJECT_NAME, OBJECT_TYPE
                FROM ALL_OBJECTS
                WHERE OWNER = :schema
                  AND OBJECT_NAME = :table_name
                  AND OBJECT_TYPE IN ('VIEW', 'MATERIALIZED VIEW', 'TABLE')
                """,
                schema=schema_upper, table_name=target_table.upper(),
            )
        else:
            # Find all views + tables without PKs
            cursor.execute(
                """
                SELECT obj.OBJECT_NAME, obj.OBJECT_TYPE
                FROM ALL_OBJECTS obj
                WHERE obj.OWNER = :schema
                  AND obj.OBJECT_TYPE IN ('VIEW', 'MATERIALIZED VIEW')
                UNION ALL
                SELECT obj.OBJECT_NAME, obj.OBJECT_TYPE
                FROM ALL_OBJECTS obj
                WHERE obj.OWNER = :schema
                  AND obj.OBJECT_TYPE = 'TABLE'
                  AND NOT EXISTS (
                      SELECT 1 FROM ALL_CONSTRAINTS ac
                      WHERE ac.OWNER = obj.OWNER
                        AND ac.TABLE_NAME = obj.OBJECT_NAME
                        AND ac.CONSTRAINT_TYPE = 'P'
                  )
                  AND NOT EXISTS (
                      SELECT 1 FROM ALL_INDEXES ai
                      WHERE ai.TABLE_OWNER = obj.OWNER
                        AND ai.TABLE_NAME = obj.OBJECT_NAME
                        AND ai.UNIQUENESS = 'UNIQUE'
                  )
                ORDER BY 1
                """,
                schema=schema_upper,
            )

        objects = cursor.fetchall()
        cursor.close()

        for obj_name, obj_type in objects:
            logger.info("Profiling Oracle %s: %s.%s", obj_type, schema_upper, obj_name)
            profile = _profile_oracle_single(
                conn, schema_upper, obj_name, obj_type,
                source_name, max_rows,
            )
            profiles.append(profile)

    finally:
        conn.close()

    return profiles


def _profile_oracle_single(
    conn,
    schema: str,
    view_name: str,
    obj_type: str,
    source_name: str,
    max_rows: int | None = None,
) -> ViewProfile:
    """Profile a single Oracle view/table for PK candidates."""
    profile = ViewProfile(
        source_name=source_name,
        schema_name=schema,
        view_name=view_name,
        object_type=obj_type,
    )

    cursor = conn.cursor()

    # Get column metadata
    cursor.execute(
        """
        SELECT COLUMN_NAME, DATA_TYPE, NULLABLE, DATA_PRECISION, DATA_SCALE
        FROM ALL_TAB_COLUMNS
        WHERE OWNER = :schema AND TABLE_NAME = :table_name
        ORDER BY COLUMN_ID
        """,
        schema=schema, table_name=view_name,
    )
    columns = cursor.fetchall()

    # Get total row count (with optional sampling)
    if max_rows:
        cursor.execute(
            f"SELECT COUNT(*) FROM (SELECT 1 FROM {schema}.{view_name} WHERE ROWNUM <= :mr)",
            mr=max_rows,
        )
    else:
        cursor.execute(f"SELECT COUNT(*) FROM {schema}.{view_name}")

    profile.total_rows = cursor.fetchone()[0]
    logger.info("  Total rows: %d", profile.total_rows)

    if profile.total_rows == 0:
        logger.warning("  Empty view — cannot profile")
        cursor.close()
        return profile

    # Profile each column
    sample_clause = f"WHERE ROWNUM <= {max_rows}" if max_rows else ""

    for col_name, data_type, nullable, *_ in columns:
        col_profile = ColumnProfile(
            column_name=col_name,
            data_type=data_type,
            is_nullable=(nullable == 'Y'),
            total_rows=profile.total_rows,
            naming_match=_is_pk_name_pattern(col_name),
        )

        try:
            if max_rows:
                cursor.execute(
                    f"""
                    SELECT COUNT(DISTINCT "{col_name}"), 
                           SUM(CASE WHEN "{col_name}" IS NULL THEN 1 ELSE 0 END)
                    FROM (SELECT "{col_name}" FROM {schema}.{view_name} WHERE ROWNUM <= :mr)
                    """,
                    mr=max_rows,
                )
            else:
                cursor.execute(
                    f"""
                    SELECT COUNT(DISTINCT "{col_name}"), 
                           SUM(CASE WHEN "{col_name}" IS NULL THEN 1 ELSE 0 END)
                    FROM {schema}.{view_name}
                    """
                )

            distinct_count, null_count = cursor.fetchone()
            col_profile.distinct_count = distinct_count or 0
            col_profile.null_count = null_count or 0
            col_profile.uniqueness_ratio = (
                col_profile.distinct_count / profile.total_rows
                if profile.total_rows > 0 else 0
            )
            col_profile.is_unique = (
                col_profile.distinct_count == profile.total_rows
                and col_profile.null_count == 0
            )

            # Score
            if col_profile.is_unique and not col_profile.is_nullable:
                col_profile.pk_candidate_score = "CONFIRMED UNIQUE"
            elif col_profile.is_unique and col_profile.is_nullable:
                col_profile.pk_candidate_score = "UNIQUE (but nullable)"
            elif col_profile.uniqueness_ratio > 0.99 and not col_profile.is_nullable:
                col_profile.pk_candidate_score = "HIGH (>99% unique, not null)"
            elif col_profile.naming_match and not col_profile.is_nullable:
                col_profile.pk_candidate_score = "MEDIUM (PK name pattern, not null)"
            elif not col_profile.is_nullable:
                col_profile.pk_candidate_score = "MEDIUM (not null)"
            else:
                col_profile.pk_candidate_score = "LOW"

        except Exception as e:
            logger.warning("  Error profiling %s.%s: %s", view_name, col_name, e)
            col_profile.pk_candidate_score = "ERROR"

        profile.column_profiles.append(col_profile)

    cursor.close()

    # Test composite keys from top candidates
    _test_composite_keys_oracle(conn, schema, view_name, profile, max_rows)
    _generate_recommendation(profile)

    return profile


def _test_composite_keys_oracle(
    conn, schema: str, view_name: str, profile: ViewProfile, max_rows: int | None,
):
    """Test composite key combinations for uniqueness."""
    # Get high/medium candidates that aren't already unique alone
    candidates = [
        cp for cp in profile.column_profiles
        if cp.pk_candidate_score in ("HIGH (>99% unique, not null)", "MEDIUM (PK name pattern, not null)", "MEDIUM (not null)")
        and not cp.is_unique
        and not cp.is_nullable
    ]

    if len(candidates) < 2:
        return

    cursor = conn.cursor()

    # Test pairs of top candidates (limit to avoid explosion)
    tested = 0
    for i, col_a in enumerate(candidates[:5]):
        for col_b in candidates[i + 1:5]:
            if tested >= 10:
                break

            try:
                concat_expr = f'"{col_a.column_name}" || ''\x1F'' || "{col_b.column_name}"'
                if max_rows:
                    cursor.execute(
                        f"""
                        SELECT COUNT(DISTINCT ({concat_expr}))
                        FROM (SELECT "{col_a.column_name}", "{col_b.column_name}" 
                              FROM {schema}.{view_name} WHERE ROWNUM <= :mr)
                        """,
                        mr=max_rows,
                    )
                else:
                    cursor.execute(
                        f"""
                        SELECT COUNT(DISTINCT ({concat_expr}))
                        FROM {schema}.{view_name}
                        """
                    )

                distinct_count = cursor.fetchone()[0]
                test = CompositeKeyTest(
                    columns=[col_a.column_name, col_b.column_name],
                    total_rows=profile.total_rows,
                    distinct_count=distinct_count,
                    is_unique=(distinct_count == profile.total_rows),
                )
                profile.composite_tests.append(test)

                if test.is_unique:
                    logger.info(
                        "  COMPOSITE KEY FOUND: (%s, %s) — %d distinct = %d total",
                        col_a.column_name, col_b.column_name,
                        distinct_count, profile.total_rows,
                    )

                tested += 1

            except Exception as e:
                logger.debug("  Composite test failed for (%s, %s): %s",
                             col_a.column_name, col_b.column_name, e)

    cursor.close()


# ============================================================================
# SQL Server Profiling
# ============================================================================

def _profile_sqlserver_views(
    source_name: str,
    schema: str,
    target_table: str | None = None,
    max_rows: int | None = None,
) -> list[ViewProfile]:
    """Profile SQL Server views to find PK candidates."""
    import pyodbc

    source = get_source(source_name)
    if source.source_type != SourceType.SQL_SERVER:
        return []

    conn_str = (
        f"DRIVER={{{config.ODBC_DRIVER}}};"
        f"SERVER={source.host},{source.port};"
        f"DATABASE={source.service_or_database};"
        f"UID={source.user};"
        f"PWD={source.password};"
        "TrustServerCertificate=yes;"
    )

    conn = pyodbc.connect(conn_str, autocommit=True)
    profiles = []

    try:
        cursor = conn.cursor()

        if target_table:
            cursor.execute(
                """
                SELECT o.name, o.type_desc
                FROM sys.objects o
                WHERE o.schema_id = SCHEMA_ID(?)
                  AND o.name = ?
                  AND o.type IN ('U', 'V')
                """,
                schema, target_table,
            )
        else:
            # Views + tables without PKs
            cursor.execute(
                """
                SELECT o.name, o.type_desc
                FROM sys.objects o
                WHERE o.schema_id = SCHEMA_ID(?)
                  AND o.type = 'V'
                UNION ALL
                SELECT o.name, o.type_desc
                FROM sys.objects o
                WHERE o.schema_id = SCHEMA_ID(?)
                  AND o.type = 'U'
                  AND NOT EXISTS (
                      SELECT 1 FROM sys.indexes i
                      WHERE i.object_id = o.object_id AND i.is_primary_key = 1
                  )
                  AND NOT EXISTS (
                      SELECT 1 FROM sys.indexes i
                      WHERE i.object_id = o.object_id AND i.is_unique = 1
                  )
                ORDER BY 1
                """,
                schema, schema,
            )

        objects = cursor.fetchall()
        cursor.close()

        for obj_name, obj_type in objects:
            logger.info("Profiling SQL Server %s: %s.%s", obj_type, schema, obj_name)
            profile = _profile_sqlserver_single(
                conn, schema, obj_name, obj_type,
                source_name, max_rows,
            )
            profiles.append(profile)

    finally:
        conn.close()

    return profiles


def _profile_sqlserver_single(
    conn,
    schema: str,
    view_name: str,
    obj_type: str,
    source_name: str,
    max_rows: int | None = None,
) -> ViewProfile:
    """Profile a single SQL Server view/table for PK candidates."""
    profile = ViewProfile(
        source_name=source_name,
        schema_name=schema,
        view_name=view_name,
        object_type=obj_type,
    )

    cursor = conn.cursor()

    # Get column metadata
    cursor.execute(
        """
        SELECT c.name, TYPE_NAME(c.user_type_id), c.is_nullable, c.is_identity
        FROM sys.columns c
        JOIN sys.objects o ON c.object_id = o.object_id
        WHERE o.schema_id = SCHEMA_ID(?)
          AND o.name = ?
        ORDER BY c.column_id
        """,
        schema, view_name,
    )
    columns = cursor.fetchall()

    # Get total row count
    if max_rows:
        cursor.execute(
            f"SELECT COUNT(*) FROM (SELECT TOP (?) 1 AS x FROM [{schema}].[{view_name}]) sub",
            max_rows,
        )
    else:
        cursor.execute(f"SELECT COUNT(*) FROM [{schema}].[{view_name}]")

    profile.total_rows = cursor.fetchone()[0]
    logger.info("  Total rows: %d", profile.total_rows)

    if profile.total_rows == 0:
        logger.warning("  Empty view — cannot profile")
        cursor.close()
        return profile

    # Profile each column
    top_clause = f"TOP ({max_rows})" if max_rows else ""

    for col_name, data_type, is_nullable, is_identity in columns:
        col_profile = ColumnProfile(
            column_name=col_name,
            data_type=data_type,
            is_nullable=bool(is_nullable),
            total_rows=profile.total_rows,
            naming_match=_is_pk_name_pattern(col_name),
        )

        try:
            cursor.execute(
                f"""
                SELECT COUNT(DISTINCT [{col_name}]),
                       SUM(CASE WHEN [{col_name}] IS NULL THEN 1 ELSE 0 END)
                FROM (SELECT {top_clause} [{col_name}] FROM [{schema}].[{view_name}]) sub
                """
            )

            distinct_count, null_count = cursor.fetchone()
            col_profile.distinct_count = distinct_count or 0
            col_profile.null_count = null_count or 0
            col_profile.uniqueness_ratio = (
                col_profile.distinct_count / profile.total_rows
                if profile.total_rows > 0 else 0
            )
            col_profile.is_unique = (
                col_profile.distinct_count == profile.total_rows
                and col_profile.null_count == 0
            )

            # Score
            if col_profile.is_unique and not col_profile.is_nullable:
                col_profile.pk_candidate_score = "CONFIRMED UNIQUE"
            elif is_identity:
                col_profile.pk_candidate_score = "IDENTITY (likely unique)"
            elif col_profile.is_unique and col_profile.is_nullable:
                col_profile.pk_candidate_score = "UNIQUE (but nullable)"
            elif col_profile.uniqueness_ratio > 0.99 and not col_profile.is_nullable:
                col_profile.pk_candidate_score = "HIGH (>99% unique, not null)"
            elif col_profile.naming_match and not col_profile.is_nullable:
                col_profile.pk_candidate_score = "MEDIUM (PK name pattern, not null)"
            elif not col_profile.is_nullable:
                col_profile.pk_candidate_score = "MEDIUM (not null)"
            else:
                col_profile.pk_candidate_score = "LOW"

        except Exception as e:
            logger.warning("  Error profiling %s.%s: %s", view_name, col_name, e)
            col_profile.pk_candidate_score = "ERROR"

        profile.column_profiles.append(col_profile)

    cursor.close()

    # Test composite keys
    _test_composite_keys_sqlserver(conn, schema, view_name, profile, max_rows)
    _generate_recommendation(profile)

    return profile


def _test_composite_keys_sqlserver(
    conn, schema: str, view_name: str, profile: ViewProfile, max_rows: int | None,
):
    """Test composite key combinations for uniqueness in SQL Server."""
    candidates = [
        cp for cp in profile.column_profiles
        if cp.pk_candidate_score in ("HIGH (>99% unique, not null)", "MEDIUM (PK name pattern, not null)", "MEDIUM (not null)")
        and not cp.is_unique
        and not cp.is_nullable
    ]

    if len(candidates) < 2:
        return

    cursor = conn.cursor()
    top_clause = f"TOP ({max_rows})" if max_rows else ""

    tested = 0
    for i, col_a in enumerate(candidates[:5]):
        for col_b in candidates[i + 1:5]:
            if tested >= 10:
                break

            try:
                # Use CHECKSUM for fast composite distinct counting
                cursor.execute(
                    f"""
                    SELECT COUNT(DISTINCT CONCAT([{col_a.column_name}], CHAR(31), [{col_b.column_name}]))
                    FROM (SELECT {top_clause} [{col_a.column_name}], [{col_b.column_name}] 
                          FROM [{schema}].[{view_name}]) sub
                    """
                )

                distinct_count = cursor.fetchone()[0]
                test = CompositeKeyTest(
                    columns=[col_a.column_name, col_b.column_name],
                    total_rows=profile.total_rows,
                    distinct_count=distinct_count,
                    is_unique=(distinct_count == profile.total_rows),
                )
                profile.composite_tests.append(test)

                if test.is_unique:
                    logger.info(
                        "  COMPOSITE KEY FOUND: (%s, %s) — %d distinct = %d total",
                        col_a.column_name, col_b.column_name,
                        distinct_count, profile.total_rows,
                    )

                tested += 1

            except Exception as e:
                logger.debug("  Composite test failed for (%s, %s): %s",
                             col_a.column_name, col_b.column_name, e)

    cursor.close()


# ============================================================================
# Recommendation Engine
# ============================================================================

def _generate_recommendation(profile: ViewProfile):
    """Generate PK recommendation from profiling results."""
    # Check for single-column unique
    unique_singles = [
        cp for cp in profile.column_profiles
        if cp.pk_candidate_score == "CONFIRMED UNIQUE"
    ]

    if unique_singles:
        # Prefer: naming match > identity > first unique
        named_uniques = [cp for cp in unique_singles if cp.naming_match]
        if named_uniques:
            profile.recommended_pk = [named_uniques[0].column_name]
            profile.recommendation_reason = (
                f"Single unique column with PK naming pattern: "
                f"{named_uniques[0].column_name} ({named_uniques[0].distinct_count} distinct values)"
            )
        else:
            profile.recommended_pk = [unique_singles[0].column_name]
            profile.recommendation_reason = (
                f"Single unique column: {unique_singles[0].column_name} "
                f"({unique_singles[0].distinct_count} distinct values)"
            )
        return

    # Check for composite unique
    unique_composites = [ct for ct in profile.composite_tests if ct.is_unique]
    if unique_composites:
        # Prefer smallest composite
        best = min(unique_composites, key=lambda ct: len(ct.columns))
        profile.recommended_pk = best.columns
        profile.recommendation_reason = (
            f"Composite key: ({', '.join(best.columns)}) — "
            f"{best.distinct_count} distinct = {best.total_rows} total rows"
        )
        return

    # No uniqueness found
    high_candidates = [
        cp for cp in profile.column_profiles
        if "HIGH" in cp.pk_candidate_score or "IDENTITY" in cp.pk_candidate_score
    ]
    if high_candidates:
        profile.recommended_pk = [high_candidates[0].column_name]
        profile.recommendation_reason = (
            f"BEST GUESS (not confirmed unique): {high_candidates[0].column_name} — "
            f"{high_candidates[0].uniqueness_ratio:.4%} unique. Manual verification required."
        )
    else:
        profile.recommendation_reason = (
            "NO PK CANDIDATE FOUND — manual analysis of view SQL required"
        )


# ============================================================================
# Report Output
# ============================================================================

def print_report(profiles: list[ViewProfile]):
    """Print comprehensive profiling report."""
    print("\n" + "=" * 100)
    print("VIEW PRIMARY KEY CANDIDATE PROFILING REPORT")
    print("=" * 100)

    # Summary
    total = len(profiles)
    with_pk = sum(1 for p in profiles if p.recommended_pk)
    no_pk = total - with_pk

    print(f"\nProfiled {total} objects: {with_pk} with PK candidates, {no_pk} needing manual analysis")

    # Critical: no PK candidates
    if no_pk > 0:
        print(f"\n{'!' * 80}")
        print(f"CRITICAL: {no_pk} objects have NO PK candidate identified:")
        for p in profiles:
            if not p.recommended_pk:
                print(f"  - {p.source_name}.{p.schema_name}.{p.view_name} ({p.object_type})")
        print(f"{'!' * 80}")

    # Detailed per-view reports
    for profile in profiles:
        print(f"\n{'─' * 100}")
        print(f"  {profile.source_name}.{profile.schema_name}.{profile.view_name}")
        print(f"  Type: {profile.object_type}  |  Rows: {profile.total_rows:,}")
        print(f"{'─' * 100}")

        if profile.recommended_pk:
            print(f"  ✓ RECOMMENDED PK: {', '.join(profile.recommended_pk)}")
            print(f"    Reason: {profile.recommendation_reason}")
        else:
            print(f"  ✗ NO PK CANDIDATE FOUND")
            print(f"    {profile.recommendation_reason}")

        # Column details (top candidates only)
        candidates = [
            cp for cp in profile.column_profiles
            if cp.pk_candidate_score not in ("LOW", "ERROR")
        ]

        if candidates:
            print(f"\n  {'Column':<30} {'Type':<15} {'Nullable':<10} {'Distinct':>12} "
                  f"{'Null':>8} {'Unique%':>10} {'Score'}")
            print(f"  {'─' * 115}")

            for cp in sorted(candidates, key=lambda x: x.uniqueness_ratio, reverse=True):
                print(
                    f"  {cp.column_name:<30} {cp.data_type:<15} "
                    f"{'YES' if cp.is_nullable else 'NO':<10} "
                    f"{cp.distinct_count:>12,} {cp.null_count:>8,} "
                    f"{cp.uniqueness_ratio:>9.4%} {cp.pk_candidate_score}"
                )

        # Composite key results
        unique_composites = [ct for ct in profile.composite_tests if ct.is_unique]
        if unique_composites:
            print(f"\n  Composite Keys Found:")
            for ct in unique_composites:
                print(f"    ✓ ({', '.join(ct.columns)}) — {ct.distinct_count:,} distinct = {ct.total_rows:,} rows")

        non_unique_composites = [ct for ct in profile.composite_tests if not ct.is_unique]
        if non_unique_composites:
            print(f"\n  Composite Keys Tested (NOT unique):")
            for ct in non_unique_composites:
                ratio = ct.distinct_count / ct.total_rows if ct.total_rows else 0
                print(f"    ✗ ({', '.join(ct.columns)}) — {ct.distinct_count:,} distinct / {ct.total_rows:,} rows ({ratio:.4%})")

    # Generate UPDATE statements for recommended PKs
    print(f"\n{'=' * 100}")
    print("GENERATED SQL — Apply to General.dbo.UdmTablesColumnsList")
    print(f"{'=' * 100}")

    for profile in profiles:
        if not profile.recommended_pk:
            continue

        table_name = profile.view_name  # Will match via StageTableName or SourceObjectName
        for col in profile.recommended_pk:
            print(
                f"\nUPDATE General.dbo.UdmTablesColumnsList "
                f"SET IsPrimaryKey = 1 "
                f"WHERE SourceName = '{profile.source_name}' "
                f"AND TableName = '{table_name}' "  
                f"AND ColumnName = '{col}';"
            )

    print()


# ============================================================================
# Main
# ============================================================================

def main():
    parser = argparse.ArgumentParser(
        description="Profile view columns to identify PK candidates for the UDM pipeline"
    )
    parser.add_argument("--source", help="Source name filter (DNA, CCM, EPICOR)")
    parser.add_argument("--table", help="Specific table/view to profile")
    parser.add_argument("--max-rows", type=int, default=None,
                        help="Max rows to sample (for large views)")

    args = parser.parse_args()

    all_profiles: list[ViewProfile] = []

    # Read pipeline config to know which sources/schemas to profile
    import connectorx as cx
    uri = connections.general_connectorx_uri()
    tables_df = cx.read_sql(
        uri,
        "SELECT DISTINCT SourceName, SourceSchemaName, SourceObjectName "
        "FROM dbo.UdmTablesList WHERE StageLoadTool = 'Python'",
        return_type="polars",
    )

    # Group by source for efficient connection reuse
    sources = {}
    for row in tables_df.iter_rows(named=True):
        sn = row["SourceName"]
        if args.source and sn.upper() != args.source.upper():
            continue
        if sn not in sources:
            sources[sn] = {
                "schema": row["SourceSchemaName"],
                "tables": [],
            }
        if args.table:
            if row["SourceObjectName"].upper() == args.table.upper():
                sources[sn]["tables"].append(row["SourceObjectName"])
        else:
            sources[sn]["tables"].append(row["SourceObjectName"])

    for source_name, info in sources.items():
        source = get_source(source_name)
        schema = info["schema"]
        target = args.table if args.table else None

        logger.info("=" * 60)
        logger.info("Profiling source: %s (schema: %s)", source_name, schema)
        logger.info("=" * 60)

        if source.source_type == SourceType.ORACLE:
            profiles = _profile_oracle_views(
                source_name, schema, target, args.max_rows
            )
        else:
            profiles = _profile_sqlserver_views(
                source_name, schema, target, args.max_rows
            )

        all_profiles.extend(profiles)

    if all_profiles:
        print_report(all_profiles)
    else:
        logger.info("No views or PK-less tables found to profile.")


if __name__ == "__main__":
    main()