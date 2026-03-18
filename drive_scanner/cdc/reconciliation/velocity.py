"""SCD2 version velocity monitoring (E-13)."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

import connections
from extract.udm_connectorx_extractor import table_exists

from cdc.reconciliation.models import VersionVelocityResult

if TYPE_CHECKING:
    from orchestration.table_config import TableConfig

logger = logging.getLogger(__name__)


def check_version_velocity(
    table_config: TableConfig,
    high_velocity_threshold: int = 10,
) -> VersionVelocityResult:
    """E-13: Check SCD2 version velocity in Bronze.

    Queries the average and max number of versions per PK. High version
    counts indicate either genuine high-change-rate data or a pipeline bug
    creating spurious versions (phantom updates).

    Args:
        table_config: Table configuration.
        high_velocity_threshold: PKs with more than this many versions
            are flagged for investigation.

    Returns:
        VersionVelocityResult with velocity metrics.
    """
    result = VersionVelocityResult(
        table_name=table_config.source_object_name,
        source_name=table_config.source_name,
    )

    bronze_table = table_config.bronze_full_table_name
    pk_columns = table_config.pk_columns

    if not pk_columns:
        result.errors.append("No PK columns configured")
        return result

    if not table_exists(bronze_table):
        result.errors.append(f"Bronze table {bronze_table} does not exist")
        return result

    pk_group = ", ".join(f"[{c}]" for c in pk_columns)

    try:
        conn = connections.get_bronze_connection()
        try:
            cursor = conn.cursor()

            # Average and max versions per PK
            cursor.execute(f"""
                SELECT AVG(version_count * 1.0) AS avg_versions,
                       MAX(version_count) AS max_versions
                FROM (
                    SELECT {pk_group}, COUNT(*) AS version_count
                    FROM {bronze_table}
                    GROUP BY {pk_group}
                ) v
            """)
            row = cursor.fetchone()
            if row:
                result.avg_versions = float(row[0]) if row[0] is not None else 0.0
                result.max_versions = int(row[1]) if row[1] is not None else 0

            # Count PKs with high velocity
            cursor.execute(f"""
                SELECT COUNT(*) FROM (
                    SELECT {pk_group}
                    FROM {bronze_table}
                    GROUP BY {pk_group}
                    HAVING COUNT(*) > {high_velocity_threshold}
                ) hv
            """)
            result.high_velocity_pks = cursor.fetchone()[0]

            cursor.close()
        finally:
            conn.close()

        logger.info(
            "E-13: Version velocity for %s.%s: avg=%.2f, max=%d, "
            "PKs with >%d versions: %d",
            table_config.source_name, table_config.source_object_name,
            result.avg_versions, result.max_versions,
            high_velocity_threshold, result.high_velocity_pks,
        )

        if result.high_velocity_pks > 0:
            logger.warning(
                "E-13: %d PKs with >%d versions in %s — investigate for "
                "spurious version creation.",
                result.high_velocity_pks, high_velocity_threshold, bronze_table,
            )

    except Exception as e:
        result.errors.append(str(e))
        logger.debug(
            "E-13: Version velocity check failed for %s.%s",
            table_config.source_name, table_config.source_object_name,
            exc_info=True,
        )

    return result
