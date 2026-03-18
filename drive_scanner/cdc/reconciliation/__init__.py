"""Hash collision reconciliation utility (P3-4).

Periodic full column-by-column comparison (not hash-based) for a table's
CDC stage data vs source. Detects hash collisions and any other subtle bugs
in the CDC logic.

Design: Compare current Stage rows (where _cdc_is_current=1) against a fresh
extraction from source. For each PK-matched pair, compare every source column
value directly. Any mismatch indicates either a hash collision or a CDC logic bug.

Usage:
    python3 -c "
    from cdc.reconciliation import reconcile_table
    from orchestration.table_config import TableConfigLoader
    loader = TableConfigLoader()
    configs = loader.load_small_tables(table_name='ACCT', source_name='DNA')
    result = reconcile_table(configs[0])
    print(result)
    "

Intended to be run as a weekly job on a random subset of tables.
"""

# --- Models ---
from cdc.reconciliation.models import (
    ActivePKReconciliationResult,
    AggregateReconciliationResult,
    BronzeReconciliationResult,
    CountReconciliationResult,
    DistributionShiftResult,
    ReconciliationResult,
    ReferentialIntegrityResult,
    SCD2IntegrityResult,
    TransformationBoundaryResult,
    VersionVelocityResult,
)

# --- Persistence ---
from cdc.reconciliation.persistence import ensure_reconciliation_log_table

# --- Core reconciliation ---
from cdc.reconciliation.core import reconcile_table, reconcile_table_windowed

# --- Count reconciliation ---
from cdc.reconciliation.counts import reconcile_active_pks, reconcile_counts

# --- SCD2 integrity ---
from cdc.reconciliation.scd2_integrity import reconcile_bronze, validate_scd2_integrity

# --- Data quality ---
from cdc.reconciliation.data_quality import (
    detect_distribution_shift,
    reconcile_aggregates,
)

# --- Boundaries ---
from cdc.reconciliation.boundaries import (
    check_referential_integrity,
    reconcile_transformation_boundary,
)

# --- Velocity ---
from cdc.reconciliation.velocity import check_version_velocity

__all__ = [
    # Models
    "ReconciliationResult",
    "CountReconciliationResult",
    "ActivePKReconciliationResult",
    "VersionVelocityResult",
    "AggregateReconciliationResult",
    "BronzeReconciliationResult",
    "SCD2IntegrityResult",
    "DistributionShiftResult",
    "TransformationBoundaryResult",
    "ReferentialIntegrityResult",
    # Persistence
    "ensure_reconciliation_log_table",
    # Core
    "reconcile_table",
    "reconcile_table_windowed",
    # Counts
    "reconcile_counts",
    "reconcile_active_pks",
    # SCD2 integrity
    "validate_scd2_integrity",
    "reconcile_bronze",
    # Data quality
    "reconcile_aggregates",
    "detect_distribution_shift",
    # Boundaries
    "reconcile_transformation_boundary",
    "check_referential_integrity",
    # Velocity
    "check_version_velocity",
]
