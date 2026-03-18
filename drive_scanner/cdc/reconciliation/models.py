"""Reconciliation result dataclasses."""

from __future__ import annotations

from dataclasses import dataclass, field


@dataclass
class ReconciliationResult:
    """Results from a reconciliation run."""

    table_name: str
    source_name: str
    source_rows: int = 0
    stage_rows: int = 0
    matched_rows: int = 0
    mismatched_rows: int = 0
    source_only_rows: int = 0
    stage_only_rows: int = 0
    mismatched_columns: dict[str, int] = field(default_factory=dict)
    errors: list[str] = field(default_factory=list)

    @property
    def is_clean(self) -> bool:
        return (
            self.mismatched_rows == 0
            and self.source_only_rows == 0
            and self.stage_only_rows == 0
            and not self.errors
        )


@dataclass
class CountReconciliationResult:
    """W-11: Results from lightweight count reconciliation."""

    table_name: str
    source_name: str
    source_count: int = 0
    stage_count: int = 0
    bronze_active_count: int = 0
    count_delta_source_stage: int = 0
    count_delta_stage_bronze: int = 0
    errors: list[str] = field(default_factory=list)

    @property
    def is_clean(self) -> bool:
        return (
            self.count_delta_source_stage == 0
            and self.count_delta_stage_bronze == 0
            and not self.errors
        )


@dataclass
class ActivePKReconciliationResult:
    """E-7: Results from full-PK reconciliation for windowed delete drift."""

    table_name: str
    source_name: str
    source_pk_count: int = 0
    bronze_active_pk_count: int = 0
    ghost_count: int = 0       # Active in Bronze but not in source
    missing_count: int = 0     # In source but not active in Bronze
    errors: list[str] = field(default_factory=list)

    @property
    def is_clean(self) -> bool:
        return self.ghost_count == 0 and self.missing_count == 0 and not self.errors


@dataclass
class VersionVelocityResult:
    """E-13: Results from SCD2 version velocity check."""

    table_name: str
    source_name: str
    avg_versions: float = 0.0
    max_versions: int = 0
    high_velocity_pks: int = 0
    errors: list[str] = field(default_factory=list)


@dataclass
class AggregateReconciliationResult:
    """E-17: Results from aggregate column reconciliation."""

    table_name: str
    source_name: str
    columns_checked: int = 0
    mismatches: dict[str, dict] = field(default_factory=dict)
    errors: list[str] = field(default_factory=list)

    @property
    def is_clean(self) -> bool:
        return not self.mismatches and not self.errors


@dataclass
class BronzeReconciliationResult:
    """Results from Bronze layer reconciliation."""

    table_name: str
    source_name: str
    stage_current_rows: int = 0
    bronze_active_rows: int = 0
    matched_rows: int = 0
    hash_mismatches: int = 0
    orphaned_bronze_rows: int = 0
    missing_bronze_rows: int = 0
    duplicate_active_pks: int = 0
    errors: list[str] = field(default_factory=list)

    @property
    def is_clean(self) -> bool:
        return (
            self.hash_mismatches == 0
            and self.orphaned_bronze_rows == 0
            and self.missing_bronze_rows == 0
            and self.duplicate_active_pks == 0
            and not self.errors
        )


@dataclass
class SCD2IntegrityResult:
    """Results from SCD2 structural integrity validation."""

    table_name: str
    source_name: str
    overlapping_intervals: int = 0
    zero_active_pks: int = 0
    version_gaps: int = 0
    sample_overlapping_pks: list[str] = field(default_factory=list)
    sample_zero_active_pks: list[str] = field(default_factory=list)
    sample_gap_pks: list[str] = field(default_factory=list)
    errors: list[str] = field(default_factory=list)

    @property
    def is_clean(self) -> bool:
        return (
            self.overlapping_intervals == 0
            and self.zero_active_pks == 0
            and self.version_gaps == 0
            and not self.errors
        )


@dataclass
class DistributionShiftResult:
    """B-10: Results from distribution shift monitoring."""

    table_name: str
    source_name: str
    columns_checked: int = 0
    shifts_detected: dict[str, dict] = field(default_factory=dict)
    errors: list[str] = field(default_factory=list)

    @property
    def is_clean(self) -> bool:
        return not self.shifts_detected and not self.errors


@dataclass
class TransformationBoundaryResult:
    """B-11: Results from transformation boundary reconciliation."""

    table_name: str
    source_name: str
    source_layer: str = ""
    target_layer: str = ""
    source_count: int = 0
    target_count: int = 0
    count_delta: int = 0
    orphaned_keys: int = 0
    null_rate_anomalies: list[str] = field(default_factory=list)
    errors: list[str] = field(default_factory=list)

    @property
    def is_clean(self) -> bool:
        return (
            self.count_delta == 0
            and self.orphaned_keys == 0
            and not self.null_rate_anomalies
            and not self.errors
        )


@dataclass
class ReferentialIntegrityResult:
    """B-12: Results from cross-table referential integrity check."""

    fact_table: str
    dimension_table: str
    fk_column: str
    orphaned_fks: int = 0
    ambiguous_fks: int = 0
    total_fact_rows: int = 0
    errors: list[str] = field(default_factory=list)

    @property
    def is_clean(self) -> bool:
        return self.orphaned_fks == 0 and self.ambiguous_fks == 0 and not self.errors
