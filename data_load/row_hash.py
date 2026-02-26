"""Row hashing — polars-hash SHA-256 and hashlib fallback.

Single source of truth for deterministic row hashing used by CDC change
detection and SCD2 version comparison. All hash output is a full SHA-256
hex string (64 characters, VARCHAR(64) in SQL Server).

B-1: Full 256-bit SHA-256 — no truncation. Collision probability is
effectively zero up to ~9.2 x 10^25 rows.

A-1 HASH SCOPE CONSTRAINT:
  _row_hash (Stage) and UdmHash (Bronze) are used EXCLUSIVELY for per-PK
  CDC change detection — each row's new hash is compared only against that
  same PK's previous hash. Per-PK comparison is NOT subject to the birthday
  paradox; even 64-bit hashes would be safe (~1.6×10⁻¹⁰ at 1B PKs × 3
  versions each). Full SHA-256 is defense-in-depth for future extensibility.

  WARNING: Do NOT repurpose _row_hash / UdmHash for cross-PK operations
  (surrogate keys, deduplication, reconciliation joins across different PKs)
  without understanding that the birthday paradox DOES apply to those use
  cases. At 3B rows, a 64-bit hash has ~24% collision probability in a
  global uniqueness context. The current 256-bit SHA-256 is safe for any
  use case, but if the hash were ever downgraded to 64-bit for CDC speed,
  cross-PK operations would become unsafe at scale.
"""

from __future__ import annotations

import logging

import polars as pl

logger = logging.getLogger(__name__)

# V-1: Decimal places for float normalization before hashing.
# Prevents phantom CDC updates from IEEE 754 representation differences
# between Oracle and SQL Server (e.g. 0.30000000000000004 vs 0.3).
FLOAT_HASH_PRECISION = 10

# W-2: NULL sentinel uses Unit Separator (\x1F) wrapping instead of null bytes (\x00).
# Null bytes risk C-string truncation in logging, debugging tools, FFI boundaries,
# and serialization layers — silently shortening hash inputs and producing incorrect
# hashes. \x1F is consistent with the column separator strategy (P0-6) and equally
# unlikely in real data without the truncation risk.
_NULL_SENTINEL = "\x1fNULL\x1f"

# W-3: IEEE 754 edge case sentinels for float columns.
# NaN != NaN and ±0.0 have different bit patterns but equal comparison.
# These sentinels ensure consistent hash input for special float values.
_NAN_SENTINEL = "\x1fNaN\x1f"
_INF_SENTINEL = "\x1fINF\x1f"
_NEG_INF_SENTINEL = "\x1f-INF\x1f"


def _normalize_for_hashing(
    df: pl.DataFrame,
    source_is_oracle: bool = False,
) -> tuple[pl.DataFrame, list[pl.Expr]]:
    """M-1: Shared normalization logic for both hash implementations.

    Applies all DataFrame-level normalizations (E-1, V-2, E-4, E-20) and
    builds the hash expression list (P0-5, P0-6, V-1, W-2, W-3).

    The DataFrame normalizations (empty string → NULL, NFC, RTRIM) are applied
    to the DataFrame itself, not just the hash input — this ensures BCP CSV
    output is also normalized.

    Args:
        df: DataFrame with source columns.
        source_is_oracle: If True, normalize empty strings to NULL (E-1).

    Returns:
        Tuple of (normalized_df, hash_exprs) where hash_exprs is a list of
        Polars expressions ready for pl.concat_str().
    """
    source_cols = [c for c in df.columns if not c.startswith("_")]

    # V-1: Identify float columns for normalization before hashing.
    float_cols = {
        c for c in source_cols
        if df[c].dtype in (pl.Float32, pl.Float64)
    }
    if float_cols:
        logger.debug("V-1: Float columns found for hash normalization: %s", float_cols)

    # V-2: Identify string columns for Unicode NFC normalization.
    string_cols = {
        c for c in source_cols
        if df[c].dtype in (pl.Utf8, pl.String)
    }
    if string_cols:
        # Item-8: Single-pass string normalization — combines E-1 (Oracle empty→NULL),
        # N-1/V-2 (NFC normalization), and E-4 (RTRIM) into one with_columns call.
        # Eliminates two unnecessary intermediate DataFrame materializations.
        exprs = []
        for c in string_cols:
            expr = pl.col(c)
            if source_is_oracle:
                # E-1: Oracle treats '' as NULL; normalize before hashing.
                expr = pl.when(expr == "").then(None).otherwise(expr)
            # N-1: NFC normalization (Rust-native, SIMD).
            # E-4: RTRIM trailing spaces for hash stability.
            expr = expr.str.normalize("NFC").str.strip_chars_end(" ")
            exprs.append(expr.alias(c))
        df = df.with_columns(exprs)
        if source_is_oracle:
            logger.debug("E-1: Normalized empty strings to NULL for %d Oracle string columns", len(string_cols))
        logger.debug("V-2/E-4: NFC-normalized and RTRIM'd %d string columns (single pass)", len(string_cols))

    # E-20: Categorical column safety.
    cat_cols = [c for c in source_cols if df[c].dtype == pl.Categorical]
    if cat_cols:
        logger.warning(
            "E-20: Categorical columns found before hashing: %s. "
            "Casting to Utf8 to prevent physical encoding hash.",
            cat_cols,
        )
        df = df.with_columns([pl.col(c).cast(pl.Utf8) for c in cat_cols])
        string_cols = string_cols | set(cat_cols)

    # Build hash expressions — one per source column.
    hash_exprs = []
    for c in source_cols:
        if c in float_cols:
            # W-3: Map NaN/Infinity to deterministic sentinels, normalize -0.0 to +0.0.
            hash_exprs.append(
                pl.when(pl.col(c).is_nan())
                .then(pl.lit(_NAN_SENTINEL))
                .when(pl.col(c).is_infinite() & (pl.col(c) > 0))
                .then(pl.lit(_INF_SENTINEL))
                .when(pl.col(c).is_infinite() & (pl.col(c) < 0))
                .then(pl.lit(_NEG_INF_SENTINEL))
                .otherwise(
                    pl.when(pl.col(c) == 0.0)
                    .then(pl.lit(0.0))
                    .otherwise(pl.col(c))
                    .round(FLOAT_HASH_PRECISION)
                    .cast(pl.Utf8)
                )
                .fill_null(_NULL_SENTINEL)
            )
        else:
            hash_exprs.append(
                pl.col(c).cast(pl.Utf8).fill_null(_NULL_SENTINEL)
            )

    return df, hash_exprs


def add_row_hash(df: pl.DataFrame, source_is_oracle: bool = False) -> pl.DataFrame:
    """Add _row_hash column using polars-hash full SHA-256 hex string.

    B-1: Stores the full 256-bit SHA-256 as a 64-character hex string (VARCHAR(64)
    in SQL Server). At 256 bits, collision probability is effectively zero up to
    ~9.2 x 10^25 rows — no collision risk at any practical scale.

    Uses the polars-hash plugin (Rust-native, near-native performance).
    Unlike Polars' built-in hash_rows(), SHA-256 output is stable across
    Python sessions — critical for large table windowed CDC where fresh hashes
    are compared against stored Stage hashes from previous process invocations.

    M-2: Thin wrapper around _normalize_for_hashing() + polars-hash SHA-256.
    See _normalize_for_hashing() for full documentation of normalizations applied
    (V-1, V-2, E-1, E-4, W-2, W-3, E-20).

    Args:
        df: DataFrame with source columns.
        source_is_oracle: If True, normalize empty strings to NULL (E-1).

    E-21 KNOWN LIMITATION — Oracle NUMBER precision loss:
      ConnectorX converts Oracle NUMBER to Python float64 (IEEE 754 double precision),
      which has ~15 significant digits. Oracle NUMBER supports up to 38 digits.
      Values like 123.4567890123456789 lose precision during ConnectorX extraction,
      BEFORE any pipeline processing occurs. The float normalization (V-1) rounds to
      10 decimal places, but precision is already lost for >15 significant digits.
      Impact: Rare — most business data uses <15 significant digits. For critical
      high-precision Oracle NUMBER columns (e.g. 18-digit account numbers stored as
      NUMBER), consider casting to VARCHAR2 in the extraction SQL to preserve all
      digits through the pipeline.
    """
    # W-9 TODO — polars-hash upgrade evaluation:
    #   polars-hash has had 20+ releases since the pipeline pinned v0.4.5.
    #   Version 0.5.6 (January 2026) may include bug fixes and Polars compat
    #   improvements. To evaluate: create a test harness computing hashes for
    #   a reference dataset (all dtypes: strings, ints, floats, NULLs, dates)
    #   with both 0.4.5 and 0.5.6. If hashes match, upgrade safely. If they
    #   differ (H-2 risk), upgrading requires a one-time full rehash of all
    #   Stage tables — schedule during a maintenance window.
    import polars_hash  # noqa: F401 — registers .chash namespace

    df, hash_exprs = _normalize_for_hashing(df, source_is_oracle)

    # E-19: Unit Separator (\x1F) prevents cross-column hash collisions.
    concat_expr = pl.concat_str(hash_exprs, separator="\x1f")

    # B-1: Full SHA-256 hex string (64 chars) — no truncation.
    df = df.with_columns(
        concat_expr
        .chash.sha2_256()
        .alias("_row_hash")
    )

    return df


def add_row_hash_fallback(df: pl.DataFrame, source_is_oracle: bool = False) -> pl.DataFrame:
    """V-11: Fallback row hash using Python's hashlib (standard library).

    B-1: Produces the same full SHA-256 hex string output as add_row_hash()
    but uses hashlib.sha256 instead of the polars-hash plugin. Significantly
    slower (Python callback per row instead of Rust-native), but zero external
    dependency risk.

    M-2: Thin wrapper around _normalize_for_hashing() + hashlib SHA-256.

    Use this if polars-hash becomes incompatible with a future Polars version
    or is abandoned. To activate: replace add_row_hash with add_row_hash_fallback
    in prepare_dataframe_for_bcp() and verify hash output matches on a test table.

    Args:
        df: DataFrame with source columns.
        source_is_oracle: If True, normalize empty strings to NULL (E-1).

    Returns:
        DataFrame with _row_hash column (Utf8, full SHA-256 hex string).
    """
    import hashlib

    df, hash_exprs = _normalize_for_hashing(df, source_is_oracle)

    # E-19: \x1F separator prevents cross-column hash collisions.
    concat_expr = pl.concat_str(hash_exprs, separator="\x1f")

    def _hashlib_sha256_hex(s: str) -> str:
        """B-1: Full SHA-256 hex string (64 chars) — matches add_row_hash() output."""
        return hashlib.sha256(s.encode("utf-8")).hexdigest()

    df = df.with_columns(
        concat_expr
        .map_elements(_hashlib_sha256_hex, return_dtype=pl.Utf8)
        .alias("_row_hash")
    )

    logger.debug("V-11: Used hashlib fallback for row hashing (%d rows)", len(df))
    return df
