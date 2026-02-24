"""Row hashing — polars-hash SHA-256 and hashlib fallback.

Single source of truth for deterministic row hashing used by CDC change
detection and SCD2 version comparison. All hash output is a full SHA-256
hex string (64 characters, VARCHAR(64) in SQL Server).

B-1: Full 256-bit SHA-256 — no truncation. Collision probability is
effectively zero up to ~9.2 x 10^25 rows.
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


def add_row_hash(df: pl.DataFrame, source_is_oracle: bool = False) -> pl.DataFrame:
    """Add _row_hash column using polars-hash full SHA-256 hex string.

    B-1: Stores the full 256-bit SHA-256 as a 64-character hex string (VARCHAR(64)
    in SQL Server). At 256 bits, collision probability is effectively zero up to
    ~9.2 x 10^25 rows — no collision risk at any practical scale.

    Previous implementation truncated to 64 bits (Int64), which had ~24% collision
    probability at 3B rows. This was identified as a showstopper in the Billion-Row
    Scale Validation research.

    Uses the polars-hash plugin (Rust-native, near-native performance).
    Unlike Polars' built-in hash_rows(), SHA-256 output is stable across
    Python sessions — critical for large table windowed CDC where fresh hashes
    are compared against stored Stage hashes from previous process invocations.

    V-1/W-3: Float32/Float64 columns are rounded to FLOAT_HASH_PRECISION decimal
    places, with ±0.0 normalized to +0.0 and NaN/Infinity mapped to deterministic
    sentinels, before string conversion. Prevents phantom CDC updates from IEEE 754
    representation differences between Oracle and SQL Server.

    V-2: Utf8/String columns are NFC-normalized before hashing (and in the
    DataFrame itself) to prevent phantom CDC updates from Unicode normalization
    form differences (e.g. e as U+00E9 vs e + U+0301).

    E-1: For Oracle sources, empty strings are normalized to NULL before hashing.
    Oracle treats '' as NULL; SQL Server does not. Without this, every row with
    an empty-string field hashes differently every run, creating phantom updates.

    E-4: All string columns are RTRIM'd before hashing to prevent phantom hash
    differences from trailing space divergence (Oracle CHAR padding, SQL Server
    ANSI padding rules).

    W-2: NULL sentinel uses \\x1F wrapping instead of \\x00 to avoid C-string
    truncation risk in logging/debugging/FFI layers.

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
    import unicodedata

    # W-9 TODO — polars-hash upgrade evaluation:
    #   polars-hash has had 20+ releases since the pipeline pinned v0.4.5.
    #   Version 0.5.6 (January 2026) may include bug fixes and Polars compat
    #   improvements. To evaluate: create a test harness computing hashes for
    #   a reference dataset (all dtypes: strings, ints, floats, NULLs, dates)
    #   with both 0.4.5 and 0.5.6. If hashes match, upgrade safely. If they
    #   differ (H-2 risk), upgrading requires a one-time full rehash of all
    #   Stage tables — schedule during a maintenance window.
    #
    # W-18 TODO — Hash algorithm alternatives:
    #   If hash computation becomes a throughput bottleneck (>10% of pipeline
    #   runtime), evaluate XXH3-128 (~10x faster than SHA-256, 128-bit output,
    #   collision probability ~10^-21 at 3B rows) or BLAKE3 (~5x faster,
    #   cryptographic strength). Would require Polars plugin or Rust extension
    #   and schema change to BINARY(16) or two BIGINT columns in Stage/Bronze.
    #   Current SHA-256 truncated to 64-bit is functionally correct — this is
    #   purely a performance optimization opportunity.
    import polars_hash  # noqa: F401 — registers .chash namespace

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
        # E-1: Normalize Oracle empty string/NULL equivalence.
        # Oracle treats '' as NULL; SQL Server does not. Without this,
        # every row with an empty string field hashes differently every run.
        # Applied to DataFrame itself so BCP CSV output is also normalized.
        if source_is_oracle:
            df = df.with_columns([
                pl.when(pl.col(c) == "").then(None).otherwise(pl.col(c)).alias(c)
                for c in string_cols
            ])
            logger.debug("E-1: Normalized empty strings to NULL for %d Oracle string columns", len(string_cols))

        # NFC normalization applied to the DataFrame itself (not just hash input),
        # so BCP CSV output is also consistently NFC-normalized. ASCII-only data:
        # NFC is a no-op, hash output identical.
        df = df.with_columns([
            pl.col(c).map_elements(
                lambda s: unicodedata.normalize("NFC", s) if s is not None else s,
                return_dtype=pl.Utf8,
            ).alias(c)
            for c in string_cols
        ])
        logger.debug("V-2: NFC-normalized %d string columns", len(string_cols))

        # E-4: RTRIM all string values before hashing to prevent phantom
        # hash differences from trailing space divergence between Oracle
        # CHAR padding and SQL Server ANSI padding rules.
        # Applied to DataFrame itself so BCP CSV output is also trimmed.
        df = df.with_columns([
            pl.col(c).str.rstrip(" ").alias(c)
            for c in string_cols
        ])
        logger.debug("E-4: RTRIM'd %d string columns for hash stability", len(string_cols))

    # E-20: Categorical column safety — polars-hash hashes the underlying physical
    # integer encoding for Categorical columns, not the logical string value
    # (Polars Issue #21533). The same string could hash differently depending on
    # construction order. Cast to Utf8 before hashing to ensure consistency.
    cat_cols = [c for c in source_cols if df[c].dtype == pl.Categorical]
    if cat_cols:
        logger.warning(
            "E-20: Categorical columns found before hashing: %s. "
            "Casting to Utf8 to prevent physical encoding hash.",
            cat_cols,
        )
        df = df.with_columns([pl.col(c).cast(pl.Utf8) for c in cat_cols])
        # Update string_cols set since these are now Utf8
        string_cols = string_cols | set(cat_cols)

    # P0-5: Use sentinel for NULLs so NULL and empty string hash differently.
    # P0-6: Use Unit Separator (\x1F) instead of pipe to avoid collision when
    #        source data contains the separator character.
    # V-1/W-3: Float columns: normalize ±0.0, NaN, Infinity, then round + cast.
    # W-2: NULL sentinel uses \x1F wrapping (not \x00) to avoid C-string truncation.
    hash_exprs = []
    for c in source_cols:
        if c in float_cols:
            # W-3: Map NaN/Infinity to deterministic sentinels, normalize -0.0 to +0.0,
            # then round to FLOAT_HASH_PRECISION and cast to Utf8.
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

    # E-19: Unit Separator (\x1F) prevents cross-column hash collisions.
    # Without a separator, ("AB", "CD") and ("A", "BCD") produce the same
    # concatenated string "ABCD" and therefore the same hash. The \x1F
    # separator makes them "AB\x1FCD" vs "A\x1FBCD" — distinct inputs,
    # distinct hashes. \x1F is chosen because it never appears in real
    # business data (it's a non-printable control character) and is
    # consistent with the NULL sentinel strategy (W-2).
    concat_expr = pl.concat_str(hash_exprs, separator="\x1f")

    # B-1: Full SHA-256 hex string (64 chars) — no truncation.
    # Previous: truncated to 16 hex chars -> UInt64 -> Int64 (64-bit, ~24% collision at 3B rows).
    # Now: full 256-bit hash as VARCHAR(64) — effectively zero collision risk.
    df = df.with_columns(
        concat_expr
        .chash.sha2_256()           # Full SHA-256 hex string (64 chars)
        .alias("_row_hash")
    )

    return df


def add_row_hash_fallback(df: pl.DataFrame, source_is_oracle: bool = False) -> pl.DataFrame:
    """V-11: Fallback row hash using Python's hashlib (standard library).

    B-1: Produces the same full SHA-256 hex string output as add_row_hash()
    but uses hashlib.sha256 instead of the polars-hash plugin. Significantly
    slower (Python callback per row instead of Rust-native), but zero external
    dependency risk.

    Applies the same V-1/W-3 float normalization, V-2 NFC normalization,
    E-1 Oracle empty string normalization, E-4 RTRIM, and W-2 NULL sentinel
    (\\x1F) as add_row_hash().

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
    import unicodedata

    source_cols = [c for c in df.columns if not c.startswith("_")]

    # V-1: Float normalization (same as add_row_hash)
    float_cols = {
        c for c in source_cols
        if df[c].dtype in (pl.Float32, pl.Float64)
    }

    # V-2: Unicode NFC normalization (same as add_row_hash)
    string_cols = {
        c for c in source_cols
        if df[c].dtype in (pl.Utf8, pl.String)
    }
    if string_cols:
        # E-1: Oracle empty string -> NULL (same as add_row_hash)
        if source_is_oracle:
            df = df.with_columns([
                pl.when(pl.col(c) == "").then(None).otherwise(pl.col(c)).alias(c)
                for c in string_cols
            ])

        df = df.with_columns([
            pl.col(c).map_elements(
                lambda s: unicodedata.normalize("NFC", s) if s is not None else s,
                return_dtype=pl.Utf8,
            ).alias(c)
            for c in string_cols
        ])

        # E-4: RTRIM trailing spaces (same as add_row_hash)
        df = df.with_columns([
            pl.col(c).str.rstrip(" ").alias(c)
            for c in string_cols
        ])

    # E-20: Categorical column safety (same as add_row_hash)
    cat_cols = [c for c in source_cols if df[c].dtype == pl.Categorical]
    if cat_cols:
        logger.warning(
            "E-20: Categorical columns found before hashing (fallback): %s. "
            "Casting to Utf8 to prevent physical encoding hash.",
            cat_cols,
        )
        df = df.with_columns([pl.col(c).cast(pl.Utf8) for c in cat_cols])
        string_cols = string_cols | set(cat_cols)

    # Build the same concat string as add_row_hash, then hash with hashlib.
    # W-2: Use \x1F NULL sentinel (same as add_row_hash).
    # W-3: Normalize ±0.0, NaN, Infinity (same as add_row_hash).
    hash_exprs = []
    for c in source_cols:
        if c in float_cols:
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

    # E-19: \x1F separator prevents cross-column hash collisions (see add_row_hash).
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
