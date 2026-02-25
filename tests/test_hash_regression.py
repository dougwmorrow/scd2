"""Hash regression test harness for polars-hash version upgrades.

Item-7 / W-9: Compares hash output between polars-hash versions to determine
if an upgrade is safe (hashes match) or requires a one-time rehash migration
(hashes differ).

Usage:
    1. Run with current polars-hash version (0.4.5):
       python3 tests/test_hash_regression.py --save-baseline
       -> Saves hashes to tests/hash_baseline.json

    2. Upgrade polars-hash:
       pip install polars-hash==0.5.6

    3. Run comparison:
       python3 tests/test_hash_regression.py --compare
       -> Compares new hashes against saved baseline

    4. If hashes match: upgrade is safe, no rehash needed.
       If hashes differ: upgrade requires a one-time full rehash of all Stage tables.
       Schedule during a maintenance window.
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import date, datetime
from pathlib import Path

import polars as pl

# Add project root to path for imports
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from data_load.row_hash import add_row_hash, add_row_hash_fallback


def _build_reference_dataset() -> pl.DataFrame:
    """Build a deterministic reference dataset covering all dtypes.

    Covers: strings (ASCII, Unicode, empty, NULL), integers (positive, negative,
    zero, NULL), floats (normal, NaN, ±Inf, -0.0, NULL), dates, datetimes, booleans.
    """
    return pl.DataFrame({
        # String columns — ASCII, Unicode, empty, NULL, trailing spaces
        "str_ascii": ["hello", "world", "foo bar", "", None, "trailing   ", "UPPER", "MiXeD", "123numeric", "special!@#"],
        "str_unicode": ["café", "日本語", "€100", "naïve", None, "résumé", "Ω≈ç", "emoji😀", "Ñoño", "Ää"],
        # NFC normalization test: combining characters vs precomposed
        "str_nfc": ["e\u0301", "n\u0303", "a\u030A", "o\u0308", None, "café", "normal", "abc", "test", "xyz"],

        # Integer columns
        "int_positive": [1, 100, 999999, 2147483647, None, 42, 0, 1, -1, 100],
        "int_negative": [-1, -100, -999999, -2147483647, None, -42, 0, -1, 1, -100],
        "int_zero_null": [0, None, 0, None, 0, None, 0, None, 0, None],

        # Float columns — includes IEEE 754 edge cases (W-3)
        "float_normal": [1.5, 2.7, 3.14159, 0.0, None, -1.5, 100.001, 0.1 + 0.2, 1e10, 1e-10],
        "float_edge": [float("nan"), float("inf"), float("-inf"), -0.0, None, 0.0, 1.0, -1.0, float("nan"), float("inf")],

        # Date/datetime columns
        "dt_date": [
            date(2025, 1, 1), date(2025, 6, 15), date(2000, 1, 1), date(1999, 12, 31), None,
            date(2025, 2, 28), date(2024, 2, 29), date(2025, 12, 31), date(1970, 1, 1), date(2030, 6, 15),
        ],
        "dt_datetime": [
            datetime(2025, 1, 1, 0, 0, 0), datetime(2025, 6, 15, 12, 30, 45),
            datetime(2000, 1, 1, 23, 59, 59), datetime(1999, 12, 31, 0, 0, 1), None,
            datetime(2025, 2, 28, 8, 15, 0), datetime(2024, 2, 29, 16, 45, 30),
            datetime(2025, 12, 31, 23, 59, 59), datetime(1970, 1, 1, 0, 0, 0),
            datetime(2030, 6, 15, 12, 0, 0),
        ],

        # Boolean column
        "bool_col": [True, False, True, None, False, True, False, None, True, False],
    })


def _compute_hashes(source_is_oracle: bool = False) -> dict[str, list[str | None]]:
    """Compute hashes for the reference dataset using both implementations.

    Returns dict with keys 'polars_hash' and 'hashlib_fallback', each mapping
    to a list of hash strings (or None for null results).
    """
    df = _build_reference_dataset()

    # polars-hash implementation
    df_ph = add_row_hash(df.clone(), source_is_oracle=source_is_oracle)
    ph_hashes = df_ph["_row_hash"].to_list()

    # hashlib fallback implementation
    df_hl = add_row_hash_fallback(df.clone(), source_is_oracle=source_is_oracle)
    hl_hashes = df_hl["_row_hash"].to_list()

    return {
        "polars_hash": ph_hashes,
        "hashlib_fallback": hl_hashes,
    }


def _run_self_check() -> bool:
    """Verify polars-hash and hashlib fallback produce identical hashes.

    This is a prerequisite — if these two don't match, the fallback cannot
    serve as a reference for version comparison.
    """
    print("Running self-check: polars-hash vs hashlib fallback...")

    for oracle_mode in (False, True):
        label = "Oracle" if oracle_mode else "non-Oracle"
        hashes = _compute_hashes(source_is_oracle=oracle_mode)
        ph = hashes["polars_hash"]
        hl = hashes["hashlib_fallback"]

        mismatches = []
        for i, (a, b) in enumerate(zip(ph, hl)):
            if a != b:
                mismatches.append((i, a, b))

        if mismatches:
            print(f"  FAIL ({label}): {len(mismatches)} mismatches found:")
            for idx, a, b in mismatches[:5]:
                print(f"    Row {idx}: polars-hash={a}, hashlib={b}")
            return False
        print(f"  PASS ({label}): All {len(ph)} hashes match between implementations.")

    return True


def save_baseline(output_path: Path) -> None:
    """Save hash baseline to JSON for future comparison."""
    import polars_hash  # noqa: F401

    baseline = {
        "polars_hash_version": getattr(polars_hash, "__version__", "unknown"),
        "polars_version": pl.__version__,
        "hashes_non_oracle": _compute_hashes(source_is_oracle=False),
        "hashes_oracle": _compute_hashes(source_is_oracle=True),
    }

    output_path.write_text(json.dumps(baseline, indent=2))
    print(f"Baseline saved to {output_path}")
    print(f"  polars-hash version: {baseline['polars_hash_version']}")
    print(f"  polars version: {baseline['polars_version']}")
    print(f"  Rows: {len(baseline['hashes_non_oracle']['polars_hash'])}")


def compare_baseline(baseline_path: Path) -> bool:
    """Compare current hashes against saved baseline."""
    import polars_hash  # noqa: F401

    if not baseline_path.exists():
        print(f"ERROR: Baseline file not found: {baseline_path}")
        print("Run with --save-baseline first.")
        return False

    baseline = json.loads(baseline_path.read_text())
    current_version = getattr(polars_hash, "__version__", "unknown")

    print(f"Comparing hashes:")
    print(f"  Baseline: polars-hash {baseline['polars_hash_version']}, polars {baseline['polars_version']}")
    print(f"  Current:  polars-hash {current_version}, polars {pl.__version__}")
    print()

    all_match = True
    for mode, key in [("non-Oracle", "hashes_non_oracle"), ("Oracle", "hashes_oracle")]:
        baseline_ph = baseline[key]["polars_hash"]
        current = _compute_hashes(source_is_oracle=(mode == "Oracle"))
        current_ph = current["polars_hash"]

        mismatches = []
        for i, (old, new) in enumerate(zip(baseline_ph, current_ph)):
            if old != new:
                mismatches.append((i, old, new))

        if mismatches:
            print(f"  {mode}: MISMATCH — {len(mismatches)} of {len(baseline_ph)} hashes differ")
            for idx, old, new in mismatches[:5]:
                print(f"    Row {idx}: old={old[:16]}... new={new[:16]}...")
            all_match = False
        else:
            print(f"  {mode}: MATCH — all {len(baseline_ph)} hashes identical")

    print()
    if all_match:
        print("RESULT: Safe to upgrade — hashes are identical.")
        print("No rehash migration needed.")
    else:
        print("RESULT: Hashes differ — upgrade requires one-time full rehash.")
        print("Schedule during a maintenance window. All Stage _row_hash and")
        print("Bronze UdmHash values will be recomputed on first pipeline run.")

    return all_match


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Hash regression test harness for polars-hash version upgrades (Item-7/W-9)."
    )
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument(
        "--save-baseline", action="store_true",
        help="Compute and save hash baseline for current polars-hash version.",
    )
    group.add_argument(
        "--compare", action="store_true",
        help="Compare current hashes against saved baseline.",
    )
    group.add_argument(
        "--self-check", action="store_true",
        help="Verify polars-hash and hashlib fallback produce identical hashes.",
    )
    parser.add_argument(
        "--baseline-file", type=Path,
        default=Path(__file__).parent / "hash_baseline.json",
        help="Path to baseline JSON file (default: tests/hash_baseline.json).",
    )
    args = parser.parse_args()

    if args.self_check:
        ok = _run_self_check()
        sys.exit(0 if ok else 1)
    elif args.save_baseline:
        if not _run_self_check():
            print("\nSelf-check failed — fix implementation mismatch before saving baseline.")
            sys.exit(1)
        save_baseline(args.baseline_file)
    elif args.compare:
        ok = compare_baseline(args.baseline_file)
        sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
