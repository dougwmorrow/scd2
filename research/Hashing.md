# Hashing — Research & Validated Decisions

*Consolidated from pipeline research and edge case audits. All items resolved.*

---

## 64-Bit Hash Safety for CDC — RESOLVED

**Conclusion (Answer C):** Per-PK CDC comparison was safe at 64-bit; adjacent operations were not. The pipeline upgraded to full SHA-256 VARCHAR(64) as defense-in-depth.

### The Birthday Paradox Does Not Apply to Per-Key Change Detection

The ~24.4% collision probability widely cited for 3 billion rows at 64-bit is mathematically real but answers the wrong question. It calculates the odds that *any two rows in the entire dataset* share a hash — a condition irrelevant to CDC, where each primary key's hash is compared only against that same key's previous hash.

**Per-PK CDC threat model:** Row PK=X has hash H₁. Source data changes. Pipeline computes hash H₂ for the new state. If H₁ = H₂ (probability 1/2⁶⁴ ≈ 5.4×10⁻²⁰), this specific change is missed. Whether PK=Y also has hash H₁ is entirely immaterial — the pipeline never checks PK=X against PK=Y.

**The math:**
- **Birthday model (incorrect for CDC):** P ≈ (3×10⁹)²/(2×2⁶⁴) ≈ 24.4% — counts ~4.5×10¹⁸ cross-PK pairs the CDC pipeline never evaluates
- **Per-PK model (correct for CDC):** P ≤ 2×10⁹/2⁶⁴ ≈ 1.08×10⁻¹⁰ — each PK compared only against itself
- **Overestimation factor:** ~1 billion (N² pairs vs N×k pairs)

This maps to the cryptographic distinction between *collision resistance* (birthday-bound, 2^(b/2)) and *second-preimage resistance* (full 2^b security). At 1.6×10⁻¹⁰, running this pipeline every day for 10,000 years would expect ~0.6 missed changes total.

### What the Cited Sources Actually Analyze

All sources cited in the B-1 analysis (Lemire, Cook, Preshing, Scalefree) analyze **global uniqueness** — the probability that any two items in a set collide. None discuss CDC, data pipelines, or per-key comparison. Their math was correctly computed but incorrectly applied to CDC.

- **Daniel Lemire** (2019): frames the problem as assigning random identifiers requiring global uniqueness
- **John D. Cook**: rule of thumb for cryptographic collision resistance (~√N values before collisions)
- **Jeff Preshing**: birthday probability tables explicitly about "at least two of k values being equal"
- **Scalefree Data Vault**: 128-bit minimum recommendation is for **hash keys** (surrogate keys requiring global uniqueness), not **hash diffs** (per-PK change detection)

### Where 64-Bit Genuinely Fails

Adjacent operations that trigger the birthday paradox at billion-row scale:
- **Hash-based surrogate keys** for joining: ~24.4% incorrect join risk at 3B rows. Use 128-bit minimum.
- **Full-table deduplication/reconciliation** comparing all hashes against all other hashes
- **Hash indexes and lookups** may degrade with many collisions
- **Future repurposing** of 64-bit hash column as a surrogate or join key

### Why Full SHA-256 Anyway

The upgrade to VARCHAR(64) eliminates the need to reason about which collision model applies. Storage cost (64 bytes vs 8 bytes per row) is negligible against data integrity. The pipeline's reconciliation, future use cases, and organizational simplicity all benefit.

---

## Hash Computation Pipeline

All source columns are concatenated and hashed via polars-hash's SHA-256:

### Algorithm
- SHA-256 via polars-hash plugin → full 64-character hex string → stored as VARCHAR(64)
- Column separator: Unit Separator `\x1F` (P0-6) — prevents cross-column collisions where `("AB","CD")` and `("A","BCD")` would otherwise produce identical hashes (E-19)
- NULL sentinel: `\x1FNULL\x1F` (W-2) — Unit Separator wrapping instead of null bytes which risk C-string truncation in FFI/logging layers

### Pre-Hash Normalizations

Four normalizations applied before hashing — more thorough than any published framework:

1. **Oracle empty string → NULL (E-1):** Oracle treats `''` as `NULL`; SQL Server does not. `add_row_hash(source_is_oracle=True)` normalizes empty strings to NULL before hashing. Without this, every Oracle-sourced row with empty string fields generates phantom updates indefinitely.

2. **Unicode NFC normalization (V-2):** All string columns are NFC-normalized. Prevents phantom updates from normalization form differences (e.g., ñ as U+00F1 vs U+006E + U+0303). NFC is W3C standard and the default for Windows keyboard input.

3. **Float normalization (V-1/W-3):** Floats rounded to 10 decimal places. IEEE 754 edge cases: `±0.0` → `+0.0`, `NaN` → `\x1FNaN\x1F`, `Infinity` → `\x1FINF\x1F`, `-Infinity` → `\x1F-INF\x1F`. Prevents phantom updates from cross-platform representation differences.

4. **Trailing space RTRIM (E-4):** All string columns are RTRIM'd. Oracle CHAR columns are blank-padded to declared length; SQL Server ANSI padding treats `'abc' = 'abc  '` in equality but hash sees different bytes. Applied after NFC normalization and Oracle empty string normalization.

### polars-hash Library

polars-hash is the correct and required choice over Polars built-in hashing:
- Polars' built-in `.hash()` uses random seeds, explicitly not stable across sessions (GitHub #3966, #7758)
- polars-hash provides stable cryptographic hashing (SHA-256, SHA-512, MD5) via `chash` namespace
- ~14K weekly PyPI downloads, active maintenance (v0.5.6 January 2026), MIT license
- **Critical caveat (E-20):** polars-hash hashes Categorical columns' physical integer encoding, not the logical string value (Polars Issue #21533). `add_row_hash()` detects Categoricals and casts to Utf8 first.
- **Version pinning (H-2):** polars-hash pinned in requirements.txt. Hash output can change between versions, causing mass false-positive updates.
- **Fallback (V-11):** `add_row_hash_fallback()` provides pure-Python hashlib SHA-256 fallback. Performance ~5-10× slower but functionally equivalent.

---

## Edge Cases — All Resolved

| ID | Category | Issue | Resolution |
|----|----------|-------|------------|
| B-1 | Width | 64-bit truncation at 3B rows | Full SHA-256 VARCHAR(64); migration script |
| P0-6 | Separator | Cross-column concatenation collisions | `\x1F` Unit Separator between columns |
| W-2 | Sentinel | Null bytes risk C-string truncation | `\x1FNULL\x1F` sentinel |
| W-3 | Float | IEEE 754 edge cases cause phantom updates | ±0.0, NaN, Infinity normalized |
| V-1 | Float | Cross-platform float representation | Round to 10 decimal places |
| V-2 | Unicode | Different Unicode forms hash differently | NFC normalization |
| E-1 | Oracle | Empty string/NULL mismatch | Oracle empty → NULL normalization |
| E-4 | Trailing | Trailing space divergence | RTRIM all strings before hashing |
| E-19 | Separator | Column concatenation collision proof | `\x1F` documented and tested |
| E-20 | Categorical | Physical vs logical encoding | Cast to Utf8 before hashing |
| H-1 | Concatenation | polars-hash collision `("ab","c")` = `("a","bc")` | Already mitigated by P0-6 |
| H-2 | Versioning | polars-hash upgrade changes output | Version pinned in requirements.txt |
| H-3 | Collision | xxhash64 probability at scale | Upgraded to full SHA-256; documented |

---

*Sources: Daniel Lemire (2019, 2023), John D. Cook, Jeff Preshing birthday probability tables, Scalefree Data Vault community (September 2025 xxhash64 article), Auth0 birthday attack article, Snowflake HASH() documentation, ngalemmo on Kimball Group Forum (2012), polars-hash GitHub, Polars GitHub issues #3966, #7758, #21533.*