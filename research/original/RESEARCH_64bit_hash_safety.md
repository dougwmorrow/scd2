# RESEARCH BRIEF: Is SHA-256 Truncated to 64-bit Safe for CDC at Billion-Row Scale?

*Status: OPEN — requires independent research to resolve contradictory claims across pipeline audits.*

---

## The Contradiction

Two incompatible claims exist in the pipeline's research documentation:

### Claim A: "64-bit is safe" (Edge Cases Audit, Initial Validation)

> "SHA-256 truncated to 64-bit is safe for per-PK CDC comparison (birthday paradox doesn't apply)."

Listed under "Validated architecture decisions" in both `scd2_cdc_edge_cases_todo.md` and `scd2_cdc_initial_todo.md`.

**Reasoning:** The birthday paradox accumulates collision probability across the entire set of stored hashes, but in per-PK CDC comparison, each PK's hash is only compared against itself across time — not against the hashes of other PKs. Therefore, the collision risk is per-row (1 in 2^64 ≈ 5.4 × 10^-20 per comparison), not across the full dataset.

### Claim B: "64-bit is a showstopper" (Billion-Row Scale Research)

> "The birthday paradox formula gives collision probability ≈ n² / (2 × 2^64). At 3 billion rows: (3 × 10^9)² / (3.69 × 10^19) ≈ 0.244 — a ~24.4% chance of at least one hash collision across the dataset."

Classified as B-1 (P0) — the single highest-severity item identified. The research explicitly states this contradicts Claim A:

> "⚠️ This contradicts the prior assessment... That assessment assumed per-PK comparison never accumulates collision risk across the full dataset. The research from Daniel Lemire, John D. Cook, Preshing birthday probability tables, and the Scalefree Data Vault community confirms the collision probability applies to the total distinct hash values stored across Bronze, not just per-PK comparisons."

### Current State

B-1 has been implemented — the pipeline now uses full SHA-256 as VARCHAR(64). This is the conservative and safe choice regardless of which claim is correct. However, the analytical disagreement remains unresolved, and the team should understand which model is correct.

---

## Key Questions to Research

### Question 1: Does the birthday paradox apply to per-PK CDC hash comparison?

The core disagreement is about what constitutes a "collision" in the CDC context:

- **Claim A's model:** A collision only matters if the *same PK* produces the same hash for two *different* row states. The hash space is 2^64 and each PK is compared only against itself. The probability of a false negative for any single PK comparison is 1/2^64 — negligible regardless of dataset size.

- **Claim B's model:** A collision matters if *any two distinct rows in the entire dataset* produce the same hash. With 3B distinct hash values stored across Bronze, the birthday paradox gives ~24% probability that at least two rows share a hash. If those two rows happen to be the before/after states of the same PK, a changed row is silently classified as unchanged.

**The critical question:** In a CDC hash comparison, is the collision domain "per-PK across time" or "all rows in the dataset"?

If per-PK: Only rows sharing the same PK can collide meaningfully. The effective comparison pool per PK is the number of historical versions (typically 1–10), not 3 billion. Birthday paradox is irrelevant.

If all rows: Any two rows in the 3B dataset that happen to share a hash create a potential false negative if one of them later changes to match the other's state. This is the birthday paradox at full scale.

### Question 2: What is the actual threat model for a hash collision in this pipeline?

Walk through the exact sequence of events that would cause silent data loss:

1. Row A with PK=X has hash H₁ in Bronze (active version)
2. Source data for PK=X changes, producing new row state A'
3. The pipeline computes hash H₂ for A'
4. CDC compares H₁ vs H₂
5. **If H₁ = H₂ (collision):** The change is missed — A' is classified as unchanged

For this to happen, the *new state of this specific PK* must hash to the *same value as its previous state*. This is a per-PK event with probability 1/2^64 regardless of how many other rows exist in the dataset.

**Counter-argument from Claim B:** Consider a different scenario:
1. Row A (PK=X, state S₁) has hash H
2. Row B (PK=Y, state S₂) also has hash H (birthday collision)
3. Later, PK=X's source data changes to state S₃ which happens to also hash to H
4. The collision with PK=Y is irrelevant — but if S₃ coincidentally produces H, the change is missed

Does the existence of the PK=Y collision increase the risk for PK=X? Or is the risk for PK=X strictly 1/2^64 per comparison regardless?

### Question 3: What do authoritative sources actually say?

The B-1 research cites Daniel Lemire, John D. Cook, Preshing birthday tables, and the Scalefree Data Vault community. Research should verify:

- **Daniel Lemire's work on hash collisions:** Does he address per-key comparison vs global collision risk? What context are his collision probability calculations for?
- **Scalefree Data Vault community (MD5/128-bit minimum recommendation):** Is their recommendation for hash-based CDC comparison, or for hash-based *deduplication* / *surrogate key generation* where the birthday paradox clearly applies?
- **Preshing birthday probability tables:** These give global collision probability. Are they applicable to per-PK CDC comparison?
- **John D. Cook's hash collision analysis:** Same question — global vs per-key context?

### Question 4: Does the Data Vault community distinguish between hash uses?

In Data Vault methodology, hashes serve multiple purposes:
1. **Hash keys (surrogate keys):** Hash of business key, used for joining. Birthday paradox DOES apply because two different business keys producing the same hash causes incorrect joins.
2. **Hash diffs (change detection):** Hash of non-key columns, used to detect row changes. Is the birthday paradox relevant here?

If the Scalefree 128-bit minimum recommendation is for hash *keys* (where global uniqueness matters), it may not apply to hash *diffs* (where only per-PK comparison matters).

### Question 5: What is the practical risk at 3B rows with 64-bit hashes?

Even if the birthday paradox doesn't directly apply to per-PK CDC, there may be related risks:

- **Hash table / index collisions:** If Bronze has a hash index for lookup performance, 64-bit collisions cause index probes to match wrong rows. This is a different failure mode than CDC comparison.
- **Reconciliation hash comparison:** If the weekly full reconciliation compares hashes across the dataset (not per-PK), then birthday paradox applies to the reconciliation layer even if not to daily CDC.
- **Future use cases:** If hashes are ever used for deduplication, surrogate key generation, or cross-table joins, global collision risk becomes real.

---

## Research Approach

1. **Primary sources:** Read Daniel Lemire's actual publications on hash collisions, not summaries. Same for John D. Cook. Determine whether their analysis applies to per-key comparison or global uniqueness.

2. **Data Vault sources:** Find the Scalefree 128-bit minimum recommendation in its original context. Determine whether it applies to hash keys, hash diffs, or both.

3. **CDC-specific literature:** Search for analysis of hash collision risk specifically in the context of hash-based CDC (change data capture), not general hash collision analysis.

4. **Mathematical verification:** For the per-PK model — if each PK has an average of 3 historical versions, and there are 1 billion distinct PKs, what is the probability that at least one PK has two versions with the same 64-bit hash? This is `1 - (1 - 1/2^64)^(1B × 3)` ≈ `3B / 2^64` ≈ 1.6 × 10^-10 — negligible. Verify this reasoning.

5. **Practical test:** If feasible, compute SHA-256 truncated to 64-bit for a large sample of production row data and check for actual collisions between different states of the same PK.

---

## Expected Outcome

One of these conclusions:

**A) Claim A is correct (per-PK model):** The birthday paradox does not apply to per-PK CDC hash comparison. 64-bit truncation was safe for CDC. The B-1 analysis incorrectly applied global collision probability to a per-key comparison context. The upgrade to full SHA-256 was unnecessary for CDC correctness but is still beneficial for (a) future-proofing, (b) hash key use cases, and (c) eliminating the need to reason about this at all.

**B) Claim B is correct (global model):** The birthday paradox does apply because hash collisions between any two rows in the dataset create failure modes beyond per-PK comparison (index collisions, reconciliation errors, future use cases). 64-bit was genuinely unsafe. The upgrade was necessary.

**C) Both are partially correct:** Per-PK CDC comparison is safe at 64-bit, but other pipeline operations (reconciliation, indexing, future hash key usage) are not. The upgrade addresses the broader risk even though per-PK CDC alone was safe.

---

*Created: 2026-02-23*
*Related items: B-1 (P0), H-3 (P2), "Validated architecture decisions" in multiple TODO files*