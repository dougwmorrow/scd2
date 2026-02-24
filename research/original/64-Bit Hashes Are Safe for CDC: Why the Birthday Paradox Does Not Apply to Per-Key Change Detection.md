# 64-bit hashes are safe for CDC — the birthday paradox doesn't apply here

**For per-primary-key change data capture, 64-bit truncated SHA-256 is astronomically safe even at 3+ billion rows.** The ~24.4% collision probability widely cited for 3 billion rows at 64-bit is mathematically real but answers the wrong question. It calculates the odds that *any two rows in the entire dataset* share a hash — a condition irrelevant to CDC, where each primary key's hash is compared only against that same key's previous hash. The actual per-PK collision risk at billion-row scale is approximately **1.6×10⁻¹⁰** (one in six billion), roughly nine orders of magnitude safer than the birthday-paradox figure suggests. This distinction maps precisely onto a well-established concept in cryptography: *collision resistance* (birthday-bound, 2^(b/2)) versus *second-preimage resistance* (full 2^b security).

The confusion arises because the authoritative sources most frequently cited — Daniel Lemire, John D. Cook, Jeff Preshing, and the Scalefree Data Vault community — all analyze global uniqueness scenarios where the birthday paradox genuinely applies. None of them discuss per-PK change detection. Their math is being correctly computed but incorrectly applied to CDC pipelines.

## The birthday paradox answers a question CDC never asks

The birthday paradox computes the probability that among *n* items drawn from a space of size *N*, at least one pair shares the same value. The number of pairwise comparisons grows quadratically — with 3 billion rows, there are roughly **4.5×10¹⁸ possible pairs**, and the probability that at least one pair collides reaches ~24.4% in a 64-bit space. This is the standard formula: P ≈ n²/(2×2⁶⁴).

But CDC doesn't perform 4.5 quintillion comparisons. The pipeline executes exactly one operation per primary key: compare PK=X's new hash against PK=X's old hash. Two different primary keys producing the same hash is completely irrelevant — they are never compared against each other. The actual number of comparisons is **n**, not n(n-1)/2. Each comparison independently has probability 1/2⁶⁴ ≈ 5.4×10⁻²⁰ of a false negative.

The failure scenario is precise: Row PK=X has hash H₁ in the target. Source data changes. The pipeline computes hash H₂ for the new state. If H₁ = H₂ (probability 1/2⁶⁴), this specific change is missed. Whether PK=Y also happens to have hash H₁ is entirely immaterial — the pipeline never checks PK=X against PK=Y. This is why 3 billion rows with other PKs sharing the same hash value create zero additional risk for PK=X.

## What Lemire, Cook, and Preshing actually analyze

All three authors exclusively discuss **global uniqueness** — the probability that any two items in a set collide. None mention CDC, data pipelines, or per-key comparison.

**Daniel Lemire's** most relevant post, "Are 64-bit random identifiers free from collision?" (2019), frames the problem as assigning random identifiers to objects in a system and asks whether any two will match. His conclusion — "If a collision is a critical flaw, you probably should not use only 64 bits" — is correct *for identifiers that must be globally unique*. He also warns that birthday-paradox calculations are "best case scenarios" because real hash functions may not be perfectly random. His 2023 post, "How accurate is the birthday's paradox formula?," empirically validates the P ≈ 1 − exp(−r²/(2N)) approximation but again only for the all-pairs setting.

**John D. Cook** provides the rule of thumb: "A hash function with range of size N can hash on the order of √N values before running into collisions." For 64-bit, that's ~2³² ≈ 4.3 billion items. His analysis is about cryptographic collision resistance — specifically, the number of hashes an attacker needs to compute to find *any* matching pair. This is the birthday-attack scenario relevant to surrogate keys, not per-PK comparison.

**Jeff Preshing's** widely referenced collision probability table shows ~25% collision probability at 3 billion items in a 64-bit space. His framing is explicit: "Given k randomly generated values... what is the probability that at least two of them are equal?" This is the birthday problem, and his table is correct for that problem. His context is hash values used as unique IDs.

**None of these sources are wrong. They are being applied to a problem they were not analyzing.** Their birthday-paradox calculations correctly describe the risk for hash-based surrogate keys, deduplication, or any scenario requiring global uniqueness. They do not describe the risk for per-PK change detection.

## Scalefree's 128-bit minimum is about hash keys, not hash diffs

The Data Vault 2.0 methodology uses hashes for two fundamentally different purposes with different risk profiles. **Hash keys** are surrogate keys derived from business keys, used for joining Hubs, Links, and Satellites — they must be globally unique, making the birthday paradox fully applicable. **Hash diffs** are fingerprints of non-key columns used to detect whether a row's attributes have changed — they are only compared within the same business key, making them a per-PK comparison.

Scalefree's September 2025 article on xxhash64 versus MD5, their most detailed treatment of hash sizing, frames the collision argument entirely around hash keys: "A hash collision occurs when two different inputs produce the same hash value. In Data Vault, this means **two different business keys could be treated as the same Hub, Link, or Satellite record**." Their "Why 128-Bit is the Safe Minimum" section explicitly references surrogate key integrity. Hash diffs are not mentioned in the sizing analysis.

However, Scalefree and the broader Data Vault community use the same algorithm (typically MD5 at 128-bit) for both hash keys and hash diffs, without publishing separate sizing guidance. This is driven by **standardization and simplicity**, not by a mathematical analysis showing hash diffs need 128-bit. Dan Linstedt's book treats hash keys and hash diffs identically for collision resolution, recommending the same upgrade path (MD5 → SHA-1) for both.

The only practitioner found to explicitly articulate the per-PK distinction is ngalemmo on the Kimball Group Forum, who wrote in 2012: "When using hashes for change detection... you are only comparing two rows, the old row and the new row based on a match on the natural key. You are not comparing a new row against all rows in the table." He argued that even **32-bit** hashing is acceptable for change detection, calling MD5 "overkill for a change detection application."

## The math confirms per-PK safety by a factor of one billion

For **1 billion distinct PKs** with an average of **3 historical versions** each:

**Global birthday model (incorrect for CDC):** All 3 billion rows treated as one pool. P ≈ (3×10⁹)²/(2×2⁶⁴) ≈ **24.4%**. This counts ~4.5×10¹⁸ cross-PK pairs that the CDC pipeline never evaluates.

**Per-PK sequential model (correct for CDC):** Each PK generates 2 sequential comparisons (new vs. previous), each with probability 1/2⁶⁴. Total comparisons: 2 billion. By the union bound: P(any false negative) ≤ 2×10⁹/2⁶⁴ ≈ **1.08×10⁻¹⁰**.

**Per-PK birthday model (slightly more conservative):** If considering all pairs within each PK's 3 versions: 3 pairs per PK. P ≤ 10⁹ × 3/2⁶⁴ ≈ **1.63×10⁻¹⁰**.

The global model overestimates by a factor of approximately **one billion** because it counts N² pairs instead of N×k pairs. This maps directly to the cryptographic distinction between collision resistance (2^(b/2) security) and second-preimage resistance (2^b security). In CDC, each comparison asks: "given this specific hash H₁, does the new data hash to exactly H₁?" This is a second-preimage question, not a collision-finding question. An authoritative Auth0 article on birthday attacks makes the point clearly: the birthday paradox tells you the chance of *any* collision in the room, not the chance of someone matching *your specific* birthday.

To put 1.6×10⁻¹⁰ in context: if you ran this pipeline every day for **10,000 years**, you would expect roughly **0.6 missed changes total** across the entire dataset.

## Adjacent risks where 64-bit genuinely fails

The safety of 64-bit for per-PK CDC does not extend to all pipeline operations. Several common adjacent use cases trigger the birthday paradox at billion-row scale:

- **Hash-based surrogate keys for joining** require global uniqueness. At 3 billion rows, a 64-bit hash key has a ~24.4% chance of causing an incorrect join between two unrelated business keys. This is catastrophic. Use 128-bit minimum.
- **Full-table deduplication or reconciliation** that compares all hashes against all other hashes to find duplicates operates in the birthday-paradox regime. Weekly reconciliation jobs comparing source hashes to target hashes across different PKs would encounter false matches.
- **Hash indexes and hash-based lookups** may degrade in performance if many distinct PKs share hash values, though this affects speed rather than correctness in most database implementations.
- **Future repurposing** of a 64-bit hash column as a surrogate key, join key, or deduplication key would silently introduce birthday-paradox risk into operations that were safe when the hash was only used for CDC.

Snowflake's documentation explicitly warns about its native 64-bit HASH() function: "Do not use HASH to create unique keys... if the input is on the order of 2³² rows or more, the function is reasonably likely to return at least one duplicate value." This warning applies to surrogate keys, not change detection, but most practitioners read it as a blanket prohibition on 64-bit hashing.

## The verdict: per-PK CDC is safe, but defensive engineering still matters

**The answer is (C): 64-bit hashing is safe for per-PK CDC change detection, but other pipeline operations at billion-row scale are not.** Claim A is correct — the birthday paradox does not apply to per-key sequential comparison, and the per-PK collision probability at 64-bit is negligible (~10⁻¹⁰ across 1 billion PKs). Claim B is also mathematically correct but describes a different problem — global uniqueness — that is irrelevant to CDC's comparison logic.

The practical recommendation depends on pipeline architecture. If the hash column serves exclusively for CDC change detection and will never be repurposed for joining, deduplication, or reconciliation, **64-bit truncated SHA-256 is mathematically safe at any realistic scale**. If the same hash column might serve dual purposes — or if organizational standards require a single hash width across all uses — **128-bit MD5 or full SHA-256 eliminates the need to reason about which collision model applies**, at the cost of doubled storage and marginally slower computation. The performance difference between xxhash64 and MD5 may be significant in compute-bound pipelines processing billions of rows daily, making the per-PK safety argument operationally relevant, not merely academic.

The deeper lesson is that collision probability analysis must always begin with the threat model: what comparisons does the system actually perform? The birthday paradox is not a universal law that applies to any large collection of hashes. It applies specifically when the system checks for collisions *across* items. When the system only checks each item against its own history, the relevant probability is the vastly smaller second-preimage probability, and 64 bits provides approximately 10¹⁰ years of safety margin at billion-row scale.