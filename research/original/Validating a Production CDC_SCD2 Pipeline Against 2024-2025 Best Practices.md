# Validating a production CDC/SCD2 pipeline against 2024–2025 best practices

This pipeline's architecture — Polars + ConnectorX + BCP on Linux feeding a SQL Server medallion (Stage CDC → Bronze SCD2) — is **fundamentally sound and aligns with current best practices**, with several important exceptions that require attention. The most critical findings: the **64-bit hash truncation is safe for row-level CDC but not for surrogate keys**, the **SQL Server MERGE statement remains unsafe** (new bug found February 2025), **ConnectorX Oracle support is still unreliable**, and a **newly available Polars streaming engine dramatically changes memory management options**. The BCP UTF-8 codepage issue on Linux was finally fixed in December 2025. What follows is a section-by-section validation with specific, production-relevant guidance.

---

## Hash-based CDC holds up well, with one critical nuance on 64-bit truncation

**Hash-based CDC remains the correct approach** for this pipeline's constraints (no WAL access, cross-system comparison, batch periodicity). Log-based CDC via Debezium is the gold standard when transaction log access exists, but hash-based diff is the established pattern for the described scenario — validated by the Data Vault 2.0 hashdiff pattern used in production by dbtvault/automate-dv across thousands of deployments.

**The SHA-256 truncated to 64-bit (Int64) question requires careful analysis.** The birthday paradox gives an alarming ~21.6% collision probability at 3 billion rows — but this metric is misleading for this use case. The birthday paradox applies when checking for *any* collision across a set of hashes (relevant for surrogate keys or deduplication). For row-level CDC change detection, where you compare the old hash against the new hash for the *same primary key*, the false-negative probability is **1 in 2^64 (~5.4 × 10⁻²⁰) per row comparison** — effectively zero. If hashes are used exclusively for change detection (not as join keys or surrogate identifiers), **64-bit truncation remains adequate at any practical row count**. If hashes are also used for deduplication or as keys in any data structure, upgrade to 128-bit minimum. For performance-sensitive pipelines considering alternatives, **XXH3-128** offers ~10× speed improvement over SHA-256 with 128-bit output, and **BLAKE3** provides cryptographic strength at ~5× SHA-256 speed.

**Float normalization via rounding to 10 decimal places** is a reasonable default. The key refinements: always normalize **±0.0 to +0.0** (IEEE 754 defines them as equal but with different bit patterns), map **NaN** and **Infinity** to consistent sentinels, and prefer **fixed-point integer representation** where domain semantics allow (e.g., money as cents). Epsilon-based comparison is mathematically incompatible with hashing due to transitivity violations.

**Unicode NFC normalization before hashing** remains the recommended approach. NFC is the W3C standard, the default for Windows keyboard input, and the form most existing data already uses. The primary risk is macOS sources, which historically normalize filenames to NFD. Apply NFC consistently to all string values; use the `NFC_Quick_Check` property to skip already-normalized strings for performance.

**The Unit Separator (\\x1F) as column delimiter** is technically superior to the commonly used `||` separator in Data Vault 2.0. Since 0x1F is a non-printable ASCII control character specifically designed for data delimiting, it eliminates delimiter collision risks where real data values contain the separator. The only tradeoff is reduced debuggability in logs.

**NULL sentinel values (\\x00NULL\\x00)**: The wrapping approach is sound but carries a risk — some processing layers treat `\\x00` as a C-style string terminator, potentially truncating hash inputs. A safer alternative is **\\x1FNULL\\x1F** (wrapping with Unit Separator), which avoids null-byte truncation while remaining equally unlikely in real data. The essential principles: NULLs must produce a distinct hash contribution, NULL must differ from empty string, and NULL in different column positions must produce different hash inputs.

**polars-hash is healthy and required.** Version **0.5.6** released January 2026, with 20+ releases since October 2023. It provides stable cryptographic hashing (SHA-256, SHA-512, MD5) via its `chash` namespace. The critical reason to use it: **Polars' built-in `.hash()` explicitly does not guarantee stable results across versions** — upgrading Polars would trigger false change detection on every row. There is no Polars-native stable hashing alternative.

---

## SCD2 implementation is validated, but MERGE remains dangerous

**INSERT-first SCD2 ordering is confirmed as the correct pattern.** If the pipeline crashes after INSERT but before UPDATE, you get duplicate active rows — a detectable, recoverable state. UPDATE-first (expire old rows, then insert new) risks losing the active record entirely on crash — an unrecoverable data loss scenario. Both steps should be wrapped in an explicit transaction with `SET XACT_ABORT ON`, but the INSERT-first ordering provides defense-in-depth.

**The MERGE statement in SQL Server 2022 is still unsafe for production ETL.** Hugo Kornelis conducted the most thorough re-evaluation in September 2023, testing against SQL Server 2022 CU7, and found most original bugs fixed — but a critical bug with MERGE DELETE on indexed views remains unfixed. More importantly, **Paul White validated a new MERGE bug in February 2025**: MERGE fails with assertion errors when updating through a view (outer join scenario), reproduced on SQL Server 2008 through 2022 CU17 inclusive. The expert consensus (Brent Ozar, Aaron Bertrand, Michael J. Swart, Erik Darling) remains unanimous: **use separate INSERT and UPDATE statements**. New bugs continue to surface, confirming the pattern of Microsoft not prioritizing MERGE reliability.

**The two-step staging table + UPDATE JOIN pattern** is the production standard. The recommended change detection hierarchy for SQL Server: `HASHBYTES('SHA2_256', CONCAT(...))` is most reliable but CPU-intensive; `BINARY_CHECKSUM()` is fast but collision-prone; column-by-column `EXISTS/EXCEPT/INTERSECT` is most explicit.

**SCD2 integrity validation** should include four core checks run after every SCD2 merge:

- **Overlapping intervals**: Self-join detecting records where date ranges overlap for the same business key
- **Zero-active records**: Business keys with no row where `ActiveFlag=1`
- **Multiple active records**: Business keys with more than one active row (detects INSERT-first crash residue)
- **Version gaps**: Using `LEAD(BeginDate)` windowed against `EndDate` to detect non-contiguous version chains

**BIGINT IDENTITY surrogate keys** are correct for billion-row scale. BIGINT's range (~9.2 quintillion) would take approximately 292,000 years to exhaust at 1 million inserts per second. Keep `IDENTITY_CACHE = ON` (the default) — gaps are irrelevant for surrogate keys, and disabling the cache measurably reduces INSERT throughput. The 8-byte storage cost versus INT's 4 bytes adds ~4GB per billion rows in the clustered index alone but is a necessary investment.

**The filtered unique index `WHERE ActiveFlag=1`** remains the best approach for enforcing single active row per business key. It serves double duty as both a constraint and an index for active-row queries. Known limitations: it cannot back foreign key constraints, parameterized queries may not match the filter predicate (use literal values or `OPTION (RECOMPILE)`), and it's incompatible with partition switching. For billion-row SCD2 tables, consider a **hybrid indexing strategy**: clustered columnstore for analytical scans, plus nonclustered B-tree indexes for point lookups. SQL Server 2022's **ordered clustered columnstore** (`ORDER (BusinessKey, EffectiveDateTime)`) enables segment elimination for point-in-time queries.

---

## Windowed CDC is the riskiest part of this pipeline

Date-scoped CDC comparison is the area with the most documented failure modes. The fundamental issue: **timestamp-based extraction is a heuristic, not true CDC**. Several silent failure modes deserve explicit mitigation.

**Delete detection scoped to an extraction window cannot detect hard deletes** — this is a universal, structural limitation. A `WHERE modified_at > @last_run` query only returns existing rows; deleted rows are invisible. Mitigation options ranked by reliability: periodic full-table anti-join reconciliation by primary key (most reliable), soft-delete flags in source tables (requires source schema control), or log-based CDC for delete-critical tables.

**Phantom deletes from date column value migration** represent a subtle, hard-to-diagnose failure. When a row's scoping date column changes (e.g., `order_date` moves from January 15 to January 20), it disappears from the January 15 extraction window. If that window was already processed, the pipeline sees the row as deleted. Mitigation: **scope extraction by `modified_at`, not by mutable business date columns**. If business date scoping is required, the modified_at column must also be checked as a secondary filter.

**Late-arriving data beyond the lookback window** requires a configurable overlap buffer. dbt's microbatch model implements a `lookback` parameter (default: 1 batch) that reprocesses recent batches automatically. For custom pipelines, maintain a configurable lookback period and log any rows detected during the overlap — increasing detection frequency indicates a systemic latency issue requiring lookback expansion.

**Cross-midnight transaction splitting** is best handled with overlap windows — extract midnight-to-midnight plus a configurable buffer on each side, using upsert/dedup semantics at the target to handle duplicates from the overlap. For tables with high cross-boundary transaction rates, consider extracting by `modified_at` rather than business date boundaries.

**Per-day checkpoint/resume for backfills** should use a control table tracking per-table, per-date processing status with watermarks and completion flags. The pattern: process one date chunk, commit results, update the control table checkpoint, then proceed to the next chunk. On failure, resume from the last committed checkpoint. This is the standard pattern across Databricks Structured Streaming, Apache Spark, and Azure Data Factory.

---

## BCP on Linux now has proper UTF-8 support

**The `-C 65001` codepage issue has been fixed.** Microsoft released **mssql-tools18 v18.6.1.1 in December 2025**, which adds "Code Page options on BCP command line" for Linux/macOS — the first version to explicitly support this feature. Prior to this version, the `-C 65001` flag returned "The code page specified for bcp operation is not supported on the machine" on Linux. **Upgrade to mssql-tools18 v18.6.1.1** is the single most impactful infrastructure change for this pipeline.

**BCP row terminator on Linux** requires explicit specification. A May 2024 VladDBA blog post documented subtle errors when importing LF-terminated files into SQL Server expecting CRLF — including "Unexpected EOF", "Invalid character value for cast specification", and "Numeric value out of range" errors. Always explicitly specify both `-t` (field terminator) and `-r` (row terminator) in every BCP command. When generating files on Linux, use `-r "0x0a"` to match the LF line ending.

**`quote_style='never'` with tab delimiter remains the safest BCP approach.** BCP has no native CSV quoting support — it interprets quote characters as literal data. Tab delimiter avoids issues with commas in data values. For production pipelines, use **BCP format files** (XML `.fmt`) for explicit, version-controlled column mappings.

**BOM corruption is a real risk** with format-file or native-mode operations. BCP never writes a BOM on export, and correctly skips BOMs in `-c` text mode. However, in native/format-file mode, BOM bytes become the first bytes of row 1 column 1, causing corruption. If upstream processes generate BOMs, strip them before loading.

**BCP row count verification** should use the staging table pattern: load into staging, verify `SELECT COUNT(*)` against source row count, validate data quality, then move to the final table. Always capture rejected rows with `-e error_file.log` and parse BCP's stdout "N rows copied" message for automated verification.

---

## ConnectorX needs a contingency plan for Oracle

**ConnectorX Oracle support is still not recommended for production.** A critical unresolved bug (Issue #644, June 2024) panics on dates like `9999-12-31` — a common SCD2 sentinel value. The dlt documentation explicitly states ConnectorX is "not recommended for Oracle." An additional regression (Issue #634) causes datetime/datetime2 panics for SQL Server since v0.3.3, though this may be resolved in later versions.

**ConnectorX has a known memory leak** in loop-based extraction patterns. Memory is not released even after `del df` and `gc.collect()` due to the Rust-Python FFI boundary. For multi-table pipelines, consider **process-level isolation** (subprocess per table extraction) to prevent memory accumulation.

**The ADBC ecosystem is emerging as a viable alternative.** Columnar (launched October 2025, backed by Databricks, dbt Labs, Microsoft, and Snowflake) released ADBC drivers for SQL Server and 9 other databases. ADBC delivers near-COPY performance via end-to-end Arrow RecordBatch passing. However, **no ADBC driver exists for Oracle yet** — Oracle remains the gap across all modern extraction tools. DuckDB's ODBC scanner extension provides Oracle connectivity through ODBC as an alternative path.

**ConnectorX v0.4.5** (January 2026) remains actively maintained with SQL Server improvements, including `EncryptionLevel::Off` support and Oracle schema query parameters. SQL Server partitioning remains limited to numerical, non-nullable columns — use manual pre-partitioned queries for date-based extraction.

For the pipeline's Oracle sources, the recommended contingency: use ConnectorX for SQL Server extraction (where it's reliable) and evaluate **oracledb** (Python's native Oracle driver) with PyArrow backend or DuckDB's ODBC scanner for Oracle extraction. Cast extreme date values (`9999-12-31`) in SQL before extraction to avoid the panic bug.

---

## Polars has matured significantly, with critical memory management guidance

**Polars reached v1.38.1** (February 2026), with the landmark v1.0.0 release in July 2024. The most significant development is a **completely redesigned streaming engine** (available since v1.31.1, January 2025) based on morsel-driven parallelism. It delivers **3–7× faster performance** than the in-memory engine on benchmarks and supports anti-joins, group-bys, and all standard operations natively in streaming mode. For a 64GB RAM system processing billion-row datasets, this is transformative — Polars has demonstrated processing 300M rows on systems with as little as 512MB RAM using streaming mode.

**glibc memory arena fragmentation is a documented, serious issue.** A Polars GitHub issue (#23128, June 2025) reported 10× memory bloat — 300GB of actual data consuming 2.7TB RSS. The root cause: glibc malloc creates up to **8 × num_cores memory arenas** for multithreaded applications, and Polars' Rust operations create/destroy allocations across arenas that aren't returned to the OS. The critical mitigation: **set `MALLOC_ARENA_MAX=2`** as an environment variable before starting the pipeline process. This limits glibc to 2 arenas at a small cost to allocation throughput. Additional mitigations: use the streaming engine (`collect(engine="streaming")`), downcast data types (Int32 instead of Int64 where possible, Categorical for low-cardinality strings), and call `df.shrink_to_fit()` after large operations.

**Anti-join for CDC is well-supported** but requires awareness of NULL behavior. Since Polars v0.20 (December 2023), **`join_nulls=False` is the default** — NULL values never produce matches in joins. This is SQL-compliant behavior. For CDC anti-joins, this means rows with NULL join keys always appear in anti-join output. If upgrading from pre-0.20, anti-join results will change for any rows with NULL join keys. Use `join_nulls=True` explicitly if your CDC semantics require NULL=NULL matching.

**`diagonal_relaxed` crash bug (#12543) is fixed.** The segfault when concatenating DataFrames with large schema differences has been resolved. However, **issue #18911 remains open**: lazy diagonal concat with list-type columns in non-first LazyFrames throws `SchemaError`. For production CDC with schema evolution, **prefer explicit schema alignment** before concatenation rather than relying on `diagonal_relaxed` — cast and align schemas manually, especially with nested types.

**Implicit casting risks** exist in `vertical_relaxed` and `diagonal_relaxed` concat — Polars silently coerces columns to common supertypes (e.g., Int64 → Float64, losing precision for large integers). Add **schema validation assertions** (`assert df.schema == expected_schema`) before concatenation in production pipelines.

---

## The medallion architecture and resilience patterns are validated

**Stage CDC → Bronze SCD2 separation remains best practice.** Azure Databricks documentation explicitly warns: "Do not write to silver tables directly from ingestion. You'll introduce failures due to schema changes or corrupt records." The staging layer absorbs schema drift and data quality issues before SCD2 processing. Store raw data with minimal transformation in Bronze (consider string/variant types for maximum flexibility), then apply SCD2 historization at the Silver layer.

**INSERT-first with dedup-on-next-run** remains viable for crash recovery. The 2024–2025 consensus has evolved toward a principle: **"Idempotency is not about preventing retries — it's about making retries safe."** The preferred patterns are partition overwrite (DELETE-then-INSERT for a date partition) and MERGE/upsert semantics. For this pipeline's INSERT-first SCD2, the dedup-on-next-run design is correct — the "multiple active records" validation query acts as both a safety check and a recovery mechanism.

**`sp_getapplock` for concurrent run protection** remains the recommended approach. Use transaction-scoped locks (released automatically on COMMIT/ROLLBACK) with `@LockTimeout=0` for non-blocking "skip if already running" semantics. Key pitfall: under RCSI, avoid calling `sp_releaseapplock` explicitly before COMMIT — let the lock release happen implicitly at COMMIT time to prevent a race condition where another process reads pre-transaction state.

**Schema evolution handling** (ADD new columns, WARN removed, ERROR type changes) aligns with the industry-standard "additive-first evolution" pattern. Store raw payloads in Bronze to absorb unknown fields. Route records failing schema contracts to a quarantine table with rejection reasons. The Debezium and Confluent ecosystems have hardened this pattern through schema registries.

**Weekly full reconciliation** remains the recommended cadence for most production tables. For high-value or compliance-critical tables, increase to daily. For stable dimension tables, monthly suffices. Consider separating **count reconciliation** (fast, cheap, run daily) from **full-column hash reconciliation** (thorough, expensive, run weekly). Track reconciliation divergence trends in a metadata table — increasing divergence indicates a pipeline problem requiring investigation.

---

## The ecosystem has new options worth evaluating

**pydbzengine** (released February 2025) is the most significant new development for Python-based CDC. It provides a Pythonic interface to the Debezium embedded engine via JPype, supporting all Debezium connectors including SQL Server and Oracle. It offers pluggable event handlers including integration with dlt (Data Load Tool) and Apache Iceberg. For this pipeline's Oracle sources where ConnectorX is unreliable, pydbzengine could provide reliable log-based CDC extraction.

**dbt snapshots gained useful features in v1.9+**: `dbt_valid_to_current` configuration lets you set a specific end-date value (e.g., `9999-12-31`) instead of NULL for current records, and `dbt_is_deleted` properly handles hard deletes. However, dbt snapshots still **cannot handle out-of-order records** or capture intermediate states between runs — documented limitations that make custom SCD2 implementations superior for high-fidelity history tracking.

**SQL Server 2022 temporal tables** provide automatic SCD2-like versioning with native `FOR SYSTEM_TIME AS OF` query syntax. They're worth evaluating for dimension tables where column-level change tracking granularity isn't needed. Limitations: you cannot control which column changes trigger a new version (any update creates a version), and the history table must be in the same database.

**SQL Server 2022 ordered clustered columnstore indexes** (`CREATE CLUSTERED COLUMNSTORE INDEX ... ORDER (BusinessKey, EffectiveDateTime)`) are a meaningful improvement for billion-row SCD2 tables, enabling segment elimination for point-in-time queries without sacrificing columnstore compression benefits.

**Debezium 3.x** (current: 3.4.0.Final, December 2025) continues to improve SQL Server support, including a `data.query.mode=direct` option that bypasses `fn_cdc_get_all_changes` for better performance, and signaling support for multitask SQL Server deployments. The project moved to the Commonhaus Foundation for vendor-neutral governance.

---

## Conclusion

This pipeline's architecture is well-designed for its constraints. The core patterns — hash-based CDC with staging, INSERT-first SCD2 with separate INSERT/UPDATE, BCP bulk loading, and medallion layer separation — all align with production-proven practices. The three highest-priority actions are: **(1)** upgrade to mssql-tools18 v18.6.1.1 for the BCP UTF-8 fix, **(2)** set `MALLOC_ARENA_MAX=2` and adopt Polars' new streaming engine for memory management, and **(3)** develop a ConnectorX contingency for Oracle extraction (evaluate pydbzengine or oracledb with PyArrow). The 64-bit hash truncation is safe for row-level change detection but should not be used as a surrogate key at billion-row scale. The MERGE statement continues to harbor undiscovered bugs — the separate INSERT/UPDATE approach remains correct. Weekly full reconciliation, explicit schema validation before Polars operations, and the four core SCD2 integrity checks provide the safety net this pipeline needs at scale.