# Why BCP bulk loads hang on SQL Server: a complete diagnostic guide

**The most common reason a BCP load of 300,000 rows hangs indefinitely is a lock conflict** — another session holds an exclusive or schema modification lock on the target table, forcing BCP to wait with no visible error or timeout. Running BCP without the `-b` batch flag compounds this: all 300K rows execute as a single transaction, guaranteeing lock escalation to a table-level exclusive lock, consuming massive transaction log space, and maximizing the window during which blocking can occur. This guide covers every major cause of BCP stalls on SQL Server (specifically on Linux with mssql-tools18) and provides copy-paste diagnostic queries to identify the root cause in minutes.

---

## 1. Table-level locks create silent, indefinite waits

BCP without the TABLOCK hint acquires **row-level exclusive (X) locks** on each inserted row, **intent exclusive (IX) locks** on pages and the table, and a **schema stability (Sch-S) lock** on the table object. When another session holds an incompatible lock, BCP enters a wait state with no output, no error, and no timeout (the default lock timeout is **-1, meaning infinite**).

The most common blocking scenarios for a 300K-row BCP load are:

- **Long-running UPDATE/DELETE with lock escalation.** A concurrent transaction on the target table accumulates over **5,000 row locks**, triggering automatic escalation to a table-level X lock. BCP's IX request is incompatible with X, producing a `LCK_M_IX` wait that persists until the other transaction commits or rolls back.
- **Open transaction from application code.** An uncommitted `BEGIN TRANSACTION` with any DML on the target table holds locks that block BCP. This is especially insidious when connection pooling keeps the transaction open long after the application code has moved on.
- **Application locks via sp_getapplock.** ETL frameworks sometimes use `sp_getapplock @LockMode='Exclusive'` to serialize pipeline stages. These produce `LCK_M_X` waits on an `xp_userlock` resource that are notoriously difficult to diagnose because they don't appear as standard table locks.
- **Another BCP/ETL process loading the same table.** If a prior BCP run (also without `-b`) escalated to a table X lock, a second BCP run requesting IX will block completely.

**Key lock compatibility rule:** IX is incompatible with S and X table locks. Any session holding a shared (S) table lock (from `SELECT WITH (TABLOCK)` or SERIALIZABLE isolation) or exclusive (X) lock blocks BCP inserts.

| Wait type | Cause |
|-----------|-------|
| `LCK_M_IX` | BCP waiting for intent exclusive lock — blocked by S or X table lock |
| `LCK_M_X` | BCP after lock escalation — blocked by any incompatible lock |
| `LCK_M_BU` | BCP with TABLOCK waiting for bulk update lock — blocked by non-BU lock |
| `LCK_M_SCH_S` | BCP waiting for schema stability — blocked by SCH-M from DDL |

---

## 2. Schema modification locks from index rebuilds block everything

BCP always requires a **Schema Stability (Sch-S) lock**, and the only lock type that blocks Sch-S is **Schema Modification (Sch-M)**. An offline `ALTER INDEX REBUILD`, `CREATE INDEX`, `ALTER TABLE`, or `TRUNCATE TABLE` holds Sch-M for its entire duration, which can range from seconds to hours on large tables.

**The cascading effect is worse than it appears.** In SQL Server 2012 and later, even a *waiting* Sch-M request blocks subsequent Sch-S requests. If Session A runs a long SELECT (holds Sch-S), Session B requests `ALTER INDEX REBUILD` (waits for Sch-M), and then BCP starts (needs Sch-S) — BCP is blocked by Session B's *waiting* Sch-M, not just by a granted one. This creates a three-session chain where BCP hangs even though the DDL hasn't started executing yet.

Online index rebuilds (`ONLINE = ON`) hold Sch-M only briefly at the beginning and end, but during those brief windows, BCP can still be blocked. SQL Server 2014+ offers `WAIT_AT_LOW_PRIORITY` to mitigate this:

```sql
ALTER INDEX [IX_Name] ON UDM_Stage.YourTable REBUILD
WITH (ONLINE = ON (WAIT_AT_LOW_PRIORITY 
    (MAX_DURATION = 1 MINUTES, ABORT_AFTER_WAIT = SELF)));
```

This causes the rebuild to abort itself rather than create a blocking chain.

---

## 3. Without the -b flag, 300K rows become one massive transaction

Running BCP without `-b` means all **300,000 rows are a single atomic transaction**. This has cascading consequences that make hangs far more likely:

**Lock escalation is guaranteed.** SQL Server attempts lock escalation when a single transaction accumulates approximately **5,000 locks on a single HoBt** (heap or B-tree). With 300K rows, escalation from row locks to a table-level X lock is virtually certain. Once escalated, the table X lock blocks all other sessions for the entire duration of the load — potentially minutes.

**Transaction log cannot be reused.** The entire 300K-row insert must be held in the active transaction log. Under FULL recovery model with fully logged inserts, this consumes roughly **1.5–2× the raw data size** in log space. A 60MB CSV file can generate 90–150MB of log records that cannot be truncated until the transaction commits. If the log fills, BCP hangs waiting for log space.

**Failure means total loss.** If BCP fails at row 250,000 (network drop, timeout, kill), all 300K rows roll back — and rollback is single-threaded, potentially taking longer than the original insert.

Using `-b 10000` changes the equation dramatically:

| Factor | Without `-b` | With `-b 10000` | With `-b 5000` |
|--------|-------------|-----------------|----------------|
| Lock escalation | Table X lock for full duration | Per-batch, released between batches | Stays below 5K threshold — row locks only |
| Lock duration | Minutes (entire load) | Seconds per batch | Sub-second per batch |
| Active log space | All 300K rows | ~10K rows | ~5K rows |
| Failure recovery | Lose everything | Lose only current batch | Lose only current batch |
| Blocking probability | **Very high** | **Low** | **Very low** |

**The optimal batch size is 5,000 or lower** to stay under the lock escalation threshold entirely. With `-b 5000`, BCP commits every 5,000 rows, releases all locks, and the next batch starts fresh. Other sessions have brief access windows between batches.

---

## 4. Transaction log pressure under FULL recovery can silently stall BCP

Under the **FULL recovery model** without TABLOCK, every BCP row insert is **fully logged** — complete log records for each row including data values. This is true regardless of whether the table is a heap or has indexes. The TABLOCK hint does not enable minimal logging under FULL recovery; it only changes the lock type.

Minimal logging requires **all** of these conditions simultaneously: SIMPLE or BULK_LOGGED recovery model, TABLOCK hint specified, table not replicated, and specific table structure conditions (empty heap = best case). Even under BULK_LOGGED recovery with TABLOCK, inserts into **non-empty tables with clustered indexes are still fully logged**.

| Recovery model + hints | Logging level | Log pressure | Hang risk from log |
|----------------------|---------------|-------------|-------------------|
| FULL, no TABLOCK | Full | **Highest** | **Highest** |
| FULL + TABLOCK | Full | **Highest** | **Highest** |
| BULK_LOGGED, no TABLOCK | Full | High | High |
| BULK_LOGGED + TABLOCK (empty heap) | **Minimal** | Very low | Low |
| SIMPLE + TABLOCK (empty heap) | **Minimal** | Very low | **Lowest** |

When the transaction log fills, BCP blocks with `WRITELOG` or `LOGBUFFER` wait types. If autogrow is enabled but configured with small increments (the common 1MB default), each growth event **blocks all transactions** while the new space is zero-initialized. Log files never benefit from Instant File Initialization — they must always be zero-filled for security. On slow storage (NFS mounts, spinning disks common on Linux deployments), growing the log from 1MB to 200MB in 1MB increments causes **~200 separate blocking pauses**.

**VLF fragmentation compounds the problem.** Each small autogrow creates 4 Virtual Log Files. Hundreds of small growths produce thousands of VLFs, degrading log write throughput by up to **2×** compared to a properly pre-sized log.

Diagnose with:
```sql
-- Check log space and what's preventing truncation
SELECT name, recovery_model_desc, log_reuse_wait_desc 
FROM sys.databases WHERE database_id = DB_ID();

SELECT total_log_size_in_bytes / 1048576.0 AS total_mb,
       used_log_space_in_bytes / 1048576.0 AS used_mb,
       used_log_space_in_percent
FROM sys.dm_db_log_space_usage;

-- Count VLFs (target: under 100)
SELECT * FROM sys.dm_db_log_info(DB_ID());  -- SQL Server 2016 SP2+
```

If `log_reuse_wait_desc = 'ACTIVE_TRANSACTION'`, the BCP single-transaction load itself is preventing log space reuse. The only fix is to commit (via `-b` batching) or increase log file size.

---

## 5. Index maintenance overhead can make BCP appear frozen

Each nonclustered index on the target table forces SQL Server to **update that index's B-tree for every inserted row**, adding random I/O, page splits, and additional log records. Tables with **5+ nonclustered indexes** can see BCP throughput degrade by **5–10×** compared to an unindexed heap. For 300K rows, this can push a load that should take 10 seconds into multi-minute territory, during which BCP produces no output and appears hung.

**Disabling nonclustered indexes before the load** is a well-established best practice:

```sql
-- Disable nonclustered indexes (NEVER disable the clustered index!)
ALTER INDEX [IX_Index1] ON UDM_Stage.YourTable DISABLE;
ALTER INDEX [IX_Index2] ON UDM_Stage.YourTable DISABLE;

-- Run BCP load...

-- Rebuild all indexes after
ALTER INDEX ALL ON UDM_Stage.YourTable REBUILD;
```

Disabling a clustered index renders the entire table inaccessible — no reads, no writes. Only disable nonclustered indexes. Performance gains from disabling indexes before bulk load are typically **30–60%** when combined with TABLOCK and BULK_LOGGED recovery.

---

## 6. Network, ODBC, and Linux-specific issues cause phantom hangs

**Firewall idle timeouts** are a major cause of BCP appearing hung on Linux. Network firewalls commonly drop idle TCP connections after 30–60 minutes. When this happens silently, BCP continues sending data into a dead connection. With Linux's default `tcp_retries2=15`, the TCP stack retransmits for **13–30 minutes** before declaring the connection dead — during which BCP shows no error.

Linux TCP keepalive defaults are dangerously permissive for BCP:

| Parameter | Default | Recommended |
|-----------|---------|-------------|
| `tcp_keepalive_time` | **7,200s (2 hours)** | 120s |
| `tcp_keepalive_intvl` | 75s | 30s |
| `tcp_keepalive_probes` | 9 | 4 |

With defaults, a dead connection takes over **2 hours** to detect. Configure via `/etc/sysctl.conf` or per-connection in ODBC Driver 18's `odbcinst.ini` (`KeepAlive=30; KeepAliveInterval=1`).

**ODBC Driver 18 defaults to `Encrypt=yes`**, a breaking change from Driver 17. If SQL Server uses a self-signed certificate, the TLS handshake fails or hangs. Use the `-No` flag (no encryption) or `-Yo` flag (trust server certificate) with mssql-tools18 BCP.

**Linux-specific BCP pitfalls** include UTF-8 BOM in CSV files (the BOM bytes `EF BB BF` are treated as data in the first column, causing conversion failures), Windows CRLF line endings (the `\r` is appended to the last column's data), and missing locale settings. Always ensure `LC_ALL=en_US.UTF-8` is set and strip BOMs with `sed -i '1s/^\xEF\xBB\xBF//' file.csv`.

**The `-h` hints flag (including TABLOCK) may not be supported on Linux BCP** depending on the mssql-tools version. If TABLOCK is required, alternatives include setting `sp_tableoption 'table_name', 'table lock on bulk load', 1` on the server side, or using `BULK INSERT` via sqlcmd instead of BCP.

---

## 7. Python subprocess.run() creates orphaned transaction risk

When `subprocess.run(timeout=N)` expires, Python sends **SIGKILL** (signal 9) — which cannot be caught or handled. The BCP process terminates instantly with no ODBC cleanup, no orderly TDS disconnect, and no TCP FIN handshake. SQL Server detects the dead connection via TCP reset or keepalive failure and initiates rollback of the entire transaction.

**The critical risk is cascading blockage.** If a firewall silently dropped the connection and SQL Server hasn't detected it yet, the server session remains active with all its locks. A retry BCP run blocks on those locks, times out, gets killed, and creates *another* orphaned session. This cascade can leave multiple zombie sessions holding locks on the target table.

Mitigation requires a multi-pronged approach:

```python
# Use Popen with SIGTERM escalation instead of subprocess.run()
proc = subprocess.Popen(bcp_cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
try:
    stdout, stderr = proc.communicate(timeout=timeout_seconds)
except subprocess.TimeoutExpired:
    proc.terminate()  # SIGTERM first — allows cleanup
    try:
        proc.wait(timeout=10)
    except subprocess.TimeoutExpired:
        proc.kill()  # SIGKILL as last resort
```

Before each BCP run, check for and kill orphaned sessions:
```sql
SELECT session_id, status, command, blocking_session_id
FROM sys.dm_exec_requests WHERE command = 'BULK INSERT';
```

---

## 8. Disk I/O bottlenecks produce no errors, only silence

Slow storage causes BCP to stall on `PAGEIOLATCH_EX` (data page writes) and `WRITELOG` (log flushes) waits. These waits produce no client-side error — BCP simply stops progressing. Average write latency exceeding **20ms for data files** or **2ms for log files** indicates an I/O bottleneck.

Data file autogrow without Instant File Initialization (IFI) blocks the insert while new space is zero-initialized. On Linux, IFI for data files requires configuration via `mssql-conf`. **Transaction log files never benefit from IFI** (except SQL Server 2022 for growths under 64MB) — they always zero-initialize.

```sql
-- Check I/O latency per file
SELECT DB_NAME(vfs.database_id) AS db, mf.name, mf.type_desc,
    CASE WHEN vfs.num_of_writes > 0 
         THEN vfs.io_stall_write_ms / vfs.num_of_writes ELSE 0 
    END AS avg_write_latency_ms
FROM sys.dm_io_virtual_file_stats(DB_ID(), NULL) vfs
JOIN sys.master_files mf ON vfs.database_id = mf.database_id 
    AND vfs.file_id = mf.file_id
ORDER BY avg_write_latency_ms DESC;
```

---

## 9. Complete diagnostic query toolkit

When BCP hangs, run these queries in sequence to identify the root cause within minutes.

**Step 1 — Find the BCP session and what it's waiting on:**
```sql
SELECT s.session_id, s.login_name, s.host_name, s.program_name,
    r.command, r.status, r.wait_type, r.wait_time, r.wait_resource,
    r.blocking_session_id, r.total_elapsed_time, r.row_count
FROM sys.dm_exec_sessions s
LEFT JOIN sys.dm_exec_requests r ON s.session_id = r.session_id
WHERE r.command = 'BULK INSERT' OR s.program_name LIKE '%bcp%';
```

**Step 2 — Identify the full blocking chain with SQL text:**
```sql
WITH cteBL (session_id, blocking_these) AS (
    SELECT s.session_id,
        (SELECT CAST(er.session_id AS VARCHAR(6)) + ', '
         FROM sys.dm_exec_requests er
         WHERE er.blocking_session_id = s.session_id
         FOR XML PATH('')) AS blocking_these
    FROM sys.dm_exec_sessions s
)
SELECT s.session_id, r.blocking_session_id AS blocked_by,
    bl.blocking_these, r.command, r.wait_type, r.wait_time,
    r.wait_resource, ib.event_info AS sql_text,
    s.login_name, s.host_name, s.program_name
FROM sys.dm_exec_sessions s
LEFT JOIN sys.dm_exec_requests r ON r.session_id = s.session_id
INNER JOIN cteBL bl ON s.session_id = bl.session_id
OUTER APPLY sys.dm_exec_input_buffer(s.session_id, NULL) ib
WHERE bl.blocking_these IS NOT NULL OR r.blocking_session_id > 0
ORDER BY LEN(bl.blocking_these) DESC;
```

**Step 3 — Check all locks on the target table:**
```sql
SELECT tl.request_session_id, tl.resource_type, tl.request_mode,
    tl.request_status, s.program_name, r.command, t.text
FROM sys.dm_tran_locks tl
LEFT JOIN sys.dm_exec_sessions s ON tl.request_session_id = s.session_id
LEFT JOIN sys.dm_exec_requests r ON tl.request_session_id = r.session_id
OUTER APPLY sys.dm_exec_sql_text(r.sql_handle) t
LEFT JOIN sys.partitions p ON p.hobt_id = tl.resource_associated_entity_id
WHERE tl.resource_database_id = DB_ID()
  AND (p.object_id = OBJECT_ID('UDM_Stage.YourTable')
       OR tl.resource_associated_entity_id = OBJECT_ID('UDM_Stage.YourTable'))
ORDER BY tl.request_status DESC, tl.request_mode DESC;
```

**Step 4 — Check transaction log pressure:**
```sql
SELECT total_log_size_in_bytes/1048576.0 AS total_mb,
    used_log_space_in_bytes/1048576.0 AS used_mb,
    used_log_space_in_percent
FROM sys.dm_db_log_space_usage;

SELECT name, recovery_model_desc, log_reuse_wait_desc
FROM sys.databases WHERE database_id = DB_ID();
```

**Step 5 — Find long-running open transactions:**
```sql
SELECT tat.transaction_id, tat.transaction_begin_time,
    DATEDIFF(SECOND, tat.transaction_begin_time, GETDATE()) AS age_seconds,
    tst.session_id, s.login_name, s.program_name,
    ib.event_info AS last_sql
FROM sys.dm_tran_active_transactions tat
JOIN sys.dm_tran_session_transactions tst ON tat.transaction_id = tst.transaction_id
LEFT JOIN sys.dm_exec_sessions s ON tst.session_id = s.session_id
OUTER APPLY sys.dm_exec_input_buffer(tst.session_id, NULL) ib
WHERE tst.session_id > 50
ORDER BY tat.transaction_begin_time;
```

**Step 6 — Kill the blocker (after confirming identity):**
```sql
KILL 57;                    -- Replace with actual blocking SPID
KILL 57 WITH STATUSONLY;    -- Monitor rollback progress
```

---

## Conclusion

The root cause of a hung BCP load into a SQL Server staging table almost always falls into one of three categories: **lock conflicts** (another session blocking access), **transaction log exhaustion** (log fills during a massive single-transaction insert), or **infrastructure issues** (network timeouts, slow I/O, ODBC driver quirks on Linux).

The single most impactful fix is adding the **`-b 5000` flag** to the BCP command. This transforms a monolithic 300K-row transaction into 60 small batches, each committing independently. Batching stays below the 5,000-lock escalation threshold (preventing table-level X locks), keeps active log space minimal, enables log reuse between batches, limits failure scope to a single batch, and creates brief access windows for concurrent queries. Pre-sizing the transaction log to at least 2× the expected data size and setting autogrow to **256MB–1GB increments** eliminates growth-related stalls. On Linux specifically, reducing TCP keepalive to 120 seconds, ensuring `Encrypt` and `TrustServerCertificate` are configured for ODBC Driver 18, and stripping UTF-8 BOMs from CSV files addresses the platform-specific hang scenarios. Finally, scheduling BCP loads outside of index maintenance windows — or using `WAIT_AT_LOW_PRIORITY` on rebuilds — eliminates the most common Sch-M blocking chain.