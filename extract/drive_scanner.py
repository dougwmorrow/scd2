"""Network drive file metadata scanner — os.walk() + os.stat() extraction.

Walks a mounted network drive and collects file metadata (never opens files)
into a Polars DataFrame. Handles permission errors, broken symlinks, and
network glitches gracefully by logging and skipping.

Read-only: uses only os.stat(), os.lstat(), os.walk(), pathlib, pwd, grp.
Never opens, modifies, or locks any files.

Memory-bounded: accumulates metadata in batches of DRIVE_SCANNER_BATCH_SIZE
rows (default 50,000), converting each batch to a Polars DataFrame chunk
to avoid holding millions of dicts in memory simultaneously.
"""

from __future__ import annotations

import grp
import logging
import os
import pwd
import stat
from datetime import datetime, timezone
from fnmatch import fnmatch
from pathlib import Path
from typing import TYPE_CHECKING

import polars as pl

import config
from data_load.bcp_csv import prepare_dataframe_for_bcp, write_bcp_csv

if TYPE_CHECKING:
    from orchestration.drive_config import DriveConfig

logger = logging.getLogger(__name__)

# Column schema for the metadata DataFrame — defines order and types.
_METADATA_SCHEMA = {
    "drive_name": pl.Utf8,
    "full_file_path": pl.Utf8,
    "file_name": pl.Utf8,
    "file_extension": pl.Utf8,
    "parent_directory": pl.Utf8,
    "directory_depth": pl.Int32,
    "file_size_bytes": pl.Int64,
    "created_time": pl.Datetime,
    "modified_time": pl.Datetime,
    "accessed_time": pl.Datetime,
    "owner_uid": pl.Int32,
    "owner_name": pl.Utf8,
    "group_gid": pl.Int32,
    "group_name": pl.Utf8,
    "permissions_octal": pl.Utf8,
    "is_symlink": pl.Int8,
    "is_hidden": pl.Int8,
    "inode": pl.Int64,
    "hard_link_count": pl.Int32,
    "mount_path": pl.Utf8,
}

# Batch size for accumulating metadata rows before converting to DataFrame.
_BATCH_SIZE = int(os.getenv("DRIVE_SCANNER_BATCH_SIZE", "50000"))

# Directories to always skip (system/hidden dirs common on Windows shares).
_SYSTEM_EXCLUDE_DIRS = {
    "$RECYCLE.BIN", "System Volume Information", ".snapshot",
    ".Trash-1000", "lost+found",
}

# Cache for uid→username and gid→groupname lookups (avoids repeated syscalls).
_uid_cache: dict[int, str] = {}
_gid_cache: dict[int, str] = {}


def scan_drive(
    drive_config: DriveConfig,
    output_dir: str | Path,
) -> tuple[pl.DataFrame, Path]:
    """Scan a network drive and collect file metadata into a DataFrame and BCP CSV.

    Read-only: never opens, modifies, or locks any files.
    Uses os.walk() for traversal and os.stat()/os.lstat() for metadata.

    Args:
        drive_config: Drive configuration from NetworkDriveConfig.
        output_dir: Directory for temp BCP CSV files.

    Returns:
        Tuple of (DataFrame with metadata, CSV path).

    Raises:
        FileNotFoundError: If mount_path doesn't exist.
        PermissionError: If mount_path isn't accessible.
    """
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    mount_path = drive_config.mount_path
    drive_name = drive_config.drive_name

    # Validate mount point
    _validate_mount(mount_path, drive_name)

    logger.info(
        "Starting scan of %s at %s (max_depth=%s, follow_symlinks=%s)",
        drive_name, mount_path, drive_config.max_depth, drive_config.follow_symlinks,
    )

    # Parse patterns
    exclude_patterns = drive_config.exclude_patterns or []
    include_patterns = drive_config.include_patterns or []

    # Walk and collect metadata
    batch: list[dict] = []
    chunks: list[pl.DataFrame] = []
    total_files = 0
    total_errors = 0
    total_permission_errors = 0

    mount_path_len = len(mount_path.rstrip(os.sep))

    def _on_walk_error(err: OSError) -> None:
        nonlocal total_errors
        total_errors += 1
        logger.warning("os.walk error on %s: %s", err.filename, err)

    walker = os.walk(
        mount_path,
        topdown=True,
        onerror=_on_walk_error,
        followlinks=drive_config.follow_symlinks,
    )

    for dirpath, dirnames, filenames in walker:
        # --- Depth check ---
        rel_path = dirpath[mount_path_len:]
        depth = rel_path.count(os.sep) if rel_path else 0
        if drive_config.max_depth is not None and depth >= drive_config.max_depth:
            dirnames.clear()  # Prune deeper recursion
            continue

        # --- Prune excluded directories in-place ---
        dirnames[:] = [
            d for d in dirnames
            if d not in _SYSTEM_EXCLUDE_DIRS
            and not _matches_any(d, exclude_patterns)
        ]

        # --- Process files ---
        for filename in filenames:
            # Include/exclude pattern filtering
            if exclude_patterns and _matches_any(filename, exclude_patterns):
                continue
            if include_patterns and not _matches_any(filename, include_patterns):
                continue

            full_path = os.path.join(dirpath, filename)

            try:
                row = _collect_file_metadata(
                    full_path, filename, dirpath, mount_path,
                    mount_path_len, drive_name,
                )
            except PermissionError:
                total_permission_errors += 1
                logger.debug("Permission denied: %s", full_path)
                continue
            except FileNotFoundError:
                # File deleted between walk and stat (race condition)
                logger.debug("File vanished: %s", full_path)
                continue
            except OSError as exc:
                total_errors += 1
                logger.warning("stat error on %s: %s", full_path, exc)
                continue

            batch.append(row)
            total_files += 1

            # Convert batch to DataFrame chunk when full
            if len(batch) >= _BATCH_SIZE:
                chunks.append(pl.DataFrame(batch, schema=_METADATA_SCHEMA))
                logger.info(
                    "Scan progress: %s — %d files collected so far",
                    drive_name, total_files,
                )
                batch = []

    # Final batch
    if batch:
        chunks.append(pl.DataFrame(batch, schema=_METADATA_SCHEMA))

    # Combine all chunks
    if chunks:
        df = pl.concat(chunks, how="vertical")
    else:
        df = pl.DataFrame(schema=_METADATA_SCHEMA)

    logger.info(
        "Scan complete: %s — %d files, %d permission errors, %d other errors",
        drive_name, total_files, total_permission_errors, total_errors,
    )

    # Prepare for BCP and write CSV
    df = prepare_dataframe_for_bcp(df, source_is_oracle=False)
    csv_path = (
        output_dir
        / f"{drive_config.source_name}_{drive_config.table_name}_scan.csv"
    )
    write_bcp_csv(df, csv_path)

    return df, csv_path


# ---------------------------------------------------------------------------
# File metadata collection
# ---------------------------------------------------------------------------

def _collect_file_metadata(
    full_path: str,
    filename: str,
    dirpath: str,
    mount_path: str,
    mount_path_len: int,
    drive_name: str,
) -> dict:
    """Collect metadata for a single file via os.lstat() / os.stat().

    Uses os.lstat() to detect symlinks without following them,
    then os.stat() for the actual file metadata (follows symlinks).

    Returns:
        Dict of metadata columns matching _METADATA_SCHEMA.

    Raises:
        PermissionError, FileNotFoundError, OSError on inaccessible files.
    """
    # lstat first to detect symlinks without following
    lst = os.lstat(full_path)
    is_symlink = stat.S_ISLNK(lst.st_mode)

    # For non-symlinks, lstat == stat. For symlinks, stat follows the link.
    if is_symlink:
        try:
            st = os.stat(full_path)
        except (FileNotFoundError, OSError):
            # Broken symlink — use lstat data
            st = lst
    else:
        st = lst

    # Relative path from mount root
    rel_path = full_path[mount_path_len:] if len(full_path) > mount_path_len else full_path

    # Extension (lowercased, includes dot)
    _, ext = os.path.splitext(filename)
    extension = ext.lower() if ext else ""

    # Directory depth from mount root
    rel_dir = dirpath[mount_path_len:]
    depth = rel_dir.count(os.sep) if rel_dir else 0

    # Timestamps — convert to UTC datetime
    created_time = _epoch_to_datetime(getattr(st, "st_birthtime", st.st_ctime))
    modified_time = _epoch_to_datetime(st.st_mtime)
    accessed_time = _epoch_to_datetime(st.st_atime)

    # Owner/group name resolution (cached)
    owner_name = _resolve_uid(st.st_uid)
    group_name = _resolve_gid(st.st_gid)

    # Permissions as octal string
    perms = oct(stat.S_IMODE(st.st_mode))

    return {
        "drive_name": drive_name,
        "full_file_path": rel_path,
        "file_name": filename,
        "file_extension": extension,
        "parent_directory": dirpath[mount_path_len:] or os.sep,
        "directory_depth": depth,
        "file_size_bytes": st.st_size,
        "created_time": created_time,
        "modified_time": modified_time,
        "accessed_time": accessed_time,
        "owner_uid": st.st_uid,
        "owner_name": owner_name,
        "group_gid": st.st_gid,
        "group_name": group_name,
        "permissions_octal": perms,
        "is_symlink": 1 if is_symlink else 0,
        "is_hidden": 1 if filename.startswith(".") else 0,
        "inode": st.st_ino,
        "hard_link_count": st.st_nlink,
        "mount_path": mount_path,
    }


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _validate_mount(mount_path: str, drive_name: str) -> None:
    """Validate that the mount path exists and is accessible.

    Raises:
        FileNotFoundError: If path doesn't exist.
        PermissionError: If path isn't readable.
    """
    if not os.path.isdir(mount_path):
        raise FileNotFoundError(
            f"Mount path for {drive_name} does not exist or is not a directory: "
            f"{mount_path}"
        )
    if not os.access(mount_path, os.R_OK | os.X_OK):
        raise PermissionError(
            f"Mount path for {drive_name} is not readable: {mount_path}"
        )


def _epoch_to_datetime(epoch: float) -> datetime:
    """Convert epoch seconds to UTC datetime."""
    try:
        return datetime.fromtimestamp(epoch, tz=timezone.utc)
    except (OSError, ValueError, OverflowError):
        # Some filesystems return 0 or negative epochs
        return datetime(1970, 1, 1, tzinfo=timezone.utc)


def _resolve_uid(uid: int) -> str:
    """Resolve UID to username with caching. Empty string on failure."""
    if uid in _uid_cache:
        return _uid_cache[uid]
    try:
        name = pwd.getpwuid(uid).pw_name
    except (KeyError, OverflowError):
        name = ""
    _uid_cache[uid] = name
    return name


def _resolve_gid(gid: int) -> str:
    """Resolve GID to group name with caching. Empty string on failure."""
    if gid in _gid_cache:
        return _gid_cache[gid]
    try:
        name = grp.getgrgid(gid).gr_name
    except (KeyError, OverflowError):
        name = ""
    _gid_cache[gid] = name
    return name


def _matches_any(name: str, patterns: list[str]) -> bool:
    """Check if a filename matches any glob pattern in the list."""
    return any(fnmatch(name, p) for p in patterns)
