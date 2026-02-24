"""W-13: BCP XML format file generation from target table metadata."""

from __future__ import annotations

import logging
from pathlib import Path

logger = logging.getLogger(__name__)

# SQL Server type -> BCP XML xsi:type mapping
_SQL_TO_BCP_TYPE: dict[str, str] = {
    "BIGINT": "SQLBIGINT",
    "INT": "SQLINT",
    "SMALLINT": "SQLSMALLINT",
    "TINYINT": "SQLTINYINT",
    "BIT": "SQLBIT",
    "FLOAT": "SQLFLT8",
    "REAL": "SQLFLT4",
    "DATE": "SQLDATE",
    "DATETIME": "SQLDATETIME",
    "DATETIME2": "SQLDATETIME2",
    "TIME": "SQLTIME",
    "NVARCHAR": "SQLNVARCHAR",
    "VARCHAR": "SQLVARCHAR",
    "NCHAR": "SQLNCHAR",
    "CHAR": "SQLCHAR",
    "DECIMAL": "SQLDECIMAL",
    "NUMERIC": "SQLNUMERIC",
    "VARBINARY": "SQLVARYBIN",
}


def generate_bcp_format_file(
    full_table_name: str,
    output_path: str | Path,
    exclude_columns: set[str] | None = None,
) -> Path:
    """W-13: Generate BCP XML format file from target table metadata.

    Creates a version-controlled .fmt file that explicitly maps CSV fields
    to table columns, independent of INFORMATION_SCHEMA per-write queries.
    The format file uses character mode (matching -c flag) with tab delimiters
    and LF row terminators per the BCP CSV Contract.

    Args:
        full_table_name: Fully qualified table name (e.g. 'UDM_Stage.DNA.ACCT_cdc').
        output_path: Path to write the .fmt file.
        exclude_columns: Columns to exclude (e.g. {'_scd2_key'} for IDENTITY).

    Returns:
        Path to the generated format file.
    """
    from data_load.schema_utils import get_column_metadata

    if exclude_columns is None:
        exclude_columns = set()

    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    columns = [
        c for c in get_column_metadata(full_table_name)
        if c.column_name not in exclude_columns
    ]

    if not columns:
        raise ValueError(f"W-13: No columns found for {full_table_name} after exclusions")

    # Build XML format file
    lines = [
        '<?xml version="1.0"?>',
        '<BCPFORMAT xmlns="http://schemas.microsoft.com/sqlserver/2004/bulkload/format"',
        '           xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">',
        '  <RECORD>',
    ]

    for i, col in enumerate(columns, 1):
        # Last field uses LF terminator, all others use tab
        terminator = "\\n" if i == len(columns) else "\\t"
        # Use CharTerm for character mode (-c) BCP
        lines.append(
            f'    <FIELD ID="{i}" xsi:type="CharTerm" '
            f'TERMINATOR="{terminator}" MAX_LENGTH="0" />'
        )

    lines.append('  </RECORD>')
    lines.append('  <ROW>')

    for i, col in enumerate(columns, 1):
        # Map SQL type to BCP XML type
        base_type = col.data_type.upper().split("(")[0]
        bcp_type = _SQL_TO_BCP_TYPE.get(base_type, "SQLNVARCHAR")
        lines.append(
            f'    <COLUMN SOURCE="{i}" NAME="{col.column_name}" xsi:type="{bcp_type}" />'
        )

    lines.append('  </ROW>')
    lines.append('</BCPFORMAT>')

    output_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    logger.info("W-13: Generated BCP format file: %s (%d columns)", output_path, len(columns))
    return output_path
