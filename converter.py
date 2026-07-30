import os

import re
import time
import uuid
import duckdb
from typing import Dict, Any, Iterable
from pathlib import Path

from duckdb_config import (
    MAX_CSV_MAX_LINE_SIZE,
    create_duckdb_connection,
    csv_read_expression,
    get_duckdb_settings,
)
from errors import DatasetValidationError

_COLUMN_ALIASES = {
    "SESSION": {
        "SESSION",
        "SESSIONID",
        "VEHICLESESSION",
        "VEHICLESESSIONID",
    },
    "EVENT_PATH": {
        "EVENTPATH",
        "SESSIONPATH",
        "SESSIONPATHCAPPEDATTWOREPEATS",
    },
    "TOTAL_EVENTS": {
        "TOTALEVENTS",
        "EVENTCOUNT",
        "STEPCOUNT",
        "STEPCOUNTAFTERREPEATCAP",
    },
}


def _connect() -> duckdb.DuckDBPyConnection:
    return create_duckdb_connection()


def _normalize_column_name(column_name: str) -> str:
    return re.sub(r"[^A-Z0-9]+", "", column_name.upper())


def _is_semantic_column(canonical_name: str, normalized_name: str) -> bool:
    if normalized_name in _COLUMN_ALIASES[canonical_name]:
        return True
    if canonical_name == "SESSION":
        return normalized_name.endswith("SESSION") and "PATH" not in normalized_name
    if canonical_name == "EVENT_PATH":
        return "PATH" in normalized_name
    return (
        "COUNT" in normalized_name
        and ("STEP" in normalized_name or "EVENT" in normalized_name)
    )


def resolve_dataset_columns(column_names: Iterable[str]) -> Dict[str, str]:
    """Map source headers to the canonical columns used by analytics."""
    columns = list(column_names)
    normalized = {column: _normalize_column_name(column) for column in columns}
    mapping = {}
    for canonical_name in ("SESSION", "EVENT_PATH", "TOTAL_EVENTS"):
        exact = [
            column
            for column, normalized_name in normalized.items()
            if normalized_name == _normalize_column_name(canonical_name)
        ]
        if len(exact) == 1:
            mapping[canonical_name] = exact[0]
            continue
        candidates = [
            column
            for column, normalized_name in normalized.items()
            if _is_semantic_column(canonical_name, normalized_name)
        ]
        if not candidates:
            raise DatasetValidationError(
                f"Dataset has no column that can be mapped to {canonical_name}",
                "Available columns: "
                + ", ".join(columns)
                + ". Rename the corresponding header or use a recognized semantic name.",
            )
        if len(candidates) > 1:
            raise DatasetValidationError(
                f"Dataset has ambiguous columns for {canonical_name}: "
                + ", ".join(candidates),
                "Keep one matching header or rename the intended column to "
                f"{canonical_name}.",
            )
        mapping[canonical_name] = candidates[0]
    return mapping


def _quote_identifier(identifier: str) -> str:
    escaped_identifier = identifier.replace('"', '""')
    return f'"{escaped_identifier}"'


def dataset_read_expression(
    connection: duckdb.DuckDBPyConnection, file_path: str
) -> str:
    """Return a query that exposes arbitrary supported headers canonically."""
    clean_path = file_path.replace("'", "''")
    base_expression = (
        f"read_parquet('{clean_path}')"
        if file_path.lower().endswith((".parquet", ".pq"))
        else csv_read_expression(file_path)
    )
    if file_path.lower().endswith((".parquet", ".pq")):
        return base_expression
    schema_rows = connection.execute(
        f"DESCRIBE SELECT * FROM {base_expression}"
    ).fetchall()
    columns = [row[0] for row in schema_rows]
    mapping = resolve_dataset_columns(columns)
    canonical_by_source = {source: canonical for canonical, source in mapping.items()}
    projection = []
    for column in columns:
        quoted_column = _quote_identifier(column)
        canonical_name = canonical_by_source.get(column)
        if canonical_name:
            projection.append(
                f"{quoted_column} AS {_quote_identifier(canonical_name)}"
            )
        else:
            projection.append(quoted_column)
    return f"(SELECT {', '.join(projection)} FROM {base_expression})"


def validate_dataset_schema(file_path: str) -> Dict[str, str]:
    """Validate the columns required by every analyzer operation."""
    con = _connect()
    try:
        read_expr = dataset_read_expression(con, file_path)
        schema = {
            row[0]: row[1]
            for row in con.execute(f"DESCRIBE SELECT * FROM {read_expr}").fetchall()
        }
    except duckdb.Error as exc:
        raise DatasetValidationError(
            f"Could not read dataset '{Path(file_path).name}': {exc}",
            "Verify that the file is a valid UTF-8 CSV with a header or a readable Parquet file.",
        ) from exc
    finally:
        con.close()
    if not any(
        numeric_type in schema["TOTAL_EVENTS"].upper()
        for numeric_type in ("INT", "DECIMAL", "DOUBLE", "FLOAT")
    ):
        raise DatasetValidationError(
            "TOTAL_EVENTS must be numeric",
            "Regenerate the export or remove non-numeric values from TOTAL_EVENTS.",
        )
    return schema


def convert_csv_to_parquet(
    csv_path: str,
    parquet_path: str,
    compression: str = "ZSTD",
    row_group_size: int = 122880
) -> Dict[str, Any]:
    """
    Stream-converts a CSV file to Parquet format using DuckDB.
    """
    if not os.path.exists(csv_path):
        raise FileNotFoundError(f"CSV file not found: {csv_path}")

    start_time = time.time()
    csv_size_bytes = os.path.getsize(csv_path)

    # Initialize in-memory DuckDB connection with max memory safety settings
    con = _connect()
    
    # Configure DuckDB for high-throughput streaming
    con.execute("PRAGMA preserve_insertion_order=false;")
    
    # Escape single quotes in file paths for SQL execution
    output_path = Path(parquet_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    partial_path = output_path.with_name(
        f".{output_path.name}.{uuid.uuid4().hex}.partial.parquet"
    )
    clean_parquet_path = str(partial_path).replace("'", "''")

    print(f"[*] Starting conversion: '{csv_path}' ({csv_size_bytes / (1024**2):.2f} MB)")
    print(f"[*] Output path: '{parquet_path}' (Compression: {compression})")

    try:
        read_expr = dataset_read_expression(con, csv_path)
        copy_query = f"""
        COPY (
            SELECT * FROM {read_expr}
        ) TO '{clean_parquet_path}' (
            FORMAT PARQUET,
            COMPRESSION '{compression}',
            ROW_GROUP_SIZE {row_group_size}
        );
        """
        con.execute(copy_query)
        validate_dataset_schema(str(partial_path))
        os.replace(partial_path, output_path)
    except duckdb.Error as exc:
        partial_path.unlink(missing_ok=True)
        if "Maximum line size" in str(exc):
            configured_limit = get_duckdb_settings().csv_max_line_size
            hint = (
                "The CSV contains a record larger than the configured "
                f"TRISHULA_CSV_MAX_LINE_SIZE={configured_limit} bytes. If it "
                "is a legitimate record, increase the setting up to "
                f"{MAX_CSV_MAX_LINE_SIZE} bytes. If DuckDB reports a "
                "single-line file, first verify the export's row delimiters "
                "and quoting."
            )
        else:
            hint = (
                "Check quoting, delimiter consistency, encoding, and column "
                "types near the reported row."
            )
        raise DatasetValidationError(
            f"CSV conversion failed: {exc}",
            hint,
        ) from exc
    except Exception:
        partial_path.unlink(missing_ok=True)
        raise
    finally:
        con.close()

    elapsed = time.time() - start_time
    parquet_size_bytes = os.path.getsize(parquet_path)
    compression_ratio = (1 - (parquet_size_bytes / csv_size_bytes)) * 100 if csv_size_bytes > 0 else 0

    # Get row count from generated parquet
    count_con = _connect()
    final_path = parquet_path.replace("'", "''")
    try:
        row_count = count_con.execute(
            f"SELECT COUNT(*) FROM read_parquet('{final_path}')"
        ).fetchone()[0]
    finally:
        count_con.close()

    result = {
        "csv_path": csv_path,
        "parquet_path": parquet_path,
        "csv_size_mb": round(csv_size_bytes / (1024**2), 2),
        "parquet_size_mb": round(parquet_size_bytes / (1024**2), 2),
        "compression_ratio_percent": round(compression_ratio, 2),
        "row_count": row_count,
        "elapsed_seconds": round(elapsed, 2),
        "rows_per_second": int(row_count / elapsed) if elapsed > 0 else 0
    }
    return result

def inspect_file(file_path: str, limit: int = 5) -> Dict[str, Any]:
    """
    Inspects schema and fetches sample rows from CSV or Parquet file in milliseconds.
    """
    con = _connect()
    clean_path = file_path.replace("'", "''")
    
    if file_path.endswith(".parquet") or file_path.endswith(".pq"):
        read_expr = f"read_parquet('{clean_path}')"
        schema_info = con.execute(f"DESCRIBE SELECT * FROM {read_expr}").fetchall()
        total_rows = con.execute(f"SELECT COUNT(*) FROM {read_expr}").fetchone()[0]
        sample_df = con.execute(f"SELECT * FROM {read_expr} LIMIT {limit}").fetchdf()
    else:
        read_expr = csv_read_expression(file_path)
        schema_info = con.execute(f"DESCRIBE SELECT * FROM {read_expr}").fetchall()
        # Fast sample without full 7.2GB CSV row scan
        sample_df = con.execute(f"SELECT * FROM {read_expr} LIMIT {limit}").fetchdf()
        total_rows = "Calculated during conversion"
    
    con.close()

    return {
        "file_path": file_path,
        "total_rows": total_rows,
        "file_size_mb": round(os.path.getsize(file_path) / (1024**2), 2),
        "columns": [{"name": row[0], "type": row[1]} for row in schema_info],
        "sample_df": sample_df
    }

if __name__ == "__main__":
    import sys
    if len(sys.argv) > 2:
        res = convert_csv_to_parquet(sys.argv[1], sys.argv[2])
        print("Conversion Complete:", res)
    elif len(sys.argv) > 1:
        res = inspect_file(sys.argv[1])
        print("Inspection Results:", res)
