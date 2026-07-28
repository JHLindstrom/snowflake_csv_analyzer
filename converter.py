import sys
import os

user_site = os.path.expanduser("~/Library/Python/3.9/lib/python/site-packages")
if os.path.exists(user_site) and user_site not in sys.path:
    sys.path.insert(0, user_site)

import time
import duckdb
from typing import Dict, Any

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
    con = duckdb.connect(database=":memory:")
    
    # Configure DuckDB for high-throughput streaming
    con.execute("PRAGMA preserve_insertion_order=false;")
    
    # Escape single quotes in file paths for SQL execution
    clean_csv_path = csv_path.replace("'", "''")
    clean_parquet_path = parquet_path.replace("'", "''")

    print(f"[*] Starting conversion: '{csv_path}' ({csv_size_bytes / (1024**2):.2f} MB)")
    print(f"[*] Output path: '{parquet_path}' (Compression: {compression})")

    copy_query = f"""
    COPY (
        SELECT * FROM read_csv_auto('{clean_csv_path}', header=True)
    ) TO '{clean_parquet_path}' (
        FORMAT PARQUET,
        COMPRESSION '{compression}',
        ROW_GROUP_SIZE {row_group_size}
    );
    """

    con.execute(copy_query)
    elapsed = time.time() - start_time
    
    parquet_size_bytes = os.path.getsize(parquet_path)
    compression_ratio = (1 - (parquet_size_bytes / csv_size_bytes)) * 100 if csv_size_bytes > 0 else 0

    # Get row count from generated parquet
    row_count = con.execute(f"SELECT COUNT(*) FROM read_parquet('{clean_parquet_path}')").fetchone()[0]
    con.close()

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
    con = duckdb.connect(database=":memory:")
    clean_path = file_path.replace("'", "''")
    
    if file_path.endswith(".parquet") or file_path.endswith(".pq"):
        read_expr = f"read_parquet('{clean_path}')"
        schema_info = con.execute(f"DESCRIBE SELECT * FROM {read_expr}").fetchall()
        total_rows = con.execute(f"SELECT COUNT(*) FROM {read_expr}").fetchone()[0]
        sample_df = con.execute(f"SELECT * FROM {read_expr} LIMIT {limit}").fetchdf()
    else:
        read_expr = f"read_csv_auto('{clean_path}', header=True)"
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
