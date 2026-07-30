import os
import re
from dataclasses import dataclass

import duckdb


DEFAULT_DUCKDB_MEMORY_LIMIT = "1GB"
DEFAULT_DUCKDB_THREADS = 4
MAX_DUCKDB_THREADS = 64
DEFAULT_CSV_MAX_LINE_SIZE = 32 * 1024 * 1024
MIN_CSV_MAX_LINE_SIZE = 2_000_000
MAX_CSV_MAX_LINE_SIZE = 256 * 1024 * 1024
_MEMORY_LIMIT_PATTERN = re.compile(
    r"^(\d+(?:\.\d+)?)\s*(B|KB|MB|GB|TB)$",
    re.IGNORECASE,
)


@dataclass(frozen=True)
class DuckDBSettings:
    memory_limit: str
    threads: int
    csv_max_line_size: int


def get_duckdb_settings() -> DuckDBSettings:
    memory_limit = os.getenv(
        "TRISHULA_DUCKDB_MEMORY_LIMIT", DEFAULT_DUCKDB_MEMORY_LIMIT
    ).strip()
    memory_match = _MEMORY_LIMIT_PATTERN.fullmatch(memory_limit)
    if not memory_match or float(memory_match.group(1)) <= 0:
        raise ValueError(
            "TRISHULA_DUCKDB_MEMORY_LIMIT must be a positive size such as "
            "1GB, 512MB, or 4GB"
        )
    memory_limit = f"{memory_match.group(1)}{memory_match.group(2)}"

    raw_threads = os.getenv("TRISHULA_DUCKDB_THREADS", str(DEFAULT_DUCKDB_THREADS))
    try:
        threads = int(raw_threads)
    except ValueError as exc:
        raise ValueError(
            "TRISHULA_DUCKDB_THREADS must be an integer between "
            f"1 and {MAX_DUCKDB_THREADS}"
        ) from exc
    if not 1 <= threads <= MAX_DUCKDB_THREADS:
        raise ValueError(
            "TRISHULA_DUCKDB_THREADS must be an integer between "
            f"1 and {MAX_DUCKDB_THREADS}"
        )

    raw_csv_max_line_size = os.getenv(
        "TRISHULA_CSV_MAX_LINE_SIZE", str(DEFAULT_CSV_MAX_LINE_SIZE)
    )
    try:
        csv_max_line_size = int(raw_csv_max_line_size)
    except ValueError as exc:
        raise ValueError(
            "TRISHULA_CSV_MAX_LINE_SIZE must be an integer number of bytes "
            f"between {MIN_CSV_MAX_LINE_SIZE} and {MAX_CSV_MAX_LINE_SIZE}"
        ) from exc
    if not MIN_CSV_MAX_LINE_SIZE <= csv_max_line_size <= MAX_CSV_MAX_LINE_SIZE:
        raise ValueError(
            "TRISHULA_CSV_MAX_LINE_SIZE must be an integer number of bytes "
            f"between {MIN_CSV_MAX_LINE_SIZE} and {MAX_CSV_MAX_LINE_SIZE}"
        )

    return DuckDBSettings(
        memory_limit=memory_limit.upper(),
        threads=threads,
        csv_max_line_size=csv_max_line_size,
    )


def csv_read_expression(file_path: str) -> str:
    """Return a consistently bounded DuckDB CSV reader expression."""
    clean_path = file_path.replace("'", "''")
    max_line_size = get_duckdb_settings().csv_max_line_size
    return (
        f"read_csv_auto('{clean_path}', header=true, "
        f"max_line_size={max_line_size})"
    )


def create_duckdb_connection() -> duckdb.DuckDBPyConnection:
    settings = get_duckdb_settings()
    connection = duckdb.connect(database=":memory:")
    try:
        connection.execute(f"SET memory_limit = '{settings.memory_limit}'")
        connection.execute(f"SET threads = {settings.threads}")
    except Exception:
        connection.close()
        raise
    return connection
