import os
import re
from dataclasses import dataclass

import duckdb


DEFAULT_DUCKDB_MEMORY_LIMIT = "1GB"
DEFAULT_DUCKDB_THREADS = 4
MAX_DUCKDB_THREADS = 64
_MEMORY_LIMIT_PATTERN = re.compile(
    r"^(\d+(?:\.\d+)?)\s*(B|KB|MB|GB|TB)$",
    re.IGNORECASE,
)


@dataclass(frozen=True)
class DuckDBSettings:
    memory_limit: str
    threads: int


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

    return DuckDBSettings(memory_limit=memory_limit.upper(), threads=threads)


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
