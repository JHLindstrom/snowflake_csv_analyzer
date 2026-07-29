import json
import os
import platform
import resource
import shutil
import tempfile
import time
from pathlib import Path
from typing import Optional

from converter import convert_csv_to_parquet
from event_parser import get_event_frequencies
from generate_mock_data import generate_csv
from insights import get_transition_matrix


def _peak_rss_mb() -> float:
    peak = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    # macOS reports bytes; Linux reports KiB.
    divisor = 1024**2 if platform.system() == "Darwin" else 1024
    return round(peak / divisor, 2)


def run_benchmark(
    rows: int,
    output_dir: Optional[str] = None,
    keep_files: bool = False,
) -> dict:
    if rows < 1:
        raise ValueError("Benchmark rows must be positive")

    temporary = None
    if output_dir:
        root = Path(output_dir).expanduser().resolve()
        root.mkdir(parents=True, exist_ok=True)
    elif keep_files:
        root = Path(tempfile.mkdtemp(prefix="trishula-benchmark-"))
    else:
        temporary = tempfile.TemporaryDirectory(prefix="trishula-benchmark-")
        root = Path(temporary.name)

    csv_path = root / "benchmark.csv"
    parquet_path = root / "benchmark.parquet"
    disk_before = shutil.disk_usage(root).free

    started = time.perf_counter()
    generate_csv(str(csv_path), rows)
    generation_seconds = time.perf_counter() - started

    conversion = convert_csv_to_parquet(str(csv_path), str(parquet_path))
    analysis_started = time.perf_counter()
    frequencies = get_event_frequencies(str(parquet_path), top_n=20)
    analysis_seconds = time.perf_counter() - analysis_started
    heatmap_started = time.perf_counter()
    transition_matrix = get_transition_matrix(
        str(parquet_path), top_n=8, dedupe_mode="consecutive"
    )
    heatmap_seconds = time.perf_counter() - heatmap_started
    disk_after = shutil.disk_usage(root).free

    result = {
        "rows": rows,
        "generation_seconds": round(generation_seconds, 3),
        "conversion_seconds": conversion["elapsed_seconds"],
        "analysis_seconds": round(analysis_seconds, 3),
        "heatmap_seconds": round(heatmap_seconds, 3),
        "rows_per_second": conversion["rows_per_second"],
        "csv_size_mb": conversion["csv_size_mb"],
        "parquet_size_mb": conversion["parquet_size_mb"],
        "compression_ratio_percent": conversion["compression_ratio_percent"],
        "peak_process_rss_mb": _peak_rss_mb(),
        "disk_consumed_mb": round(max(0, disk_before - disk_after) / 1024**2, 2),
        "top_events_returned": len(frequencies),
        "heatmap_nonzero_cells": int((transition_matrix.values > 0).sum()),
        "output_directory": str(root) if output_dir or keep_files else None,
    }

    if output_dir:
        report_path = root / "benchmark-results.json"
        report_path.write_text(json.dumps(result, indent=2), encoding="utf-8")
    elif temporary and not keep_files:
        temporary.cleanup()

    return result
