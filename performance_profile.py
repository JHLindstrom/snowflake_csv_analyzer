"""Reproducible CPU profiling for the synthetic Trishula benchmark."""

import cProfile
import json
import pstats
from pathlib import Path

from benchmark import run_benchmark


def run_performance_profile(rows, output_dir, top_functions=40):
    root = Path(output_dir).expanduser().resolve()
    root.mkdir(parents=True, exist_ok=True)
    profile_path = root / "trishula-profile.pstats"
    summary_path = root / "trishula-profile.txt"

    profiler = cProfile.Profile()
    profiler.enable()
    result = run_benchmark(rows, output_dir=str(root), keep_files=True)
    profiler.disable()
    profiler.dump_stats(profile_path)

    with summary_path.open("w", encoding="utf-8") as stream:
        pstats.Stats(profiler, stream=stream).strip_dirs().sort_stats(
            "cumulative"
        ).print_stats(top_functions)
    (root / "benchmark-results.json").write_text(
        json.dumps(result, indent=2), encoding="utf-8"
    )
    return {
        "benchmark": result,
        "profile": str(profile_path),
        "summary": str(summary_path),
    }
