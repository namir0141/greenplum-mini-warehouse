#!/usr/bin/env python3
"""Run the full Greenplum mini data warehouse pipeline from one command.

    python run.py                     # generate -> ingest -> ELT -> star schema -> analytics
    python run.py --profile smoke     # small end-to-end run
    python run.py --benchmark         # also run the Level 4 benchmark suite
    python run.py --skip-generate     # reuse previously generated raw data

Each stage is a separate, independently-replayable script:
    python src/generate_data.py --profile smoke
    python src/ingest.py
    python src/elt.py
    python src/star_schema.py
    python src/analytics.py
    python src/benchmark.py --quick
    python src/incremental.py --init --run
"""
from __future__ import annotations

import argparse
import subprocess
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT / "src"))

from config import SCALE_PROFILES, load_config  # noqa: E402
from db import fmt_dur  # noqa: E402

STAGES = ["generate_data", "ingest", "elt", "star_schema", "analytics"]


def run_stage(script: str, args: list[str]) -> None:
    cmd = [sys.executable, str(ROOT / "src" / script), *args]
    print(f"\n{'=' * 70}\n$ {' '.join(cmd)}\n{'=' * 70}")
    t0 = time.perf_counter()
    proc = subprocess.run(cmd, cwd=ROOT)
    if proc.returncode != 0:
        print(f"\n[FAILED] stage {script} (exit {proc.returncode})")
        sys.exit(proc.returncode)
    print(f"[ok] {script} finished in {fmt_dur(time.perf_counter() - t0)}")


def main() -> None:
    ap = argparse.ArgumentParser(description="Greenplum mini data warehouse pipeline")
    ap.add_argument("--profile", choices=list(SCALE_PROFILES),
                    help="scale profile for data generation (default from config.yaml)")
    ap.add_argument("--benchmark", action="store_true", help="include the Level 4 benchmark suite")
    ap.add_argument("--quick-benchmark", action="store_true", help="benchmark with 1 run per query")
    ap.add_argument("--skip-generate", action="store_true", help="reuse existing raw data")
    args = ap.parse_args()

    cfg = load_config()
    print(f"Greenplum Mini Data Warehouse — profile: {cfg.scale['profile']} "
          f"({cfg.n_orders:,} orders)")

    for stage in STAGES:
        if stage == "generate_data" and args.skip_generate:
            continue
        extra = ["--profile", args.profile] if stage == "generate_data" and args.profile else []
        run_stage(f"{stage}.py", extra)

    if args.benchmark or args.quick_benchmark:
        run_stage("benchmark.py", ["--quick"] if args.quick_benchmark else [])

    print("\nAll stages complete. Next steps:")
    print("  python src/incremental.py --init --run   # Level 6 incremental pipeline")
    print("  python src/benchmark.py                  # Level 4 full benchmark suite")
    print("  python run.py --benchmark                # include benchmarks next run")


if __name__ == "__main__":
    main()
