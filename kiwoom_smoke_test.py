"""Run a bounded, credentialed Kiwoom KRX real-time smoke test locally.

The runner reads ``KIWOOM_APP_KEY`` and ``KIWOOM_APP_SECRET`` only from the
environment.  It never prints or saves them.  During KRX market hours, a pass
requires OAuth/WebSocket login plus persisted trade, BBO, and L2 data for the
requested stock code.
"""

from __future__ import annotations

import argparse
import logging
import os
import threading
import time
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from typing import Iterable

import pandas as pd

from kiwoom_collector import KiwoomStreamCollector
from stream_parquet_store import StreamFlushLoop, StreamParquetCompactor, StreamParquetStore

PROJECT_ROOT = Path(__file__).resolve().parent
DEFAULT_SECONDS = 90
DEFAULT_FLUSH_SECONDS = 5
REQUIRED_DATASETS = frozenset({"trades", "bbo", "l2_snapshots"})


def default_output_dir() -> Path:
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    return PROJECT_ROOT / "output" / f"kiwoom_smoke_{stamp}"


def summarize(root: Path, stock_code: str) -> dict[str, int]:
    """Return stored row counts by Kiwoom dataset, including staging parts."""
    rows: dict[str, int] = defaultdict(int)
    dataset_root = root / "kiwoom" / stock_code
    for path in sorted(dataset_root.rglob("*.parquet")):
        relative = path.relative_to(dataset_root)
        if relative.parts:
            rows[relative.parts[0]] += len(pd.read_parquet(path))
    return dict(rows)


def _tail(path: Path, lines: int = 30) -> str:
    if not path.exists():
        return "(collector log was not created)"
    return "\n".join(path.read_text(errors="replace").splitlines()[-lines:])


def parse_args(arguments: Iterable[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--seconds", type=float, default=DEFAULT_SECONDS, help="collection duration; default: 90")
    parser.add_argument("--stock-code", default="000660", help="KRX stock code; default: 000660")
    parser.add_argument("--mode", choices=("real", "demo"), default=os.getenv("KIWOOM_MODE", "real"))
    parser.add_argument("--output-dir", type=Path, help="new directory for smoke-test data")
    return parser.parse_args(arguments)


def main(arguments: Iterable[str] | None = None) -> int:
    args = parse_args(arguments)
    if args.seconds < DEFAULT_FLUSH_SECONDS:
        raise SystemExit(f"--seconds must be at least {DEFAULT_FLUSH_SECONDS:g}")
    app_key = os.getenv("KIWOOM_APP_KEY")
    app_secret = os.getenv("KIWOOM_APP_SECRET")
    if not app_key or not app_secret:
        raise SystemExit("Set KIWOOM_APP_KEY and KIWOOM_APP_SECRET in the shell before running this test.")

    output_dir = (args.output_dir or default_output_dir()).expanduser().resolve()
    if output_dir.exists():
        raise SystemExit(f"Refusing to overwrite existing smoke output: {output_dir}")
    output_dir.mkdir(parents=True)
    log_path = output_dir / "collector.log"
    logging.basicConfig(
        level=os.getenv("TRADING_LOG_LEVEL", "INFO").upper(),
        format="%(asctime)s %(levelname)s %(message)s",
        handlers=[logging.FileHandler(log_path, encoding="utf-8"), logging.StreamHandler()],
        force=True,
    )

    store = StreamParquetStore(output_dir, flush_rows=10_000, finalization_grace_seconds=300)
    flush_loop = StreamFlushLoop(store, DEFAULT_FLUSH_SECONDS)
    compactor = StreamParquetCompactor(output_dir, finalization_grace_seconds=300)
    collector = KiwoomStreamCollector(store, [args.stock_code], app_key, app_secret, mode=args.mode)
    flush_thread = threading.Thread(target=flush_loop.run, name="kiwoom-smoke-flush", daemon=True)
    worker = threading.Thread(target=collector.run, name="kiwoom-smoke-stream", daemon=True)

    print(f"Collecting Kiwoom {args.stock_code} ({args.mode}) for {args.seconds:g} seconds")
    print(f"Output: {output_dir}")
    flush_thread.start()
    worker.start()
    login_completed = False
    try:
        time.sleep(args.seconds)
    except KeyboardInterrupt:
        print("Stopping early")
    finally:
        login_completed = collector._logged_in
        collector.stop()
        worker.join(timeout=15)
        flush_loop.stop()
        flush_thread.join(timeout=15)
        compactor.compact_all()

    summary = summarize(output_dir, args.stock_code)
    print("dataset\trows")
    for dataset, rows in sorted(summary.items()):
        print(f"{dataset}\t{rows}")
    print(f"Collector log: {log_path}")

    missing = sorted(dataset for dataset in REQUIRED_DATASETS if summary.get(dataset, 0) == 0)
    if not login_completed:
        print("\nKiwoom smoke test FAILED: WebSocket login did not complete. Recent collector log:")
        print(_tail(log_path))
        return 1
    if missing:
        print("\nKiwoom smoke test FAILED: no persisted rows for:")
        print("\n".join(f"- {dataset}" for dataset in missing))
        print("Run during KRX market hours and inspect the collector log for entitlement or IP-policy errors.")
        return 1
    print("\nKiwoom smoke test PASSED: OAuth/WebSocket login and all KRX datasets persisted.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
