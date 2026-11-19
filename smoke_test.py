"""Run a short local public-stream smoke test and validate its Parquet output.

The runner starts ``main.py`` for a bounded period, sends SIGINT for a graceful
flush, then verifies that high-frequency raw Parquet output
was persisted. It deliberately clears Kiwoom credentials so a public crypto
smoke test never uses a brokerage account by accident.
"""

from __future__ import annotations

import argparse
import os
import signal
import subprocess
import sys
import time
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parent
DEFAULT_SECONDS = 60
DEFAULT_FLUSH_SECONDS = 5
HIGH_FREQUENCY_PART_GLOB = "part-*.parquet"
REQUIRED_STREAM_DATASETS = frozenset({
    "binance/SKHYNIXUSDT/agg_trades",
    "binance/SKHYNIXUSDT/l2_updates",
    "binance/SKHYNIXUSDT/bbo",
    "binance/SKHYUSDT/agg_trades",
    "binance/SKHYUSDT/l2_updates",
    "binance/SKHYUSDT/bbo",
    "hyperliquid/xyz_SKHX/trades",
    "hyperliquid/xyz_SKHX/l2_snapshots",
    "hyperliquid/xyz_SKHX/bbo",
    "hyperliquid/xyz_SKHY/trades",
    "hyperliquid/xyz_SKHY/l2_snapshots",
    "hyperliquid/xyz_SKHY/bbo",
})


def _utc_now_text() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


def default_output_dir() -> Path:
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    return PROJECT_ROOT / "output" / f"smoke_{stamp}"


def collector_environment(output_dir: Path, flush_seconds: float, ca_bundle: Path | None) -> dict[str, str]:
    """Build a non-secret, bounded-run configuration for the collector child."""
    environment = os.environ.copy()
    environment.update({
        "TRADING_DATA_DIR": str(output_dir),
        "TRADING_STREAM_FLUSH_SECONDS": str(flush_seconds),
        "TRADING_STREAM_FLUSH_ROWS": "10000",
        "TRADING_FUNDING_BACKFILL_START": _utc_now_text(),
        "TRADING_FUNDING_POLL_SECONDS": "3600",
    })
    for name in ("KIWOOM_APP_KEY", "KIWOOM_APP_SECRET", "KIWOOM_MODE", "KIWOOM_STOCK_CODES"):
        environment.pop(name, None)
    if ca_bundle is not None:
        environment["TRADING_WS_CA_BUNDLE"] = str(ca_bundle)
    return environment


def _dataset_name(part: Path, root: Path) -> str:
    relative = part.relative_to(root)
    if len(relative.parts) < 4:
        return str(relative.parent)
    return "/".join(relative.parts[:3])


def _high_frequency_files(root: Path) -> list[Path]:
    """Return immutable high-frequency parts from the collector's raw root."""
    raw_root = root / "raw"
    return sorted(raw_root.rglob(HIGH_FREQUENCY_PART_GLOB)) if raw_root.is_dir() else []


def summarize_parts(root: Path) -> dict[str, dict[str, object]]:
    """Return row counts and timestamp bounds for high-frequency Parquet files."""
    summary: dict[str, dict[str, object]] = defaultdict(lambda: {
        "parts": 0,
        "rows": 0,
        "event_time_min": None,
        "event_time_max": None,
    })
    raw_root = root / "raw"
    for part in _high_frequency_files(root):
        frame = pd.read_parquet(part)
        dataset = summary[_dataset_name(part, raw_root)]
        dataset["parts"] = int(dataset["parts"]) + 1
        dataset["rows"] = int(dataset["rows"]) + len(frame)
        if "event_time" not in frame or frame.empty:
            continue
        timestamps = pd.to_datetime(frame["event_time"], utc=True)
        lower, upper = timestamps.min(), timestamps.max()
        current_lower, current_upper = dataset["event_time_min"], dataset["event_time_max"]
        dataset["event_time_min"] = lower if current_lower is None else min(current_lower, lower)
        dataset["event_time_max"] = upper if current_upper is None else max(current_upper, upper)
    return dict(summary)


def missing_required_datasets(summary: dict[str, dict[str, object]]) -> list[str]:
    """Identify subscribed high-frequency datasets with no persisted rows."""
    return sorted(
        dataset for dataset in REQUIRED_STREAM_DATASETS
        if int(summary.get(dataset, {}).get("rows", 0)) == 0
    )


def _tail(path: Path, lines: int = 25) -> str:
    if not path.exists():
        return "(collector log was not created)"
    content = path.read_text(errors="replace").splitlines()
    return "\n".join(content[-lines:])


def run_collector(seconds: float, output_dir: Path, ca_bundle: Path | None) -> int:
    """Run the collector for ``seconds`` and request graceful shutdown."""
    log_path = output_dir / "collector.log"
    output_dir.mkdir(parents=True, exist_ok=False)
    environment = collector_environment(output_dir, DEFAULT_FLUSH_SECONDS, ca_bundle)
    with log_path.open("w", encoding="utf-8") as log_file:
        process = subprocess.Popen(
            [sys.executable, "main.py"],
            cwd=PROJECT_ROOT,
            env=environment,
            stdout=log_file,
            stderr=subprocess.STDOUT,
            text=True,
        )
        deadline = time.monotonic() + seconds
        while process.poll() is None and time.monotonic() < deadline:
            time.sleep(min(0.5, deadline - time.monotonic()))
        if process.poll() is None:
            process.send_signal(signal.SIGINT)
        try:
            return process.wait(timeout=15)
        except subprocess.TimeoutExpired:
            process.terminate()
            return process.wait(timeout=10)


def print_summary(summary: dict[str, dict[str, object]]) -> None:
    if not summary:
        print("No high-frequency Parquet files were written.")
        return
    print("dataset\tparts\trows\tfirst event (UTC)\tlast event (UTC)")
    for dataset, values in sorted(summary.items()):
        print(
            f"{dataset}\t{values['parts']}\t{values['rows']}\t"
            f"{values['event_time_min']}\t{values['event_time_max']}"
        )


def parse_args(arguments: Iterable[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--seconds", type=float, default=DEFAULT_SECONDS, help="collection duration; default: 60")
    parser.add_argument("--output-dir", type=Path, help="new directory for smoke-test data")
    parser.add_argument(
        "--ca-bundle", type=Path,
        help="organization CA PEM used by a TLS-intercepting proxy; TLS verification stays enabled",
    )
    return parser.parse_args(arguments)


def main(arguments: Iterable[str] | None = None) -> int:
    args = parse_args(arguments)
    if args.seconds < 5:
        raise SystemExit("--seconds must be at least 5 so the configured flush can run")
    output_dir = (args.output_dir or default_output_dir()).expanduser().resolve()
    if output_dir.exists():
        raise SystemExit(f"Refusing to overwrite existing smoke output: {output_dir}")
    if args.ca_bundle is not None and not args.ca_bundle.is_file():
        raise SystemExit(f"CA bundle does not exist: {args.ca_bundle}")

    print(f"Collecting public Binance and Hyperliquid streams for {args.seconds:g} seconds")
    print(f"Output: {output_dir}")
    exit_code = run_collector(args.seconds, output_dir, args.ca_bundle)
    summary = summarize_parts(output_dir)
    print_summary(summary)
    print(f"Collector log: {output_dir / 'collector.log'}")

    if not summary:
        print("\nSmoke test FAILED: no live stream records were persisted. Recent collector log:")
        print(_tail(output_dir / "collector.log"))
        return 1
    missing = missing_required_datasets(summary)
    if missing:
        print("\nSmoke test FAILED: required live datasets have no persisted rows:")
        print("\n".join(f"- {dataset}" for dataset in missing))
        print("Recent collector log:")
        print(_tail(output_dir / "collector.log"))
        return 1
    if exit_code not in (0, -signal.SIGINT):
        print(f"\nSmoke test WARNING: collector exit code was {exit_code} after output was written.")
    print("\nSmoke test PASSED: high-frequency Parquet output was persisted.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
