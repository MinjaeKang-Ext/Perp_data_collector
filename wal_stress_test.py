"""Offline WAL-first stress test; never opens an exchange connection.

The synthetic frames pass through the production collector callbacks, WAL,
projectors, and Parquet writer. Use a disposable --root on the target disk.
"""

from __future__ import annotations

import argparse
import json
import os
import platform
import resource
import shutil
import sys
import tempfile
import threading
import time
from collections import Counter
from datetime import datetime
from pathlib import Path
from statistics import mean
from typing import Callable
from zoneinfo import ZoneInfo

import pyarrow.parquet as pq
import pandas as pd

from kiwoom_collector import KiwoomStreamCollector
from reference_price_collector import BinanceReferencePriceCollector, HyperliquidReferencePriceCollector
from stream_parquet_store import StreamFlushLoop, StreamParquetStore
from streaming_collector import BinancePublicStreamCollector, BinanceStreamCollector, HyperliquidStreamCollector


class _BenchmarkSocket:
    def close(self) -> None:
        pass


def _percentiles(values: list[float]) -> dict[str, float | None]:
    if not values:
        return {"p50": None, "p95": None, "p99": None, "max": None}
    ordered = sorted(values)
    return {
        f"p{percent}": round(ordered[min(len(ordered) - 1, int((len(ordered) - 1) * percent / 100))], 3)
        for percent in (50, 95, 99)
    } | {"max": round(ordered[-1], 3)}


def _peak_rss_bytes() -> int:
    value = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    return value if sys.platform == "darwin" else value * 1024


def _message(lane: str, sequence: int) -> tuple[str, dict[str, int]]:
    now_ms = int(time.time() * 1000)
    if lane == "binance_market":
        payload = {"data": {"e": "aggTrade", "E": now_ms, "s": "SKHYUSDT", "a": sequence,
                            "p": "100.5", "q": "3", "f": sequence, "l": sequence,
                            "T": now_ms, "m": True}}
        return json.dumps(payload), {"binance/agg_trades": 1}
    if lane == "binance_l2":
        bids = [[str(100 - level / 100), "2"] for level in range(10)]
        asks = [[str(101 + level / 100), "2"] for level in range(10)]
        payload = {"e": "depthUpdate", "E": now_ms, "s": "SKHYUSDT",
                   "U": sequence, "u": sequence, "pu": sequence - 1, "b": bids, "a": asks}
        return json.dumps(payload), {"binance/l2_updates": 20}
    if lane == "hyperliquid":
        if sequence % 3 == 0:
            payload = {"channel": "trades", "data": [{"coin": "xyz:SKHY", "side": "B",
                "px": "200", "sz": "1", "hash": f"0x{sequence:x}", "time": now_ms, "tid": sequence}]}
            expected = {"hyperliquid/trades": 1}
        elif sequence % 3 == 1:
            levels = [[{"px": str(200 - level / 100), "sz": "1", "n": 2} for level in range(10)],
                      [{"px": str(201 + level / 100), "sz": "1", "n": 2} for level in range(10)]]
            payload = {"channel": "l2Book", "data": {"coin": "xyz:SKHY", "time": now_ms,
                                                    "levels": levels}}
            expected = {"hyperliquid/l2_snapshots": 20}
        else:
            payload = {"channel": "bbo", "data": {"coin": "xyz:SKHY", "time": now_ms,
                "bbo": [{"px": "200", "sz": "1"}, {"px": "201", "sz": "1"}]}}
            expected = {"hyperliquid/bbo": 1}
        return json.dumps(payload), expected
    if lane == "reference":
        if sequence % 2 == 0:
            payload = {"data": {"e": "markPriceUpdate", "E": now_ms, "s": "SKHYUSDT",
                                "i": "100", "p": "101", "P": "101", "r": "0.0001",
                                "T": now_ms + 3600000}}
            expected = {"binance/index_prices": 1, "binance/mark_prices": 1}
        else:
            payload = {"channel": "activeAssetCtx", "data": {"coin": "xyz:SKHY",
                "ctx": {"oraclePx": "200", "markPx": "201", "midPx": "200.5",
                        "funding": "0.0001", "openInterest": "1000"}}}
            expected = {"hyperliquid/oracle_prices": 1, "hyperliquid/mark_prices": 1}
        return json.dumps(payload), expected
    if lane == "kiwoom":
        payload = {"trnm": "REAL", "data": [{"type": "0B", "item": "000660",
            "values": {"20": datetime.now(ZoneInfo("Asia/Seoul")).strftime("%H%M%S"),
                       "10": "+200000", "15": "+12"}}]}
        return json.dumps(payload), {"kiwoom/trades": 1}
    raise ValueError(lane)


def run_benchmark(
    root: Path, *, sustained_fps: float, sustained_seconds: float, burst_fps: float,
    burst_seconds: float, flush_seconds: float, flush_rows: int, wal_segment_bytes: int,
    max_wal_bytes: int, drain_timeout_seconds: float, projector_delay_ms: float = 0,
) -> dict:
    if root.exists() and any(root.iterdir()):
        raise ValueError("--root must be an empty disposable directory")
    market = StreamParquetStore(root / "market" / "raw", wal_first=True,
                                flush_rows=flush_rows, wal_segment_bytes=wal_segment_bytes,
                                max_wal_bytes=max_wal_bytes)
    reference = StreamParquetStore(root / "reference" / "raw", wal_first=True,
                                   flush_rows=flush_rows, wal_segment_bytes=wal_segment_bytes,
                                   max_wal_bytes=max_wal_bytes)
    market_loop = StreamFlushLoop(market, flush_seconds)
    reference_loop = StreamFlushLoop(reference, flush_seconds)
    collectors = {
        "binance_market": BinanceStreamCollector(market, ["SKHYUSDT"]),
        "binance_l2": BinancePublicStreamCollector(market, ["SKHYUSDT"]),
        "hyperliquid": HyperliquidStreamCollector(market, ["xyz:SKHY"]),
        "kiwoom": KiwoomStreamCollector(market, ["000660"], "benchmark-key", "benchmark-secret"),
        "reference": None,
    }
    binance_reference = BinanceReferencePriceCollector(reference, ["SKHYUSDT"])
    hyperliquid_reference = HyperliquidReferencePriceCollector(reference, ["xyz:SKHY"])
    callback: dict[str, Callable[[object, str], None]] = {
        lane: collector._on_message for lane, collector in collectors.items() if collector is not None
    }
    callback["reference"] = lambda _, message: (
        binance_reference._on_message(None, message)
        if '"markPriceUpdate"' in message else hyperliquid_reference._on_message(None, message)
    )
    weights = {"binance_market": 0.30, "binance_l2": 0.25, "hyperliquid": 0.30,
               "reference": 0.10, "kiwoom": 0.05}
    expected: Counter[str] = Counter()
    accepted: Counter[str] = Counter()
    acknowledgements: dict[str, list[float]] = {lane: [] for lane in weights}
    projection_lags: list[float] = []
    errors: list[str] = []
    peak_wal_bytes = 0
    baseline_rss_bytes = _peak_rss_bytes()
    initial_free_bytes = shutil.disk_usage(root).free
    stop_monitor = threading.Event()
    lock = threading.Lock()
    socket = _BenchmarkSocket()

    for store in (market, reference):
        original_write = store._write_rows

        def measured_write(key, rows, part_id, *, durable=False, _original=original_write):
            if projector_delay_ms:
                time.sleep(projector_delay_ms / 1000)
            _original(key, rows, part_id, durable=durable)
            if rows:
                oldest = min(pd.Timestamp(row["received_at"]) for row in rows)
                lag_ms = (pd.Timestamp.now(tz="UTC") - oldest).total_seconds() * 1000
                with lock:
                    projection_lags.append(lag_ms)

        store._write_rows = measured_write

    def monitor() -> None:
        nonlocal peak_wal_bytes
        while not stop_monitor.wait(0.05):
            used = market.metrics_snapshot()["wal_bytes"] + reference.metrics_snapshot()["wal_bytes"]
            peak_wal_bytes = max(peak_wal_bytes, used)

    def produce(lane: str, rate: float, seconds: float, start: float, offset: int) -> None:
        count = int(rate * seconds)
        for index in range(count):
            due = start + index / rate
            if due > time.monotonic():
                time.sleep(due - time.monotonic())
            message, rows = _message(lane, offset + index)
            begun = time.perf_counter_ns()
            try:
                callback[lane](socket, message)
            except Exception as error:
                with lock:
                    errors.append(f"{lane}: {type(error).__name__}: {error}")
                return
            elapsed_ms = (time.perf_counter_ns() - begun) / 1_000_000
            with lock:
                acknowledgements[lane].append(elapsed_ms)
                expected.update(rows)
                accepted[lane] += 1

    flush_threads = [threading.Thread(target=loop.run, daemon=True)
                     for loop in (market_loop, reference_loop)]
    for thread in flush_threads:
        thread.start()
    watcher = threading.Thread(target=monitor, daemon=True)
    watcher.start()
    started = time.monotonic()
    drain_seconds = 0.0
    phases = (("sustained", sustained_fps, sustained_seconds),
              ("burst", burst_fps, burst_seconds))
    phase_results = []
    try:
        for phase_index, (phase_name, total_rate, seconds) in enumerate(phases):
            phase_start = time.monotonic()
            accepted_before = sum(accepted.values())
            workers = [threading.Thread(target=produce, args=(lane, total_rate * weight, seconds,
                                                              phase_start,
                                                              phase_index * 10_000_000 + index * 1_000_000))
                       for index, (lane, weight) in enumerate(weights.items())]
            for worker in workers:
                worker.start()
            for worker in workers:
                worker.join()
            elapsed = time.monotonic() - phase_start
            phase_results.append({"phase": phase_name, "target_fps": total_rate,
                                  "planned_frames": sum(int(total_rate * weight * seconds)
                                                        for weight in weights.values()),
                                  "elapsed_seconds": round(elapsed, 3),
                                  "actual_fps": round((sum(accepted.values()) - accepted_before) / elapsed, 2),
                                  "wal_bytes_after_phase": market.metrics_snapshot()["wal_bytes"]
                                      + reference.metrics_snapshot()["wal_bytes"]})
            if errors:
                break
        drain_started = time.monotonic()
        while not errors and time.monotonic() - drain_started < drain_timeout_seconds:
            if market.fatal_error or reference.fatal_error:
                errors.append(str(market.fatal_error or reference.fatal_error))
                break
            if market.metrics_snapshot()["wal_bytes"] + reference.metrics_snapshot()["wal_bytes"] == 0:
                break
            time.sleep(0.1)
        drain_seconds = time.monotonic() - drain_started
    finally:
        stop_monitor.set()
        watcher.join()
        try:
            market.close_for_appends()
            reference.close_for_appends()
            market_loop.stop()
            reference_loop.stop()
        except Exception as error:
            errors.append(f"final drain: {type(error).__name__}: {error}")
        finally:
            for thread in flush_threads:
                thread.join(timeout=5)
            market.close_wal()
            reference.close_wal()

    actual: Counter[str] = Counter()
    parts = 0
    trade_ids: dict[str, list[int]] = {"binance/agg_trades": [], "hyperliquid/trades": []}
    for store in (market, reference):
        for path in store.root.rglob("part-*.parquet"):
            dataset_key = f"{path.parts[-7]}/{path.parts[-5]}"
            actual[dataset_key] += pq.ParquetFile(path).metadata.num_rows
            if dataset_key in trade_ids:
                column = "aggregate_trade_id" if dataset_key.startswith("binance/") else "trade_id"
                trade_ids[dataset_key].extend(pq.read_table(path, columns=[column]).column(0).to_pylist())
            parts += 1
    remaining_wal = market.metrics_snapshot()["wal_bytes"] + reference.metrics_snapshot()["wal_bytes"]
    if actual != expected:
        errors.append(f"Parquet row mismatch: expected={dict(expected)} actual={dict(actual)}")
    for dataset_key, identifiers in trade_ids.items():
        if len(set(identifiers)) != len(identifiers):
            errors.append(f"Duplicate synthetic trade IDs in {dataset_key}")
    if remaining_wal:
        errors.append(f"Undrained WAL: {remaining_wal} bytes")
    return {
        "passed": not errors, "errors": errors, "host": {"platform": platform.platform(),
            "python": platform.python_version(), "root": str(root.resolve())},
        "configuration": {"sustained_fps": sustained_fps, "sustained_seconds": sustained_seconds,
            "burst_fps": burst_fps, "burst_seconds": burst_seconds,
            "flush_seconds": flush_seconds, "flush_rows": flush_rows,
            "projector_delay_ms": projector_delay_ms,
            "wal_segment_bytes": wal_segment_bytes, "max_wal_bytes": max_wal_bytes},
        "phases": phase_results, "accepted_frames": dict(accepted),
        "acknowledgement_ms": {lane: _percentiles(values) for lane, values in acknowledgements.items()},
        "overall_acknowledgement_ms": _percentiles([value for values in acknowledgements.values()
                                                     for value in values]),
        "oldest_row_to_parquet_ms_by_part": _percentiles(projection_lags),
        "mean_acknowledgement_ms": round(mean([value for values in acknowledgements.values()
                                               for value in values]), 3) if sum(accepted.values()) else None,
        "peak_wal_bytes": peak_wal_bytes, "remaining_wal_bytes": remaining_wal,
        "drain_seconds_after_producers": round(drain_seconds, 3),
        "baseline_process_rss_bytes": baseline_rss_bytes,
        "peak_process_rss_bytes": _peak_rss_bytes(), "parquet_parts": parts,
        "gap_markers": sum(len(list(store.spool_directory.glob("gaps/*.json")))
                           for store in (market, reference)),
        "initial_free_disk_bytes": initial_free_bytes,
        "final_free_disk_bytes": shutil.disk_usage(root).free,
        "filesystem_device_id": os.stat(root).st_dev,
        "expected_rows": dict(expected), "actual_rows": dict(actual),
        "elapsed_seconds": round(time.monotonic() - started, 3),
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, help="empty disposable directory on target disk")
    parser.add_argument("--sustained-fps", type=float, default=50)
    parser.add_argument("--sustained-seconds", type=float, default=20)
    parser.add_argument("--burst-fps", type=float, default=200)
    parser.add_argument("--burst-seconds", type=float, default=10)
    parser.add_argument("--flush-seconds", type=float, default=5)
    parser.add_argument("--flush-rows", type=int, default=10000)
    parser.add_argument("--wal-segment-bytes", type=int, default=8 * 1024 * 1024)
    parser.add_argument("--max-wal-bytes", type=int, default=4 * 1024 * 1024 * 1024)
    parser.add_argument("--drain-timeout-seconds", type=float, default=30)
    parser.add_argument("--projector-delay-ms", type=float, default=0,
                        help="inject a delay before each Parquet part write")
    args = parser.parse_args()
    if min(args.sustained_fps, args.sustained_seconds, args.burst_fps,
           args.burst_seconds, args.flush_seconds, args.drain_timeout_seconds,
           args.flush_rows, args.wal_segment_bytes, args.max_wal_bytes) <= 0:
        parser.error("all rate, duration, and size options must be positive")
    if args.wal_segment_bytes > args.max_wal_bytes:
        parser.error("WAL segment bytes must not exceed WAL max bytes")
    if args.projector_delay_ms < 0:
        parser.error("projector delay must be non-negative")
    if args.root is None:
        with tempfile.TemporaryDirectory(prefix="perp-wal-stress-") as temporary:
            result = run_benchmark(Path(temporary), sustained_fps=args.sustained_fps,
                sustained_seconds=args.sustained_seconds, burst_fps=args.burst_fps,
                burst_seconds=args.burst_seconds, flush_seconds=args.flush_seconds,
                flush_rows=args.flush_rows, wal_segment_bytes=args.wal_segment_bytes,
                max_wal_bytes=args.max_wal_bytes,
                drain_timeout_seconds=args.drain_timeout_seconds,
                projector_delay_ms=args.projector_delay_ms)
    else:
        result = run_benchmark(args.root, sustained_fps=args.sustained_fps,
            sustained_seconds=args.sustained_seconds, burst_fps=args.burst_fps,
            burst_seconds=args.burst_seconds, flush_seconds=args.flush_seconds,
            flush_rows=args.flush_rows, wal_segment_bytes=args.wal_segment_bytes,
            max_wal_bytes=args.max_wal_bytes,
            drain_timeout_seconds=args.drain_timeout_seconds,
            projector_delay_ms=args.projector_delay_ms)
    print(json.dumps(result, indent=2, ensure_ascii=False))
    return 0 if result["passed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
