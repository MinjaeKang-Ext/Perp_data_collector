import tempfile
import threading
import time
import unittest
from pathlib import Path

from stream_parquet_store import StreamFlushLoop, StreamParquetStore
from wal_stress_test import run_benchmark


class WALStressHarnessTests(unittest.TestCase):
    def test_wal_ack_percentiles_appear_in_operational_log(self):
        with tempfile.TemporaryDirectory() as temporary:
            store = StreamParquetStore(temporary, wal_first=True)
            store.append("binance", "SKHYUSDT", "bbo", [{
                "event_time": "2026-10-29T00:00:00Z", "price": 1,
            }])
            loop = StreamFlushLoop(store, interval_seconds=0.02,
                                   metrics_interval_seconds=0.02)
            with self.assertLogs(level="INFO") as captured:
                thread = threading.Thread(target=loop.run)
                thread.start()
                deadline = time.monotonic() + 2
                while (not any("wal_ack_samples=1" in line for line in captured.output)
                       and time.monotonic() < deadline):
                    time.sleep(0.01)
                loop.stop()
                thread.join(timeout=2)
            store.close_wal()
            self.assertTrue(any("wal_ack_p99_ms=" in line and "wal_ack_samples=1" in line
                                for line in captured.output))

    def test_mixed_collector_callbacks_project_exact_row_counts(self):
        with tempfile.TemporaryDirectory() as temporary:
            result = run_benchmark(
                Path(temporary) / "benchmark", sustained_fps=40,
                sustained_seconds=1, burst_fps=80, burst_seconds=1,
                flush_seconds=0.2, flush_rows=100,
                wal_segment_bytes=4096, max_wal_bytes=1024 * 1024,
                drain_timeout_seconds=5,
            )
        self.assertTrue(result["passed"], result["errors"])
        self.assertEqual(result["remaining_wal_bytes"], 0)
        self.assertEqual(result["expected_rows"], result["actual_rows"])
        self.assertEqual(result["gap_markers"], 0)


if __name__ == "__main__":
    unittest.main()
