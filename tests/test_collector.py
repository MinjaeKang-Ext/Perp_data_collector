import json
import os
import signal
import subprocess
import sys
import tempfile
import threading
import unittest
from pathlib import Path
from unittest.mock import patch

import pandas as pd

from kiwoom_collector import KiwoomStreamCollector, parse_kiwoom_message
from main import BINANCE_SYMBOLS, HYPERLIQUID_COINS
from parquet_store import default_data_root
from process_raw_data import RawParquetProcessor
from reference_price_collector import (
    BinanceReferencePriceCollector,
    HyperliquidReferencePriceCollector,
    _ReconnectableWebSocket as ReferenceReconnectableWebSocket,
    reference_data_root,
)
from reconcile_parquet import reconcile_tree
from stream_parquet_store import (
    StorageFatalError, StreamFlushLoop, StreamParquetCompactor, StreamParquetStore,
    _atomic_parquet_write, wait_for_producers,
)
from stream_wal import WALCorruptionError, read_records
from streaming_collector import (
    BinancePublicStreamCollector,
    BinanceStreamCollector,
    HyperliquidStreamCollector,
    _ReconnectableWebSocket as MarketReconnectableWebSocket,
    parse_binance_message,
    parse_hyperliquid_message,
)


class StreamCollectorTests(unittest.TestCase):
    def setUp(self):
        self.received_at = pd.Timestamp("2026-10-09T01:00:00Z")

    def test_perpetual_universe_is_skhynix_common_share_and_adr(self):
        self.assertEqual(BINANCE_SYMBOLS, ("SKHYNIXUSDT", "SKHYUSDT"))
        self.assertEqual(HYPERLIQUID_COINS, ("xyz:SKHX", "xyz:SKHY"))

    def test_generic_data_directory_setting_replaces_personal_prefix(self):
        with patch.dict("os.environ", {"TRADING_DATA_DIR": "/tmp/trading-data-test"}, clear=False):
            self.assertEqual(default_data_root(), Path("/tmp/trading-data-test"))

    def test_binance_aggregate_trade_is_one_100ms_aggregate_event(self):
        symbol, dataset, rows = parse_binance_message({"data": {
            "e": "aggTrade", "E": 1_700_000_000_100, "s": "SKHYUSDT", "a": 55,
            "p": "100.5", "q": "3", "f": 51, "l": 54, "T": 1_700_000_000_000, "m": True,
        }}, self.received_at)
        self.assertEqual((symbol, dataset), ("SKHYUSDT", "agg_trades"))
        self.assertEqual(rows[0]["aggregate_trade_id"], 55)
        self.assertEqual(rows[0]["event_time"], pd.Timestamp("2023-11-14T22:13:20Z"))
        self.assertTrue(rows[0]["buyer_is_maker"])

    def test_binance_l2_keeps_each_changed_level_and_sequence_ids(self):
        symbol, dataset, rows = parse_binance_message({
            "e": "depthUpdate", "E": 1_700_000_000_100, "T": 1_700_000_000_050,
            "s": "SKHYNIXUSDT", "U": 100, "u": 102, "pu": 99,
            "b": [["100", "2"], ["99", "0"]], "a": [["101", "3"]],
        }, self.received_at)
        self.assertEqual((symbol, dataset), ("SKHYNIXUSDT", "l2_updates"))
        self.assertEqual([(row["side"], row["price"], row["quantity"]) for row in rows], [
            ("bid", 100.0, 2.0), ("bid", 99.0, 0.0), ("ask", 101.0, 3.0),
        ])
        self.assertEqual({row["final_update_id"] for row in rows}, {102})

    def test_binance_bbo_is_event_driven(self):
        symbol, dataset, rows = parse_binance_message({
            "e": "bookTicker", "E": 1_700_000_000_100, "T": 1_700_000_000_090,
            "s": "SKHYNIXUSDT", "u": 7, "b": "100", "B": "2", "a": "101", "A": "3",
        }, self.received_at)
        self.assertEqual((symbol, dataset), ("SKHYNIXUSDT", "bbo"))
        self.assertEqual(rows[0]["best_bid_price"], 100.0)
        self.assertEqual(rows[0]["best_ask_quantity"], 3.0)

    def test_binance_subscription_acknowledgement_is_not_treated_as_feed_data(self):
        self.assertIsNone(parse_binance_message({"result": None, "id": 1}, self.received_at))

    def test_hyperliquid_trade_events_retain_individual_trade_times(self):
        coin, dataset, rows = parse_hyperliquid_message({"channel": "trades", "data": [
            {"coin": "xyz:SKHY", "side": "B", "px": "200", "sz": "1.5", "hash": "0xabc", "time": 1_700_000_000_001, "tid": 23},
            {"coin": "xyz:SKHY", "side": "A", "px": "201", "sz": "2", "hash": "0xdef", "time": 1_700_000_000_002, "tid": 24},
        ]}, self.received_at)
        self.assertEqual((coin, dataset), ("xyz:SKHY", "trades"))
        self.assertEqual([row["trade_id"] for row in rows], [23, 24])
        self.assertLess(rows[0]["event_time"], rows[1]["event_time"])

    def test_hyperliquid_l2_is_snapshot_rows_and_bbo_handles_empty_side(self):
        coin, dataset, rows = parse_hyperliquid_message({"channel": "l2Book", "data": {
            "coin": "xyz:SKHX", "time": 1_700_000_000_000,
            "levels": [[{"px": "100", "sz": "2", "n": 3}], [{"px": "101", "sz": "4", "n": 5}]],
        }}, self.received_at)
        self.assertEqual((coin, dataset), ("xyz:SKHX", "l2_snapshots"))
        self.assertEqual([(row["side"], row["level"]) for row in rows], [("bid", 1), ("ask", 1)])
        _, bbo_dataset, bbo_rows = parse_hyperliquid_message({"channel": "bbo", "data": {
            "coin": "xyz:SKHX", "time": 1_700_000_000_000,
            "bbo": [{"px": "100", "sz": "2", "n": 3}, None],
        }}, self.received_at)
        self.assertEqual(bbo_dataset, "bbo")
        self.assertIsNone(bbo_rows[0]["best_ask_price"])

    def test_reference_price_messages_persist_in_separate_dataset_directories(self):
        with tempfile.TemporaryDirectory() as temporary:
            store = StreamParquetStore(temporary)
            binance = BinanceReferencePriceCollector(store, ["SKHYUSDT"])
            hyperliquid = HyperliquidReferencePriceCollector(store, ["xyz:SKHY"])
            with patch("reference_price_collector._utc_now", return_value=self.received_at):
                binance._on_message(None, json.dumps({"data": {
                "e": "markPriceUpdate", "E": 1_700_000_000_100, "s": "SKHYUSDT",
                "p": "101.25", "i": "100.75", "r": "0.0001", "T": 1_700_000_800_000,
                }}))
                hyperliquid._on_message(None, json.dumps({
                    "channel": "activeAssetCtx",
                    "data": {"coin": "xyz:SKHY", "ctx": {"oraclePx": "100.0", "markPx": "100.5"}},
                }))
            self.assertEqual(store.flush(), 4)
            root = Path(temporary)
            self.assertTrue(list(root.glob("binance/SKHYUSDT/index_prices/date=*/hour=*/parts/part-*.parquet")))
            self.assertTrue(list(root.glob("binance/SKHYUSDT/mark_prices/date=*/hour=*/parts/part-*.parquet")))
            self.assertTrue(list(root.glob("hyperliquid/xyz_SKHY/oracle_prices/date=*/hour=*/parts/part-*.parquet")))
            self.assertTrue(list(root.glob("hyperliquid/xyz_SKHY/mark_prices/date=*/hour=*/parts/part-*.parquet")))

    def test_kiwoom_normalizes_trade_bbo_and_ten_level_l2(self):
        received_at = pd.Timestamp("2026-10-09T00:00:00Z")
        parsed = parse_kiwoom_message({"trnm": "REAL", "data": [
            {"type": "0B", "item": "000660", "values": {"20": "090001", "10": "+200000", "15": "+12", "13": "100"}},
            {"type": "0C", "item": "000660", "values": {"20": "090001", "27": "200100", "28": "200000"}},
            {"type": "0D", "item": "000660", "values": {"20": "090001", "41": "200100", "51": "200000", "61": "7", "71": "9"}},
        ]}, received_at)
        by_dataset = {dataset: rows for _, dataset, rows in parsed}
        self.assertEqual(by_dataset["trades"][0]["side"], "buy")
        self.assertEqual(by_dataset["trades"][0]["quantity"], 12.0)
        self.assertEqual(by_dataset["bbo"][0]["best_bid_price"], 200000.0)
        self.assertEqual([(row["side"], row["quantity"]) for row in by_dataset["l2_snapshots"]], [
            ("ask", 7.0), ("bid", 9.0),
        ])

    def test_kiwoom_logs_in_before_registering_live_streams(self):
        class FakeWebSocket:
            def __init__(self):
                self.sent = []

            def send(self, message):
                self.sent.append(__import__("json").loads(message))

            def close(self):
                raise AssertionError("login should not fail")

        with tempfile.TemporaryDirectory() as temporary:
            collector = KiwoomStreamCollector(
                StreamParquetStore(temporary), ["000660"], "not-a-real-key", "not-a-real-secret"
            )
            collector.token_provider.issue = lambda: "short-lived-token"
            socket = FakeWebSocket()
            collector._on_open(socket)
            self.assertEqual(socket.sent, [{"trnm": "LOGIN", "token": "short-lived-token"}])
            collector._on_message(socket, '{"trnm":"LOGIN","return_code":0}')
            self.assertEqual(socket.sent[1]["trnm"], "REG")
            self.assertEqual(socket.sent[1]["data"][0], {"item": ["000660"], "type": ["0B", "0C", "0D"]})

    def test_kiwoom_login_rejection_closes_socket_for_fresh_token_on_reconnect(self):
        class FakeWebSocket:
            def __init__(self):
                self.sent = []
                self.close_calls = 0

            def send(self, message):
                self.sent.append(__import__("json").loads(message))

            def close(self):
                self.close_calls += 1

        with tempfile.TemporaryDirectory() as temporary:
            collector = KiwoomStreamCollector(
                StreamParquetStore(temporary), ["000660"], "not-a-real-key", "not-a-real-secret"
            )
            tokens = iter(("first-token", "replacement-token"))
            collector.token_provider.issue = lambda: next(tokens)
            rejected_socket = FakeWebSocket()
            collector._on_open(rejected_socket)
            collector._on_message(rejected_socket, '{"trnm":"LOGIN","return_code":-1,"return_msg":"expired"}')
            self.assertEqual(rejected_socket.sent, [{"trnm": "LOGIN", "token": "first-token"}])
            self.assertEqual(rejected_socket.close_calls, 1)

            reconnected_socket = FakeWebSocket()
            collector._on_open(reconnected_socket)
            self.assertEqual(reconnected_socket.sent, [{"trnm": "LOGIN", "token": "replacement-token"}])

    def test_stream_store_writes_append_only_kst_parquet_parts(self):
        with tempfile.TemporaryDirectory() as temporary:
            store = StreamParquetStore(temporary, flush_rows=10)
            store.append("binance", "SKHYUSDT", "bbo", [{
                "event_time": pd.Timestamp("2026-10-08T15:30:00Z"),
                "received_at": self.received_at,
                "best_bid_price": 100.0,
            }])
            store.append("binance", "SKHYUSDT", "bbo", [{
                "event_time": pd.Timestamp("2026-10-08T15:30:01Z"),
                "received_at": self.received_at,
                "best_bid_price": 101.0,
            }])
            self.assertEqual(store.flush(), 2)
            directory = (
                Path(temporary) / "binance" / "SKHYUSDT" / "bbo"
                / "date=2026-10-09" / "hour=00" / "parts"
            )
            parts = list(directory.glob("part-*.parquet"))
            self.assertEqual(len(parts), 1)
            saved = pd.read_parquet(parts[0])
            self.assertEqual(saved.event_time.tolist(), [
                pd.Timestamp("2026-10-09T00:30:00+09:00"),
                pd.Timestamp("2026-10-09T00:30:01+09:00"),
            ])

    def test_stream_store_metrics_report_buffer_and_persisted_rows(self):
        with tempfile.TemporaryDirectory() as temporary:
            store = StreamParquetStore(temporary, flush_rows=3)
            store.append("binance", "SKHYUSDT", "bbo", [{
                "event_time": self.received_at, "received_at": self.received_at,
                "best_bid_price": 100.0,
            }] * 2)
            before = store.metrics_snapshot()
            self.assertEqual((before["appended_rows"], before["buffered_rows"], before["max_buffered_rows"]), (2, 2, 2))
            self.assertEqual(before["persisted_rows"], 0)
            self.assertEqual(store.flush(), 2)
            after = store.metrics_snapshot()
            self.assertEqual((after["buffered_rows"], after["persisted_rows"], after["explicit_flush_calls"]), (0, 2, 1))
            before["buffered_rows"] = 999
            self.assertEqual(store.metrics_snapshot()["buffered_rows"], 0)
            store.metrics_snapshot(reset_interval=True)
            self.assertEqual(store.metrics_snapshot()["persisted_rows"], 0)

    def test_row_threshold_write_blocks_another_callback_and_is_measured(self):
        with tempfile.TemporaryDirectory() as temporary:
            store = StreamParquetStore(temporary, flush_rows=1)
            write_entered = threading.Event()
            release_write = threading.Event()
            second_done = threading.Event()
            errors = []

            def slow_write(frame, output):
                write_entered.set()
                if not release_write.wait(2):
                    raise TimeoutError("test write was not released")
                _atomic_parquet_write(frame, output)

            def append_row(price, done=None):
                try:
                    store.append("binance", "SKHYUSDT", "bbo", [{
                        "event_time": self.received_at,
                        "received_at": self.received_at,
                        "best_bid_price": price,
                    }])
                except Exception as error:
                    errors.append(error)
                finally:
                    if done is not None:
                        done.set()

            with patch("stream_parquet_store._atomic_parquet_write", side_effect=slow_write):
                first = threading.Thread(target=append_row, args=(100.0,))
                second = threading.Thread(target=append_row, args=(101.0, second_done))
                try:
                    first.start()
                    self.assertTrue(write_entered.wait(2))
                    second.start()
                    self.assertFalse(second_done.wait(0.05))
                finally:
                    release_write.set()
                    first.join(2)
                    if second.ident is not None:
                        second.join(2)
            self.assertFalse(errors)
            self.assertTrue(second_done.is_set())
            metrics = store.metrics_snapshot()
            self.assertEqual((metrics["appended_rows"], metrics["persisted_rows"], metrics["row_threshold_flushes"]), (2, 2, 2))
            self.assertGreater(metrics["max_append_lock_wait_seconds"], 0)
            self.assertGreater(metrics["max_write_seconds"], 0)

    def test_stream_store_metrics_count_write_failure(self):
        with tempfile.TemporaryDirectory() as temporary:
            store = StreamParquetStore(temporary, flush_rows=10)
            store.append("binance", "SKHYUSDT", "bbo", [{
                "event_time": self.received_at, "received_at": self.received_at,
                "best_bid_price": 100.0,
            }])
            with patch("stream_parquet_store._atomic_parquet_write", side_effect=OSError("disk full")):
                with self.assertRaisesRegex(OSError, "disk full"):
                    store.flush()
            metrics = store.metrics_snapshot()
            self.assertEqual(metrics["write_failures"], 1)
            self.assertEqual(metrics["persisted_rows"], 0)

    def test_async_writer_keeps_parquet_io_outside_callback_lock(self):
        with tempfile.TemporaryDirectory() as temporary:
            store = StreamParquetStore(temporary, flush_rows=1, async_writes=True, max_pending_rows=3)
            loop = StreamFlushLoop(store, 5)
            write_entered = threading.Event()
            release_write = threading.Event()
            worker = threading.Thread(target=loop.run)

            def slow_write(frame, output):
                write_entered.set()
                if not release_write.wait(2):
                    raise TimeoutError("test write was not released")
                _atomic_parquet_write(frame, output)

            try:
                with patch("stream_parquet_store._atomic_parquet_write", side_effect=slow_write):
                    worker.start()
                    store.append("binance", "SKHYUSDT", "bbo", [{
                        "event_time": self.received_at, "received_at": self.received_at,
                        "best_bid_price": 100.0,
                    }])
                    self.assertTrue(write_entered.wait(2))
                    # The writer is still inside Parquet I/O, yet a second
                    # WebSocket callback can enqueue without waiting for it.
                    second_done = threading.Event()
                    second = threading.Thread(target=lambda: (
                        store.append("binance", "SKHYUSDT", "bbo", [{
                            "event_time": self.received_at, "received_at": self.received_at,
                            "best_bid_price": 101.0,
                        }]), second_done.set()
                    ))
                    second.start()
                    self.assertTrue(second_done.wait(0.5))
                    second.join(2)
            finally:
                release_write.set()
                if worker.ident is not None:
                    loop.stop()
                    worker.join(2)
            self.assertFalse(worker.is_alive())
            parts = list(Path(temporary).rglob("part-*.parquet"))
            self.assertEqual(sum(len(pd.read_parquet(part)) for part in parts), 2)

    def test_async_writer_spools_overflow_without_waiting_for_slow_parquet(self):
        with tempfile.TemporaryDirectory() as temporary:
            store = StreamParquetStore(temporary, flush_rows=1, async_writes=True, max_pending_rows=1)
            loop = StreamFlushLoop(store, 5)
            write_entered = threading.Event()
            release_write = threading.Event()
            second_done = threading.Event()
            worker = threading.Thread(target=loop.run)

            def slow_write(frame, output):
                write_entered.set()
                if not release_write.wait(2):
                    raise TimeoutError("test write was not released")
                _atomic_parquet_write(frame, output)

            def append_second():
                store.append("binance", "SKHYUSDT", "bbo", [{
                    "event_time": self.received_at, "received_at": self.received_at,
                    "best_bid_price": 101.0,
                }])
                second_done.set()

            second = None
            with patch("stream_parquet_store._atomic_parquet_write", side_effect=slow_write):
                worker.start()
                try:
                    store.append("binance", "SKHYUSDT", "bbo", [{
                        "event_time": self.received_at, "received_at": self.received_at,
                        "best_bid_price": 100.0,
                    }])
                    self.assertTrue(write_entered.wait(2))
                    second = threading.Thread(target=append_second)
                    second.start()
                    self.assertTrue(second_done.wait(0.5))
                    self.assertEqual(store.metrics_snapshot()["buffered_rows"], 1)
                    self.assertEqual(store.metrics_snapshot()["spooled_files"], 1)
                finally:
                    release_write.set()
                    if second is not None:
                        second.join(2)
                    loop.stop()
                    worker.join(2)
            self.assertTrue(second_done.is_set())
            self.assertGreaterEqual(store.metrics_snapshot()["backpressure_events"], 1)
            self.assertEqual(store.metrics_snapshot()["buffered_rows"], 0)
            self.assertEqual(store.metrics_snapshot()["spooled_files"], 0)

    def test_async_spool_replays_after_restart_and_removes_journal(self):
        with tempfile.TemporaryDirectory() as temporary:
            store = StreamParquetStore(temporary, flush_rows=1, async_writes=True, max_pending_rows=1)
            for price in (100, 101):
                store.append("binance", "SKHYUSDT", "bbo", [{
                    "event_time": self.received_at, "received_at": self.received_at, "price": price,
                }])
            self.assertEqual(store.metrics_snapshot()["spooled_files"], 1)
            # Model an abrupt process death: the ordinary RAM batch is lost,
            # whereas the acknowledged overflow file survives process restart.
            recovered = StreamParquetStore(temporary, flush_rows=1, async_writes=True, max_pending_rows=1)
            self.assertEqual(recovered.flush(), 1)
            self.assertEqual(recovered.metrics_snapshot()["spooled_files"], 0)
            self.assertEqual(list((Path(temporary) / ".stream_spool").glob("*.json")), [])
            prices = [price for part in Path(temporary).rglob("part-*.parquet")
                      for price in pd.read_parquet(part)["price"]]
            self.assertEqual(prices, [101])

    def test_wal_first_recovers_acknowledged_frame_below_memory_threshold(self):
        with tempfile.TemporaryDirectory() as temporary:
            store = StreamParquetStore(temporary, wal_first=True, flush_rows=100)
            collector = BinanceStreamCollector(store, ["SKHYUSDT"])
            collector._on_message(None, json.dumps({"data": {
                "e": "aggTrade", "E": 1_700_000_000_100, "s": "SKHYUSDT", "a": 55,
                "p": "100.5", "q": "3", "f": 51, "l": 54,
                "T": 1_700_000_000_000, "m": True,
            }}))
            self.assertFalse(list(Path(temporary).rglob("part-*.parquet")))
            self.assertEqual(store.metrics_snapshot()["wal_frames"], 1)
            interval = store.metrics_snapshot(reset_interval=True)
            self.assertEqual(interval["wal_ack_samples"], 1)
            self.assertGreaterEqual(interval["wal_ack_p99_ms"], 0)
            self.assertEqual(store.metrics_snapshot(reset_interval=True)["wal_ack_samples"], 0)
            store.close_wal()  # Simulate process death without a graceful flush.
            active = next((Path(temporary) / ".stream_wal").glob("*.open"))
            journaled = list(read_records(active))
            self.assertEqual(journaled[0]["kind"], "frame")
            self.assertEqual(journaled[0]["source"], "binance-market")

            recovered = StreamParquetStore(temporary, wal_first=True, flush_rows=100)
            BinanceStreamCollector(recovered, ["SKHYUSDT"])
            self.assertEqual(recovered.flush(), 1)
            parts = list(Path(temporary).rglob("part-*.parquet"))
            self.assertEqual(len(parts), 1)
            self.assertEqual(pd.read_parquet(parts[0]).aggregate_trade_id.tolist(), [55])
            self.assertEqual(recovered.metrics_snapshot()["wal_bytes"], 0)
            recovered.close_wal()

    def test_wal_first_survives_real_sigkill_after_acknowledged_write(self):
        with tempfile.TemporaryDirectory() as temporary:
            script = (
                "import time\n"
                "from stream_parquet_store import StreamParquetStore\n"
                f"store = StreamParquetStore({temporary!r}, wal_first=True)\n"
                "store.append('binance', 'SKHYUSDT', 'bbo', "
                "[{'event_time': '2026-10-09T01:00:00Z', 'price': 321}])\n"
                "print('ACK', flush=True)\n"
                "time.sleep(30)\n"
            )
            child = subprocess.Popen(
                [sys.executable, "-u", "-c", script],
                cwd=Path(__file__).resolve().parents[1],
                stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
            )
            try:
                self.assertEqual(child.stdout.readline().strip(), "ACK")
                os.kill(child.pid, signal.SIGKILL)
                child.wait(timeout=5)
            finally:
                if child.poll() is None:
                    child.kill()
                    child.wait(timeout=5)
                child.stdout.close()
                child.stderr.close()
            recovered = StreamParquetStore(temporary, wal_first=True)
            self.assertEqual(recovered.flush(), 1)
            self.assertEqual([price for part in Path(temporary).rglob("part-*.parquet")
                              for price in pd.read_parquet(part).price], [321])
            recovered.close_wal()

    def test_wal_first_replays_reference_message_atomically_without_duplicate_parts(self):
        with tempfile.TemporaryDirectory() as temporary:
            store = StreamParquetStore(temporary, wal_first=True, flush_rows=10)
            collector = BinanceReferencePriceCollector(store, ["SKHYUSDT"])
            collector._on_message(None, json.dumps({"data": {
                "e": "markPriceUpdate", "E": 1_700_000_000_100, "s": "SKHYUSDT",
                "p": "101.25", "i": "100.75", "r": "0.0001", "T": 1_700_000_800_000,
            }}))
            calls = 0

            def fail_second(frame, output):
                nonlocal calls
                calls += 1
                if calls == 2:
                    raise OSError("projector interrupted")
                _atomic_parquet_write(frame, output)

            with patch("stream_parquet_store._atomic_parquet_write", side_effect=fail_second):
                with self.assertRaisesRegex(OSError, "projector interrupted"):
                    store.flush()
            self.assertEqual(len(list(Path(temporary).rglob("part-*.parquet"))), 1)
            store.close_wal()
            restarted = StreamParquetStore(temporary, wal_first=True, flush_rows=10)
            BinanceReferencePriceCollector(restarted, ["SKHYUSDT"])
            self.assertEqual(restarted.flush(), 2)
            self.assertEqual(len(list(Path(temporary).rglob("part-*.parquet"))), 2)
            self.assertEqual(sum(len(pd.read_parquet(part)) for part in Path(temporary).rglob("part-*.parquet")), 2)
            restarted.close_wal()

    def test_wal_first_slow_parquet_projection_does_not_hold_receiver(self):
        with tempfile.TemporaryDirectory() as temporary:
            store = StreamParquetStore(temporary, wal_first=True, wal_segment_bytes=128)
            store.register_frame_parser("test", lambda _message, stamp: [
                ("binance", "SKHYUSDT", "bbo", [{"event_time": stamp, "received_at": stamp, "price": 1}])
            ])
            loop = StreamFlushLoop(store, interval_seconds=5)
            entered = threading.Event()
            release = threading.Event()
            worker = threading.Thread(target=loop.run)

            def slow_write(frame, output):
                entered.set()
                if not release.wait(2):
                    raise TimeoutError("test Parquet writer was not released")
                _atomic_parquet_write(frame, output)

            try:
                with patch("stream_parquet_store._atomic_parquet_write", side_effect=slow_write):
                    worker.start()
                    store.ingest_frame("test", "x" * 200, self.received_at)
                    self.assertTrue(entered.wait(2))
                    done = threading.Event()
                    receiver = threading.Thread(target=lambda: (store.ingest_frame(
                        "test", "second", self.received_at
                    ), done.set()))
                    receiver.start()
                    self.assertTrue(done.wait(0.5))
                    receiver.join(2)
            finally:
                release.set()
                if worker.ident is not None:
                    loop.stop()
                    worker.join(2)
                store.close_wal()
            self.assertFalse(worker.is_alive())

    def test_wal_first_syncs_new_partition_ancestor_directories(self):
        with tempfile.TemporaryDirectory() as temporary:
            store = StreamParquetStore(temporary, wal_first=True)
            store.append("binance", "SKHYUSDT", "bbo", [{
                "event_time": self.received_at, "received_at": self.received_at, "price": 100,
            }])
            from stream_parquet_store import _fsync_directory
            with patch("stream_parquet_store._fsync_directory", wraps=_fsync_directory) as synced:
                self.assertEqual(store.flush(), 1)
            synced_paths = {call.args[0] for call in synced.call_args_list}
            output = next(Path(temporary).rglob("part-*.parquet"))
            current = output.parent
            while True:
                self.assertIn(current, synced_paths)
                if current == Path(temporary):
                    break
                current = current.parent
            store.close_wal()

    def test_wal_first_partial_multi_hour_replay_keeps_part_ids_stable(self):
        with tempfile.TemporaryDirectory() as temporary:
            store = StreamParquetStore(temporary, wal_first=True, flush_rows=10)
            store.append("binance", "SKHYUSDT", "bbo", [{
                "event_time": pd.Timestamp("2026-10-08T15:30:00Z"),
                "received_at": self.received_at, "price": 100,
            }, {
                "event_time": pd.Timestamp("2026-10-08T16:30:00Z"),
                "received_at": self.received_at, "price": 101,
            }])
            calls = 0

            def fail_second(frame, output):
                nonlocal calls
                calls += 1
                if calls == 2:
                    raise OSError("second partition failed")
                _atomic_parquet_write(frame, output)

            with patch("stream_parquet_store._atomic_parquet_write", side_effect=fail_second):
                with self.assertRaisesRegex(OSError, "second partition failed"):
                    store.flush()
            store.close_wal()
            recovered = StreamParquetStore(temporary, wal_first=True, flush_rows=10)
            self.assertEqual(recovered.flush(), 2)
            parts = list(Path(temporary).rglob("part-*.parquet"))
            self.assertEqual(len(parts), 2)
            self.assertEqual(sum(len(pd.read_parquet(part)) for part in parts), 2)
            recovered.close_wal()

    def test_wal_first_fails_closed_on_torn_tail(self):
        with tempfile.TemporaryDirectory() as temporary:
            store = StreamParquetStore(temporary, wal_first=True)
            store.register_frame_parser("test", lambda _message, _stamp: [])
            store.ingest_frame("test", "{}", self.received_at)
            store.close_wal()
            open_file = next((Path(temporary) / ".stream_wal").glob("*.open"))
            with open_file.open("ab") as output:
                output.write(b"\x01")
            with self.assertRaisesRegex(WALCorruptionError, "Incomplete WAL record header"):
                StreamParquetStore(temporary, wal_first=True)

    def test_wal_first_fails_closed_on_checksum_mismatch(self):
        with tempfile.TemporaryDirectory() as temporary:
            store = StreamParquetStore(temporary, wal_first=True)
            store.register_frame_parser("test", lambda _message, _stamp: [])
            store.ingest_frame("test", "{}", self.received_at)
            store.close_wal()
            path = next((Path(temporary) / ".stream_wal").glob("*.open"))
            with path.open("r+b") as output:
                output.seek(12)
                original = output.read(1)
                output.seek(12)
                output.write(bytes([original[0] ^ 1]))
            with self.assertRaisesRegex(WALCorruptionError, "checksum mismatch"):
                StreamParquetStore(temporary, wal_first=True)

    def test_wal_first_quota_failure_sets_fatal_and_gap_marker(self):
        with tempfile.TemporaryDirectory() as temporary:
            store = StreamParquetStore(temporary, wal_first=True, max_wal_bytes=128, wal_segment_bytes=64)
            store.register_frame_parser("test", lambda _message, _stamp: [])
            with self.assertLogs(level="CRITICAL"):
                with self.assertRaises(StorageFatalError):
                    store.ingest_frame("test", "x" * 1000, self.received_at)
            self.assertIsNotNone(store.fatal_error)
            self.assertTrue(list((Path(temporary) / ".stream_spool/gaps").glob("*.json")))
            store.close_wal()

    def test_wal_first_disk_sync_failure_is_fatal(self):
        with tempfile.TemporaryDirectory() as temporary:
            store = StreamParquetStore(temporary, wal_first=True)
            store.register_frame_parser("test", lambda _message, _stamp: [])
            from stream_wal import _fsync_directory
            calls = 0

            def fail_first_sync(path):
                nonlocal calls
                calls += 1
                if calls == 1:
                    raise OSError("simulated WAL disk fault")
                return _fsync_directory(path)

            with patch("stream_wal._fsync_directory", side_effect=fail_first_sync), \
                    self.assertLogs(level="CRITICAL"):
                with self.assertRaises(StorageFatalError):
                    store.ingest_frame("test", "{}", self.received_at)
            self.assertIsNotNone(store.fatal_error)
            store.close_wal()

    def test_wal_first_kiwoom_journals_real_without_control_or_token(self):
        class FakeSocket:
            def __init__(self):
                self.sent = []

            def send(self, message):
                self.sent.append(message)

            def close(self):
                pass

        with tempfile.TemporaryDirectory() as temporary:
            store = StreamParquetStore(temporary, wal_first=True)
            collector = KiwoomStreamCollector(store, ["000660"], "fake-key", "fake-secret")
            socket = FakeSocket()
            collector._on_message(socket, '{"trnm":"PING"}')
            collector._on_message(socket, '{"trnm":"LOGIN","return_code":0,"token":"private"}')
            collector._on_message(socket, json.dumps({"trnm": "REAL", "data": [{
                "type": "0B", "item": "000660",
                "values": {"20": "090001", "10": "+200000", "15": "+12"},
            }]}))
            store.close_wal()
            records = list(read_records(next((Path(temporary) / ".stream_wal").glob("*.open"))))
            self.assertEqual(len(records), 1)
            self.assertEqual(records[0]["source"], "kiwoom-market")
            self.assertNotIn("private", str(records))
            recovered = StreamParquetStore(temporary, wal_first=True)
            KiwoomStreamCollector(recovered, ["000660"], "fake-key", "fake-secret")
            self.assertEqual(recovered.flush(), 1)
            recovered.close_wal()

    def test_wal_first_rejects_second_owner_and_old_overflow_spool(self):
        with tempfile.TemporaryDirectory() as temporary:
            store = StreamParquetStore(temporary, wal_first=True)
            with self.assertRaisesRegex(WALCorruptionError, "Another collector"):
                StreamParquetStore(temporary, wal_first=True)
            store.close_wal()
            old_spool = Path(temporary) / ".stream_spool"
            old_spool.mkdir()
            (old_spool / "old.json").write_text("old")
            with self.assertRaisesRegex(StorageFatalError, "old overflow spool"):
                StreamParquetStore(temporary, wal_first=True)
            (old_spool / "old.json").unlink()
            (old_spool / "unfinished.tmp").write_text("partial")
            with self.assertRaisesRegex(StorageFatalError, "old overflow spool"):
                StreamParquetStore(temporary, wal_first=True)

    def test_wal_first_rejects_projection_setting_change_until_pending_segment_drains(self):
        with tempfile.TemporaryDirectory() as temporary:
            store = StreamParquetStore(temporary, wal_first=True, flush_rows=1)
            store.append("binance", "SKHYUSDT", "bbo", [{
                "event_time": self.received_at, "price": 1,
            }, {
                "event_time": self.received_at, "price": 2,
            }])
            original_write = store._write_rows
            calls = 0

            def fail_second_write(*args, **kwargs):
                nonlocal calls
                calls += 1
                if calls == 2:
                    raise OSError("simulated partial projection")
                return original_write(*args, **kwargs)

            with patch.object(store, "_write_rows", side_effect=fail_second_write):
                with self.assertRaisesRegex(OSError, "partial projection"):
                    store.flush()
            store.close_wal()
            with self.assertRaisesRegex(StorageFatalError, "configuration changed"):
                StreamParquetStore(temporary, wal_first=True, flush_rows=2)
            recovered = StreamParquetStore(temporary, wal_first=True, flush_rows=1)
            self.assertEqual(recovered.flush(), 2)
            recovered.close_wal()
            changed = StreamParquetStore(temporary, wal_first=True, flush_rows=2)
            changed.close_wal()
            prices = [price for part in Path(temporary).rglob("part-*.parquet")
                      for price in pd.read_parquet(part).price]
            self.assertEqual(sorted(prices), [1, 2])

    def test_wal_first_concurrent_sources_preserve_every_row(self):
        with tempfile.TemporaryDirectory() as temporary:
            store = StreamParquetStore(temporary, wal_first=True, flush_rows=7,
                                       wal_segment_bytes=2048)
            errors = []

            def produce(source):
                try:
                    for offset in range(30):
                        store.append("binance", str(source), "bbo", [{
                            "event_time": self.received_at, "price": source * 30 + offset,
                        }])
                except Exception as error:
                    errors.append(error)

            producers = [threading.Thread(target=produce, args=(source,)) for source in range(4)]
            for producer in producers:
                producer.start()
            wait_for_producers(producers, interval_seconds=0.1)
            self.assertFalse(errors)
            self.assertEqual(store.flush(), 120)
            prices = [price for part in Path(temporary).rglob("part-*.parquet")
                      for price in pd.read_parquet(part).price]
            self.assertEqual(sorted(prices), list(range(120)))
            store.close_wal()

    def test_wal_first_projects_binance_public_and_hyperliquid_market_and_reference(self):
        with tempfile.TemporaryDirectory() as temporary:
            store = StreamParquetStore(temporary, wal_first=True, flush_rows=20)
            binance = BinancePublicStreamCollector(store, ["SKHYUSDT"])
            hyperliquid = HyperliquidStreamCollector(store, ["xyz:SKHY"])
            reference = HyperliquidReferencePriceCollector(store, ["xyz:SKHY"])
            binance._on_message(None, json.dumps({"e": "depthUpdate", "E": 1_700_000_000_100,
                "s": "SKHYUSDT", "U": 100, "u": 101, "pu": 99,
                "b": [["100", "2"]], "a": []}))
            hyperliquid._on_message(None, json.dumps({"channel": "trades", "data": [{
                "coin": "xyz:SKHY", "side": "B", "px": "200", "sz": "1", "hash": "0xabc",
                "time": 1_700_000_000_001, "tid": 23,
            }]}))
            reference._on_message(None, json.dumps({"channel": "activeAssetCtx", "data": {
                "coin": "xyz:SKHY", "ctx": {"oraclePx": "100", "markPx": "101"},
            }}))
            self.assertEqual(store.flush(), 4)
            datasets = {path.parts[-5] for path in Path(temporary).rglob("part-*.parquet")}
            self.assertEqual(datasets, {"l2_updates", "trades", "oracle_prices", "mark_prices"})
            store.close_wal()

    def test_wal_first_concurrent_raw_frames_preserve_every_frame(self):
        with tempfile.TemporaryDirectory() as temporary:
            store = StreamParquetStore(temporary, wal_first=True, flush_rows=17,
                                       wal_segment_bytes=2048)
            store.register_frame_parser("test", lambda message, stamp: [
                ("binance", "SKHYUSDT", "bbo", [{
                    "event_time": stamp, "received_at": stamp, "price": int(message),
                }])
            ])
            errors = []

            def produce(source):
                try:
                    for offset in range(25):
                        store.ingest_frame("test", str(source * 25 + offset), self.received_at)
                except Exception as error:
                    errors.append(error)

            producers = [threading.Thread(target=produce, args=(source,)) for source in range(4)]
            for producer in producers:
                producer.start()
            wait_for_producers(producers, interval_seconds=0.1)
            self.assertFalse(errors)
            self.assertEqual(store.flush(), 100)
            prices = [price for part in Path(temporary).rglob("part-*.parquet")
                      for price in pd.read_parquet(part).price]
            self.assertEqual(sorted(prices), list(range(100)))
            store.close_wal()

    def test_async_spool_replay_is_idempotent_across_partial_multi_hour_write(self):
        with tempfile.TemporaryDirectory() as temporary:
            store = StreamParquetStore(temporary, flush_rows=10, async_writes=True, max_pending_rows=10)
            rows = [
                {"event_time": pd.Timestamp("2026-10-08T15:30:00Z"), "price": 100},
                {"event_time": pd.Timestamp("2026-10-08T16:30:00Z"), "price": 101},
            ]
            # Fill the RAM cap, then place both hourly partitions in one journal.
            store.append("binance", "SKHYUSDT", "bbo", [{
                "event_time": self.received_at, "price": 99 + offset,
            } for offset in range(10)])
            store.append("binance", "SKHYUSDT", "bbo", rows)
            recovered = StreamParquetStore(temporary, flush_rows=10, async_writes=True, max_pending_rows=10)
            calls = 0

            def fail_second_partition(frame, output):
                nonlocal calls
                calls += 1
                if calls == 2:
                    raise OSError("test partition failure")
                _atomic_parquet_write(frame, output)

            with patch("stream_parquet_store._atomic_parquet_write", side_effect=fail_second_partition):
                with self.assertRaisesRegex(OSError, "test partition failure"):
                    recovered.flush()
            restarted = StreamParquetStore(temporary, flush_rows=10, async_writes=True, max_pending_rows=10)
            self.assertEqual(restarted.flush(), 2)
            parts = list(Path(temporary).rglob("part-*.parquet"))
            self.assertEqual(len(parts), 2)
            self.assertEqual(sum(len(pd.read_parquet(part)) for part in parts), 2)

    def test_async_byte_cap_spools_large_message(self):
        with tempfile.TemporaryDirectory() as temporary:
            store = StreamParquetStore(temporary, flush_rows=10, async_writes=True,
                                       max_pending_rows=10, max_pending_bytes=128)
            store.append("hyperliquid", "xyz:SKHX", "l2_snapshots", [{
                "event_time": self.received_at, "received_at": self.received_at,
                "raw_values": "x" * 10_000,
            }])
            metrics = store.metrics_snapshot()
            self.assertEqual(metrics["buffered_bytes"], 0)
            self.assertEqual(metrics["spooled_files"], 1)
            self.assertEqual(store.flush(), 1)

    def test_async_concurrent_spool_writes_preserve_every_row(self):
        with tempfile.TemporaryDirectory() as temporary:
            store = StreamParquetStore(temporary, flush_rows=10, async_writes=True,
                                       max_pending_rows=10, max_spool_files=250)
            errors = []

            def produce(producer):
                try:
                    for offset in range(50):
                        store.append("binance", "SKHYUSDT", "bbo", [{
                            "event_time": self.received_at, "received_at": self.received_at,
                            "price": producer * 50 + offset,
                        }])
                except Exception as error:
                    errors.append(error)

            producers = [threading.Thread(target=produce, args=(number,)) for number in range(4)]
            for producer in producers:
                producer.start()
            wait_for_producers(producers, interval_seconds=0.1)
            self.assertFalse(errors)
            self.assertEqual(store.flush(), 200)
            prices = [price for part in Path(temporary).rglob("part-*.parquet")
                      for price in pd.read_parquet(part)["price"]]
            self.assertEqual(sorted(prices), list(range(200)))

    def test_async_spool_quota_failure_is_fatal_and_marks_gap(self):
        with tempfile.TemporaryDirectory() as temporary:
            store = StreamParquetStore(temporary, flush_rows=1, async_writes=True,
                                       max_pending_rows=1, max_spool_bytes=1)
            store.append("binance", "SKHYUSDT", "bbo", [{"event_time": self.received_at}])
            with self.assertLogs(level="CRITICAL") as captured:
                with self.assertRaises(StorageFatalError):
                    store.append("binance", "SKHYUSDT", "bbo", [{"event_time": self.received_at}])
            self.assertIn("COLLECTION GAP", captured.output[0])
            self.assertIsNotNone(store.fatal_error)
            self.assertEqual(len(list((Path(temporary) / ".stream_spool/gaps").glob("*.json"))), 1)
            with self.assertRaisesRegex(StorageFatalError, "gap"):
                StreamParquetStore(temporary, flush_rows=1, async_writes=True, max_pending_rows=1)

    def test_async_spool_io_failure_is_fatal_and_preserves_gap_marker(self):
        with tempfile.TemporaryDirectory() as temporary:
            store = StreamParquetStore(temporary, flush_rows=1, async_writes=True, max_pending_rows=1)
            store.append("binance", "SKHYUSDT", "bbo", [{"event_time": self.received_at}])
            from stream_parquet_store import _fsync_directory
            calls = 0

            def fail_first_sync(path):
                nonlocal calls
                calls += 1
                if calls == 1:
                    raise OSError("simulated disk I/O failure")
                return _fsync_directory(path)

            with patch("stream_parquet_store._fsync_directory", side_effect=fail_first_sync), \
                    self.assertLogs(level="CRITICAL"):
                with self.assertRaises(StorageFatalError):
                    store.append("binance", "SKHYUSDT", "bbo", [{"event_time": self.received_at}])
            self.assertTrue(list((Path(temporary) / ".stream_spool/gaps").glob("*.json")))

    def test_incomplete_spool_temp_file_blocks_automatic_restart(self):
        with tempfile.TemporaryDirectory() as temporary:
            spool = Path(temporary) / ".stream_spool"
            spool.mkdir()
            (spool / "unfinished.tmp").write_text("partial")
            with self.assertRaisesRegex(StorageFatalError, "temp file"):
                StreamParquetStore(temporary, async_writes=True)

    def test_persistent_async_parquet_failure_becomes_fatal(self):
        with tempfile.TemporaryDirectory() as temporary:
            store = StreamParquetStore(temporary, flush_rows=1, async_writes=True, max_pending_rows=2)
            loop = StreamFlushLoop(store, interval_seconds=0.01)
            store.append("binance", "SKHYUSDT", "bbo", [{"event_time": self.received_at}])
            worker = threading.Thread(target=loop.run)
            with patch("stream_parquet_store._atomic_parquet_write", side_effect=OSError("disk fault")), \
                    self.assertLogs(level="ERROR"):
                worker.start()
                worker.join(2)
            self.assertFalse(worker.is_alive())
            self.assertIsNotNone(store.fatal_error)
            self.assertEqual(store.metrics_snapshot()["buffered_rows"], 1)
            self.assertTrue(list((Path(temporary) / ".stream_spool/gaps").glob("*.json")))

    def test_shutdown_waits_for_last_producer_before_final_drain(self):
        with tempfile.TemporaryDirectory() as temporary:
            store = StreamParquetStore(temporary, flush_rows=10, async_writes=True, max_pending_rows=10)
            release = threading.Event()
            producer = threading.Thread(target=lambda: (
                release.wait(), store.append("binance", "SKHYUSDT", "bbo", [{
                    "event_time": self.received_at, "price": 1,
                }])
            ))
            producer.start()
            release.set()
            wait_for_producers([producer], interval_seconds=0.01)
            store.close_for_appends()
            self.assertEqual(store.flush(), 1)
            with self.assertRaises(StorageFatalError):
                store.append("binance", "SKHYUSDT", "bbo", [{"event_time": self.received_at}])

    def test_async_global_cap_wakes_writer_for_subthreshold_datasets(self):
        with tempfile.TemporaryDirectory() as temporary:
            store = StreamParquetStore(temporary, flush_rows=3, async_writes=True, max_pending_rows=3)
            loop = StreamFlushLoop(store, interval_seconds=5)
            worker = threading.Thread(target=loop.run)
            worker.start()
            for symbol in ("A", "B", "C"):
                store.append("binance", symbol, "bbo", [{
                    "event_time": self.received_at, "received_at": self.received_at,
                    "best_bid_price": 100.0,
                }])
            fourth_done = threading.Event()

            def append_fourth():
                store.append("binance", "D", "bbo", [{
                    "event_time": self.received_at, "received_at": self.received_at,
                    "best_bid_price": 101.0,
                }])
                fourth_done.set()

            fourth = threading.Thread(target=append_fourth)
            try:
                fourth.start()
                self.assertTrue(fourth_done.wait(1))
            finally:
                loop.stop()
                fourth.join(2)
                worker.join(2)
            self.assertFalse(worker.is_alive())
            self.assertEqual(sum(len(pd.read_parquet(part)) for part in Path(temporary).rglob("part-*.parquet")), 4)

    def test_async_writer_retries_partial_multi_hour_failure_without_duplicate_parts(self):
        with tempfile.TemporaryDirectory() as temporary:
            store = StreamParquetStore(temporary, flush_rows=10, async_writes=True, max_pending_rows=20)
            store.append("binance", "SKHYUSDT", "bbo", [
                {"event_time": pd.Timestamp("2026-10-08T15:30:00Z"), "received_at": self.received_at,
                 "best_bid_price": 100.0},
                {"event_time": pd.Timestamp("2026-10-08T16:30:00Z"), "received_at": self.received_at,
                 "best_bid_price": 101.0},
            ])
            calls = 0

            def fail_second_partition(frame, output):
                nonlocal calls
                calls += 1
                if calls == 2:
                    raise OSError("temporary write failure")
                _atomic_parquet_write(frame, output)

            with patch("stream_parquet_store._atomic_parquet_write", side_effect=fail_second_partition):
                with self.assertRaisesRegex(OSError, "temporary write failure"):
                    store.flush()
            self.assertEqual(store.metrics_snapshot()["buffered_rows"], 2)
            self.assertEqual(len(list(Path(temporary).rglob("part-*.parquet"))), 1)
            self.assertEqual(store.flush(), 2)
            parts = list(Path(temporary).rglob("part-*.parquet"))
            self.assertEqual(len(parts), 2)
            self.assertEqual(sum(len(pd.read_parquet(part)) for part in parts), 2)
            metrics = store.metrics_snapshot()
            self.assertEqual((metrics["buffered_rows"], metrics["persisted_rows"], metrics["write_failures"]), (0, 2, 1))

    def test_async_periodic_flush_splits_large_buffers_into_bounded_parts(self):
        with tempfile.TemporaryDirectory() as temporary:
            store = StreamParquetStore(temporary, flush_rows=2, async_writes=True, max_pending_rows=10)
            rows = [{
                "event_time": self.received_at, "received_at": self.received_at,
                "best_bid_price": float(price),
            } for price in range(5)]
            store.append("binance", "SKHYUSDT", "bbo", rows)
            self.assertEqual(store.flush(), 5)
            parts = list(Path(temporary).rglob("part-*.parquet"))
            self.assertEqual(sorted(len(pd.read_parquet(part)) for part in parts), [1, 2, 2])

    def test_async_threshold_wakes_do_not_postpone_periodic_flush(self):
        clock = [0.0]
        calls = []

        class FakeStore:
            async_writes = True

            def wait_for_pending(self, timeout):
                if len(calls) < 2:
                    clock[0] += 1
                    return True
                clock[0] = 5
                return False

            def flush_pending(self):
                calls.append("threshold")

            def flush_current(self):
                calls.append("periodic")
                loop.stop_event.set()

        loop = StreamFlushLoop(FakeStore(), interval_seconds=5)
        with patch("stream_parquet_store.time.monotonic", side_effect=lambda: clock[0]):
            loop.run()
        self.assertEqual(calls, ["threshold", "threshold", "periodic"])

    def test_async_graceful_stop_drains_every_accepted_row(self):
        with tempfile.TemporaryDirectory() as temporary:
            store = StreamParquetStore(temporary, flush_rows=7, async_writes=True, max_pending_rows=70)
            loop = StreamFlushLoop(store, interval_seconds=5)
            worker = threading.Thread(target=loop.run)
            worker.start()
            try:
                for price in range(50):
                    store.append("binance", "SKHYUSDT", "bbo" if price % 2 else "trades", [{
                        "event_time": self.received_at,
                        "received_at": self.received_at,
                        "price": float(price),
                    }])
            finally:
                loop.stop()
                worker.join(2)
            self.assertFalse(worker.is_alive())
            parts = list(Path(temporary).rglob("part-*.parquet"))
            self.assertEqual(sum(len(pd.read_parquet(part)) for part in parts), 50)
            self.assertEqual(store.metrics_snapshot()["buffered_rows"], 0)

    def test_async_multi_producer_stress_preserves_rows_under_cap(self):
        with tempfile.TemporaryDirectory() as temporary:
            store = StreamParquetStore(temporary, flush_rows=100, async_writes=True, max_pending_rows=300)
            loop = StreamFlushLoop(store, interval_seconds=0.1)
            worker = threading.Thread(target=loop.run)
            worker.start()
            errors = []

            def produce(producer):
                try:
                    for offset in range(250):
                        store.append("binance", "SKHYUSDT", "bbo", [{
                            "event_time": self.received_at,
                            "received_at": self.received_at,
                            "price": producer * 250 + offset,
                        }])
                except Exception as error:
                    errors.append(error)

            producers = [threading.Thread(target=produce, args=(producer,)) for producer in range(4)]
            try:
                for producer in producers:
                    producer.start()
                for producer in producers:
                    producer.join(10)
            finally:
                loop.stop()
                worker.join(10)
            self.assertFalse(errors)
            self.assertTrue(all(not producer.is_alive() for producer in producers))
            self.assertFalse(worker.is_alive())
            parts = list(Path(temporary).rglob("part-*.parquet"))
            self.assertEqual(sum(len(pd.read_parquet(part)) for part in parts), 1000)
            self.assertLessEqual(store.metrics_snapshot()["max_buffered_rows"], 300)

    def test_flush_loop_reports_storage_pressure_periodically(self):
        with tempfile.TemporaryDirectory() as temporary:
            loop = StreamFlushLoop(StreamParquetStore(temporary), 5, metrics_interval_seconds=60)
            with patch.object(loop.stop_event, "wait", side_effect=[False, True]), \
                    patch("stream_parquet_store.time.monotonic", side_effect=[0.0, 0.0, 61.0, 62.0]), \
                    self.assertLogs(level="INFO") as captured:
                loop.run()
            self.assertIn("appended=0 persisted=0 buffered=0", captured.output[0])
            self.assertIn("kst=", captured.output[0])

    def test_hourly_compaction_preserves_post_finalization_parts_for_reconciliation(self):
        with tempfile.TemporaryDirectory() as temporary:
            store = StreamParquetStore(temporary, flush_rows=10)
            for second, price in ((0, 100.0), (1, 101.0)):
                store.append("binance", "SKHYUSDT", "bbo", [{
                    "event_time": pd.Timestamp(f"2026-10-08T15:30:0{second}Z"),
                    "received_at": self.received_at,
                    "best_bid_price": price,
                }])
                self.assertEqual(store.flush(), 1)

            hour_directory = (
                Path(temporary) / "binance" / "SKHYUSDT" / "bbo"
                / "date=2026-10-09" / "hour=00"
            )
            parts_directory = hour_directory / "parts"
            self.assertEqual(len(list(parts_directory.glob("part-*.parquet"))), 2)

            compactor = StreamParquetCompactor(temporary)
            self.assertEqual(compactor.compact_all(), 2)
            output = hour_directory / "data.parquet"
            self.assertTrue(output.is_file())
            self.assertFalse(list(parts_directory.glob("part-*.parquet")))
            saved = pd.read_parquet(output)
            self.assertEqual(saved.best_bid_price.tolist(), [100.0, 101.0])

            store.append("binance", "SKHYUSDT", "bbo", [{
                "event_time": pd.Timestamp("2026-10-08T15:30:02Z"),
                "received_at": self.received_at,
                "best_bid_price": 102.0,
            }])
            store.flush()
            self.assertEqual(compactor.compact_all(), 1)
            saved = pd.read_parquet(output)
            self.assertEqual(saved.best_bid_price.tolist(), [100.0, 101.0])
            late_parts = list((hour_directory / "late_parts").glob("part-*.parquet"))
            self.assertEqual(len(late_parts), 1)
            self.assertEqual(pd.read_parquet(late_parts[0]).best_bid_price.tolist(), [102.0])

            reconciled_root = Path(temporary) / "reconciled"
            hours, rows = reconcile_tree(Path(temporary), reconciled_root)
            self.assertEqual((hours, rows), (1, 3))
            reconciled = pd.read_parquet(reconciled_root / hour_directory.relative_to(temporary) / "data.parquet")
            self.assertEqual(reconciled.best_bid_price.tolist(), [100.0, 101.0, 102.0])

    def test_writer_sends_after_watermark_record_to_late_parts(self):
        with tempfile.TemporaryDirectory() as temporary:
            store = StreamParquetStore(temporary, finalization_grace_seconds=300)
            store.append("binance", "SKHYUSDT", "bbo", [{
                "event_time": pd.Timestamp("2026-10-08T15:59:59Z"),
                "received_at": pd.Timestamp("2026-10-08T16:06:00Z"),
                "best_bid_price": 100.0,
            }])
            store.flush()
            late_directory = (
                Path(temporary) / "binance" / "SKHYUSDT" / "bbo"
                / "date=2026-10-09" / "hour=00" / "late_parts"
            )
            self.assertEqual(len(list(late_directory.glob("part-*.parquet"))), 1)
            ordinary_directory = late_directory.parent / "parts"
            self.assertFalse(ordinary_directory.exists())

    def test_compactor_waits_for_the_finalization_grace_period(self):
        with tempfile.TemporaryDirectory() as temporary:
            store = StreamParquetStore(temporary)
            store.append("binance", "SKHYUSDT", "bbo", [{
                "event_time": pd.Timestamp("2026-10-08T15:30:00Z"),
                "received_at": self.received_at,
                "best_bid_price": 100.0,
            }])
            store.flush()
            compactor = StreamParquetCompactor(temporary, finalization_grace_seconds=300)
            self.assertEqual(compactor.compact_closed_hours(pd.Timestamp("2026-10-09T01:04:59+09:00")), 0)
            self.assertEqual(compactor.compact_closed_hours(pd.Timestamp("2026-10-09T01:05:00+09:00")), 1)

    def test_raw_processor_creates_verified_ten_minute_outputs_without_hourly_compaction(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            raw_root = root / "raw"
            store = StreamParquetStore(raw_root, flush_rows=10)
            store.append("binance", "SKHYUSDT", "bbo", [{
                "event_time": pd.Timestamp("2026-10-09T00:09:59+09:00"),
                "received_at": pd.Timestamp("2026-10-09T00:09:59+09:00"),
                "best_bid_price": 100.0,
            }])
            store.flush()
            # A later deployment can add nullable fields; the Arrow processor
            # must union such part schemas without a pandas full-hour merge.
            store.append("binance", "SKHYUSDT", "bbo", [{
                "event_time": pd.Timestamp("2026-10-09T00:10:00+09:00"),
                "received_at": pd.Timestamp("2026-10-09T00:10:00+09:00"),
                "best_bid_price": 101.0,
                "optional_field": "new",
            }])
            store.flush()

            result = RawParquetProcessor(raw_root, root / "processed", processing_delay_seconds=1).process_available(
                pd.Timestamp("2026-10-09T01:00:02+09:00").to_pydatetime()
            )
            self.assertEqual((result.normal_partitions, result.rows), (2, 2))
            output_root = root / "processed" / "binance" / "SKHYUSDT" / "bbo" / "date=2026-10-09" / "hour=00"
            self.assertEqual(pd.read_parquet(output_root / "minute=00" / "data.parquet").best_bid_price.tolist(), [100.0])
            minute_ten = pd.read_parquet(output_root / "minute=10" / "data.parquet")
            self.assertEqual(minute_ten.best_bid_price.tolist(), [101.0])
            self.assertEqual(minute_ten.optional_field.tolist(), ["new"])
            manifests = list((root / "processed" / "_manifests").rglob("normal.json"))
            self.assertEqual(len(manifests), 1)

    def test_processor_preserves_late_raw_part_and_deletes_only_verified_retention_hours(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            raw_root = root / "raw"
            store = StreamParquetStore(raw_root, flush_rows=10, finalization_grace_seconds=300)
            store.append("binance", "SKHYUSDT", "bbo", [{
                "event_time": pd.Timestamp("2026-10-09T00:00:01+09:00"),
                "received_at": pd.Timestamp("2026-10-09T01:06:00+09:00"),
                "best_bid_price": 100.0,
            }])
            store.flush()
            processor = RawParquetProcessor(
                raw_root,
                root / "processed",
                raw_retention_days=1,
                delete_processed_raw=True,
            )
            result = processor.process_available(pd.Timestamp("2026-10-11T02:00:00+09:00").to_pydatetime())
            self.assertEqual((result.late_parts, result.rows, result.deleted_raw_hours), (1, 1, 1))
            output = root / "processed" / "binance" / "SKHYUSDT" / "bbo" / "date=2026-10-09" / "hour=00" / "minute=00" / "late_parts"
            self.assertEqual(len(list(output.glob("part-*.parquet"))), 1)
            self.assertFalse(list(raw_root.glob("binance/SKHYUSDT/bbo/date=*/hour=*")))

    def test_binance_l2_anchor_renames_snapshot_timestamps_without_duplicate_columns(self):
        original = pd.DataFrame({
            "time": [self.received_at],
            "event_time": [self.received_at],
            "transaction_time": [self.received_at],
        })
        normalized = original.rename(columns={"time": "event_time", "event_time": "exchange_event_time"})
        self.assertTrue(normalized.columns.is_unique)
        self.assertEqual(set(normalized.columns), {"event_time", "exchange_event_time", "transaction_time"})

    def test_binance_main_process_uses_market_endpoint_for_aggregate_trades_only(self):
        class FakeWebSocket:
            def __init__(self):
                self.sent = []

            def send(self, message):
                self.sent.append(message)

        with tempfile.TemporaryDirectory() as temporary:
            collector = BinanceStreamCollector(StreamParquetStore(temporary), ["SKHYNIXUSDT", "SKHYUSDT"])
            websocket = FakeWebSocket()
            collector._on_open(websocket)
            payload = __import__("json").loads(websocket.sent[0])
            self.assertEqual(payload["params"], [
                "skhynixusdt@aggTrade", "skhyusdt@aggTrade",
            ])
            self.assertTrue(collector._stream_ready.is_set())

    def test_reconnect_backoff_doubles_to_cap_and_resets_after_stable_connection(self):
        for websocket_class in (MarketReconnectableWebSocket, ReferenceReconnectableWebSocket):
            collector = websocket_class(
                "wss://example.test/ws",
                "test",
                reconnect_initial_seconds=0.5,
                reconnect_max_seconds=30,
                reconnect_reset_seconds=60,
                reconnect_jitter_fraction=0,
            )
            self.assertEqual(
                [collector._reconnect_delay_after_disconnect(0) for _ in range(7)],
                [0.5, 1.0, 2.0, 4.0, 8.0, 16.0, 30.0],
            )
            self.assertEqual(collector._reconnect_delay_after_disconnect(0), 30.0)
            self.assertEqual(collector._reconnect_delay_after_disconnect(60), 0.5)
            self.assertEqual(collector._jittered_delay(8), 8)

    def test_each_binance_l2_websocket_subscription_requests_one_immediate_anchor(self):
        class FakeWebSocket:
            def __init__(self):
                self.sent = []

            def send(self, message):
                self.sent.append(message)

        with tempfile.TemporaryDirectory() as temporary:
            anchor_collector = BinanceStreamCollector(StreamParquetStore(temporary), ["SKHYUSDT"])
            public_collector = BinancePublicStreamCollector(
                StreamParquetStore(temporary),
                ["SKHYUSDT"],
                snapshot_requester=anchor_collector.request_immediate_snapshot,
            )
            public_collector._on_open(FakeWebSocket())
            self.assertEqual(anchor_collector._pending_snapshot_requests, 1)
            public_collector._on_open(FakeWebSocket())  # Reconnection and resubscription.
            self.assertEqual(anchor_collector._pending_snapshot_requests, 2)
            self.assertTrue(anchor_collector._consume_snapshot_request())
            self.assertTrue(anchor_collector._consume_snapshot_request())
            self.assertFalse(anchor_collector._consume_snapshot_request())

    def test_binance_reference_process_uses_one_second_reference_prices_only(self):
        class FakeWebSocket:
            def __init__(self):
                self.sent = []

            def send(self, message):
                self.sent.append(__import__("json").loads(message))

        with tempfile.TemporaryDirectory() as temporary:
            collector = BinanceReferencePriceCollector(
                StreamParquetStore(temporary), ["SKHYNIXUSDT", "SKHYUSDT"]
            )
            websocket = FakeWebSocket()
            collector._on_open(websocket)
            self.assertEqual(websocket.sent[0]["params"], [
                "skhynixusdt@markPrice@1s", "skhyusdt@markPrice@1s",
            ])

    def test_binance_uses_public_endpoint_for_l2_and_bbo(self):
        class FakeWebSocket:
            def __init__(self):
                self.sent = []

            def send(self, message):
                self.sent.append(message)

        with tempfile.TemporaryDirectory() as temporary:
            collector = BinancePublicStreamCollector(
                StreamParquetStore(temporary), ["SKHYNIXUSDT", "SKHYUSDT"]
            )
            websocket = FakeWebSocket()
            collector._on_open(websocket)
            payload = __import__("json").loads(websocket.sent[0])
            self.assertEqual(payload["params"], [
                "skhynixusdt@depth@100ms", "skhynixusdt@bookTicker",
                "skhyusdt@depth@100ms", "skhyusdt@bookTicker",
            ])

    def test_hyperliquid_main_process_does_not_subscribe_to_reference_prices(self):
        class FakeWebSocket:
            def __init__(self):
                self.sent = []

            def send(self, message):
                self.sent.append(__import__("json").loads(message))

        with tempfile.TemporaryDirectory() as temporary:
            from streaming_collector import HyperliquidStreamCollector

            collector = HyperliquidStreamCollector(StreamParquetStore(temporary), ["xyz:SKHX", "xyz:SKHY"])
            websocket = FakeWebSocket()
            collector._on_open(websocket)
            subscriptions = [message["subscription"] for message in websocket.sent]
            self.assertEqual(subscriptions, [
                {"type": "trades", "coin": "xyz:SKHX"},
                {"type": "l2Book", "coin": "xyz:SKHX"},
                {"type": "bbo", "coin": "xyz:SKHX"},
                {"type": "trades", "coin": "xyz:SKHY"},
                {"type": "l2Book", "coin": "xyz:SKHY"},
                {"type": "bbo", "coin": "xyz:SKHY"},
            ])

    def test_hyperliquid_reference_process_uses_active_asset_context_only(self):
        class FakeWebSocket:
            def __init__(self):
                self.sent = []

            def send(self, message):
                self.sent.append(__import__("json").loads(message))

        with tempfile.TemporaryDirectory() as temporary:
            collector = HyperliquidReferencePriceCollector(
                StreamParquetStore(temporary), ["xyz:SKHX", "xyz:SKHY"]
            )
            websocket = FakeWebSocket()
            collector._on_open(websocket)
            subscriptions = [message["subscription"] for message in websocket.sent]
            self.assertEqual(subscriptions, [
                {"type": "activeAssetCtx", "coin": "xyz:SKHX"},
                {"type": "activeAssetCtx", "coin": "xyz:SKHY"},
            ])

    def test_reference_collector_uses_a_distinct_sibling_root(self):
        with tempfile.TemporaryDirectory() as temporary:
            main_root = Path(temporary) / "main"
            with patch.dict("os.environ", {"TRADING_DATA_DIR": str(main_root)}, clear=False):
                self.assertEqual(reference_data_root(), Path(f"{main_root}_reference_prices").resolve())
                with patch.dict("os.environ", {"TRADING_REFERENCE_DATA_DIR": str(main_root)}, clear=False):
                    with self.assertRaises(ValueError):
                        reference_data_root()


if __name__ == "__main__":
    unittest.main()
