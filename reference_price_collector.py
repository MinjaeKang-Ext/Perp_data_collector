"""Independent real-time collector for Binance and Hyperliquid reference prices.

This process deliberately does not subscribe to trades, L2, BBO, funding, or
Kiwoom. It owns its WebSocket and parsing code, so it does not require a
modified ``streaming_collector.py``. Its default data root is a sibling of the
full-market collector root, so it can run beside an existing ``main.py``
process without sharing WebSocket connections or writing the same dataset
directories.

Key environment variables:
  TRADING_REFERENCE_DATA_DIR=$HOME/trading_reference_data
  TRADING_STREAM_FLUSH_ROWS=10000
  TRADING_STREAM_FLUSH_SECONDS=5
  TRADING_ASYNC_PARQUET_WRITES=0  # enable only after local Stage 4 stress validation
  TRADING_WAL_FIRST=0  # experimental raw-message WAL; mutually exclusive with async overflow
  TRADING_WAL_MAX_BYTES=4294967296
  TRADING_WAL_SEGMENT_BYTES=8388608
  TRADING_ASYNC_MAX_PENDING_ROWS=50000
  TRADING_ASYNC_MAX_PENDING_BYTES=67108864
  TRADING_ASYNC_MAX_SPOOL_BYTES=4294967296
  TRADING_ASYNC_MAX_SPOOL_FILES=20000
  TRADING_HOURLY_FINALIZATION_GRACE_SECONDS=300
  TRADING_WS_CA_BUNDLE=/path/to/organization-ca.pem
  TRADING_WS_RECONNECT_INITIAL_SECONDS=0.5
  TRADING_WS_RECONNECT_MAX_SECONDS=30
  TRADING_WS_RECONNECT_RESET_SECONDS=60
  TRADING_WS_RECONNECT_JITTER_FRACTION=0.2
"""

from __future__ import annotations

import json
import logging
import os
import signal
import random
import ssl
import threading
import time
from pathlib import Path
from typing import Any, Iterable

import pandas as pd

from parquet_store import default_data_root
from stream_parquet_store import StreamFlushLoop, StreamParquetStore, wait_for_producers

# Keep the market universe local so this file can be added to an already
# running collector without modifying its main.py or streaming_collector.py.
BINANCE_SYMBOLS = ("SKHYNIXUSDT", "SKHYUSDT")
HYPERLIQUID_COINS = ("xyz:SKHX", "xyz:SKHY")
BINANCE_MARKET_WS_URL = "wss://fstream.binance.com/market/stream"
HYPERLIQUID_WS_URL = "wss://api.hyperliquid.xyz/ws"


def _utc_now() -> pd.Timestamp:
    return pd.Timestamp.now(tz="UTC")


def _timestamp(milliseconds: int | str | float) -> pd.Timestamp:
    return pd.to_datetime(int(milliseconds), unit="ms", utc=True)


class _ReconnectableWebSocket:
    """Small reconnecting WebSocket wrapper used only by this runner."""

    def __init__(
        self,
        url: str,
        name: str,
        reconnect_initial_seconds: float | None = None,
        reconnect_max_seconds: float | None = None,
        reconnect_reset_seconds: float | None = None,
        reconnect_jitter_fraction: float | None = None,
    ) -> None:
        self.url = url
        self.name = name
        self.reconnect_initial_seconds = (
            reconnect_initial_seconds
            if reconnect_initial_seconds is not None
            else _reconnect_positive_float("TRADING_WS_RECONNECT_INITIAL_SECONDS", 0.5)
        )
        self.reconnect_max_seconds = (
            reconnect_max_seconds
            if reconnect_max_seconds is not None
            else _reconnect_positive_float("TRADING_WS_RECONNECT_MAX_SECONDS", 30.0)
        )
        self.reconnect_reset_seconds = (
            reconnect_reset_seconds
            if reconnect_reset_seconds is not None
            else _reconnect_positive_float("TRADING_WS_RECONNECT_RESET_SECONDS", 60.0)
        )
        self.reconnect_jitter_fraction = (
            reconnect_jitter_fraction
            if reconnect_jitter_fraction is not None
            else _reconnect_jitter_fraction()
        )
        if self.reconnect_max_seconds < self.reconnect_initial_seconds:
            raise ValueError("TRADING_WS_RECONNECT_MAX_SECONDS must be at least the initial delay")
        if self.reconnect_initial_seconds <= 0 or self.reconnect_reset_seconds <= 0:
            raise ValueError("WebSocket reconnect delays must be positive")
        if not 0 <= self.reconnect_jitter_fraction < 1:
            raise ValueError("WebSocket reconnect jitter must be in [0, 1)")
        self.stop_event = threading.Event()
        self._app: Any | None = None
        self._connected_at: float | None = None
        self._next_reconnect_seconds = self.reconnect_initial_seconds

    def _reconnect_delay_after_disconnect(self, connection_duration_seconds: float) -> float:
        if connection_duration_seconds >= self.reconnect_reset_seconds:
            self._next_reconnect_seconds = self.reconnect_initial_seconds
        delay = self._next_reconnect_seconds
        self._next_reconnect_seconds = min(self.reconnect_max_seconds, delay * 2)
        return delay

    def _jittered_delay(self, delay: float) -> float:
        spread = delay * self.reconnect_jitter_fraction
        return random.uniform(delay - spread, delay + spread)

    def _handle_open(self, ws: Any) -> None:
        self._connected_at = time.monotonic()
        self._on_open(ws)

    def run(self) -> None:
        import websocket

        ssl_options: dict[str, Any] = {"cert_reqs": ssl.CERT_REQUIRED}
        ca_bundle = os.getenv("TRADING_WS_CA_BUNDLE")
        if ca_bundle:
            ssl_options["ca_certs"] = ca_bundle
        while not self.stop_event.is_set():
            self._connected_at = None
            self._app = websocket.WebSocketApp(
                self.url,
                on_open=self._handle_open,
                on_message=self._on_message,
                on_error=self._on_error,
                on_close=self._on_close,
            )
            self._app.run_forever(ping_interval=20, ping_timeout=10, sslopt=ssl_options)
            if self.stop_event.is_set():
                break
            opened_at = self._connected_at
            connection_duration = max(0.0, time.monotonic() - opened_at) if opened_at is not None else 0.0
            delay = self._jittered_delay(self._reconnect_delay_after_disconnect(connection_duration))
            logging.warning("%s websocket disconnected; reconnecting in %.2f seconds", self.name, delay)
            self.stop_event.wait(delay)

    def stop(self) -> None:
        self.stop_event.set()
        if self._app is not None:
            self._app.close()

    def _on_open(self, ws: Any) -> None:
        raise NotImplementedError

    def _on_message(self, _ws: Any, message: str) -> None:
        raise NotImplementedError

    def _on_error(self, _ws: Any, error: Exception) -> None:
        if not self.stop_event.is_set():
            logging.warning("%s websocket error: %s", self.name, error)

    def _on_close(self, _ws: Any, status_code: int, message: str) -> None:
        if not self.stop_event.is_set():
            logging.warning("%s websocket closed (%s): %s", self.name, status_code, message)


def _reconnect_positive_float(name: str, default: float) -> float:
    value = float(os.getenv(name, str(default)))
    if value <= 0:
        raise ValueError(f"{name} must be positive")
    return value


def _reconnect_jitter_fraction() -> float:
    value = float(os.getenv("TRADING_WS_RECONNECT_JITTER_FRACTION", "0.2"))
    if not 0 <= value < 1:
        raise ValueError("TRADING_WS_RECONNECT_JITTER_FRACTION must be in [0, 1)")
    return value


def _project_binance_reference(message: str, received_at: pd.Timestamp) -> list[tuple[str, str, str, list[dict[str, Any]]]]:
    envelope = json.loads(message)
    payload = envelope.get("data", envelope)
    if payload.get("e") != "markPriceUpdate":
        return []
    event_time = _timestamp(payload["E"])
    common = {"event_time": event_time, "received_at": received_at, "symbol": payload["s"]}
    return [
        ("binance", payload["s"], "index_prices", [{**common, "index_price": float(payload["i"])}]),
        ("binance", payload["s"], "mark_prices", [{
            **common, "mark_price": float(payload["p"]),
            "estimated_settle_price": float(payload["P"]) if payload.get("P") else None,
            "funding_rate": float(payload["r"]) if payload.get("r") is not None else None,
            "next_funding_time": _timestamp(payload["T"]) if payload.get("T") else None,
        }]),
    ]


def _project_hyperliquid_reference(message: str, received_at: pd.Timestamp) -> list[tuple[str, str, str, list[dict[str, Any]]]]:
    envelope = json.loads(message)
    if envelope.get("channel") != "activeAssetCtx":
        return []
    payload = envelope["data"]
    context = payload["ctx"]
    common = {
        "event_time": received_at, "received_at": received_at,
        "coin": payload["coin"], "event_time_source": "received_at",
    }
    return [
        ("hyperliquid", payload["coin"], "oracle_prices", [{
            **common, "oracle_price": float(context["oraclePx"]),
        }]),
        ("hyperliquid", payload["coin"], "mark_prices", [{
            **common, "mark_price": float(context["markPx"]),
            "mid_price": float(context["midPx"]) if context.get("midPx") is not None else None,
            "funding_rate": float(context["funding"]) if context.get("funding") is not None else None,
            "open_interest": float(context["openInterest"]) if context.get("openInterest") is not None else None,
        }]),
    ]


class BinanceReferencePriceCollector(_ReconnectableWebSocket):
    """Persist Binance 1-second index and mark-price updates only."""

    def __init__(self, store: StreamParquetStore, symbols: Iterable[str]) -> None:
        super().__init__(BINANCE_MARKET_WS_URL, "binance-reference-prices")
        self.store = store
        if store.wal_first:
            store.register_frame_parser("binance-reference", _project_binance_reference)
        self.symbols = tuple(symbols)

    def _on_open(self, ws: Any) -> None:
        streams = [f"{symbol.lower()}@markPrice@1s" for symbol in self.symbols]
        ws.send(json.dumps({"method": "SUBSCRIBE", "params": streams, "id": 1}))
        logging.info("Binance reference-price websocket subscribed to %d streams", len(streams))

    def _on_message(self, _ws: Any, message: str) -> None:
        if self.store.wal_first:
            self.store.ingest_frame("binance-reference", message)
            return
        try:
            for venue, symbol, dataset, rows in _project_binance_reference(message, _utc_now()):
                self.store.append(venue, symbol, dataset, rows)
        except Exception:
            logging.exception("Unable to process Binance reference-price websocket message")


class HyperliquidReferencePriceCollector(_ReconnectableWebSocket):
    """Persist Hyperliquid oracle and mark-price context updates only."""

    def __init__(self, store: StreamParquetStore, coins: Iterable[str]) -> None:
        super().__init__(HYPERLIQUID_WS_URL, "hyperliquid-reference-prices")
        self.store = store
        if store.wal_first:
            store.register_frame_parser("hyperliquid-reference", _project_hyperliquid_reference)
        self.coins = tuple(coins)

    def _on_open(self, ws: Any) -> None:
        for coin in self.coins:
            ws.send(json.dumps({"method": "subscribe", "subscription": {"type": "activeAssetCtx", "coin": coin}}))
        logging.info("Hyperliquid reference-price websocket subscribed to %d streams", len(self.coins))

    def _on_message(self, _ws: Any, message: str) -> None:
        if self.store.wal_first:
            self.store.ingest_frame("hyperliquid-reference", message)
            return
        try:
            for venue, symbol, dataset, rows in _project_hyperliquid_reference(message, _utc_now()):
                self.store.append(venue, symbol, dataset, rows)
        except Exception:
            logging.exception("Unable to process Hyperliquid reference-price websocket message")


def _positive_float(name: str, default: float) -> float:
    value = float(os.getenv(name, str(default)))
    if value <= 0:
        raise ValueError(f"{name} must be positive")
    return value


def reference_data_root() -> Path:
    """Return an isolated root unless the operator explicitly selects one."""
    full_market_root = default_data_root().expanduser().resolve()
    configured_root = os.getenv("TRADING_REFERENCE_DATA_DIR")
    root = (
        Path(configured_root).expanduser().resolve()
        if configured_root
        else full_market_root.with_name(f"{full_market_root.name}_reference_prices")
    )
    if root == full_market_root:
        raise ValueError(
            "TRADING_REFERENCE_DATA_DIR must differ from TRADING_DATA_DIR so "
            "the independent collectors never write into one data tree"
        )
    return root


def main() -> None:
    logging.basicConfig(
        level=os.getenv("TRADING_LOG_LEVEL", "INFO").upper(),
        format="%(asctime)s %(levelname)s %(message)s",
    )
    data_root = reference_data_root()
    raw_root = data_root / "raw"
    finalization_grace_seconds = _positive_float("TRADING_HOURLY_FINALIZATION_GRACE_SECONDS", 300)
    async_setting = os.getenv("TRADING_ASYNC_PARQUET_WRITES", "0").strip()
    if async_setting not in {"0", "1"}:
        raise ValueError("TRADING_ASYNC_PARQUET_WRITES must be 0 or 1")
    async_writes = async_setting == "1"
    wal_setting = os.getenv("TRADING_WAL_FIRST", "0").strip()
    if wal_setting not in {"0", "1"}:
        raise ValueError("TRADING_WAL_FIRST must be 0 or 1")
    wal_first = wal_setting == "1"
    stream_store = StreamParquetStore(
        raw_root,
        flush_rows=int(_positive_float("TRADING_STREAM_FLUSH_ROWS", 10_000)),
        finalization_grace_seconds=finalization_grace_seconds,
        async_writes=async_writes,
        max_pending_rows=int(os.getenv("TRADING_ASYNC_MAX_PENDING_ROWS", "50000")) if async_writes else 50_000,
        max_pending_bytes=int(os.getenv("TRADING_ASYNC_MAX_PENDING_BYTES", "67108864")),
        max_spool_bytes=int(os.getenv("TRADING_ASYNC_MAX_SPOOL_BYTES", "4294967296")),
        max_spool_files=int(os.getenv("TRADING_ASYNC_MAX_SPOOL_FILES", "20000")),
        wal_first=wal_first,
        max_wal_bytes=int(os.getenv("TRADING_WAL_MAX_BYTES", "4294967296")),
        wal_segment_bytes=int(os.getenv("TRADING_WAL_SEGMENT_BYTES", "8388608")),
    )
    if wal_first:
        logging.warning("Experimental WAL-first reference collector enabled; every frame is fsynced before return")
    if async_writes:
        logging.warning(
            "Experimental async Parquet writer enabled (RAM rows=%d bytes=%d, disk spool bytes=%d files=%d)",
            stream_store.max_pending_rows, stream_store.max_pending_bytes,
            stream_store.max_spool_bytes, stream_store.max_spool_files,
        )
    flush_loop = StreamFlushLoop(stream_store, _positive_float("TRADING_STREAM_FLUSH_SECONDS", 5))
    binance = BinanceReferencePriceCollector(stream_store, BINANCE_SYMBOLS)
    hyperliquid = HyperliquidReferencePriceCollector(stream_store, HYPERLIQUID_COINS)
    workers = [
        threading.Thread(target=flush_loop.run, name="reference-parquet-flush", daemon=True),
        threading.Thread(target=binance.run, name="binance-reference-prices", daemon=True),
        threading.Thread(target=hyperliquid.run, name="hyperliquid-reference-prices", daemon=True),
    ]
    logging.info("Reference-price raw Parquet data root: %s", raw_root)
    logging.info("Reference-price processed data root: %s", data_root / "processed")
    logging.info("Markets: Binance=%s Hyperliquid=%s", BINANCE_SYMBOLS, HYPERLIQUID_COINS)
    signal.signal(signal.SIGTERM, signal.default_int_handler)
    for worker in workers:
        worker.start()
    try:
        while True:
            workers[0].join(timeout=1)
            if stream_store.fatal_error is not None:
                raise stream_store.fatal_error
    except KeyboardInterrupt:
        logging.info("Stopping reference-price collector")
    finally:
        binance.stop()
        hyperliquid.stop()
        wait_for_producers(workers[1:])
        stream_store.close_for_appends()
        try:
            flush_loop.stop()
        finally:
            wait_for_producers(workers[:1])
            stream_store.close_wal()


if __name__ == "__main__":
    main()
