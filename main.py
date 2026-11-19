"""High-frequency SK hynix perpetual and spot-data collector.

Live streams (KST-partitioned Parquet):
* Binance: 100 ms aggregate trades, 100 ms L2 diffs, and event-driven BBO.
* Hyperliquid: event trades, native L2 snapshots, and event-driven BBO.
* Both: historical and incremental confirmed funding rates.

Run ``reference_price_collector.py`` separately for Binance index/mark and
Hyperliquid oracle/mark prices.

Key environment variables:
  TRADING_DATA_DIR=$HOME/trading_data
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
  TRADING_BINANCE_L2_SNAPSHOT_SECONDS=30
  TRADING_WS_RECONNECT_INITIAL_SECONDS=0.5
  TRADING_WS_RECONNECT_MAX_SECONDS=30
  TRADING_WS_RECONNECT_RESET_SECONDS=60
  TRADING_WS_RECONNECT_JITTER_FRACTION=0.2
  TRADING_FUNDING_POLL_SECONDS=300
  TRADING_FUNDING_BACKFILL_START=2026-06-02T00:00:00Z
  TRADING_WS_CA_BUNDLE=/path/to/organization-ca.pem
"""

from __future__ import annotations

import logging
import os
import signal
import threading
from datetime import datetime, timezone

from parquet_store import default_data_root
from kiwoom_collector import KiwoomStreamCollector
from stream_parquet_store import StreamFlushLoop, StreamParquetStore, wait_for_producers
from streaming_collector import (
    BinancePublicStreamCollector,
    BinanceStreamCollector,
    FundingCollector,
    FundingMarket,
    HyperliquidStreamCollector,
)

# KRX-listed SK hynix common share and Nasdaq-listed SKHY ADR, respectively.
MARKETS = (
    FundingMarket("SKHYNIXUSDT", "xyz:SKHX"),
    FundingMarket("SKHYUSDT", "xyz:SKHY"),
)
BINANCE_SYMBOLS = tuple(market.binance_symbol for market in MARKETS)
HYPERLIQUID_COINS = tuple(market.hyperliquid_coin for market in MARKETS)
KIWOOM_STOCK_CODES = ("000660",)


def _positive_float(name: str, default: float) -> float:
    value = float(os.getenv(name, str(default)))
    if value <= 0:
        raise ValueError(f"{name} must be positive")
    return value


def _backfill_start() -> datetime:
    value = os.getenv("TRADING_FUNDING_BACKFILL_START", "2026-06-02T00:00:00Z")
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    return parsed.replace(tzinfo=timezone.utc) if parsed.tzinfo is None else parsed.astimezone(timezone.utc)


def _kiwoom_collector(store: StreamParquetStore) -> KiwoomStreamCollector | None:
    """Create the optional KRX collector only when both runtime secrets exist."""
    app_key = os.getenv("KIWOOM_APP_KEY")
    app_secret = os.getenv("KIWOOM_APP_SECRET")
    if not app_key and not app_secret:
        logging.warning("Kiwoom collector disabled: set KIWOOM_APP_KEY and KIWOOM_APP_SECRET to enable KRX 000660")
        return None
    if not app_key or not app_secret:
        raise ValueError("Set both KIWOOM_APP_KEY and KIWOOM_APP_SECRET, or neither")
    configured_codes = os.getenv("KIWOOM_STOCK_CODES", ",".join(KIWOOM_STOCK_CODES))
    codes = tuple(code.strip() for code in configured_codes.split(",") if code.strip())
    if not codes:
        raise ValueError("KIWOOM_STOCK_CODES must contain at least one stock code")
    return KiwoomStreamCollector(
        store,
        codes,
        app_key,
        app_secret,
        mode=os.getenv("KIWOOM_MODE", "real").strip().lower(),
    )


def main() -> None:
    logging.basicConfig(
        level=os.getenv("TRADING_LOG_LEVEL", "INFO").upper(),
        format="%(asctime)s %(levelname)s %(message)s",
    )
    data_root = default_data_root()
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
        logging.warning("Experimental WAL-first collector enabled; every frame is fsynced before return")
    if async_writes:
        logging.warning(
            "Experimental async Parquet writer enabled (RAM rows=%d bytes=%d, disk spool bytes=%d files=%d)",
            stream_store.max_pending_rows, stream_store.max_pending_bytes,
            stream_store.max_spool_bytes, stream_store.max_spool_files,
        )
    flush_loop = StreamFlushLoop(stream_store, _positive_float("TRADING_STREAM_FLUSH_SECONDS", 5))
    binance = BinanceStreamCollector(
        stream_store,
        BINANCE_SYMBOLS,
        snapshot_seconds=_positive_float("TRADING_BINANCE_L2_SNAPSHOT_SECONDS", 30),
    )
    binance_public = BinancePublicStreamCollector(
        stream_store,
        BINANCE_SYMBOLS,
        snapshot_requester=binance.request_immediate_snapshot,
    )
    hyperliquid = HyperliquidStreamCollector(stream_store, HYPERLIQUID_COINS)
    kiwoom = _kiwoom_collector(stream_store)
    funding = FundingCollector(
        str(raw_root), MARKETS, _backfill_start(), _positive_float("TRADING_FUNDING_POLL_SECONDS", 300)
    )
    workers = [
        threading.Thread(target=flush_loop.run, name="parquet-flush", daemon=True),
        threading.Thread(target=binance.run, name="binance-market-stream", daemon=True),
        threading.Thread(target=binance_public.run, name="binance-public-stream", daemon=True),
        threading.Thread(target=hyperliquid.run, name="hyperliquid-stream", daemon=True),
        threading.Thread(target=funding.run, name="funding-history", daemon=True),
    ]
    if kiwoom is not None:
        workers.append(threading.Thread(target=kiwoom.run, name="kiwoom-stream", daemon=True))
    logging.info("Raw Parquet data root: %s", raw_root.resolve())
    logging.info("Processed data is created separately under: %s", (data_root / "processed").resolve())
    logging.info("Markets: Binance=%s Hyperliquid=%s", BINANCE_SYMBOLS, HYPERLIQUID_COINS)
    binance.start_snapshot_loop()
    signal.signal(signal.SIGTERM, signal.default_int_handler)
    for worker in workers:
        worker.start()
    try:
        while True:
            workers[0].join(timeout=1)
            if stream_store.fatal_error is not None:
                raise stream_store.fatal_error
    except KeyboardInterrupt:
        logging.info("Stopping collector")
    finally:
        binance.stop()
        binance_public.stop()
        hyperliquid.stop()
        funding.stop()
        if kiwoom is not None:
            kiwoom.stop()
        wait_for_producers([*workers[1:], binance._snapshot_thread])
        stream_store.close_for_appends()
        try:
            flush_loop.stop()
        finally:
            wait_for_producers(workers[:1])
            stream_store.close_wal()


if __name__ == "__main__":
    main()
