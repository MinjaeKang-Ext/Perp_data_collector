"""Real-time Binance and Hyperliquid market-data collectors.

Binance L2 is stored as 100 ms incremental updates plus periodic REST anchors.
Hyperliquid L2 is stored as its native block-driven snapshot feed. Trades and
BBO are stored exactly as event streams rather than artificial fixed samples.
"""

from __future__ import annotations

import json
import logging
import os
import random
import ssl
import threading
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Callable, Iterable

import pandas as pd

from binance_fetcher import BinanceFetcher
from hyperliquid_fetcher import HyperliquidFetcher
from parquet_store import ParquetStore
from stream_parquet_store import StreamParquetStore

BINANCE_MARKET_WS_URL = "wss://fstream.binance.com/market/stream"
BINANCE_PUBLIC_WS_URL = "wss://fstream.binance.com/public/stream"
HYPERLIQUID_WS_URL = "wss://api.hyperliquid.xyz/ws"


def _utc_now() -> pd.Timestamp:
    return pd.Timestamp.now(tz="UTC")


def _timestamp(milliseconds: int | str | float) -> pd.Timestamp:
    return pd.to_datetime(int(milliseconds), unit="ms", utc=True)


def parse_binance_message(message: dict[str, Any], received_at: pd.Timestamp | None = None) -> tuple[str, str, list[dict[str, Any]]] | None:
    """Normalize one Binance market-stream payload into rows for Parquet storage."""
    payload = message.get("data", message)
    received_at = received_at or _utc_now()
    event_type = payload.get("e")
    if event_type is None and "result" in payload and "id" in payload:
        return None  # WebSocket SUBSCRIBE acknowledgement.
    symbol = payload.get("s")
    if not symbol:
        raise ValueError(f"Binance stream message has no symbol: {payload!r}")
    if event_type == "aggTrade":
        return symbol, "agg_trades", [{
            "event_time": _timestamp(payload["T"]),
            "exchange_event_time": _timestamp(payload["E"]),
            "received_at": received_at,
            "symbol": symbol,
            "aggregate_trade_id": int(payload["a"]),
            "first_trade_id": int(payload["f"]),
            "last_trade_id": int(payload["l"]),
            "price": float(payload["p"]),
            "quantity": float(payload["q"]),
            "buyer_is_maker": bool(payload["m"]),
        }]
    if event_type == "depthUpdate":
        rows = []
        for side, levels in (("bid", payload.get("b", [])), ("ask", payload.get("a", []))):
            for price, quantity in levels:
                rows.append({
                    "event_time": _timestamp(payload["E"]),
                    "transaction_time": _timestamp(payload.get("T", payload["E"])),
                    "received_at": received_at,
                    "symbol": symbol,
                    "side": side,
                    "price": float(price),
                    "quantity": float(quantity),
                    "first_update_id": int(payload["U"]),
                    "final_update_id": int(payload["u"]),
                    "previous_final_update_id": int(payload["pu"]) if payload.get("pu") is not None else None,
                })
        return symbol, "l2_updates", rows
    if event_type == "bookTicker":
        return symbol, "bbo", [{
            "event_time": _timestamp(payload.get("T", payload["E"])),
            "exchange_event_time": _timestamp(payload["E"]),
            "received_at": received_at,
            "symbol": symbol,
            "update_id": int(payload["u"]) if payload.get("u") is not None else None,
            "best_bid_price": float(payload["b"]),
            "best_bid_quantity": float(payload["B"]),
            "best_ask_price": float(payload["a"]),
            "best_ask_quantity": float(payload["A"]),
        }]
    raise ValueError(f"Unsupported Binance stream event: {event_type!r}")


def parse_hyperliquid_message(message: dict[str, Any], received_at: pd.Timestamp | None = None) -> tuple[str, str, list[dict[str, Any]]] | None:
    """Normalize one Hyperliquid subscription message into rows for storage."""
    channel = message.get("channel")
    payload = message.get("data")
    received_at = received_at or _utc_now()
    if channel == "subscriptionResponse":
        return None
    if channel == "trades":
        rows = [{
            "event_time": _timestamp(trade["time"]),
            "received_at": received_at,
            "coin": trade["coin"],
            "trade_id": int(trade["tid"]),
            "side": trade["side"],
            "price": float(trade["px"]),
            "quantity": float(trade["sz"]),
            "transaction_hash": trade["hash"],
        } for trade in payload]
        return (rows[0]["coin"], "trades", rows) if rows else None
    if channel == "l2Book":
        coin = payload["coin"]
        rows = []
        for side, levels in zip(("bid", "ask"), payload["levels"], strict=True):
            for level, entry in enumerate(levels, start=1):
                rows.append({
                    "event_time": _timestamp(payload["time"]),
                    "received_at": received_at,
                    "coin": coin,
                    "side": side,
                    "level": level,
                    "price": float(entry["px"]),
                    "quantity": float(entry["sz"]),
                    "number_of_orders": int(entry["n"]),
                })
        return coin, "l2_snapshots", rows
    if channel == "bbo":
        coin = payload["coin"]
        bid, ask = payload["bbo"]
        return coin, "bbo", [{
            "event_time": _timestamp(payload["time"]),
            "received_at": received_at,
            "coin": coin,
            "best_bid_price": float(bid["px"]) if bid else None,
            "best_bid_quantity": float(bid["sz"]) if bid else None,
            "best_ask_price": float(ask["px"]) if ask else None,
            "best_ask_quantity": float(ask["sz"]) if ask else None,
        }]
    raise ValueError(f"Unsupported Hyperliquid stream channel: {channel!r}")


def _project_binance_wal(message: str, received_at: pd.Timestamp) -> list[tuple[str, str, str, list[dict[str, Any]]]]:
    parsed = parse_binance_message(json.loads(message), received_at)
    return [("binance", *parsed)] if parsed is not None else []


def _project_hyperliquid_wal(message: str, received_at: pd.Timestamp) -> list[tuple[str, str, str, list[dict[str, Any]]]]:
    parsed = parse_hyperliquid_message(json.loads(message), received_at)
    return [("hyperliquid", *parsed)] if parsed is not None else []


class _ReconnectableWebSocket:
    """A websocket-client wrapper with capped backoff and graceful stop."""

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
        """Return the next base delay and advance the capped retry schedule."""
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
            self._app.run_forever(
                ping_interval=20,
                ping_timeout=10,
                sslopt=ssl_options,
            )
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


class BinanceStreamCollector(_ReconnectableWebSocket):
    """Store Binance aggregate trades and coordinate periodic REST L2 anchors."""

    def __init__(
        self,
        store: StreamParquetStore,
        symbols: Iterable[str],
        snapshot_seconds: float = 30.0,
        snapshot_depth: int = 20,
        websocket_url: str = BINANCE_MARKET_WS_URL,
    ) -> None:
        super().__init__(websocket_url, "binance")
        self.store = store
        if store.wal_first:
            store.register_frame_parser("binance-market", _project_binance_wal)
        self.symbols = tuple(symbols)
        self.snapshot_seconds = snapshot_seconds
        self.snapshot_depth = snapshot_depth
        self.fetcher = BinanceFetcher()
        self._stream_ready = threading.Event()
        self._snapshot_requested = threading.Event()
        self._snapshot_request_lock = threading.Lock()
        self._pending_snapshot_requests = 0
        self._snapshot_thread = threading.Thread(target=self._snapshot_loop, name="binance-l2-anchor", daemon=True)

    def start_snapshot_loop(self) -> None:
        self._snapshot_thread.start()

    def _on_open(self, ws: Any) -> None:
        streams = [f"{symbol.lower()}@aggTrade" for symbol in self.symbols]
        ws.send(json.dumps({"method": "SUBSCRIBE", "params": streams, "id": 1}))
        self._stream_ready.set()
        logging.info("Binance market websocket subscribed to %d aggregate-trade streams", len(streams))

    def _on_close(self, ws: Any, status_code: int, message: str) -> None:
        self._stream_ready.clear()
        super()._on_close(ws, status_code, message)

    def request_immediate_snapshot(self) -> None:
        """Schedule a REST depth anchor after an L2 WebSocket (re)subscription."""
        with self._snapshot_request_lock:
            self._pending_snapshot_requests += 1
            self._snapshot_requested.set()

    def _consume_snapshot_request(self) -> bool:
        with self._snapshot_request_lock:
            if self._pending_snapshot_requests == 0:
                return False
            self._pending_snapshot_requests -= 1
            if self._pending_snapshot_requests == 0:
                self._snapshot_requested.clear()
            return True

    def _on_message(self, _ws: Any, message: str) -> None:
        if self.store.wal_first:
            self.store.ingest_frame("binance-market", message)
            return
        try:
            parsed = parse_binance_message(json.loads(message))
            if parsed is not None:
                symbol, dataset, records = parsed
                self.store.append("binance", symbol, dataset, records)
        except Exception:
            logging.exception("Unable to process Binance websocket message")

    def _snapshot_loop(self) -> None:
        while not self.stop_event.is_set():
            if not self._stream_ready.wait(timeout=1):
                continue
            immediate = self._snapshot_requested.wait(timeout=self.snapshot_seconds)
            if self.stop_event.is_set() or not self._stream_ready.is_set():
                continue
            if immediate and not self._consume_snapshot_request():
                continue
            self._store_snapshot_anchor()

    def _store_snapshot_anchor(self) -> None:
        """Persist one complete REST L2 anchor for every configured symbol."""
        for symbol in self.symbols:
            try:
                snapshot = self.fetcher.fetch_order_book(symbol, depth=self.snapshot_depth)
                if not snapshot.empty:
                    self.store.append(
                        "binance", symbol, "l2_snapshots",
                        snapshot.rename(columns={
                            "time": "event_time",
                            "event_time": "exchange_event_time",
                        }).to_dict("records"),
                    )
            except Exception:
                logging.exception("Binance %s L2 anchor snapshot failed", symbol)


class BinancePublicStreamCollector(_ReconnectableWebSocket):
    """Store Binance 100 ms L2 diffs and event-driven BBO on the public endpoint."""

    def __init__(
        self,
        store: StreamParquetStore,
        symbols: Iterable[str],
        websocket_url: str = BINANCE_PUBLIC_WS_URL,
        snapshot_requester: Callable[[], None] | None = None,
    ) -> None:
        super().__init__(websocket_url, "binance-public")
        self.store = store
        if store.wal_first:
            store.register_frame_parser("binance-public", _project_binance_wal)
        self.symbols = tuple(symbols)
        self.snapshot_requester = snapshot_requester

    def _on_open(self, ws: Any) -> None:
        streams = []
        for symbol in self.symbols:
            stream_symbol = symbol.lower()
            streams.extend((f"{stream_symbol}@depth@100ms", f"{stream_symbol}@bookTicker"))
        ws.send(json.dumps({"method": "SUBSCRIBE", "params": streams, "id": 1}))
        if self.snapshot_requester is not None:
            self.snapshot_requester()
        logging.info("Binance public websocket subscribed to %d L2/BBO streams", len(streams))

    def _on_message(self, _ws: Any, message: str) -> None:
        if self.store.wal_first:
            self.store.ingest_frame("binance-public", message)
            return
        try:
            parsed = parse_binance_message(json.loads(message))
            if parsed is not None:
                symbol, dataset, records = parsed
                self.store.append("binance", symbol, dataset, records)
        except Exception:
            logging.exception("Unable to process Binance public websocket message")


class HyperliquidStreamCollector(_ReconnectableWebSocket):
    """Store event trades/BBO and Hyperliquid's native L2 snapshots."""

    def __init__(self, store: StreamParquetStore, coins: Iterable[str], websocket_url: str = HYPERLIQUID_WS_URL) -> None:
        super().__init__(websocket_url, "hyperliquid")
        self.store = store
        if store.wal_first:
            store.register_frame_parser("hyperliquid-market", _project_hyperliquid_wal)
        self.coins = tuple(coins)

    def _on_open(self, ws: Any) -> None:
        for coin in self.coins:
            for subscription_type in ("trades", "l2Book", "bbo"):
                ws.send(json.dumps({"method": "subscribe", "subscription": {"type": subscription_type, "coin": coin}}))
        logging.info("Hyperliquid websocket subscribed to %d streams", len(self.coins) * 3)

    def _on_message(self, _ws: Any, message: str) -> None:
        if self.store.wal_first:
            self.store.ingest_frame("hyperliquid-market", message)
            return
        try:
            parsed = parse_hyperliquid_message(json.loads(message))
            if parsed is not None:
                coin, dataset, records = parsed
                self.store.append("hyperliquid", coin, dataset, records)
        except Exception:
            logging.exception("Unable to process Hyperliquid websocket message")


@dataclass(frozen=True)
class FundingMarket:
    binance_symbol: str
    hyperliquid_coin: str


class FundingCollector:
    """Low-volume, idempotent funding-history collector kept off the hot path."""

    def __init__(
        self, root: str, markets: Iterable[FundingMarket], backfill_start: datetime, poll_seconds: float = 300.0
    ) -> None:
        self.store = ParquetStore(root)
        self.markets = tuple(markets)
        self.backfill_start = backfill_start.astimezone(timezone.utc)
        self.poll_seconds = poll_seconds
        self.stop_event = threading.Event()
        self.binance = BinanceFetcher()
        self.hyperliquid = HyperliquidFetcher()

    def run(self) -> None:
        first_poll = True
        while not self.stop_event.is_set():
            end = datetime.now(timezone.utc)
            start = self.backfill_start if first_poll else end - timedelta(hours=48)
            self._collect_range(start, end)
            first_poll = False
            self.stop_event.wait(self.poll_seconds)

    def stop(self) -> None:
        self.stop_event.set()

    def _collect_range(self, start: datetime, end: datetime) -> None:
        start_ms, end_ms = int(start.timestamp() * 1000), int(end.timestamp() * 1000)
        for market in self.markets:
            try:
                frame = self.binance.fetch_funding_rate(market.binance_symbol, start_ms, end_ms)
                self.store.write("binance", market.binance_symbol, "funding", frame, "fundingTime")
            except Exception:
                logging.exception("Binance %s funding collection failed", market.binance_symbol)
            try:
                frame = self.hyperliquid.fetch_funding_history(market.hyperliquid_coin, start_ms, end_ms)
                if not frame.empty:
                    frame["coin"] = market.hyperliquid_coin
                self.store.write("hyperliquid", market.hyperliquid_coin, "funding", frame, "time")
            except Exception:
                logging.exception("Hyperliquid %s funding collection failed", market.hyperliquid_coin)
