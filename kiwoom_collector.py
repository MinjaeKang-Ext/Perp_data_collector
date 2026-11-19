"""Kiwoom REST/WebSocket collector for KRX spot market data.

The collector uses Kiwoom's official REST OAuth endpoint to obtain a short-lived
access token, then subscribes to WebSocket types 0B (trades), 0C (best quote),
and 0D (ten-level order-book quantities).  App credentials are read only at
runtime from environment variables and are never persisted or logged.
"""

from __future__ import annotations

import json
import logging
from datetime import datetime
from typing import Any, Iterable
from zoneinfo import ZoneInfo

import pandas as pd
import requests

from stream_parquet_store import StorageFatalError, StreamParquetStore
from streaming_collector import _ReconnectableWebSocket, _utc_now

KST = ZoneInfo("Asia/Seoul")
KIWOOM_REST_URLS = {
    "real": "https://api.kiwoom.com",
    "demo": "https://mockapi.kiwoom.com",
}
KIWOOM_WS_URLS = {
    "real": "wss://api.kiwoom.com:10000/api/dostk/websocket",
    "demo": "wss://mockapi.kiwoom.com:10000/api/dostk/websocket",
}


def _numeric(value: Any) -> float | None:
    """Parse Kiwoom signed display fields while retaining price/size magnitude."""
    if value is None or str(value).strip() == "":
        return None
    try:
        return abs(float(str(value).replace(",", "").strip()))
    except ValueError:
        return None


def _trade_side(value: Any) -> str | None:
    text = str(value or "").strip()
    if text.startswith("+"):
        return "buy"
    if text.startswith("-"):
        return "sell"
    return None


def _event_time(values: dict[str, Any], received_at: pd.Timestamp) -> pd.Timestamp:
    """Build a KST timestamp from Kiwoom FID 20 (HHMMSS), with a safe fallback."""
    raw = str(values.get("20", "")).strip()
    digits = raw[-6:]
    if len(digits) == 6 and digits.isdigit():
        try:
            local_now = received_at.tz_convert(KST)
            parsed = datetime(
                local_now.year, local_now.month, local_now.day,
                int(digits[:2]), int(digits[2:4]), int(digits[4:]), tzinfo=KST,
            )
            return pd.Timestamp(parsed).tz_convert("UTC")
        except ValueError:
            pass
    return received_at


def parse_kiwoom_message(
    message: dict[str, Any], received_at: pd.Timestamp | None = None
) -> list[tuple[str, str, list[dict[str, Any]]]]:
    """Normalize Kiwoom REAL frames without discarding their raw FID values."""
    if str(message.get("trnm", "")).upper() != "REAL":
        return []
    received_at = received_at or _utc_now()
    parsed: list[tuple[str, str, list[dict[str, Any]]]] = []
    for entry in message.get("data", []):
        if not isinstance(entry, dict):
            continue
        symbol = str(entry.get("item", "")).strip()
        data_type = str(entry.get("type", "")).upper()
        values = entry.get("values")
        if not symbol or not isinstance(values, dict):
            continue
        event_time = _event_time(values, received_at)
        raw_values = json.dumps(values, ensure_ascii=False, sort_keys=True)

        if data_type == "0B":
            quantity = _numeric(values.get("15"))
            price = _numeric(values.get("10"))
            if price is not None and quantity is not None:
                parsed.append((symbol, "trades", [{
                    "event_time": event_time,
                    "received_at": received_at,
                    "symbol": symbol,
                    "price": price,
                    "quantity": quantity,
                    "side": _trade_side(values.get("15")),
                    "cumulative_volume": _numeric(values.get("13")),
                    "raw_values": raw_values,
                }]))

        elif data_type == "0C":
            parsed.append((symbol, "bbo", [{
                "event_time": event_time,
                "received_at": received_at,
                "symbol": symbol,
                "best_bid_price": _numeric(values.get("28")),
                "best_ask_price": _numeric(values.get("27")),
                "best_bid_quantity": None,
                "best_ask_quantity": None,
                "raw_values": raw_values,
            }]))

        elif data_type == "0D":
            rows = []
            for level in range(1, 11):
                for side, price_fid, quantity_fid in (
                    ("ask", str(40 + level), str(60 + level)),
                    ("bid", str(50 + level), str(70 + level)),
                ):
                    price = _numeric(values.get(price_fid))
                    quantity = _numeric(values.get(quantity_fid))
                    if price is not None and quantity is not None:
                        rows.append({
                            "event_time": event_time,
                            "received_at": received_at,
                            "symbol": symbol,
                            "side": side,
                            "level": level,
                            "price": price,
                            "quantity": quantity,
                            "raw_values": raw_values,
                        })
            if rows:
                parsed.append((symbol, "l2_snapshots", rows))
    return parsed


def _project_kiwoom_wal(message: str, received_at: pd.Timestamp) -> list[tuple[str, str, str, list[dict[str, Any]]]]:
    return [("kiwoom", symbol, dataset, rows)
            for symbol, dataset, rows in parse_kiwoom_message(json.loads(message), received_at)]


class KiwoomTokenProvider:
    """Issue an OAuth access token without retaining the App Key/Secret on disk."""

    def __init__(self, app_key: str, app_secret: str, mode: str = "real") -> None:
        if mode not in KIWOOM_REST_URLS:
            raise ValueError("KIWOOM_MODE must be 'real' or 'demo'")
        self.app_key = app_key
        self.app_secret = app_secret
        self.mode = mode

    def issue(self) -> str:
        response = requests.post(
            f"{KIWOOM_REST_URLS[self.mode]}/oauth2/token",
            json={
                "grant_type": "client_credentials",
                "appkey": self.app_key,
                "secretkey": self.app_secret,
            },
            timeout=15,
        )
        if response.status_code != 200:
            raise RuntimeError(f"Kiwoom OAuth error {response.status_code}")
        payload = response.json()
        if str(payload.get("return_code", "0")) != "0" or not payload.get("token"):
            raise RuntimeError(f"Kiwoom OAuth rejected credentials: {payload.get('return_msg', 'unknown error')}")
        return str(payload["token"])


class KiwoomStreamCollector(_ReconnectableWebSocket):
    """Collect KRX spot trades, BBO, and ten-level L2 for supplied stock codes."""

    def __init__(
        self,
        store: StreamParquetStore,
        stock_codes: Iterable[str],
        app_key: str,
        app_secret: str,
        mode: str = "real",
    ) -> None:
        if mode not in KIWOOM_WS_URLS:
            raise ValueError("KIWOOM_MODE must be 'real' or 'demo'")
        super().__init__(KIWOOM_WS_URLS[mode], "kiwoom")
        self.store = store
        if store.wal_first:
            store.register_frame_parser("kiwoom-market", _project_kiwoom_wal)
        self.stock_codes = tuple(stock_codes)
        self.token_provider = KiwoomTokenProvider(app_key, app_secret, mode)
        self._logged_in = False

    def _on_open(self, ws: Any) -> None:
        try:
            self._logged_in = False
            ws.send(json.dumps({"trnm": "LOGIN", "token": self.token_provider.issue()}))
        except Exception:
            logging.exception("Kiwoom WebSocket login setup failed")
            ws.close()

    def _subscribe(self, ws: Any) -> None:
        ws.send(json.dumps({
            "trnm": "REG",
            "grp_no": "1",
            "refresh": "1",
            "data": [{"item": list(self.stock_codes), "type": ["0B", "0C", "0D"]}],
        }))
        logging.info("Kiwoom websocket subscribed to trades, BBO, and L2 for %s", self.stock_codes)

    def _on_message(self, ws: Any, message: str) -> None:
        try:
            received_at = _utc_now()
            payload = json.loads(message)
            if str(payload.get("trnm", "")).upper() == "PING":
                ws.send(message)
                return
            if str(payload.get("trnm", "")).upper() == "LOGIN":
                if str(payload.get("return_code", "0")) != "0":
                    message = payload.get("return_msg", "Kiwoom WebSocket login rejected")
                    logging.warning("Kiwoom WebSocket login rejected: %s; reconnecting with a new token", message)
                    ws.close()
                    return
                self._logged_in = True
                self._subscribe(ws)
                return
            if self.store.wal_first and str(payload.get("trnm", "")).upper() == "REAL":
                self.store.ingest_frame("kiwoom-market", message, received_at)
                return
            for symbol, dataset, records in parse_kiwoom_message(payload):
                self.store.append("kiwoom", symbol, dataset, records)
        except StorageFatalError:
            ws.close()
            raise
        except Exception as error:
            if self.store.wal_first:
                ws.close()
                raise self.store.report_unjournaled_frame("kiwoom-market", error) from error
            logging.exception("Unable to process Kiwoom websocket message")

    def _on_close(self, ws: Any, status_code: int, message: str) -> None:
        self._logged_in = False
        super()._on_close(ws, status_code, message)
