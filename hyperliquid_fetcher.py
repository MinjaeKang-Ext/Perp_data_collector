"""
Hyperliquid HIP-3 마켓 데이터 수집기 (xyz:SKHX, xyz:SKHY 등)

중요: Hyperliquid WebSocket은 "구독한 시점 이후"의 실시간 데이터만 push합니다.
      과거 구간 백필은 WebSocket으로는 불가능하고, REST의 candleSnapshot /
      fundingHistory로만 가능합니다. 이 파일의 HyperliquidFetcher가 그 백필을 담당하고,
      맨 아래 HyperliquidCandleStream은 "지금부터의" 실시간 캔들 수신용 보조 클래스입니다.

- candleSnapshot: 1회 호출당 최근 5000개 캔들까지만 반환 -> 시간 구간을 나눠 반복 호출
- coin 식별자: HIP-3 마켓은 "deployer:심볼" 형식 (예: "xyz:SKHX"). 정확한 표기는
  list_dex_universe()로 먼저 확인해보는 것을 권장합니다.
"""

import json
import time
from datetime import datetime, timezone
from typing import Iterable, Optional

import pandas as pd
import requests

INFO_URL = "https://api.hyperliquid.xyz/info"
WS_URL = "wss://api.hyperliquid.xyz/ws"

INTERVAL_MS = {
    "1m": 60_000, "5m": 5 * 60_000, "15m": 15 * 60_000,
    "1h": 60 * 60_000, "4h": 4 * 60 * 60_000, "1d": 24 * 60 * 60_000,
}
MAX_CANDLES_PER_CALL = 5000
REQUEST_SLEEP = 0.25
EMPTY_CANDLE_RETRIES = 2
EMPTY_CANDLE_RETRY_SLEEP = 1.0


class HyperliquidFetcher:
    def __init__(self, session: Optional[requests.Session] = None):
        self.session = session or requests.Session()

    def _post(self, payload: dict):
        resp = self.session.post(INFO_URL, json=payload, timeout=10)
        if resp.status_code != 200:
            raise RuntimeError(f"Hyperliquid API error {resp.status_code}: {resp.text}")
        return resp.json()

    def list_dex_universe(self, dex: str = "xyz") -> list:
        """해당 HIP-3 dex(xyz 등)에 상장된 코인 이름 목록.
        'xyz:SKHX' / 'xyz:SKHY' 표기가 실제와 맞는지 확인할 때 사용."""
        meta = self._post({"type": "meta", "dex": dex})
        return [asset["name"] for asset in meta.get("universe", [])]

    def fetch_candles(self, coin: str, start_ms: int, end_ms: int, interval: str = "1m") -> pd.DataFrame:
        """1분봉 캔들을 5000개 단위 시간 구간으로 나눠 순차 수집.

        Hyperliquid이 일시적으로 빈 candleSnapshot을 반환하는 경우가 있어,
        빈 응답은 즉시 결측으로 확정하지 않고 제한적으로 재시도한다.
        """
        step_ms = INTERVAL_MS[interval] * MAX_CANDLES_PER_CALL
        rows = []
        cursor = start_ms
        while cursor < end_ms:
            window_end = min(cursor + step_ms, end_ms)
            payload = {
                "type": "candleSnapshot",
                "req": {"coin": coin, "interval": interval,
                        "startTime": cursor, "endTime": window_end},
            }
            batch = []
            for attempt in range(EMPTY_CANDLE_RETRIES + 1):
                batch = self._post(payload)
                if batch:
                    break
                if attempt < EMPTY_CANDLE_RETRIES:
                    time.sleep(EMPTY_CANDLE_RETRY_SLEEP)
            if batch:
                rows.extend(batch)
                cursor = max(batch[-1]["t"] + INTERVAL_MS[interval], cursor + INTERVAL_MS[interval])
            else:
                cursor = window_end
            time.sleep(REQUEST_SLEEP)

        df = pd.DataFrame(rows)
        if df.empty:
            return df
        df = df.rename(columns={
            "t": "open_time", "T": "close_time", "o": "open", "h": "high",
            "l": "low", "c": "close", "v": "volume", "n": "trades", "s": "coin",
        })
        df["open_time"] = pd.to_datetime(df["open_time"], unit="ms", utc=True)
        df["close_time"] = pd.to_datetime(df["close_time"], unit="ms", utc=True)
        for c in ["open", "high", "low", "close", "volume"]:
            df[c] = df[c].astype(float)
        return (df.drop_duplicates(subset=["open_time"])
                  .sort_values("open_time")
                  .reset_index(drop=True))

    def fetch_funding_history(self, coin: str, start_ms: int, end_ms: int) -> pd.DataFrame:
        """펀딩비 히스토리를 반환분이 없어질 때까지 반복 호출로 수집."""
        rows = []
        cursor = start_ms
        while cursor < end_ms:
            batch = self._post({
                "type": "fundingHistory", "coin": coin,
                "startTime": cursor, "endTime": end_ms,
            })
            if not batch:
                break
            rows.extend(batch)
            next_cursor = batch[-1]["time"] + 1
            if next_cursor <= cursor:
                break
            cursor = next_cursor
            time.sleep(REQUEST_SLEEP)

        df = pd.DataFrame(rows)
        if df.empty:
            return df
        df["time"] = pd.to_datetime(df["time"], unit="ms", utc=True)
        df["fundingRate"] = df["fundingRate"].astype(float)
        df["premium"] = df["premium"].astype(float)
        return df

    def fetch_open_interest(self, coins: Iterable[str], dex: str = "xyz") -> pd.DataFrame:
        """현재 HIP-3 마켓들의 미결제약정 스냅샷을 한 요청으로 반환한다."""
        result = self._post({"type": "metaAndAssetCtxs", "dex": dex})
        if not isinstance(result, list) or len(result) != 2:
            raise RuntimeError("Unexpected Hyperliquid metaAndAssetCtxs response")
        meta, contexts = result
        universe = meta.get("universe", [])
        contexts_by_coin = {
            asset["name"]: context for asset, context in zip(universe, contexts, strict=True)
        }
        observed_at = datetime.now(timezone.utc)
        rows = []
        for coin in coins:
            context = contexts_by_coin.get(coin)
            if context is None:
                raise ValueError(f"Hyperliquid coin is not listed on {dex}: {coin}")
            rows.append({
                "coin": coin,
                "time": observed_at,
                "openInterest": float(context["openInterest"]),
                "markPx": float(context["markPx"]),
            })
        return pd.DataFrame(rows)

    def fetch_order_book(self, coin: str, depth: int = 20) -> pd.DataFrame:
        """현재 L2 호가창의 상위 ``depth`` 매수·매도 레벨을 스냅샷으로 반환한다."""
        if not 1 <= depth <= 20:
            raise ValueError("Hyperliquid order-book depth must be between 1 and 20")
        payload = self._post({"type": "l2Book", "coin": coin})
        if not isinstance(payload, dict) or "time" not in payload or "levels" not in payload:
            raise RuntimeError(f"Unexpected Hyperliquid order-book response: {payload!r}")
        rows = []
        for side, levels in zip(("bid", "ask"), payload["levels"], strict=True):
            for level, entry in enumerate(levels[:depth], start=1):
                rows.append({
                    "time": payload["time"],
                    "coin": payload.get("coin", coin),
                    "side": side,
                    "level": level,
                    "price": entry["px"],
                    "quantity": entry["sz"],
                    "number_of_orders": entry["n"],
                })
        df = pd.DataFrame(rows)
        if df.empty:
            return df
        df["time"] = pd.to_datetime(df["time"], unit="ms", utc=True)
        df["price"] = df["price"].astype(float)
        df["quantity"] = df["quantity"].astype(float)
        df["number_of_orders"] = df["number_of_orders"].astype(int)
        return df


class HyperliquidCandleStream:
    """실시간(구독 시점 이후) 1분봉 스트리밍 전용 — 과거 데이터 백필에는 사용 불가.
    pip install websocket-client 필요."""

    def __init__(self, coin: str, interval: str = "1m", on_candle=None):
        self.coin = coin
        self.interval = interval
        self.on_candle = on_candle or (lambda c: print(c))

    def run(self):
        import websocket  # noqa: local import (선택적 의존성)

        def on_open(ws):
            sub = {
                "method": "subscribe",
                "subscription": {"type": "candle", "coin": self.coin, "interval": self.interval},
            }
            ws.send(json.dumps(sub))

        def on_message(ws, message):
            data = json.loads(message)
            if data.get("channel") == "candle":
                self.on_candle(data["data"])

        ws = websocket.WebSocketApp(WS_URL, on_open=on_open, on_message=on_message)
        ws.run_forever()
