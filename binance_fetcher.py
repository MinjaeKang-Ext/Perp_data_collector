"""
Binance USDS-M 선물 데이터 수집기
- SKHYNIXUSDT / SKHYUSDT 등 1분봉(K-line) + 펀딩비 히스토리 다운로드
- REST 페이지네이션: klines 최대 1500개/요청, fundingRate 최대 1000개/요청
"""

import time
from typing import Any, Optional

import pandas as pd
import requests

BASE_URL = "https://fapi.binance.com"
KLINES_LIMIT = 1500       # Binance Futures klines 1회 최대 반환 개수
FUNDING_LIMIT = 1000      # fundingRate 1회 최대 반환 개수
REQUEST_SLEEP = 0.25      # 레이트리밋 방지용 딜레이(초)
ORDER_BOOK_LIMITS = {5, 10, 20, 50, 100, 500, 1000}


class BinanceFetcher:
    def __init__(self, session: Optional[requests.Session] = None):
        self.session = session or requests.Session()

    def _get(self, path: str, params: dict) -> Any:
        resp = self.session.get(f"{BASE_URL}{path}", params=params, timeout=10)
        if resp.status_code != 200:
            raise RuntimeError(f"Binance API error {resp.status_code}: {resp.text}")
        return resp.json()

    def fetch_klines(self, symbol: str, start_ms: int, end_ms: int, interval: str = "1m") -> pd.DataFrame:
        """1분봉 K-line을 [start_ms, end_ms) 구간 전체에 대해 페이지네이션으로 수집."""
        rows = []
        cursor = start_ms
        while cursor < end_ms:
            batch = self._get("/fapi/v1/klines", {
                "symbol": symbol,
                "interval": interval,
                "startTime": cursor,
                "endTime": end_ms,
                "limit": KLINES_LIMIT,
            })
            if not batch:
                break
            rows.extend(batch)
            next_cursor = batch[-1][0] + 60_000  # 다음 1분봉 시작점
            if next_cursor <= cursor:  # 무한루프 방지
                break
            cursor = next_cursor
            if len(batch) < KLINES_LIMIT:
                break
            time.sleep(REQUEST_SLEEP)

        cols = ["open_time", "open", "high", "low", "close", "volume",
                "close_time", "quote_volume", "trades",
                "taker_buy_base", "taker_buy_quote", "ignore"]
        df = pd.DataFrame(rows, columns=cols)
        if df.empty:
            return df
        df["open_time"] = pd.to_datetime(df["open_time"], unit="ms", utc=True)
        df["close_time"] = pd.to_datetime(df["close_time"], unit="ms", utc=True)
        for c in ["open", "high", "low", "close", "volume", "quote_volume",
                  "taker_buy_base", "taker_buy_quote"]:
            df[c] = df[c].astype(float)
        df["symbol"] = symbol
        return df.drop(columns=["ignore"])

    def fetch_funding_rate(self, symbol: str, start_ms: int, end_ms: int) -> pd.DataFrame:
        """펀딩비 히스토리를 [start_ms, end_ms) 구간 전체에 대해 페이지네이션으로 수집."""
        rows = []
        cursor = start_ms
        while cursor < end_ms:
            batch = self._get("/fapi/v1/fundingRate", {
                "symbol": symbol,
                "startTime": cursor,
                "endTime": end_ms,
                "limit": FUNDING_LIMIT,
            })
            if not batch:
                break
            rows.extend(batch)
            next_cursor = batch[-1]["fundingTime"] + 1
            if next_cursor <= cursor:
                break
            cursor = next_cursor
            if len(batch) < FUNDING_LIMIT:
                break
            time.sleep(REQUEST_SLEEP)

        df = pd.DataFrame(rows)
        if df.empty:
            return df
        df["fundingTime"] = pd.to_datetime(df["fundingTime"], unit="ms", utc=True)
        df["fundingRate"] = df["fundingRate"].astype(float)
        df["symbol"] = symbol
        return df

    def fetch_open_interest(self, symbol: str) -> pd.DataFrame:
        """현재 미결제약정 스냅샷을 반환한다.

        Binance의 이 엔드포인트는 시계열을 제공하지 않으므로, 호출 시각마다
        결과를 저장해 로컬에서 OI 시계열을 구성한다.
        """
        row = self._get("/fapi/v1/openInterest", {"symbol": symbol})
        if not isinstance(row, dict):
            raise RuntimeError(f"Unexpected Binance open-interest response: {row!r}")
        df = pd.DataFrame([row])
        df["time"] = pd.to_datetime(df["time"], unit="ms", utc=True)
        df["openInterest"] = df["openInterest"].astype(float)
        df["symbol"] = symbol
        return df

    def fetch_order_book(self, symbol: str, depth: int = 20) -> pd.DataFrame:
        """현재 호가창의 상위 ``depth`` 매수·매도 레벨을 스냅샷으로 반환한다."""
        if depth not in ORDER_BOOK_LIMITS:
            raise ValueError(f"Binance order-book depth must be one of {sorted(ORDER_BOOK_LIMITS)}")
        payload = self._get("/fapi/v1/depth", {"symbol": symbol, "limit": depth})
        if not isinstance(payload, dict):
            raise RuntimeError(f"Unexpected Binance order-book response: {payload!r}")
        snapshot_ms = payload.get("T", payload.get("E"))
        if snapshot_ms is None:
            raise RuntimeError("Binance order-book response has no event timestamp")
        rows = []
        for side, levels in (("bid", payload.get("bids", [])), ("ask", payload.get("asks", []))):
            for level, entry in enumerate(levels, start=1):
                rows.append({
                    "time": snapshot_ms,
                    "symbol": symbol,
                    "side": side,
                    "level": level,
                    "price": entry[0],
                    "quantity": entry[1],
                    "lastUpdateId": payload.get("lastUpdateId"),
                    "event_time": payload.get("E"),
                    "transaction_time": payload.get("T"),
                })
        df = pd.DataFrame(rows)
        if df.empty:
            return df
        for column in ("time", "event_time", "transaction_time"):
            df[column] = pd.to_datetime(df[column], unit="ms", utc=True)
        df["price"] = df["price"].astype(float)
        df["quantity"] = df["quantity"].astype(float)
        return df
