"""Atomic, deduplicated Parquet persistence for the market-data collector."""

from __future__ import annotations

import os
import re
import socket
import tempfile
from getpass import getuser
from pathlib import Path

import pandas as pd

STORAGE_TIMEZONE = "Asia/Seoul"


def default_data_root() -> Path:
    """Return a user- and host-scoped shared-server-safe default location."""
    configured = os.environ.get("TRADING_DATA_DIR")
    if configured:
        return Path(configured).expanduser()
    user = re.sub(r"[^A-Za-z0-9._-]+", "_", getuser())
    host = re.sub(r"[^A-Za-z0-9._-]+", "_", socket.gethostname())
    return Path("data") / f"trading_data_{user}_{host}"


def _safe_component(value: str) -> str:
    return re.sub(r"[^A-Za-z0-9._-]+", "_", value)


class ParquetStore:
    """Persist KST daily partitions without exposing incomplete files to readers."""

    def __init__(self, root: Path | str | None = None):
        self.root = Path(root) if root is not None else default_data_root()

    def dataset_exists(self, venue: str, symbol: str, dataset: str) -> bool:
        directory = self._dataset_directory(venue, symbol, dataset)
        return any(directory.glob("date=*/data.parquet")) if directory.exists() else False

    def write(
        self,
        venue: str,
        symbol: str,
        dataset: str,
        frame: pd.DataFrame,
        timestamp_column: str,
        dedupe_columns: tuple[str, ...] | None = None,
    ) -> int:
        """Merge a frame into KST-day Parquet partitions and return rows written.

        A temporary file in the destination directory is atomically renamed only
        after Parquet serialization completes. Repeated polls are idempotent when
        their timestamp values overlap.
        """
        if frame.empty:
            return 0
        if timestamp_column not in frame:
            raise ValueError(f"Missing timestamp column: {timestamp_column}")
        dedupe_columns = dedupe_columns or (timestamp_column,)
        missing_dedupe_columns = set(dedupe_columns).difference(frame.columns)
        if missing_dedupe_columns:
            raise ValueError(f"Missing deduplication columns: {sorted(missing_dedupe_columns)}")

        data = frame.copy()
        data[timestamp_column] = (
            pd.to_datetime(data[timestamp_column], utc=True).dt.tz_convert(STORAGE_TIMEZONE)
        )
        if data[timestamp_column].isna().any():
            raise ValueError(f"Invalid timestamp in {timestamp_column}")
        data["_partition_date"] = data[timestamp_column].dt.strftime("%Y-%m-%d")

        written = 0
        for date, partition in data.groupby("_partition_date", sort=True):
            target_dir = self._dataset_directory(venue, symbol, dataset) / f"date={date}"
            target_dir.mkdir(parents=True, exist_ok=True)
            target = target_dir / "data.parquet"
            incoming = partition.drop(columns=["_partition_date"])
            if target.exists():
                existing = pd.read_parquet(target, engine="pyarrow")
                merged = pd.concat([existing, incoming], ignore_index=True)
            else:
                merged = incoming
            merged[timestamp_column] = (
                pd.to_datetime(merged[timestamp_column], utc=True).dt.tz_convert(STORAGE_TIMEZONE)
            )
            merged = (
                merged.drop_duplicates(subset=list(dedupe_columns), keep="last")
                .sort_values(timestamp_column)
                .reset_index(drop=True)
            )
            with tempfile.NamedTemporaryFile(
                mode="wb", suffix=".parquet", dir=target_dir, delete=False
            ) as temporary:
                temporary_path = Path(temporary.name)
            try:
                merged.to_parquet(temporary_path, engine="pyarrow", index=False)
                os.replace(temporary_path, target)
            finally:
                temporary_path.unlink(missing_ok=True)
            written += len(incoming)
        return written

    def _dataset_directory(self, venue: str, symbol: str, dataset: str) -> Path:
        return self.root / _safe_component(venue) / _safe_component(symbol) / _safe_component(dataset)
