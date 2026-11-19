"""Buffered high-frequency Parquet storage.

The hot path writes small immutable parts under ``date=.../hour=.../parts``.
The live collectors intentionally do *not* compact them: full-hour pandas
merges caused avoidable memory spikes.  The legacy compactor remains available
for explicitly requested maintenance, but streams Parquet record batches with
PyArrow instead of materialising the complete hour in pandas.
"""

from __future__ import annotations

import logging
import json
import hashlib
import os
import re
import sys
import tempfile
import threading
import time
import uuid
from collections import defaultdict, deque
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Callable, Iterable
from zoneinfo import ZoneInfo

import pandas as pd
import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq

from parquet_store import STORAGE_TIMEZONE, default_data_root
from stream_wal import SegmentedWAL, read_records

KST = ZoneInfo(STORAGE_TIMEZONE)
PARTS_DIRECTORY = "parts"
LATE_PARTS_DIRECTORY = "late_parts"
COMPACTED_FILENAME = "data.parquet"
WAL_PROJECTION_VERSION = 1


def _safe_component(value: str) -> str:
    return re.sub(r"[^A-Za-z0-9._-]+", "_", value)


@dataclass(frozen=True)
class _WriteBatch:
    key: tuple[str, str, str, str]
    rows: list[dict[str, Any]]
    part_id: str
    size_bytes: int = 0


@dataclass(frozen=True)
class _SpoolBatch:
    path: Path
    part_id: str
    row_count: int
    disk_bytes: int


class StorageFatalError(RuntimeError):
    """A storage failure may have created a collection gap; stop producers."""


def _row_memory_bytes(row: dict[str, Any]) -> int:
    """Conservative shallow-plus-values estimate; not a process RSS limit."""
    return sys.getsizeof(row) + sum(
        sys.getsizeof(key) + sys.getsizeof(value) for key, value in row.items()
    )


def _spool_json_default(value: Any) -> Any:
    if isinstance(value, (pd.Timestamp, datetime)):
        return value.isoformat()
    if isinstance(value, np.generic):
        return value.item()
    raise TypeError(f"Unsupported spool value: {type(value).__name__}")


def _fsync_directory(path: Path) -> None:
    descriptor = os.open(path, os.O_RDONLY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


class StreamParquetStore:
    """Thread-safe buffered writer for KST-hourly stream parts.

    Records received after their hour's finalization watermark are placed in
    ``late_parts``.  They are retained for local reconciliation and never cause
    a completed server-side hourly Parquet file to be rewritten. Optional
    asynchronous writes use the flush loop as the sole normal-operation writer;
    callers still drain synchronously during graceful shutdown.
    """

    def __init__(
        self,
        root: str | Path | None = None,
        flush_rows: int = 10_000,
        finalization_grace_seconds: float | None = None,
        async_writes: bool = False,
        max_pending_rows: int = 50_000,
        max_pending_bytes: int = 64 * 1024 * 1024,
        max_spool_bytes: int = 4 * 1024 * 1024 * 1024,
        max_spool_files: int = 20_000,
        wal_first: bool = False,
        max_wal_bytes: int = 4 * 1024 * 1024 * 1024,
        wal_segment_bytes: int = 8 * 1024 * 1024,
    ) -> None:
        if flush_rows < 1:
            raise ValueError("flush_rows must be positive")
        if async_writes and max_pending_rows < flush_rows:
            raise ValueError("max_pending_rows must be at least flush_rows")
        if min(max_pending_bytes, max_spool_bytes, max_spool_files) < 1:
            raise ValueError("memory and disk spool limits must be positive")
        if wal_first and async_writes:
            raise ValueError("WAL-first replaces, rather than combines with, overflow-only async writes")
        if finalization_grace_seconds is not None and finalization_grace_seconds < 0:
            raise ValueError("finalization grace must be non-negative")
        self.root = Path(root) if root is not None else default_data_root()
        self.flush_rows = flush_rows
        self.finalization_grace_seconds = finalization_grace_seconds
        self.async_writes = async_writes
        self.wal_first = wal_first
        self.max_pending_rows = max_pending_rows
        self.max_pending_bytes = max_pending_bytes
        self.max_spool_bytes = max_spool_bytes
        self.max_spool_files = max_spool_files
        self.spool_directory = self.root / ".stream_spool"
        self._buffers: dict[tuple[str, str, str, str], list[dict[str, Any]]] = defaultdict(list)
        self._lock = threading.RLock()
        self._capacity = threading.Condition(self._lock)
        self._write_lock = threading.Lock()
        self._spool_lock = threading.Lock()
        self._pending: deque[_WriteBatch | _SpoolBatch] = deque()
        self._pending_event = threading.Event()
        self._wal_ack_samples_ms: deque[float] = deque(maxlen=20_000)
        self._frame_parsers: dict[
            str, Callable[[str, pd.Timestamp], list[tuple[str, str, str, list[dict[str, Any]]]]]
        ] = {}
        self._wal: SegmentedWAL | None = None
        self._fatal_error: StorageFatalError | None = None
        self._accepting = True
        self._buffered_rows = 0
        self._buffered_bytes = 0
        self._spooled_bytes = 0
        self._spooled_files = 0
        self._metrics: dict[str, int | float] = {
            "appended_rows": 0,
            "persisted_rows": 0,
            "append_calls": 0,
            "row_threshold_flushes": 0,
            "explicit_flush_calls": 0,
            "write_failures": 0,
            "max_buffered_rows": 0,
            "max_buffered_bytes": 0,
            "spooled_rows": 0,
            "spool_failures": 0,
            "wal_frames": 0,
            "wal_row_batches": 0,
            "max_append_lock_wait_seconds": 0.0,
            "total_append_lock_wait_seconds": 0.0,
            "max_flush_lock_wait_seconds": 0.0,
            "max_write_lock_wait_seconds": 0.0,
            "max_write_seconds": 0.0,
            "total_write_seconds": 0.0,
            "backpressure_events": 0,
            "max_backpressure_wait_seconds": 0.0,
        }
        if self.async_writes:
            self._recover_spool()
        if self.wal_first:
            if self.spool_directory.exists() and any(
                path.is_file() for path in self.spool_directory.rglob("*")
            ):
                raise StorageFatalError("Drain or recover old overflow spool before enabling WAL-first mode")
            self._wal = SegmentedWAL(
                self.root / ".stream_wal", max_bytes=max_wal_bytes,
                segment_bytes=wal_segment_bytes, serializer=_spool_json_default,
            )
            try:
                self._verify_wal_projection_identity()
            except Exception:
                self._wal.close()
                raise
            if self._wal.ready_segments():
                self._pending_event.set()

    def _verify_wal_projection_identity(self) -> None:
        """Do not replay partially published segments under different row grouping."""
        assert self._wal is not None
        path = self._wal.directory / "projection.json"
        expected = {
            "version": WAL_PROJECTION_VERSION,
            "flush_rows": self.flush_rows,
            "finalization_grace_seconds": self.finalization_grace_seconds,
            "timezone": STORAGE_TIMEZONE,
        }
        if path.exists():
            try:
                current = json.loads(path.read_text(encoding="utf-8"))
            except (OSError, UnicodeError, json.JSONDecodeError) as error:
                raise StorageFatalError(f"Unreadable WAL projection configuration: {path}") from error
            if current == expected:
                return
            if self._wal.used_bytes:
                raise StorageFatalError(
                    "WAL projection configuration changed with unprocessed segments; "
                    "restart with the original flush_rows/grace/version, then drain before changing it"
                )
        elif self._wal.used_bytes:
            raise StorageFatalError(
                "WAL projection configuration is missing with unprocessed segments; "
                "recover using the original collector version before changing configuration"
            )
        temporary = path.with_name(f"projection-{uuid.uuid4().hex}.tmp")
        try:
            with temporary.open("x", encoding="utf-8") as target:
                json.dump(expected, target, sort_keys=True)
                target.flush()
                os.fsync(target.fileno())
            os.replace(temporary, path)
            _fsync_directory(path.parent)
        finally:
            temporary.unlink(missing_ok=True)

    @property
    def fatal_error(self) -> StorageFatalError | None:
        return self._fatal_error

    def metrics_snapshot(self, reset_interval: bool = False) -> dict[str, int | float]:
        """Return storage pressure; optionally start a fresh reporting interval."""
        with self._lock:
            snapshot = {
                **self._metrics, "buffered_rows": self._buffered_rows,
                "buffered_bytes": self._buffered_bytes,
                "spooled_bytes": self._spooled_bytes,
                "spooled_files": self._spooled_files,
                "wal_bytes": self._wal.used_bytes if self._wal is not None else 0,
            }
            if reset_interval:
                ordered = sorted(self._wal_ack_samples_ms)
                snapshot["wal_ack_samples"] = len(ordered)
                snapshot["wal_ack_p95_ms"] = ordered[int((len(ordered) - 1) * 0.95)] if ordered else 0.0
                snapshot["wal_ack_p99_ms"] = ordered[int((len(ordered) - 1) * 0.99)] if ordered else 0.0
                snapshot["wal_ack_max_ms"] = ordered[-1] if ordered else 0.0
                self._wal_ack_samples_ms.clear()
            if reset_interval:
                self._metrics = {
                    key: 0.0 if isinstance(value, float) else 0
                    for key, value in self._metrics.items()
                }
                self._metrics["max_buffered_rows"] = self._buffered_rows
                self._metrics["max_buffered_bytes"] = self._buffered_bytes
            return snapshot

    def append(
        self,
        venue: str,
        symbol: str,
        dataset: str,
        records: Iterable[dict[str, Any]],
        timestamp_column: str = "event_time",
    ) -> int:
        """Buffer records; row threshold writes directly only in legacy mode."""
        rows = list(records)
        if not rows:
            return 0
        if any(timestamp_column not in row for row in rows):
            raise ValueError(f"Missing timestamp column: {timestamp_column}")
        key = (venue, symbol, dataset, timestamp_column)
        if self.wal_first:
            fixed_rows = [
                row if "received_at" in row else {**row, "received_at": pd.Timestamp.now(tz="UTC")}
                for row in rows
            ]
            self._append_wal({"kind": "rows", "key": list(key), "rows": fixed_rows}, len(rows))
            with self._lock:
                self._metrics["wal_row_batches"] += 1
            return len(rows)
        if self.async_writes:
            self._append_async(key, rows)
            return len(rows)
        wait_started = time.perf_counter()
        with self._capacity:
            if not self._accepting:
                raise StorageFatalError("Append after producers were quiesced for final drain")
            wait_seconds = time.perf_counter() - wait_started
            self._metrics["append_calls"] += 1
            self._metrics["max_append_lock_wait_seconds"] = max(
                self._metrics["max_append_lock_wait_seconds"], wait_seconds
            )
            self._metrics["total_append_lock_wait_seconds"] += wait_seconds
            self._metrics["appended_rows"] += len(rows)
            self._buffers[key].extend(rows)
            self._buffered_rows += len(rows)
            self._metrics["max_buffered_rows"] = max(
                self._metrics["max_buffered_rows"], self._buffered_rows
            )
            if len(self._buffers[key]) >= self.flush_rows:
                self._metrics["row_threshold_flushes"] += 1
                self._flush_key(key)
        return len(rows)

    def register_frame_parser(
        self, source: str,
        parser: Callable[[str, pd.Timestamp], list[tuple[str, str, str, list[dict[str, Any]]]]],
    ) -> None:
        """Register a deterministic pure projector before starting collectors."""
        if source in self._frame_parsers and self._frame_parsers[source] is not parser:
            raise ValueError(f"Duplicate WAL frame source: {source}")
        self._frame_parsers[source] = parser

    def ingest_frame(self, source: str, message: str, received_at: pd.Timestamp | None = None) -> None:
        """Durably record a raw WebSocket frame before market-data parsing."""
        if not self.wal_first or source not in self._frame_parsers:
            raise ValueError(f"WAL-first source is not registered: {source}")
        stamp = received_at if received_at is not None else pd.Timestamp.now(tz="UTC")
        self._append_wal({
            "kind": "frame", "source": source, "message": message,
            "received_at": stamp.isoformat(),
        }, 1)
        with self._lock:
            self._metrics["wal_frames"] += 1

    def report_unjournaled_frame(self, source: str, error: Exception) -> StorageFatalError:
        """Fail closed when a control/data classifier could not journal a frame."""
        self._mark_fatal(("wal", source, "unjournaled", "received_at"), 1, error)
        assert self._fatal_error is not None
        return self._fatal_error

    def _append_wal(self, record: dict[str, Any], affected_rows: int) -> None:
        assert self._wal is not None
        with self._capacity:
            if self._fatal_error is not None:
                raise self._fatal_error
            if not self._accepting:
                raise StorageFatalError("WAL append after producers were quiesced")
        started = time.perf_counter()
        try:
            sealed = self._wal.append(record)
        except Exception as error:
            self._mark_fatal(("wal", str(record.get("source", "rows")), "ingest", "received_at"), affected_rows, error)
            raise self._fatal_error from error
        with self._lock:
            self._wal_ack_samples_ms.append((time.perf_counter() - started) * 1000)
        if sealed:
            self._pending_event.set()

    def _append_async(self, key: tuple[str, str, str, str], rows: list[dict[str, Any]]) -> None:
        """Keep ordinary rows in bounded RAM; durably spool overflow outside the buffer lock."""
        rows = [
            row if "received_at" in row else {**row, "received_at": pd.Timestamp.now(tz="UTC")}
            for row in rows
        ]
        sizes = [_row_memory_bytes(row) for row in rows]
        position = 0
        wait_started = time.perf_counter()
        with self._capacity:
            self._metrics["append_calls"] += 1
            lock_wait = time.perf_counter() - wait_started
            self._metrics["max_append_lock_wait_seconds"] = max(
                self._metrics["max_append_lock_wait_seconds"], lock_wait
            )
            self._metrics["total_append_lock_wait_seconds"] += lock_wait
            if self._fatal_error is not None:
                raise self._fatal_error
            if not self._accepting:
                raise StorageFatalError("Append after producers were quiesced for final drain")
            while position < len(rows):
                size = sizes[position]
                if self._buffered_rows >= self.max_pending_rows or self._buffered_bytes + size > self.max_pending_bytes:
                    self._queue_all_buffers_locked()
                    self._metrics["backpressure_events"] += 1
                    break
                self._buffers[key].append(rows[position])
                self._buffered_rows += 1
                self._buffered_bytes += size
                self._metrics["appended_rows"] += 1
                self._metrics["max_buffered_rows"] = max(self._metrics["max_buffered_rows"], self._buffered_rows)
                self._metrics["max_buffered_bytes"] = max(self._metrics["max_buffered_bytes"], self._buffered_bytes)
                position += 1
                self._queue_full_batches_locked(key)
        if position < len(rows):
            self._spool_rows(key, rows[position:], sizes[position:])

    def _queue_full_batches_locked(self, key: tuple[str, str, str, str]) -> None:
        buffer = self._buffers[key]
        while len(buffer) >= self.flush_rows:
            rows = buffer[:self.flush_rows]
            del buffer[:self.flush_rows]
            self._pending.append(_WriteBatch(key, rows, uuid.uuid4().hex, sum(map(_row_memory_bytes, rows))))
            self._metrics["row_threshold_flushes"] += 1
            self._pending_event.set()

    def _queue_all_buffers_locked(self) -> None:
        for key, buffer in self._buffers.items():
            while buffer:
                rows = buffer[:self.flush_rows]
                del buffer[:self.flush_rows]
                self._pending.append(_WriteBatch(key, rows, uuid.uuid4().hex, sum(map(_row_memory_bytes, rows))))
                self._pending_event.set()

    def _recover_spool(self) -> None:
        if not self.spool_directory.exists():
            return
        gap_markers = list((self.spool_directory / "gaps").glob("*.json"))
        if gap_markers:
            raise StorageFatalError(
                f"Recorded collection gap(s) require investigation before restart: {gap_markers[0]}"
            )
        temporary_files = list(self.spool_directory.glob("*.tmp"))
        if temporary_files:
            raise StorageFatalError(
                f"Incomplete spool temp file requires recovery review before restart: {temporary_files[0]}"
            )
        for path in sorted(self.spool_directory.glob("*.json")):
            try:
                batch = self._read_spool_metadata(path)
            except Exception as error:
                raise StorageFatalError(f"Invalid recoverable spool file {path}: {error}") from error
            self._pending.append(batch)
            self._spooled_bytes += batch.disk_bytes
            self._spooled_files += 1
        if self._pending:
            logging.warning("Recovering %d disk-spooled batches before new writes", len(self._pending))
            self._pending_event.set()
        if self._spooled_bytes > self.max_spool_bytes or self._spooled_files > self.max_spool_files:
            raise StorageFatalError("Existing disk spool exceeds configured quota; preserve files and enlarge quota")

    def _read_spool_metadata(self, path: Path) -> _SpoolBatch:
        with path.open("r", encoding="utf-8") as source:
            header = json.loads(source.readline())
        if header.get("version") != 1 or header.get("part_id") != path.stem:
            raise ValueError("Spool version or part ID mismatch")
        if not isinstance(header.get("key"), list) or len(header["key"]) != 4:
            raise ValueError("Invalid spool key")
        if not isinstance(header.get("row_count"), int) or header["row_count"] < 1:
            raise ValueError("Invalid spool row count")
        return _SpoolBatch(path, path.stem, header["row_count"], path.stat().st_size)

    def _spool_rows(
        self, key: tuple[str, str, str, str], rows: list[dict[str, Any]], sizes: list[int]
    ) -> None:
        position = 0
        while position < len(rows):
            end = position
            total = 0
            while end < len(rows) and end - position < self.flush_rows:
                size = sizes[end]
                if end > position and total + size > self.max_pending_bytes:
                    break
                total += size
                end += 1
            chunk = rows[position:end]
            part_id = uuid.uuid4().hex
            # A missing receive time must be fixed before journal publication;
            # otherwise replay could change the late/ordinary partition.
            fixed_rows = [
                row if "received_at" in row else {**row, "received_at": pd.Timestamp.now(tz="UTC")}
                for row in chunk
            ]
            header = {"version": 1, "key": list(key), "part_id": part_id, "row_count": len(chunk)}
            try:
                payload = (json.dumps(header, separators=(",", ":")) + "\n" + "\n".join(
                    json.dumps(row, default=_spool_json_default, allow_nan=False, separators=(",", ":"))
                    for row in fixed_rows
                ) + "\n").encode("utf-8")
                with self._spool_lock:
                    if self._fatal_error is not None:
                        raise self._fatal_error
                    if self._spooled_bytes + len(payload) > self.max_spool_bytes or self._spooled_files + 1 > self.max_spool_files:
                        raise OSError("Disk spool quota reached")
                    self.spool_directory.mkdir(parents=True, exist_ok=True)
                    temporary = self.spool_directory / f"{part_id}.tmp"
                    output = self.spool_directory / f"{part_id}.json"
                    try:
                        with temporary.open("xb") as destination:
                            destination.write(payload)
                            destination.flush()
                            os.fsync(destination.fileno())
                        os.replace(temporary, output)
                        _fsync_directory(self.spool_directory)
                    finally:
                        temporary.unlink(missing_ok=True)
                    with self._capacity:
                        self._spooled_bytes += len(payload)
                        self._spooled_files += 1
                        self._metrics["appended_rows"] += len(chunk)
                        self._metrics["spooled_rows"] += len(chunk)
                        self._pending.append(_SpoolBatch(output, part_id, len(chunk), len(payload)))
                        self._pending_event.set()
            except Exception as error:
                self._mark_fatal(key, len(rows) - position, error)
                raise self._fatal_error from error
            position = end

    def _mark_fatal(self, key: tuple[str, str, str, str], lost_rows: int, error: Exception) -> None:
        first_failure = False
        with self._capacity:
            if self._fatal_error is None:
                self._fatal_error = StorageFatalError(
                    f"Storage gap possible for {key}: {lost_rows} unaccepted rows; {error}"
                )
                self._metrics["spool_failures"] += 1
                logging.critical("COLLECTION GAP / STORAGE FATAL: %s", self._fatal_error)
                first_failure = True
            self._pending_event.set()
        if first_failure:
            try:
                gap_directory = self.spool_directory / "gaps"
                gap_directory.mkdir(parents=True, exist_ok=True)
                marker = gap_directory / f"gap-{uuid.uuid4().hex}.json"
                with marker.open("xb") as output:
                    output.write(json.dumps({
                        "kst": datetime.now(KST).isoformat(), "key": list(key),
                        "unaccepted_rows": lost_rows, "error": str(error),
                    }).encode("utf-8"))
                    output.flush()
                    os.fsync(output.fileno())
                _fsync_directory(gap_directory)
            except Exception:
                logging.exception("Could not persist collection gap marker; retain the critical log")

    def _halt_recoverable(self, error: Exception) -> None:
        """Stop live ingestion but retain a complete WAL for retry after restart."""
        with self._capacity:
            if self._fatal_error is None:
                self._fatal_error = StorageFatalError(f"WAL projection halted; journal retained: {error}")
                logging.critical("WAL projection halted without deleting source data: %s", error)
            self._pending_event.set()

    def _load_spool(self, batch: _SpoolBatch) -> tuple[tuple[str, str, str, str], list[dict[str, Any]]]:
        with batch.path.open("r", encoding="utf-8") as source:
            header = json.loads(source.readline())
            if header.get("part_id") != batch.part_id:
                raise ValueError("Spool batch changed while reading")
            rows = [json.loads(line) for line in source]
        if len(rows) != batch.row_count:
            raise ValueError(f"Spool row-count mismatch: {batch.path}")
        return tuple(header["key"]), rows

    def flush(self) -> int:
        """Flush every buffered dataset and return the number of input rows."""
        if self.wal_first:
            return self._drain_wal(seal_active=True)
        if self.async_writes:
            return self._drain_pending(include_active=True, drain_all=True)
        wait_started = time.perf_counter()
        with self._lock:
            self._metrics["max_flush_lock_wait_seconds"] = max(
                self._metrics["max_flush_lock_wait_seconds"], time.perf_counter() - wait_started
            )
            self._metrics["explicit_flush_calls"] += 1
            return sum(self._flush_key(key) for key in list(self._buffers))

    def flush_pending(self) -> int:
        """Write only row-threshold batches from the dedicated writer thread."""
        if self.wal_first:
            return self._drain_wal(seal_active=False)
        if not self.async_writes:
            return 0
        return self._drain_pending(include_active=False, drain_all=False)

    def flush_current(self) -> int:
        """Write one bounded snapshot of active and pending rows."""
        if self.wal_first:
            return self._drain_wal(seal_active=True)
        if not self.async_writes:
            return self.flush()
        return self._drain_pending(include_active=True, drain_all=False)

    def wait_for_pending(self, timeout: float) -> bool:
        return self._pending_event.wait(timeout)

    def wake_writer(self) -> None:
        self._pending_event.set()

    def close_for_appends(self) -> None:
        """Call only after every producer has stopped, before the final drain."""
        with self._capacity:
            self._accepting = False

    def close_wal(self) -> None:
        if self._wal is not None:
            self._wal.close()

    def _drain_wal(self, *, seal_active: bool) -> int:
        assert self._wal is not None
        with self._write_lock:
            if seal_active:
                self._wal.seal()
            segments = self._wal.ready_segments()
            written = 0
            for segment in segments:
                written += self._project_wal_segment(segment)
                self._wal.complete(segment)
            self._wal.clear_event_if_idle(self._pending_event)
            return written

    def _project_wal_segment(self, segment: Path) -> int:
        """Rebuild fixed batches from one segment; replay overwrites the same parts."""
        buffers: dict[tuple[str, str, str, str], list[dict[str, Any]]] = defaultdict(list)
        batch_numbers: dict[tuple[str, str, str, str], int] = defaultdict(int)
        written = 0

        def publish(key: tuple[str, str, str, str], rows: list[dict[str, Any]]) -> None:
            nonlocal written
            number = batch_numbers[key]
            identity = json.dumps([segment.stem, *key, number], separators=(",", ":"))
            part_id = hashlib.sha256(identity.encode("utf-8")).hexdigest()[:32]
            self._write_rows(key, rows, part_id, durable=True)
            batch_numbers[key] += 1
            written += len(rows)

        for record in read_records(segment):
            kind = record.get("kind")
            if kind == "rows":
                key = record.get("key")
                rows = record.get("rows")
                if not isinstance(key, list) or len(key) != 4 or not isinstance(rows, list):
                    raise ValueError(f"Malformed row batch in WAL segment {segment}")
                projected = [(key[0], key[1], key[2], rows, key[3])]
            elif kind == "frame":
                source = record.get("source")
                parser = self._frame_parsers.get(source)
                if parser is None:
                    raise ValueError(f"No projector registered for WAL source {source!r}")
                received_at = pd.Timestamp(record["received_at"])
                projected = [(*entry, "event_time") for entry in parser(record["message"], received_at)]
            else:
                raise ValueError(f"Unknown WAL input kind {kind!r}")
            for venue, symbol, dataset, rows, timestamp_column in projected:
                key = (venue, symbol, dataset, timestamp_column)
                for row in rows:
                    buffers[key].append(row)
                    if len(buffers[key]) >= self.flush_rows:
                        publish(key, buffers[key])
                        buffers[key] = []
        for key, rows in buffers.items():
            if rows:
                publish(key, rows)
        return written

    def _drain_pending(self, *, include_active: bool, drain_all: bool) -> int:
        wait_started = time.perf_counter()
        with self._write_lock:
            write_lock_wait = time.perf_counter() - wait_started
            wait_started = time.perf_counter()
            with self._capacity:
                self._metrics["max_write_lock_wait_seconds"] = max(
                    self._metrics["max_write_lock_wait_seconds"], write_lock_wait
                )
                self._metrics["max_flush_lock_wait_seconds"] = max(
                    self._metrics["max_flush_lock_wait_seconds"], time.perf_counter() - wait_started
                )
                if include_active:
                    self._metrics["explicit_flush_calls"] += 1
                    self._queue_all_buffers_locked()
                batch_limit = None if drain_all else len(self._pending)
            written = 0
            completed_batches = 0
            while True:
                with self._capacity:
                    if not self._pending or (batch_limit is not None and completed_batches >= batch_limit):
                        if not self._pending:
                            self._pending_event.clear()
                        return written
                    batch = self._pending[0]
                if isinstance(batch, _SpoolBatch):
                    try:
                        key, rows = self._load_spool(batch)
                    except Exception as error:
                        self._mark_fatal(("spool", batch.part_id, "replay", "event_time"), batch.row_count, error)
                        raise self._fatal_error from error
                    self._write_rows(key, rows, batch.part_id, durable=True)
                    try:
                        with self._spool_lock:
                            batch.path.unlink()
                            _fsync_directory(self.spool_directory)
                            with self._capacity:
                                self._spooled_bytes -= batch.disk_bytes
                                self._spooled_files -= 1
                    except Exception as error:
                        self._mark_fatal(key, batch.row_count, error)
                        raise self._fatal_error from error
                else:
                    self._write_rows(batch.key, batch.rows, batch.part_id)
                with self._capacity:
                    self._pending.popleft()
                    if isinstance(batch, _SpoolBatch):
                        written += batch.row_count
                    else:
                        self._buffered_rows -= len(batch.rows)
                        self._buffered_bytes -= batch.size_bytes
                        written += len(batch.rows)
                    completed_batches += 1
                    self._capacity.notify_all()

    def _flush_key(self, key: tuple[str, str, str, str]) -> int:
        rows = self._buffers.get(key, [])
        if not rows:
            return 0
        self._buffers[key] = []
        self._buffered_rows -= len(rows)
        return self._write_rows(key, rows)

    def _write_rows(
        self, key: tuple[str, str, str, str], rows: list[dict[str, Any]],
        part_id: str | None = None,
        durable: bool = False,
    ) -> int:
        write_started = time.perf_counter()
        try:
            venue, symbol, dataset, timestamp_column = key
            frame = pd.DataFrame(rows)
            frame[timestamp_column] = pd.to_datetime(frame[timestamp_column], utc=True)
            if frame[timestamp_column].isna().any():
                raise ValueError(f"Invalid timestamp in {timestamp_column}")
            frame[timestamp_column] = frame[timestamp_column].dt.tz_convert(STORAGE_TIMEZONE)
            if "received_at" not in frame:
                frame["received_at"] = pd.Timestamp.now(tz="UTC")
            frame["received_at"] = pd.to_datetime(frame["received_at"], utc=True).dt.tz_convert(STORAGE_TIMEZONE)
            frame["_partition_date"] = frame[timestamp_column].dt.strftime("%Y-%m-%d")
            frame["_partition_hour"] = frame[timestamp_column].dt.strftime("%H")
            frame["_late"] = self._is_late(frame, timestamp_column)

            for (date, hour, is_late), partition in frame.groupby(
                ["_partition_date", "_partition_hour", "_late"], sort=True
            ):
                parts_directory = LATE_PARTS_DIRECTORY if is_late else PARTS_DIRECTORY
                directory = (
                    self.root / _safe_component(venue) / _safe_component(symbol)
                    / _safe_component(dataset) / f"date={date}" / f"hour={hour}" / parts_directory
                )
                output = directory / f"part-{part_id or uuid.uuid4().hex}.parquet"
                _atomic_parquet_write(
                    partition.drop(columns=["_partition_date", "_partition_hour", "_late"]), output
                )
                if durable:
                    with output.open("rb") as published:
                        os.fsync(published.fileno())
                    current = directory
                    while True:
                        _fsync_directory(current)
                        if current == self.root:
                            break
                        current = current.parent
        except Exception:
            with self._lock:
                self._metrics["write_failures"] += 1
            raise
        finally:
            write_seconds = time.perf_counter() - write_started
            with self._lock:
                self._metrics["max_write_seconds"] = max(self._metrics["max_write_seconds"], write_seconds)
                self._metrics["total_write_seconds"] += write_seconds
        with self._lock:
            self._metrics["persisted_rows"] += len(rows)
        return len(rows)

    def _is_late(self, frame: pd.DataFrame, timestamp_column: str) -> pd.Series:
        """Classify rows using receive time, not the delayed flush time."""
        if self.finalization_grace_seconds is None:
            return pd.Series(False, index=frame.index)
        hour_end = frame[timestamp_column].dt.floor("h") + pd.Timedelta(hours=1)
        watermark = hour_end + pd.Timedelta(seconds=self.finalization_grace_seconds)
        return frame["received_at"] >= watermark


def _atomic_parquet_write(frame: pd.DataFrame, output: Path) -> None:
    """Serialize a Parquet file before atomically publishing it to readers."""
    output.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(
        mode="wb", suffix=".parquet", dir=output.parent, delete=False
    ) as temporary:
        temporary_path = Path(temporary.name)
    try:
        frame.to_parquet(temporary_path, engine="pyarrow", index=False, compression="zstd")
        os.replace(temporary_path, output)
    finally:
        temporary_path.unlink(missing_ok=True)


class StreamFlushLoop:
    """Periodically flush a :class:`StreamParquetStore` until stopped."""

    def __init__(
        self, store: StreamParquetStore, interval_seconds: float,
        metrics_interval_seconds: float = 60.0,
    ) -> None:
        if interval_seconds <= 0:
            raise ValueError("flush interval must be positive")
        if metrics_interval_seconds <= 0:
            raise ValueError("metrics interval must be positive")
        self.store = store
        self.interval_seconds = interval_seconds
        self.metrics_interval_seconds = metrics_interval_seconds
        self.stop_event = threading.Event()

    def run(self) -> None:
        next_metrics_at = time.monotonic() + self.metrics_interval_seconds
        next_periodic_at = time.monotonic() + self.interval_seconds
        consecutive_write_failures = 0
        while not self.stop_event.is_set():
            if self.store.async_writes or self.store.wal_first:
                pending = self.store.wait_for_pending(max(0.0, next_periodic_at - time.monotonic()))
                if self.stop_event.is_set():
                    break
                try:
                    if time.monotonic() >= next_periodic_at:
                        self.store.flush_current()
                        next_periodic_at = time.monotonic() + self.interval_seconds
                    elif pending:
                        self.store.flush_pending()
                    consecutive_write_failures = 0
                except Exception:
                    logging.exception("Stream Parquet writer failed; source data retained for retry")
                    consecutive_write_failures += 1
                    if consecutive_write_failures >= 3:
                        if self.store.wal_first:
                            self.store._halt_recoverable(RuntimeError("WAL projector failed three consecutive times"))
                        else:
                            self.store._mark_fatal(
                                ("parquet_writer", "pending", "all", "event_time"),
                                self.store.metrics_snapshot()["buffered_rows"],
                                RuntimeError("Parquet writer failed three consecutive times"),
                            )
                        self.stop_event.set()
                        break
                    self.stop_event.wait(min(1.0, self.interval_seconds))
            else:
                if self.stop_event.wait(self.interval_seconds):
                    break
                self.store.flush()
            if time.monotonic() >= next_metrics_at:
                metrics = self.store.metrics_snapshot(reset_interval=True)
                logging.info(
                    "Stream store: kst=%s appended=%d persisted=%d buffered=%d peak_buffered=%d "
                    "buffered_bytes=%d peak_buffered_bytes=%d spool_files=%d spool_bytes=%d spooled_rows=%d "
                    "wal_bytes=%d wal_frames=%d wal_row_batches=%d "
                    "wal_ack_samples=%d wal_ack_p95_ms=%.1f wal_ack_p99_ms=%.1f wal_ack_max_ms=%.1f "
                    "append_calls=%d row_flushes=%d flush_calls=%d write_failures=%d backpressure=%d spool_failures=%d "
                    "max_append_lock_wait_ms=%.1f total_append_lock_wait_ms=%.1f "
                    "max_flush_lock_wait_ms=%.1f max_write_lock_wait_ms=%.1f "
                    "max_backpressure_wait_ms=%.1f max_write_ms=%.1f total_write_ms=%.1f",
                    datetime.now(KST).isoformat(timespec="seconds"),
                    metrics["appended_rows"], metrics["persisted_rows"], metrics["buffered_rows"],
                    metrics["max_buffered_rows"], metrics["buffered_bytes"], metrics["max_buffered_bytes"],
                    metrics["spooled_files"], metrics["spooled_bytes"], metrics["spooled_rows"],
                    metrics["wal_bytes"], metrics["wal_frames"], metrics["wal_row_batches"],
                    metrics["wal_ack_samples"], metrics["wal_ack_p95_ms"],
                    metrics["wal_ack_p99_ms"], metrics["wal_ack_max_ms"], metrics["append_calls"],
                    metrics["row_threshold_flushes"], metrics["explicit_flush_calls"],
                    metrics["write_failures"], metrics["backpressure_events"], metrics["spool_failures"],
                    metrics["max_append_lock_wait_seconds"] * 1000,
                    metrics["total_append_lock_wait_seconds"] * 1000,
                    metrics["max_flush_lock_wait_seconds"] * 1000,
                    metrics["max_write_lock_wait_seconds"] * 1000,
                    metrics["max_backpressure_wait_seconds"] * 1000,
                    metrics["max_write_seconds"] * 1000,
                    metrics["total_write_seconds"] * 1000,
                )
                next_metrics_at = time.monotonic() + self.metrics_interval_seconds

    def stop(self) -> None:
        self.stop_event.set()
        if self.store.async_writes or self.store.wal_first:
            self.store.wake_writer()
        try:
            self.store.flush()
        except Exception as error:
            if self.store.wal_first:
                self.store._halt_recoverable(error)
            elif self.store.async_writes:
                self.store._mark_fatal(
                    ("parquet_writer", "shutdown", "all", "event_time"),
                    self.store.metrics_snapshot()["buffered_rows"], error,
                )
            raise


def wait_for_producers(workers: Iterable[threading.Thread], interval_seconds: float = 10.0) -> None:
    """Never final-drain while an active producer can still append."""
    remaining = list(workers)
    while remaining:
        for worker in remaining:
            worker.join(timeout=interval_seconds)
        remaining = [worker for worker in remaining if worker.is_alive()]
        if remaining:
            logging.critical(
                "Waiting for producers before final Parquet drain: %s",
                ", ".join(worker.name for worker in remaining),
            )


class StreamParquetCompactor:
    """Merge immutable parts into one recoverable Parquet file per KST hour.

    Once an hour is finalized its target is never reopened on the server.  Any
    staging parts discovered after a target exists are preserved as late parts,
    so local reconciliation can include them without rewriting a large file.
    """

    def __init__(
        self,
        root: str | Path,
        interval_seconds: float = 300.0,
        finalization_grace_seconds: float = 300.0,
    ) -> None:
        if interval_seconds <= 0:
            raise ValueError("compaction interval must be positive")
        if finalization_grace_seconds < 0:
            raise ValueError("finalization grace must be non-negative")
        self.root = Path(root)
        self.interval_seconds = interval_seconds
        self.finalization_grace_seconds = finalization_grace_seconds
        self.stop_event = threading.Event()

    def run(self) -> None:
        while not self.stop_event.wait(self.interval_seconds):
            try:
                compacted = self.compact_closed_hours()
                if compacted:
                    logging.info("Compacted %d high-frequency Parquet parts", compacted)
            except Exception:
                logging.exception("Hourly Parquet compaction failed")

    def stop(self) -> None:
        self.stop_event.set()

    def compact_closed_hours(self, now: datetime | pd.Timestamp | None = None) -> int:
        """Compact KST hours whose end plus the grace period has elapsed."""
        now_kst = pd.Timestamp(now if now is not None else datetime.now(KST))
        now_kst = now_kst.tz_localize(KST) if now_kst.tzinfo is None else now_kst.tz_convert(KST)
        compacted = 0
        for hour_directory, hour_start in self._hour_directories():
            finalization_time = hour_start + pd.Timedelta(hours=1, seconds=self.finalization_grace_seconds)
            if finalization_time <= now_kst:
                compacted += self.compact_hour(hour_directory)
        return compacted

    def compact_all(self) -> int:
        """Compact all ordinary parts, including the current hour during shutdown."""
        return sum(self.compact_hour(hour_directory) for hour_directory, _ in self._hour_directories())

    def _hour_directories(self) -> Iterable[tuple[Path, pd.Timestamp]]:
        if not self.root.exists():
            return
        for directory in self.root.glob("*/*/*/date=*/hour=*"):
            date = directory.parent.name.removeprefix("date=")
            hour = directory.name.removeprefix("hour=")
            if not re.fullmatch(r"\d{4}-\d{2}-\d{2}", date) or not re.fullmatch(r"\d{2}", hour):
                continue
            try:
                yield directory, pd.Timestamp(f"{date}T{hour}:00:00", tz=KST)
            except ValueError:
                continue

    def compact_hour(self, hour_directory: Path) -> int:
        """Compact ordinary staging parts once, without reopening a target file.

        This compatibility helper is deliberately not started by the live
        collectors.  It is bounded by one Arrow record batch rather than the
        total number of rows in an hour.
        """
        parts_directory = hour_directory / PARTS_DIRECTORY
        parts = sorted(parts_directory.glob("part-*.parquet")) if parts_directory.exists() else []
        if not parts:
            return 0
        target = hour_directory / COMPACTED_FILENAME
        if target.exists():
            late_directory = hour_directory / LATE_PARTS_DIRECTORY
            late_directory.mkdir(parents=True, exist_ok=True)
            for part in parts:
                os.replace(part, late_directory / part.name)
            return len(parts)
        merge_parquet_files_to_path(parts, target)
        for part in parts:
            part.unlink(missing_ok=True)
        return len(parts)


def parquet_row_count(files: Iterable[Path]) -> int:
    """Return Parquet row counts from metadata without reading record values."""
    return sum(pq.ParquetFile(path).metadata.num_rows for path in files)


def merge_parquet_files_to_path(
    files: Iterable[Path],
    output: Path,
    *,
    batch_size: int = 65_536,
) -> int:
    """Append compatible Parquet files into ``output`` with bounded memory.

    The function preserves every source row and source-file order.  Global
    sorting and deduplication are intentionally excluded because both require
    materialising (or externally sorting) the full partition.  Callers that
    require those analytical transformations should run them separately after
    raw-data verification.
    """
    source_files = sorted(files, key=_parquet_source_order_key)
    if not source_files:
        raise ValueError("At least one Parquet file is required for merging")
    if batch_size < 1:
        raise ValueError("batch_size must be positive")

    schemas = [pq.ParquetFile(path).schema_arrow for path in source_files]
    try:
        schema = pa.unify_schemas(schemas, promote_options="permissive")
    except TypeError:  # Compatibility with older PyArrow installations.
        schema = pa.unify_schemas(schemas)

    output.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(
        mode="wb", suffix=".parquet", dir=output.parent, delete=False
    ) as temporary:
        temporary_path = Path(temporary.name)
    written_rows = 0
    writer: pq.ParquetWriter | None = None
    try:
        writer = pq.ParquetWriter(temporary_path, schema, compression="zstd")
        for source in source_files:
            parquet_file = pq.ParquetFile(source)
            for batch in parquet_file.iter_batches(batch_size=batch_size):
                table = _align_record_batch(batch, schema)
                writer.write_table(table, row_group_size=batch_size)
                written_rows += table.num_rows
        writer.close()
        writer = None
        os.replace(temporary_path, output)
        return written_rows
    finally:
        if writer is not None:
            writer.close()
        temporary_path.unlink(missing_ok=True)


def _align_record_batch(batch: pa.RecordBatch, schema: pa.Schema) -> pa.Table:
    """Add missing nullable columns and cast an Arrow batch to ``schema``."""
    source = pa.Table.from_batches([batch])
    arrays = []
    for field in schema:
        if field.name not in source.column_names:
            arrays.append(pa.nulls(source.num_rows, type=field.type))
            continue
        column = source.column(field.name)
        arrays.append(column if column.type == field.type else column.cast(field.type, safe=False))
    return pa.Table.from_arrays(arrays, schema=schema)


def _parquet_source_order_key(path: Path) -> tuple[str, str]:
    """Use first event time from metadata when available, without reading rows."""
    parquet_file = pq.ParquetFile(path)
    try:
        index = parquet_file.schema_arrow.get_field_index("event_time")
        if index >= 0:
            statistics = parquet_file.metadata.row_group(0).column(index).statistics
            if statistics is not None and statistics.has_min_max:
                return str(statistics.min), str(path)
    except (IndexError, OSError):
        pass
    return "", str(path)
