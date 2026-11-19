"""Durable, segmented write-ahead log for high-frequency stream input.

Only complete, checksummed records are replayed. An incomplete tail blocks
automatic startup instead of silently discarding a possibly received event.
This module deliberately does not know how messages are parsed or projected.
"""

from __future__ import annotations

import fcntl
import json
import os
import struct
import threading
import time
import uuid
import zlib
from pathlib import Path
from typing import Any, Callable, Iterator

_HEADER = struct.Struct("!II")  # payload length, CRC32
_MAX_RECORD_BYTES = 64 * 1024 * 1024


class WALCorruptionError(RuntimeError):
    """The journal cannot be replayed safely without manual inspection."""


def _fsync_directory(path: Path) -> None:
    descriptor = os.open(path, os.O_RDONLY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _write_all(descriptor: int, payload: bytes) -> None:
    offset = 0
    while offset < len(payload):
        written = os.write(descriptor, payload[offset:])
        if written <= 0:
            raise OSError("WAL write returned no bytes")
        offset += written


def read_records(path: Path) -> Iterator[dict[str, Any]]:
    """Validate every record, including the final byte of the segment."""
    with path.open("rb") as source:
        while True:
            header = source.read(_HEADER.size)
            if not header:
                return
            if len(header) != _HEADER.size:
                raise WALCorruptionError(f"Incomplete WAL record header: {path}")
            length, checksum = _HEADER.unpack(header)
            if length < 1 or length > _MAX_RECORD_BYTES:
                raise WALCorruptionError(f"Invalid WAL record length in {path}: {length}")
            payload = source.read(length)
            if len(payload) != length:
                raise WALCorruptionError(f"Incomplete WAL record body: {path}")
            if zlib.crc32(payload) != checksum:
                raise WALCorruptionError(f"WAL checksum mismatch: {path}")
            try:
                record = json.loads(payload)
            except (UnicodeError, json.JSONDecodeError) as error:
                raise WALCorruptionError(f"Invalid WAL JSON: {path}") from error
            if not isinstance(record, dict) or record.get("version") != 1:
                raise WALCorruptionError(f"Unsupported WAL record: {path}")
            yield record


class SegmentedWAL:
    """One-writer-per-root journal, durable on return from append()."""

    def __init__(
        self,
        directory: Path,
        *,
        max_bytes: int,
        segment_bytes: int,
        serializer: Callable[[Any], Any],
    ) -> None:
        if max_bytes < 1 or segment_bytes < 1 or segment_bytes > max_bytes:
            raise ValueError("WAL byte limits must satisfy 0 < segment <= max")
        self.directory = directory
        self.max_bytes = max_bytes
        self.segment_bytes = segment_bytes
        self.serializer = serializer
        self._lock = threading.Lock()
        self._active_descriptor: int | None = None
        self._active_path: Path | None = None
        self._active_bytes = 0
        self._used_bytes = 0
        self._closed = False
        self._lock_descriptor: int | None = None
        self.directory.mkdir(parents=True, exist_ok=True)
        try:
            # The data root itself may have just been created by mkdir(parents).
            # Persist its directory entries before any acknowledged frame.
            current = self.directory
            while True:
                _fsync_directory(current)
                if current.parent == current:
                    break
                current = current.parent
            self._lock_descriptor = os.open(self.directory / "collector.lock", os.O_CREAT | os.O_RDWR, 0o600)
            try:
                fcntl.flock(self._lock_descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError as error:
                raise WALCorruptionError(f"Another collector owns WAL root {self.directory}") from error
            self._recover()
        except Exception:
            self.close()
            raise

    @property
    def used_bytes(self) -> int:
        with self._lock:
            return self._used_bytes

    def _recover(self) -> None:
        temporary = list(self.directory.glob("*.tmp"))
        if temporary:
            raise WALCorruptionError(f"Incomplete WAL temporary file: {temporary[0]}")
        open_segments = sorted(self.directory.glob("segment-*.open"))
        for path in open_segments:
            for _ in read_records(path):
                pass
            sealed = path.with_suffix(".wal")
            if sealed.exists():
                raise WALCorruptionError(f"Both open and sealed WAL segment exist: {path}")
            os.replace(path, sealed)
            _fsync_directory(self.directory)
        for path in sorted(self.directory.glob("segment-*.wal")):
            for _ in read_records(path):
                pass
            self._used_bytes += path.stat().st_size
        if self._used_bytes > self.max_bytes:
            raise WALCorruptionError("Existing WAL exceeds configured disk quota")

    def append(self, record: dict[str, Any]) -> bool:
        body = json.dumps(
            {"version": 1, **record}, default=self.serializer,
            ensure_ascii=False, allow_nan=False, separators=(",", ":"),
        ).encode("utf-8")
        if not body or len(body) > _MAX_RECORD_BYTES:
            raise ValueError("WAL record exceeds maximum supported size")
        payload = _HEADER.pack(len(body), zlib.crc32(body)) + body
        with self._lock:
            if self._closed:
                raise RuntimeError("WAL is closed")
            if self._used_bytes + len(payload) > self.max_bytes:
                raise OSError("WAL disk quota reached")
            if self._active_descriptor is None:
                self._open_new_locked()
            assert self._active_descriptor is not None
            _write_all(self._active_descriptor, payload)
            os.fsync(self._active_descriptor)
            self._active_bytes += len(payload)
            self._used_bytes += len(payload)
            sealed = self._active_bytes >= self.segment_bytes
            if sealed:
                self._seal_locked()
        return sealed

    def _open_new_locked(self) -> None:
        name = f"segment-{time.time_ns():020d}-{uuid.uuid4().hex}.open"
        self._active_path = self.directory / name
        self._active_descriptor = os.open(self._active_path, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
        self._active_bytes = 0
        _fsync_directory(self.directory)

    def _seal_locked(self) -> None:
        if self._active_descriptor is None or self._active_path is None:
            return
        os.fsync(self._active_descriptor)
        os.close(self._active_descriptor)
        self._active_descriptor = None
        path = self._active_path
        self._active_path = None
        if self._active_bytes:
            os.replace(path, path.with_suffix(".wal"))
        else:
            path.unlink()
        _fsync_directory(self.directory)
        self._active_bytes = 0

    def seal(self) -> None:
        with self._lock:
            self._seal_locked()

    def ready_segments(self) -> list[Path]:
        with self._lock:
            return sorted(self.directory.glob("segment-*.wal"))

    def clear_event_if_idle(self, event: threading.Event) -> None:
        """Synchronize a wakeup reset with segment publication."""
        with self._lock:
            if not any(self.directory.glob("segment-*.wal")):
                event.clear()

    def complete(self, path: Path) -> None:
        with self._lock:
            size = path.stat().st_size
            path.unlink()
            _fsync_directory(self.directory)
            self._used_bytes -= size

    def close(self) -> None:
        with self._lock:
            if self._closed:
                return
            self._closed = True
            if self._active_descriptor is not None:
                os.close(self._active_descriptor)
                self._active_descriptor = None
            if self._lock_descriptor is not None:
                fcntl.flock(self._lock_descriptor, fcntl.LOCK_UN)
                os.close(self._lock_descriptor)
                self._lock_descriptor = None
