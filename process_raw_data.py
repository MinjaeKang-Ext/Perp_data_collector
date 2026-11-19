"""Turn immutable raw stream parts into verified KST 10-minute Parquet data.

This program is deliberately separate from the WebSocket collectors.  It uses
PyArrow record batches, so neither an hour nor a 10-minute partition is loaded
into pandas.  Ordinary raw parts become one ``data.parquet`` per 10-minute
bucket.  Late raw parts are preserved as immutable processed ``late_parts``
files rather than reopening a completed output.
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import re
import shutil
import tempfile
import time
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Iterable
from zoneinfo import ZoneInfo

import pyarrow as pa
import pyarrow.parquet as pq

from parquet_store import STORAGE_TIMEZONE, default_data_root
from stream_parquet_store import LATE_PARTS_DIRECTORY, PARTS_DIRECTORY, parquet_row_count

KST = ZoneInfo(STORAGE_TIMEZONE)
COMPACTED_FILENAME = "data.parquet"
MANIFEST_DIRECTORY = "_manifests"


@dataclass(frozen=True)
class ProcessingResult:
    normal_partitions: int = 0
    late_parts: int = 0
    rows: int = 0
    deleted_raw_hours: int = 0


class RawParquetProcessor:
    """Create append-safe 10-minute data from raw parts with bounded memory."""

    def __init__(
        self,
        raw_root: str | Path,
        processed_root: str | Path,
        *,
        processing_delay_seconds: float = 330.0,
        batch_size: int = 65_536,
        raw_retention_days: float | None = None,
        delete_processed_raw: bool = False,
    ) -> None:
        if processing_delay_seconds <= 0:
            raise ValueError("processing_delay_seconds must be positive")
        if batch_size < 1:
            raise ValueError("batch_size must be positive")
        if raw_retention_days is not None and raw_retention_days < 0:
            raise ValueError("raw_retention_days must be non-negative")
        self.raw_root = Path(raw_root).expanduser().resolve()
        self.processed_root = Path(processed_root).expanduser().resolve()
        if self.raw_root == self.processed_root:
            raise ValueError("raw_root and processed_root must differ")
        self.processing_delay_seconds = processing_delay_seconds
        self.batch_size = batch_size
        self.raw_retention_days = raw_retention_days
        self.delete_processed_raw = delete_processed_raw

    def process_available(self, now: datetime | None = None) -> ProcessingResult:
        """Process eligible ordinary hours and every unprocessed late part."""
        now_kst = _as_kst(now or datetime.now(KST))
        normal_partitions = late_parts = rows = 0
        for hour_directory, hour_start in self._hour_directories():
            ordinary_parts = _part_files(hour_directory / PARTS_DIRECTORY)
            if ordinary_parts and self._eligible(hour_start, now_kst):
                result = self._process_normal_hour(hour_directory, ordinary_parts)
                normal_partitions += result[0]
                rows += result[1]
            for late_part in _part_files(hour_directory / LATE_PARTS_DIRECTORY):
                if self._late_manifest(late_part).exists():
                    continue
                output_count, output_rows = self._process_late_part(late_part)
                late_parts += output_count
                rows += output_rows
        deleted = self.prune_verified_raw(now_kst) if self.delete_processed_raw else 0
        return ProcessingResult(normal_partitions, late_parts, rows, deleted)

    def prune_verified_raw(self, now: datetime | None = None) -> int:
        """Delete only aged raw hours whose every input has a verified manifest."""
        if self.raw_retention_days is None:
            return 0
        now_kst = _as_kst(now or datetime.now(KST))
        deleted = 0
        for hour_directory, hour_start in self._hour_directories():
            if hour_start + timedelta(days=self.raw_retention_days + 1 / 24) > now_kst:
                continue
            ordinary_parts = _part_files(hour_directory / PARTS_DIRECTORY)
            late_parts = _part_files(hour_directory / LATE_PARTS_DIRECTORY)
            if ordinary_parts and not self._normal_manifest(hour_directory).exists():
                continue
            recorded = self._normal_manifest_sources(hour_directory)
            if any(
                str(part.relative_to(self.raw_root)) not in recorded
                and not self._late_manifest(part).exists()
                for part in ordinary_parts
            ):
                continue
            if any(not self._late_manifest(part).exists() for part in late_parts):
                continue
            # The resolved target is always an individual date/hour directory
            # yielded by _hour_directories, never the configured root.
            shutil.rmtree(hour_directory)
            deleted += 1
        return deleted

    def _process_normal_hour(self, hour_directory: Path, files: list[Path]) -> tuple[int, int]:
        manifest = self._normal_manifest(hour_directory)
        if manifest.exists():
            # A part created in the tiny flush/process race window must never
            # disappear silently. Preserve it as a processed late part rather
            # than rewriting an already verified ten-minute output.
            recorded = self._normal_manifest_sources(hour_directory)
            partitions = rows = 0
            for source in files:
                if str(source.relative_to(self.raw_root)) in recorded or self._late_manifest(source).exists():
                    continue
                logging.warning("Processing post-finalization ordinary part as late data: %s", source)
                output_count, output_rows = self._process_late_part(source)
                partitions += output_count
                rows += output_rows
            return partitions, rows
        outputs, source_rows, written_rows = self._write_partitioned(files, late=False)
        if source_rows != written_rows:
            raise RuntimeError(
                f"Row-count verification failed for {hour_directory}: "
                f"source={source_rows}, output={written_rows}"
            )
        _atomic_json_write(manifest, {
            "kind": "normal_hour",
            "source": str(hour_directory.relative_to(self.raw_root)),
            "source_files": [str(path.relative_to(self.raw_root)) for path in files],
            "source_rows": source_rows,
            "output_rows": written_rows,
            "outputs": [str(path.relative_to(self.processed_root)) for path in outputs],
            "created_at": datetime.now(KST).isoformat(),
        })
        return len(outputs), written_rows

    def _process_late_part(self, source: Path) -> tuple[int, int]:
        manifest = self._late_manifest(source)
        outputs, source_rows, written_rows = self._write_partitioned([source], late=True)
        if source_rows != written_rows:
            raise RuntimeError(
                f"Row-count verification failed for late part {source}: "
                f"source={source_rows}, output={written_rows}"
            )
        _atomic_json_write(manifest, {
            "kind": "late_part",
            "source": str(source.relative_to(self.raw_root)),
            "source_rows": source_rows,
            "output_rows": written_rows,
            "outputs": [str(path.relative_to(self.processed_root)) for path in outputs],
            "created_at": datetime.now(KST).isoformat(),
        })
        return len(outputs), written_rows

    def _write_partitioned(self, files: list[Path], *, late: bool) -> tuple[list[Path], int, int]:
        schema = _unified_schema(files)
        source_relative = files[0].relative_to(self.raw_root)
        # venue / symbol / dataset / date=... / hour=... / parts-or-late_parts / file
        partition_prefix = Path(*source_relative.parts[:5])
        writer = _TenMinuteWriter(
            self.processed_root,
            partition_prefix,
            schema,
            late=late,
            batch_size=self.batch_size,
        )
        try:
            for source in files:
                for batch in pq.ParquetFile(source).iter_batches(batch_size=self.batch_size):
                    writer.write(_align_batch(batch, schema), source.stem)
            outputs, written_rows = writer.publish()
        except Exception:
            writer.abort()
            raise
        return outputs, parquet_row_count(files), written_rows

    def _hour_directories(self) -> Iterable[tuple[Path, datetime]]:
        if not self.raw_root.is_dir():
            return
        for directory in sorted(self.raw_root.glob("*/*/*/date=*/hour=*")):
            date = directory.parent.name.removeprefix("date=")
            hour = directory.name.removeprefix("hour=")
            if not re.fullmatch(r"\d{4}-\d{2}-\d{2}", date) or not re.fullmatch(r"\d{2}", hour):
                continue
            try:
                yield directory, datetime.fromisoformat(f"{date}T{hour}:00:00").replace(tzinfo=KST)
            except ValueError:
                continue

    def _eligible(self, hour_start: datetime, now: datetime) -> bool:
        return hour_start + timedelta(hours=1, seconds=self.processing_delay_seconds) <= now

    def _normal_manifest(self, hour_directory: Path) -> Path:
        return self.processed_root / MANIFEST_DIRECTORY / hour_directory.relative_to(self.raw_root) / "normal.json"

    def _late_manifest(self, source: Path) -> Path:
        return self.processed_root / MANIFEST_DIRECTORY / source.relative_to(self.raw_root).with_suffix(".json")

    def _normal_manifest_sources(self, hour_directory: Path) -> set[str]:
        manifest = self._normal_manifest(hour_directory)
        if not manifest.exists():
            return set()
        return set(json.loads(manifest.read_text(encoding="utf-8"))["source_files"])


class _TenMinuteWriter:
    """Own temporary Arrow writers until every target can be atomically published."""

    def __init__(
        self,
        root: Path,
        partition_prefix: Path,
        schema: pa.Schema,
        *,
        late: bool,
        batch_size: int,
    ) -> None:
        self.root = root
        self.partition_prefix = partition_prefix
        self.schema = schema
        self.late = late
        self.batch_size = batch_size
        self._writers: dict[Path, tuple[Path, pq.ParquetWriter]] = {}
        self.rows = 0

    def write(self, table: pa.Table, source_stem: str) -> None:
        if "event_time" not in table.column_names:
            raise ValueError("Raw record is missing event_time")
        buckets: dict[tuple[str, str, str], list[int]] = {}
        for index, value in enumerate(table.column("event_time").to_pylist()):
            if value is None:
                raise ValueError("Raw record has null event_time")
            event_time = _as_kst(value)
            bucket = (event_time.strftime("%Y-%m-%d"), event_time.strftime("%H"), f"{event_time.minute // 10 * 10:02d}")
            buckets.setdefault(bucket, []).append(index)
        for (date, hour, minute), indices in buckets.items():
            target = self._target(date, hour, minute, source_stem)
            temporary, parquet_writer = self._writer_for(target)
            del temporary  # The local name documents the tuple's first element.
            selected = table.take(pa.array(indices, type=pa.int64()))
            parquet_writer.write_table(selected, row_group_size=self.batch_size)
            self.rows += selected.num_rows

    def _target(self, date: str, hour: str, minute: str, source_stem: str) -> Path:
        prefix = (
            self.root / self.partition_prefix.parents[1]
            / f"date={date}" / f"hour={hour}" / f"minute={minute}"
        )
        if self.late:
            return prefix / LATE_PARTS_DIRECTORY / f"{source_stem}.parquet"
        return prefix / COMPACTED_FILENAME

    def _writer_for(self, target: Path) -> tuple[Path, pq.ParquetWriter]:
        existing = self._writers.get(target)
        if existing is not None:
            return existing
        target.parent.mkdir(parents=True, exist_ok=True)
        with tempfile.NamedTemporaryFile(mode="wb", suffix=".parquet", dir=target.parent, delete=False) as temporary:
            temporary_path = Path(temporary.name)
        writer = pq.ParquetWriter(temporary_path, self.schema, compression="zstd")
        self._writers[target] = (temporary_path, writer)
        return temporary_path, writer

    def publish(self) -> tuple[list[Path], int]:
        outputs: list[Path] = []
        for target, (temporary, writer) in self._writers.items():
            writer.close()
            os.replace(temporary, target)
            outputs.append(target)
        self._writers.clear()
        return outputs, self.rows

    def abort(self) -> None:
        for temporary, writer in self._writers.values():
            writer.close()
            temporary.unlink(missing_ok=True)
        self._writers.clear()


def _part_files(directory: Path) -> list[Path]:
    return sorted(directory.glob("part-*.parquet")) if directory.is_dir() else []


def _unified_schema(files: list[Path]) -> pa.Schema:
    schemas = [pq.ParquetFile(path).schema_arrow for path in files]
    try:
        return pa.unify_schemas(schemas, promote_options="permissive")
    except TypeError:
        return pa.unify_schemas(schemas)


def _align_batch(batch: pa.RecordBatch, schema: pa.Schema) -> pa.Table:
    source = pa.Table.from_batches([batch])
    arrays = []
    for field in schema:
        if field.name not in source.column_names:
            arrays.append(pa.nulls(source.num_rows, type=field.type))
        else:
            column = source.column(field.name)
            arrays.append(column if column.type == field.type else column.cast(field.type, safe=False))
    return pa.Table.from_arrays(arrays, schema=schema)


def _as_kst(value: datetime | str) -> datetime:
    if isinstance(value, str):
        value = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if value.tzinfo is None:
        return value.replace(tzinfo=KST)
    return value.astimezone(KST)


def _atomic_json_write(output: Path, payload: dict[str, object]) -> None:
    output.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(mode="w", encoding="utf-8", suffix=".json", dir=output.parent, delete=False) as temporary:
        temporary_path = Path(temporary.name)
        json.dump(payload, temporary, ensure_ascii=False, indent=2, sort_keys=True)
        temporary.write("\n")
    try:
        os.replace(temporary_path, output)
    finally:
        temporary_path.unlink(missing_ok=True)


def _default_raw_root() -> Path:
    configured = os.getenv("TRADING_RAW_DATA_DIR")
    return Path(configured).expanduser() if configured else default_data_root() / "raw"


def _default_processed_root() -> Path:
    configured = os.getenv("TRADING_PROCESSED_DATA_DIR")
    return Path(configured).expanduser() if configured else default_data_root() / "processed"


def parse_args(arguments: Iterable[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--raw-root", type=Path, default=_default_raw_root())
    parser.add_argument("--processed-root", type=Path, default=_default_processed_root())
    parser.add_argument("--once", action="store_true", help="process currently eligible raw input once and exit")
    parser.add_argument("--interval-seconds", type=float, default=float(os.getenv("TRADING_PROCESS_SECONDS", "300")))
    parser.add_argument("--delay-seconds", type=float, default=float(os.getenv("TRADING_PROCESS_DELAY_SECONDS", "330")))
    parser.add_argument("--batch-rows", type=int, default=int(os.getenv("TRADING_PROCESS_BATCH_ROWS", "65536")))
    parser.add_argument("--delete-processed-raw", action="store_true", help="delete only verified raw hours older than --raw-retention-days")
    parser.add_argument("--raw-retention-days", type=float, default=float(os.getenv("TRADING_RAW_RETENTION_DAYS", "3")))
    return parser.parse_args(arguments)


def main(arguments: Iterable[str] | None = None) -> int:
    args = parse_args(arguments)
    if args.interval_seconds <= 0:
        raise ValueError("interval-seconds must be positive")
    processor = RawParquetProcessor(
        args.raw_root,
        args.processed_root,
        processing_delay_seconds=args.delay_seconds,
        batch_size=args.batch_rows,
        raw_retention_days=args.raw_retention_days if args.delete_processed_raw else None,
        delete_processed_raw=args.delete_processed_raw,
    )
    while True:
        result = processor.process_available()
        if result.normal_partitions or result.late_parts or result.deleted_raw_hours:
            logging.info(
                "Processed %d normal partitions and %d late parts (%d rows); deleted %d verified raw hours",
                result.normal_partitions, result.late_parts, result.rows, result.deleted_raw_hours,
            )
        if args.once:
            return 0
        time.sleep(args.interval_seconds)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    raise SystemExit(main())
