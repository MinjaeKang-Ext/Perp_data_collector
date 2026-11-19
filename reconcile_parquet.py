"""Create locally reconciled hourly Parquet files from a copied server data root.

For each high-frequency hour, this combines the completed ``data.parquet`` with
any ordinary or late parts using bounded-memory Arrow batches, and writes a new
file under a separate output root. It never modifies server source files.
"""

from __future__ import annotations

import argparse
from pathlib import Path
from typing import Iterable

from stream_parquet_store import (
    COMPACTED_FILENAME,
    LATE_PARTS_DIRECTORY,
    PARTS_DIRECTORY,
    merge_parquet_files_to_path,
)


def _hour_directories(root: Path) -> Iterable[Path]:
    yield from sorted(root.glob("*/*/*/date=*/hour=*"))


def _source_files(hour_directory: Path) -> list[Path]:
    files = []
    target = hour_directory / COMPACTED_FILENAME
    if target.is_file():
        files.append(target)
    for parts_directory in (hour_directory / PARTS_DIRECTORY, hour_directory / LATE_PARTS_DIRECTORY):
        if parts_directory.is_dir():
            files.extend(sorted(parts_directory.glob("part-*.parquet")))
    return files


def reconcile_tree(source_root: Path, output_root: Path) -> tuple[int, int]:
    """Reconcile every high-frequency hour from ``source_root`` into ``output_root``.

    Returns ``(hour_count, row_count)``.  ``output_root`` must not already exist
    so a prior local result is never overwritten accidentally.
    """
    source_root = source_root.expanduser().resolve()
    output_root = output_root.expanduser().resolve()
    if not source_root.is_dir():
        raise ValueError(f"Source data root does not exist: {source_root}")
    if output_root == source_root:
        raise ValueError("Output root must differ from the source root")
    if output_root.exists():
        raise ValueError(f"Refusing to overwrite existing output root: {output_root}")

    hours = rows = 0
    for hour_directory in _hour_directories(source_root):
        files = _source_files(hour_directory)
        if not files:
            continue
        destination = output_root / hour_directory.relative_to(source_root) / COMPACTED_FILENAME
        output_rows = merge_parquet_files_to_path(files, destination)
        hours += 1
        rows += output_rows
    return hours, rows


def parse_args(arguments: Iterable[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source-root", type=Path, required=True, help="copied server TRADING_DATA_DIR")
    parser.add_argument("--output-root", type=Path, required=True, help="new local reconciled-data directory")
    return parser.parse_args(arguments)


def main(arguments: Iterable[str] | None = None) -> int:
    args = parse_args(arguments)
    hours, rows = reconcile_tree(args.source_root, args.output_root)
    print(f"Reconciled {hours} hourly partitions and {rows:,} rows into {args.output_root.expanduser().resolve()}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
