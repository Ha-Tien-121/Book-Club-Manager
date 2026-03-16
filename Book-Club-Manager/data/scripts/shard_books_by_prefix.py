"""
Shards cleaned Amazon books data into Parquet files keyed by `parent_asin` prefix.

Data source behavior:
    (1) Reads from `books.db` by default and scans rows from the `books` table.
    (2) Also supports the legacy JSONL format where each line is
        `{parent_asin: { ...payload... }}`.
    (3) Normalizes each row to a fixed Parquet schema before writing.

Sharding behavior:
    - Uses a 4-character lowercase prefix by default.
    - If the 4-character prefix is listed in `HEAVY_4`, uses a 5-character prefix
      instead to split oversized shards.

Args:
    source : Path to the input source.
             Supported inputs:
             - SQLite database, expected to contain table `books`
             - Legacy JSONL file keyed by `parent_asin`
    out_dir : Local directory where shard `.parquet` files are written.
              Ignored when `upload_only=True`, because a temporary staging
              directory is used instead.
    batch_size : Number of rows buffered per shard before flushing to disk.
    limit : Optional maximum number of rows to process before stopping.
    upload : If provided, upload completed shard files to S3 after local write.
    upload_only : If provided together with `upload`, stage shard files in a
                  temporary local directory and delete them after upload.
    bucket : Destination S3 bucket, required when `upload=True`.
    s3_prefix : Destination S3 key prefix for uploaded shard files.

Returns:
    Local parquet shard files named `<prefix>.parquet`.

    Parquet schema:
        parent_asin (string)
        title (string)
        author_name (string)
        average_rating (float64)
        rating_number (int64)
        description (list<string>)
        images (string)
        categories (list<string>)
        title_author_key (string)

Notes:
    - SQLite source rows are expected to match the schema produced by
      `data/scripts/amazon_books_data/books_meta_data.py`.
    - `description` and `categories` are JSON-encoded strings in SQLite and are
      decoded into Python lists before Parquet write.
    - A fixed Arrow schema is used so null-heavy columns such as `images` do not
      produce schema mismatch errors across shard flushes.

Usage:
    Run from the project root using:
    python data/scripts/shard_books_by_prefix.py

    Example with explicit source/output:
    python data/scripts/shard_books_by_prefix.py \\
      --source data/processed/books.db \\
      --out-dir data/shards/parent_asin \\
      --batch-size 5000

    Example smoke test:
    python data/scripts/shard_books_by_prefix.py \\
      --source data/processed/books.db \\
      --out-dir data/shards/test_parent_asin \\
      --batch-size 5000 \\
      --limit 1000

    Example upload-only run:
    python data/scripts/shard_books_by_prefix.py \\
      --source data/processed/books.db \\
      --batch-size 5000 \\
      --upload \\
      --upload-only \\
      --s3-prefix books/parent_asin

Time:
    Runtime depends mostly on row count, batch size, and storage speed.
    ~20-25 minutes for batch size of 10000 for local 
    ~30-35 minutes for batch size of 10000 for upload-only
"""

import argparse
import json
import os
import sqlite3
import tempfile
import time
from collections import defaultdict
from pathlib import Path
from typing import Dict, List, Optional, NamedTuple

import boto3
import pyarrow as pa
import pyarrow.parquet as pq
from dotenv import load_dotenv

load_dotenv()

# Heavy 4-char shards to split further (add more if needed)
HEAVY_4 = {
    "0312",
    "0615",
    "0692",
    "b000",
    "b001",
    "b002",
    "b003",
    "b004",
    "b005",
    "b008",
}


class UploadConfig(NamedTuple):
    """Upload target configuration."""

    bucket: str
    prefix: str


BOOK_SCHEMA = pa.schema(
    [
        pa.field("parent_asin", pa.string()),
        pa.field("title", pa.string()),
        pa.field("author_name", pa.string()),
        pa.field("average_rating", pa.float64()),
        pa.field("rating_number", pa.int64()),
        pa.field("description", pa.list_(pa.string())),
        pa.field("images", pa.string()),
        pa.field("categories", pa.list_(pa.string())),
        pa.field("title_author_key", pa.string()),
    ]
)


def shard_key(book_id: str) -> str:
    """Return shard key for a book_id, using 4-char prefix unless marked heavy (then 5-char)."""
    p4 = book_id[:4].lower()
    if p4 in HEAVY_4:
        return book_id[:5].lower()
    return p4


def sanitize_payload(payload: dict, book_id: str) -> dict:
    """
    Normalize payload to a consistent schema:
    - ensure parent_asin exists (default from outer key)
    - coerce strings to str
    - coerce lists to list of str
    - coerce numerics where possible
    """
    out = payload.copy()
    out.setdefault("parent_asin", book_id)

    def as_str(val):
        if val is None:
            return ""
        return str(val)

    out["title"] = as_str(out.get("title", ""))
    out["author_name"] = as_str(out.get("author_name", "")).strip()
    out["images"] = as_str(out.get("images", ""))

    def as_list_str(val):
        if val is None:
            return []
        if isinstance(val, (list, tuple)):
            return [as_str(x) for x in val]
        return [as_str(val)]

    out["categories"] = as_list_str(out.get("categories", []))
    out["description"] = as_list_str(out.get("description", []))

    def as_int(val):
        try:
            return int(val)
        except (TypeError, ValueError, OverflowError):
            return None

    def as_float(val):
        try:
            return float(val)
        except (TypeError, ValueError, OverflowError):
            return None

    out["rating_number"] = as_int(out.get("rating_number", None))
    out["average_rating"] = as_float(out.get("average_rating", None))
    out["title_author_key"] = as_str(out.get("title_author_key", ""))

    return out


def row_to_payload(row: sqlite3.Row) -> dict:
    """Convert a SQLite books row into the normalized payload expected by the sharder."""
    return sanitize_payload(
        {
            "parent_asin": row["parent_asin"],
            "title": row["title"],
            "author_name": row["author_name"],
            "average_rating": row["average_rating"],
            "rating_number": row["rating_number"],
            "description": json.loads(row["description"]) if row["description"] else [],
            "images": row["images"],
            "categories": json.loads(row["categories"]) if row["categories"] else [],
            "title_author_key": row["title_author_key"],
        },
        row["parent_asin"],
    )


def iter_db_payloads(source: Path):
    """Yield normalized payloads from the SQLite `books` table."""
    with sqlite3.connect(source) as conn:
        conn.row_factory = sqlite3.Row
        cur = conn.cursor()
        cur.execute(
            """
            SELECT parent_asin, title, author_name, average_rating, rating_number,
                   description, images, categories, title_author_key
            FROM books
            WHERE parent_asin IS NOT NULL AND TRIM(parent_asin) != ''
            ORDER BY parent_asin
            """
        )
        for row in cur:
            yield row["parent_asin"], row_to_payload(row)


def iter_jsonl_payloads(source: Path):
    """Yield normalized payloads from the legacy parent_asin-keyed JSONL file."""
    with source.open(encoding="utf-8") as file_obj:
        for line in file_obj:
            if not line.strip():
                continue
            obj = json.loads(line)
            if not isinstance(obj, dict) or len(obj) != 1:
                continue
            book_id, payload = tuple(obj.items())[0]
            if not book_id or not isinstance(payload, dict):
                continue
            yield book_id, sanitize_payload(payload, book_id)


def iter_source_payloads(source: Path):
    """Yield `(book_id, payload)` pairs from the supported input source."""
    if source.suffix.lower() == ".db":
        yield from iter_db_payloads(source)
        return
    yield from iter_jsonl_payloads(source)


def flush_buffer(
    shard: str,
    buffer: List[dict],
    writers: Dict[str, pq.ParquetWriter],
    out_dir: Path,
):
    """Flush a buffered list of rows to a Parquet shard file, reusing writers per shard."""
    if not buffer:
        return
    table = pa.Table.from_pylist(buffer, schema=BOOK_SCHEMA)
    out_dir.mkdir(parents=True, exist_ok=True)
    shard_path = out_dir / f"{shard}.parquet"
    if shard not in writers:
        writers[shard] = pq.ParquetWriter(shard_path, BOOK_SCHEMA)
    writers[shard].write_table(table)
    buffer.clear()


def upload_dir_to_s3(local_dir: Path, upload_config: UploadConfig):
    """Upload all parquet files in local_dir to S3 under the given prefix."""
    s3 = boto3.client("s3")
    for file in local_dir.glob("*.parquet"):
        key = f"{upload_config.prefix.rstrip('/')}/{file.name}"
        s3.upload_file(str(file), upload_config.bucket, key)
        print(f"[upload] {file} -> s3://{upload_config.bucket}/{key}")


def shard_file(
    source: Path,
    out_dir: Path,
    batch_size: int,
    limit: Optional[int] = None,
    target_cfg: Optional[UploadConfig] = None,
):
    """Stream-shard a SQLite DB or legacy JSONL source into parquet shard files."""
    if not source.exists():
        raise FileNotFoundError(f"Source not found: {source}")
    if target_cfg and not target_cfg.bucket:
        raise ValueError("Bucket required for upload")
    if limit is not None and limit <= 0:
        raise ValueError("limit must be positive when provided")

    buffers: Dict[str, List[dict]] = defaultdict(list)
    writers: Dict[str, pq.ParquetWriter] = {}
    total = 0

    for book_id, normalized in iter_source_payloads(source):
        key = shard_key(book_id)
        buffers[key].append(normalized)
        total += 1
        if len(buffers[key]) >= batch_size:
            flush_buffer(key, buffers[key], writers, out_dir)
        if limit is not None and total >= limit:
            break

    # Flush remaining
    for key, buf in buffers.items():
        flush_buffer(key, buf, writers, out_dir)
    for w in writers.values():
        w.close()

    print(f"[done] processed {total} rows into {len(writers)} shards at {out_dir}")

    if target_cfg:
        upload_dir_to_s3(out_dir, target_cfg)
        print(
            f"[done] uploaded shards to s3://{target_cfg.bucket}/"
            f"{target_cfg.prefix.rstrip('/')}/"
        )


def parse_args():
    """Parse CLI arguments."""
    p = argparse.ArgumentParser(
        description="Shard books data into Parquet shards by prefix."
    )
    p.add_argument(
        "--source",
        default="data/processed/books.db",
        help="Path to source SQLite DB or legacy JSONL file",
    )
    p.add_argument(
        "--out-dir",
        default="data/shards/parent_asin",
        help="Local output directory for shards",
    )
    p.add_argument("--batch-size", type=int, default=5000, help="Rows per shard flush")
    p.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Optional maximum number of rows to process",
    )
    p.add_argument(
        "--upload",
        action="store_true",
        help="Upload shards to S3 after writing locally",
    )
    p.add_argument(
        "--upload-only",
        action="store_true",
        help="Stage shards in a temporary local directory and remove them after upload",
    )
    p.add_argument(
        "--bucket",
        default=os.getenv("DATA_BUCKET"),
        help="S3 bucket (required if --upload)",
    )
    p.add_argument(
        "--s3-prefix",
        default="books/shard/parent_asin",
        help="S3 prefix for shard files",
    )
    return p.parse_args()


def main():
    """CLI entrypoint."""
    start = time.time()
    args = parse_args()
    if args.upload_only and not args.upload:
        raise ValueError("--upload-only requires --upload")
    target_cfg = None
    if args.upload:
        target_cfg = UploadConfig(bucket=args.bucket, prefix=args.s3_prefix)
    if args.upload_only:
        with tempfile.TemporaryDirectory(prefix="bookish-shards-") as temp_dir:
            shard_file(
                source=Path(args.source),
                out_dir=Path(temp_dir),
                batch_size=args.batch_size,
                limit=args.limit,
                target_cfg=target_cfg,
            )
            print("[done] cleaned up temporary local shard files")
    else:
        shard_file(
            source=Path(args.source),
            out_dir=Path(args.out_dir),
            batch_size=args.batch_size,
            limit=args.limit,
            target_cfg=target_cfg,
        )
    elapsed = time.time() - start
    print(f"[done] elapsed: {elapsed/60:.2f} minutes ({elapsed:.1f} seconds)")


if __name__ == "__main__":
    main()
