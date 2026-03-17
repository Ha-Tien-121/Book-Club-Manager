"""
Upload locally sharded book Parquet files to S3.

Assumes `shard_books_by_prefix.py` has already been run and produced
Parquet shards under:

    data/shards/parent_asin/*.parquet

Environment variables:
  - DATA_BUCKET: S3 bucket name to upload into
  - BOOK_SHARDS_S3_PREFIX (optional): S3 key prefix for the shards; defaults to
        "books/shard/parent_asin"
"""

from __future__ import annotations

import os
from pathlib import Path

import boto3
from dotenv import load_dotenv

load_dotenv()

# __file__ = .../data/scripts/loaders/load_book_shards_to_s3.py
# parents[2] -> .../data
DATA_DIR = Path(__file__).resolve().parents[2]
LOCAL_SHARDS_DIR = DATA_DIR / "shards" / "parent_asin"

DATA_BUCKET = os.getenv("DATA_BUCKET")
BOOK_SHARDS_S3_PREFIX = os.getenv(
    "BOOK_SHARDS_S3_PREFIX",
    "books/shard/parent_asin",
)


def upload_book_shards_to_s3(
    local_dir: Path = LOCAL_SHARDS_DIR,
    bucket: str | None = DATA_BUCKET,
    prefix: str = BOOK_SHARDS_S3_PREFIX,
) -> None:
    """Upload all parquet shard files from local_dir to S3 under the given prefix."""
    if not bucket:
        raise RuntimeError("DATA_BUCKET env not set")

    if not local_dir.exists():
        raise FileNotFoundError(f"Local shards directory not found: {local_dir}")

    s3 = boto3.client("s3")
    count = 0
    for file in sorted(local_dir.glob("*.parquet")):
        key = f"{prefix.rstrip('/')}/{file.name}"
        s3.upload_file(str(file), bucket, key)
        print(f"[upload] {file} -> s3://{bucket}/{key}")
        count += 1

    print(f"[done] uploaded {count} shard files to s3://{bucket}/{prefix.rstrip('/')}/")


def main() -> None:
    """CLI entrypoint to upload book shards to S3."""
    upload_book_shards_to_s3()


if __name__ == "__main__":
    main()
