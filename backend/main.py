"""Backend helper functions for book/detail lookups and related app utilities."""

import os
from typing import Optional

import pandas as pd

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

def shard_key(book_id: str) -> str:
    """Return shard key for a book_id, using 4-char prefix unless marked heavy (then 5-char)."""
    p4 = book_id[:4].lower()
    if p4 in HEAVY_4:
        return book_id[:5].lower()
    return p4


def get_book_detail(
    book_id: str,
    local_dir: Optional[str] = None,
    engine: str = 'pyarrow',
):
    """
    Fetch a single book from shard parquet on S3.
    Assumes shards stored at s3://<DATA_BUCKET>/books/parent_asin/<shard>.parquet.
    If local_dir is provided, reads from local_dir/<shard>.parquet instead of S3.
    Optional engine lets you specify parquet reader (e.g., 'fastparquet').
    Tries to match on 'book_id' column if present, otherwise on 'parent_asin'.
    """
    shard = shard_key(book_id)
    if local_dir:
        path = os.path.join(local_dir, f"{shard}.parquet")
    else:
        bucket = os.getenv("DATA_BUCKET")
        if not bucket:
            raise RuntimeError("DATA_BUCKET env not set")
        path = f"s3://{bucket}/books/parent_asin/{shard}.parquet"

    df = pd.read_parquet(path, engine=engine)
    if "parent_asin" in df.columns:
        match = df[df["parent_asin"] == book_id]
    else:
        return None
    if match.empty:
        return None
    return match.iloc[0].to_dict()
