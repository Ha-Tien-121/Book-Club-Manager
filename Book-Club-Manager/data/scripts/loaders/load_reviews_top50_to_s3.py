"""
Upload reviews_top50_books.json to S3.

Reads from data/processed/reviews_top50_books.json (or path from env)
and uploads to s3://bookish-data-elsie/books/reviews_top50_books.json.

Usage:
  DATA_BUCKET=bookish-data-elsie python data/scripts/upload_reviews_top50_to_s3.py
  # Or with custom path:
  REVIEWS_TOP50_LOCAL_PATH=/path/to/reviews_top50_books.json python ...
"""

import json
import os

import boto3

BASE_DIR = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "..", "..")
)
DEFAULT_LOCAL_PATH = os.path.join(
    BASE_DIR, "data", "processed", "reviews_top50_books.json"
)
BUCKET = os.getenv("DATA_BUCKET", "bookish-data-elsie")
S3_KEY = "books/reviews_top50_books.json"


def upload_reviews_top50_to_s3(
    local_path: str | None = None,
    bucket: str | None = None,
    key: str = S3_KEY,
) -> None:
    """Upload the local reviews top-50 JSON artifact to S3."""
    local_path = local_path or os.getenv("REVIEWS_TOP50_LOCAL_PATH") or DEFAULT_LOCAL_PATH
    bucket = bucket or BUCKET
    if not os.path.exists(local_path):
        raise FileNotFoundError(
            f"Local file not found: {local_path}. "
            "Place reviews_top50_books.json there or set REVIEWS_TOP50_LOCAL_PATH."
        )
    with open(local_path, "r", encoding="utf-8") as f:
        payload = json.load(f)
    # Count items (list or list inside a key like "books") for verification
    if isinstance(payload, list):
        count = len(payload)
    elif isinstance(payload, dict):
        count = 0
        for k in ("books", "items", "data"):
            if isinstance(payload.get(k), list):
                count = len(payload[k])
                break
    else:
        count = 0
    s3 = boto3.client("s3")
    body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
    s3.put_object(
        Bucket=bucket,
        Key=key,
        Body=body,
        ContentType="application/json",
    )
    print(f"Uploaded {local_path} to s3://{bucket}/{key} ({count} items, {len(body):,} bytes)")


if __name__ == "__main__":
    upload_reviews_top50_to_s3()
