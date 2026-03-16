"""
Convenience loader: upload SPL top-50 checkout books JSON to S3.

Thin wrapper around data.scripts.spl_data.load_top50_to_s3 so you can run:

    python -m data.scripts.loaders.load_spl_top50_to_s3

instead of importing the spl_data module directly.
"""

from __future__ import annotations

import json
import os

import boto3
from dotenv import load_dotenv

load_dotenv()

# __file__ = .../data/scripts/loaders/load_spl_top50_to_s3.py
# parents[2] -> .../data
DATA_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
LOCAL_TOP50_PATH = os.path.join(
    DATA_DIR,
    "processed",
    "spl_top50_checkouts_in_books.json",
)

DATA_BUCKET = os.getenv("DATA_BUCKET")
TOP50_S3_KEY = os.getenv(
    "TOP50_S3_KEY",
    "books/spl_top50_checkouts_in_books.json",
)


def upload_top50_to_s3(
    local_path: str = LOCAL_TOP50_PATH,
    bucket: str | None = DATA_BUCKET,
    key: str = TOP50_S3_KEY,
) -> None:
    """Read the local top-50 JSON file and upload it to S3."""
    if not bucket:
        raise RuntimeError("DATA_BUCKET env not set")

    if not os.path.exists(local_path):
        raise FileNotFoundError(f"Local file not found: {local_path}")

    with open(local_path, "r", encoding="utf-8") as f:
        payload = json.load(f)

    s3 = boto3.client("s3")
    s3.put_object(
        Bucket=bucket,
        Key=key,
        Body=json.dumps(payload, ensure_ascii=False).encode("utf-8"),
        ContentType="application/json",
    )

    print(f"Uploaded {local_path} to s3://{bucket}/{key}")


def main() -> None:
    """Upload local spl_top50_checkouts_in_books.json to S3."""
    upload_top50_to_s3()


if __name__ == "__main__":
    main()