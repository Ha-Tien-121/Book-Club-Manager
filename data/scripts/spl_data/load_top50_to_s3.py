"""
Upload the SPL top-50 checkout books JSON to S3.

Assumes `spl_checkout_data.py` has already been run and produced:
  data/processed/spl_top50_checkouts_in_books.json

Environment variables:
  - DATA_BUCKET: S3 bucket name to upload into
  - TOP50_S3_KEY (optional): S3 key for the object; defaults to
        "books/spl_top50_checkouts_in_books.json"
"""

import json
import os

import boto3
from dotenv import load_dotenv


load_dotenv()

BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", ".."))
LOCAL_TOP50_PATH = os.path.join(
    BASE_DIR,
    "data",
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
    """
    Read the local top-50 JSON file and upload it to S3.
    """
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


if __name__ == "__main__":
    upload_top50_to_s3()

