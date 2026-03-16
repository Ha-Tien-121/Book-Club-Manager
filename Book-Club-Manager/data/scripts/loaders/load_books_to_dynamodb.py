"""
Load books from books.db to DynamoDB (without description column).

Usage:
    python -m data.scripts.loaders.load_books_to_dynamodb [--limit N]
    python -m data.scripts.loaders.load_books_to_dynamodb --all
    python -m data.scripts.loaders.load_books_to_dynamodb --all --create-table
    python -m data.scripts.loaders.load_books_to_dynamodb --all --clear-table

Requires: boto3, AWS credentials configured (env vars or ~/.aws/credentials)
"""

import argparse
import json
import os
import sqlite3
from decimal import Decimal
from pathlib import Path

import boto3
from botocore.exceptions import ClientError
from dotenv import load_dotenv

load_dotenv()

# Paths
# __file__ = .../Book-Club-Manager/data/scripts/loaders/load_books_to_dynamodb.py
# parent      -> loaders/
# parent[1]   -> scripts/
# parent[2]   -> data/
DATA_DIR = Path(__file__).resolve().parents[2]
PROCESSED_DIR = DATA_DIR / "processed"
BOOKS_DB = Path(os.getenv("BOOKS_DB_PATH", str(PROCESSED_DIR / "books.db")))

# DynamoDB
TABLE_NAME = os.getenv("BOOKS_TABLE", "books")
BATCH_SIZE = 25  # DynamoDB batch_write_item limit
READ_CAPACITY = int(os.getenv("DYNAMODB_READ_CAPACITY", "5"))
WRITE_CAPACITY = int(os.getenv("DYNAMODB_WRITE_CAPACITY", "25"))

# Default book cover when images is missing (env override supported)
DEFAULT_BOOK_IMAGE_URL = os.getenv(
    "DEFAULT_BOOK_IMAGE_URL",
    "https://bookish-data-elsie.s3.us-west-2.amazonaws.com/images/book_icon.png",
)


def clear_table(table_name: str = TABLE_NAME) -> None:
    """Delete all items in the books table (scan + batch delete). Use before reloading a smaller catalog."""
    dynamodb = boto3.resource("dynamodb")
    table = dynamodb.Table(table_name)
    deleted = 0
    while True:
        resp = table.scan(ProjectionExpression="parent_asin")
        items = resp.get("Items", [])
        if not items:
            break
        with table.batch_writer() as writer:
            for item in items:
                writer.delete_item(Key={"parent_asin": item["parent_asin"]})
                deleted += 1
        if deleted % 10000 == 0 and deleted > 0:
            print(f"  Deleted {deleted:,} items...")
    print(f"Cleared table '{table_name}' ({deleted:,} items deleted)")


def ensure_table_exists(table_name: str = TABLE_NAME) -> None:
    """Create DynamoDB table if it does not exist."""
    client = boto3.client("dynamodb")
    try:
        client.describe_table(TableName=table_name)
        print(f"Table '{table_name}' already exists")
        return
    except ClientError as e:
        if e.response["Error"]["Code"] != "ResourceNotFoundException":
            raise

    print(f"Creating table '{table_name}'...")
    dynamodb = boto3.resource("dynamodb")
    table = dynamodb.create_table(
        TableName=table_name,
        KeySchema=[{"AttributeName": "parent_asin", "KeyType": "HASH"}],
        AttributeDefinitions=[{"AttributeName": "parent_asin", "AttributeType": "S"}],
        BillingMode="PROVISIONED",
        ProvisionedThroughput={
            "ReadCapacityUnits": READ_CAPACITY,
            "WriteCapacityUnits": WRITE_CAPACITY,
        },
    )
    table.wait_until_exists()
    print(f"Table '{table_name}' created")


def row_to_item(row: sqlite3.Row) -> dict:
    """Convert SQLite row to DynamoDB item (excludes description). Uses default book icon when images is missing."""
    raw_images = row["images"]
    images = (raw_images or "").strip() if raw_images else ""
    if not images:
        images = DEFAULT_BOOK_IMAGE_URL
    item = {
        "parent_asin": row["parent_asin"],
        "title": row["title"] or "",
        "author_name": row["author_name"] or "",
        "average_rating": Decimal(str(row["average_rating"])) if row["average_rating"] is not None else Decimal("0"),
        "rating_number": int(row["rating_number"]) if row["rating_number"] is not None else 0,
        "images": images,
        "categories": json.loads(row["categories"]) if row["categories"] else [],
        "title_author_key": row["title_author_key"] or "",
    }
    return item


def load_books_to_dynamodb(
    db_path: Path = BOOKS_DB,
    table_name: str = TABLE_NAME,
    limit: int | None = 100,
) -> int:
    """Load books from books.db to DynamoDB (without description)."""
    if not db_path.exists():
        raise FileNotFoundError(f"Books DB not found: {db_path}")

    dynamodb = boto3.resource("dynamodb")
    table = dynamodb.Table(table_name)

    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        cur = conn.cursor()
        sql = """
            SELECT parent_asin, title, author_name, average_rating, rating_number,
                   images, categories, title_author_key
            FROM books
            WHERE parent_asin IS NOT NULL AND TRIM(parent_asin) != ''
            ORDER BY parent_asin
        """
        if limit is not None:
            sql += " LIMIT ?"
            cur.execute(sql, (limit,))
        else:
            cur.execute(sql)

        written = 0
        batch = []
        for row in cur:
            batch.append(row)
            if len(batch) >= BATCH_SIZE:
                with table.batch_writer() as writer:
                    for r in batch:
                        writer.put_item(Item=row_to_item(r))
                        written += 1
                batch = []
                if written % 10000 == 0 and written > 0:
                    print(f"  Loaded {written:,} books...")

        if batch:
            with table.batch_writer() as writer:
                for r in batch:
                    writer.put_item(Item=row_to_item(r))
                    written += 1

    return written


def main() -> None:
    """Load books to DynamoDB."""
    parser = argparse.ArgumentParser(description="Load books from books.db to DynamoDB")
    parser.add_argument("--limit", type=int, help="Number of books to load (default: 100)")
    parser.add_argument("--all", action="store_true", help="Load all books")
    parser.add_argument("--create-table", action="store_true", help="Create DynamoDB table if it does not exist")
    parser.add_argument(
        "--clear-table",
        action="store_true",
        help="Delete all items in the table before loading (use when replacing with a smaller catalog)",
    )
    args = parser.parse_args()

    if args.create_table:
        ensure_table_exists()

    if args.clear_table:
        clear_table()

    limit = None if args.all else (args.limit if args.limit is not None else 100)
    msg = "all" if limit is None else f"first {limit}"
    print(f"Loading {msg} books from {BOOKS_DB} to DynamoDB table '{TABLE_NAME}' (no description)")
    written = load_books_to_dynamodb(limit=limit)
    print(f"Loaded {written} books")


if __name__ == "__main__":
    main()
