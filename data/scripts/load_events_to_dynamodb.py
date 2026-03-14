"""
Load events from cleaned JSON to DynamoDB (events table).

The events table must already exist. This script reads
data/processed/book_events_clean.json, sets TTL to event start time
(start_iso as epoch seconds), and upserts each record. Items expire when the
event starts. Enable TTL on the table in AWS Console with attribute name "ttl".

Usage:
    python -m data.scripts.load_events_to_dynamodb
    python -m data.scripts.load_events_to_dynamodb --limit 10

Requires: boto3, AWS credentials configured.
"""

import argparse
import hashlib
import json
import os
from datetime import datetime, timezone
from pathlib import Path

import boto3
from dotenv import load_dotenv

load_dotenv()

# Paths
BASE_DIR = Path(__file__).resolve().parent.parent.parent
PROCESSED_DIR = BASE_DIR / "data" / "processed"
EVENTS_JSON = Path(
    os.getenv(
        "BOOK_EVENTS_CLEAN_PATH",
        str(PROCESSED_DIR / "book_events_clean.json"),
    )
)

# DynamoDB (table must already exist)
TABLE_NAME = os.getenv("EVENTS_TABLE", "events")
BATCH_SIZE = 25

def ttl_seconds_from_start_iso(start_iso: str) -> int | None:
    """Return Unix timestamp (seconds) for TTL: when the event starts. None if invalid."""
    if not start_iso or not str(start_iso).strip():
        return None
    try:
        raw = str(start_iso).strip().replace("Z", "+00:00")
        dt = datetime.fromisoformat(raw)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return int(dt.timestamp())
    except (ValueError, TypeError):
        return None


def event_id_from_link(link: str) -> str:
    """Deterministic 16-char hex from link (matches clean_book_events)."""
    return hashlib.sha256(str(link or "").encode("utf-8")).hexdigest()[:16]


def _str_val(val: object) -> str:
    """Coerce to string; None/nan -> empty."""
    if val is None:
        return ""
    s = str(val).strip()
    return "" if s.lower() == "nan" else s


def record_to_item(record: dict) -> dict:
    """Convert JSON record to DynamoDB item. Tags already a list; add ttl."""
    link = _str_val(record.get("link"))
    event_id = _str_val(record.get("event_id")) or event_id_from_link(link)
    start_iso = _str_val(record.get("start_iso"))
    # Use ttl from clean script if present and valid; else compute from start_iso
    ttl = record.get("ttl")
    if not isinstance(ttl, int):
        ttl = ttl_seconds_from_start_iso(start_iso)

    tags = record.get("tags")
    if not isinstance(tags, list):
        tags = []

    item = {
        "event_id": event_id,
        "title": _str_val(record.get("title")),
        "description": _str_val(record.get("description")),
        "book_title": _str_val(record.get("book_title")),
        "book_author": _str_val(record.get("book_author")),
        "title_author_key": _str_val(record.get("title_author_key")),
        "parent_asin": _str_val(record.get("parent_asin")),
        "tags": tags,
        "day_of_week_start": _str_val(record.get("day_of_week_start")),
        "start_time": _str_val(record.get("start_time")),
        "start_iso": start_iso,
        "city_state": _str_val(record.get("city_state")),
        "venue": _str_val(record.get("venue")),
        "link": link,
        "thumbnail": _str_val(record.get("thumbnail")),
    }
    if ttl is not None:
        item["ttl"] = ttl
    return item




def load_events_to_dynamodb(
    json_path: Path = EVENTS_JSON,
    table_name: str = TABLE_NAME,
    limit: int | None = None,
) -> int:
    """Load events from cleaned JSON to DynamoDB. Returns number of items written."""
    if not json_path.exists():
        raise FileNotFoundError(f"Events JSON not found: {json_path}")

    with open(json_path, encoding="utf-8") as f:
        records = json.load(f)
    if not isinstance(records, list):
        records = []
    if limit is not None:
        records = records[:limit]

    dynamodb = boto3.resource("dynamodb")
    table = dynamodb.Table(table_name)
    written = 0
    batch = []

    for record in records:
        if not _str_val(record.get("link")) or not _str_val(record.get("title")):
            continue
        batch.append(record_to_item(record))
        if len(batch) >= BATCH_SIZE:
            with table.batch_writer() as writer:
                for item in batch:
                    writer.put_item(Item=item)
                    written += 1
            batch = []
            if written % 50 == 0 and written > 0:
                print(f"  Loaded {written} events...")

    if batch:
        with table.batch_writer() as writer:
            for item in batch:
                writer.put_item(Item=item)
                written += 1

    return written


def main() -> None:
    """Load events from cleaned JSON to DynamoDB. Table must already exist."""
    parser = argparse.ArgumentParser(
        description="Load events from book_events_clean.json to DynamoDB"
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Load only first N events (default: all)",
    )
    args = parser.parse_args()

    print(f"Loading events from {EVENTS_JSON} to DynamoDB table '{TABLE_NAME}'")
    if args.limit:
        print(f"Limit: first {args.limit} rows")
    written = load_events_to_dynamodb(limit=args.limit)
    print(f"Loaded {written} events. TTL attribute 'ttl' set to event start_iso (item expires when event starts).")


if __name__ == "__main__":
    main()
