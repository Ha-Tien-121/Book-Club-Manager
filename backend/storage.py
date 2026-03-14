"""Read and write storage helpers for books, events, users, catalogs, and forums."""

import os
from typing import Any, Optional

import boto3
import pandas as pd

# DynamoDB table names
BOOKS_TABLE = os.getenv("BOOKS_TABLE", "books")
EVENTS_TABLE = os.getenv("EVENTS_TABLE", "events")
USER_LIBRARY_TABLE = os.getenv("USER_LIBRARY_TABLE", "user_library")

# parent_asin prefixes with high book counts; these use 5-char shards instead of 4-char (add more as needed)
HEAVY_SHARD_PREFIXES = {
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


def _get_shard_key(book_id: str) -> str:
    """Return shard key for a book_id, using 4-char prefix unless marked heavy (then 5-char)."""
    p4 = book_id[:4].lower()
    if p4 in HEAVY_SHARD_PREFIXES:
        return book_id[:5].lower()
    return p4


def get_book_details(parent_asin: str, local_dir: Optional[str] = None, engine: str = "pyarrow") -> Optional[dict[str, Any]]:
    """
    Get book with description. Intended for book details page.
    Fetches from shard parquet on S3 or local_dir.
    """
    shard = _get_shard_key(parent_asin)
    if local_dir:
        path = os.path.join(local_dir, f"{shard}.parquet")
    else:
        bucket = os.getenv("DATA_BUCKET")
        if not bucket:
            raise RuntimeError("DATA_BUCKET env not set")
        path = f"s3://{bucket}/books/parent_asin/{shard}.parquet"

    df = pd.read_parquet(path, engine=engine)
    if "parent_asin" not in df.columns:
        return None
    match = df[df["parent_asin"] == parent_asin]
    if match.empty:
        return None
    item = match.iloc[0].to_dict()
    item["average_rating"] = float(item["average_rating"])
    return item


def get_book_metadata(parent_asin: str) -> Optional[dict[str, Any]]:
    """
    Get book metadata without description from DynamoDB. Intended for homepage, library, etc.
    """
    dynamodb = boto3.resource("dynamodb")
    table = dynamodb.Table(BOOKS_TABLE)
    try:
        resp = table.get_item(Key={"parent_asin": parent_asin})
    except Exception:
        return None
    item = resp.get("Item")
    if item is None:
        return None
    item["average_rating"] = float(item["average_rating"])
    return item


def get_event_details(event_id: str) -> Optional[dict[str, Any]]:
    """
    Get all event details from DynamoDB.
    """
    dynamodb = boto3.resource("dynamodb")
    table = dynamodb.Table(EVENTS_TABLE)
    try:
        resp = table.get_item(Key={"event_id": event_id})
    except Exception:
        return None
    item = resp.get("Item")
    if item is None:
        return None
    return item


def increment_library_actions_since_recs(
    user_id: str,
    threshold: int = 3,
    counter_attr: str = "actions_since_recs",
) -> Optional[dict[str, Any]]:
    """
    Atomically increment a per-user library action counter in DynamoDB.

    Returns a dict:
      - user_id
      - actions_since_recs (int)
      - should_run_recommender (bool)  -> True when counter >= threshold

    Notes:
    - Assumes USER_LIBRARY_TABLE has partition key `user_id`.
    - Does not run the recommender; callers decide what to do.
    """
    # TODO: implement
    dynamodb = boto3.resource("dynamodb")
    table = dynamodb.Table(USER_LIBRARY_TABLE)

    try:
        resp = table.update_item(
            Key={"user_id": user_id},
            UpdateExpression=f"SET {counter_attr} = if_not_exists({counter_attr}, :zero) + :inc",
            ExpressionAttributeValues={":inc": 1, ":zero": 0},
            ReturnValues="UPDATED_NEW",
        )
    except Exception:
        return None

    updated = resp.get("Attributes") or {}
    raw_count = updated.get(counter_attr, 0)
    try:
        count = int(raw_count)
    except Exception:
        count = 0

    return {
        "user_id": user_id,
        "actions_since_recs": count,
        "should_run_recommender": count >= threshold,
    }


def reset_library_actions_since_recs(
    user_id: str,
    counter_attr: str = "actions_since_recs",
) -> bool:
    """
    Reset the user's library action counter back to 0.
    Returns True on success, False otherwise.
    """
    # TODO: implement
    dynamodb = boto3.resource("dynamodb")
    table = dynamodb.Table(USER_LIBRARY_TABLE)
    try:
        table.update_item(
            Key={"user_id": user_id},
            UpdateExpression=f"SET {counter_attr} = :zero",
            ExpressionAttributeValues={":zero": 0},
        )
    except Exception:
        return False
    return True


def get_cached_event_recs(user_id: str) -> Optional[dict[str, Any]]:
    """
    Get cached event recommendations for a user.

    Expected shape (when implemented):
      {
        "events": [ ... top 10 event dicts ... ],
        "generated_at": <ISO 8601 string or epoch>,
        "next_expiry": <ISO 8601 string or epoch>,
      }
    """
    # TODO: implement using a DynamoDB table or another store.
    # Examples of what you might do here:
    # - Look up an item by user_id in a `user_event_recs` table.
    # - Deserialize any JSON payload stored for the recommendations.
    return None


def put_cached_event_recs(user_id: str, payload: dict[str, Any]) -> bool:
    """
    Store cached event recommendations for a user.

    Intended to be called by recommender_service after recomputing the top 10
    events. The `payload` should already contain `events`, `generated_at`, and
    `next_expiry` fields.
    """
    # TODO: implement using a DynamoDB table or another store.
    # Examples of what you might do here:
    # - Upsert an item in a `user_event_recs` table keyed by user_id.
    # - Serialize `payload` to JSON if storing as a single string attribute.
    return False


def get_catalog(parent_asin: str) -> Optional[list[dict[str, Any]]]:
    """
    Get all catalog data matching the book.
    """
    # TODO: implement
    return None


def get_user_accounts(user_id: str) -> Optional[dict[str, Any]]:
    """
    Get user account data.
    """
    # TODO: implement
    return None


def get_user_books(user_id: str) -> Optional[list[dict[str, Any]]]:
    """
    Get user's books.
    """
    # TODO: implement
    return None


def get_user_clubs(user_id: str) -> Optional[list[dict[str, Any]]]:
    """
    Get user's clubs.
    """
    # TODO: implement
    return None


def get_user_forums(user_id: str) -> Optional[list[dict[str, Any]]]:
    """
    Get user's forum activity.
    """
    # TODO: implement
    return None


def get_form_thread(parent_asin: str) -> Optional[dict[str, Any]]:
    """
    Get forum thread for a book.
    """
    # TODO: implement
    return None
