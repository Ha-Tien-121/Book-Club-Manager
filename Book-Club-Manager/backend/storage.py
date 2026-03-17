"""Read and write storage helpers for books, events, users, catalogs, and forums."""

# Lint exception note
# backend/storage.py currently exceeds pylint’s default line-count threshold.
# We intentionally suppress too-many-lines, too-many-public-methods, and
# too-many-nested-blocks for this module to avoid late-stage
# refactor risk. This preserves runtime stability while keeping other lint checks
# and tests enforced in CI.
# broad-exception-caught is also intentional here because storage adapters
# must degrade gracefully across local files, sqlite, DynamoDB, and S3.
# pylint: disable=too-many-lines,too-many-public-methods,too-many-nested-blocks,broad-exception-caught

import json
import logging
import os
from decimal import Decimal
from typing import Any, Optional

import boto3
from boto3.dynamodb.conditions import Key
from boto3.dynamodb.types import TypeDeserializer
import pandas as pd

from backend import config as _config
from backend.forum_store import load_forum_store, save_forum_store
from backend.user_store import (
    load_user_store,
    save_user_accounts,
    save_user_books,
    save_user_clubs,
    save_user_forum,
)


def _from_dynamo(obj: Any) -> Any:
    """Convert DynamoDB item to JSON-friendly types (Decimal -> int/float)."""
    if obj is None:
        return None
    if isinstance(obj, Decimal):
        return int(obj) if obj % 1 == 0 else float(obj)
    if isinstance(obj, dict):
        return {k: _from_dynamo(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_from_dynamo(v) for v in obj]
    return obj


def _to_dynamo(obj: Any) -> Any:
    """Convert JSON data to DynamoDB types (float -> Decimal)."""
    if isinstance(obj, float):
        # Cast via string to avoid float precision issues in Decimal
        return Decimal(str(obj))
    if isinstance(obj, dict):
        return {k: _to_dynamo(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_to_dynamo(v) for v in obj]
    return obj


def _forum_post_to_item(post: dict, pk: str, sk: str, pk_value: str) -> dict:
    """Build a DynamoDB-put_item compatible dict. pk/sk are key attributes; table expects String (S) for both."""
    raw_id = post.get("id") or post.get("post_id") or post.get("sk") or 0
    try:
        post_id = int(raw_id)
    except (TypeError, ValueError):
        post_id = 0
    item = dict(post)
    item["id"] = post_id
    item["post_id"] = post_id
    item[pk] = str(pk_value)
    item[sk] = str(post_id)
    return item


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


def get_book_details(
    parent_asin: str,
    local_dir: Optional[str] = None,
    engine: str = "pyarrow",
) -> Optional[dict[str, Any]]:
    """
    Get book with description. Intended for book details page.
    Fetches from shard parquet on S3 (using shard key from parent_asin prefix) or local_dir.
    """
    parent_asin = (parent_asin or "").strip()
    if not parent_asin:
        return None
    shard = _get_shard_key(parent_asin)
    if local_dir:
        path = os.path.join(local_dir, f"{shard}.parquet")
    else:
        try:
            bucket = getattr(_config, "DATA_BUCKET", None) or os.getenv("DATA_BUCKET")
            prefix = getattr(_config, "BOOK_SHARDS_S3_PREFIX", None) or os.getenv(
                "BOOK_SHARDS_S3_PREFIX",
                "books/shard/book_shards",
            )
        except (RuntimeError, ValueError, TypeError, KeyError):
            bucket = os.getenv("DATA_BUCKET")
            prefix = os.getenv("BOOK_SHARDS_S3_PREFIX", "books/shard/book_shards")
        if not bucket:
            raise RuntimeError("DATA_BUCKET env not set")
        path = f"s3://{bucket}/{prefix.rstrip('/')}/{shard}.parquet"

    df = pd.read_parquet(path, engine=engine)
    if "parent_asin" not in df.columns:
        return None
    match = df[df["parent_asin"] == parent_asin]
    if match.empty:
        return None
    item = match.iloc[0].to_dict()
    if "average_rating" in item and item["average_rating"] is not None:
        try:
            item["average_rating"] = float(item["average_rating"])
        except (TypeError, ValueError):
            pass
    return item


def get_book_metadata(parent_asin: str) -> Optional[dict[str, Any]]:
    """
    Get book metadata without description from DynamoDB. Intended for homepage, library, etc.
    """
    # Pin region so local dev doesn't depend on AWS default region.
    dynamodb = boto3.resource("dynamodb", region_name=getattr(_config, "AWS_REGION", None))
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
    dynamodb = boto3.resource("dynamodb", region_name=getattr(_config, "AWS_REGION", None))
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
    dynamodb = boto3.resource("dynamodb", region_name=getattr(_config, "AWS_REGION", None))
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
    dynamodb = boto3.resource("dynamodb", region_name=getattr(_config, "AWS_REGION", None))
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


def get_cached_event_recs(_user_id: str) -> Optional[dict[str, Any]]:
    """
    Get cached event recommendations for a user.

    Expected shape (when implemented):
      {
        "events": [ ... top 10 event dicts ... ],
        "generated_at": <ISO 8601 string or epoch>,
        "next_expiry": <ISO 8601 string or epoch>,
      }
    """
    return None


def put_cached_event_recs(_user_id: str, _payload: dict[str, Any]) -> bool:
    """
    Store cached event recommendations for a user.

    Intended to be called by recommender_service after recomputing the top 10
    events. The `payload` should already contain `events`, `generated_at`, and
    `next_expiry` fields.
    """
    return False


def get_catalog(_parent_asin: str) -> Optional[list[dict[str, Any]]]:
    """
    Get all catalog data matching the book.
    """
    return None


def get_user_accounts(_user_id: str) -> Optional[dict[str, Any]]:
    """
    Get user account data.
    """
    return None


def get_user_books(_user_id: str) -> Optional[list[dict[str, Any]]]:
    """
    Get user's books.
    """
    return None


def get_user_clubs(_user_id: str) -> Optional[list[dict[str, Any]]]:
    """
    Get user's clubs.
    """
    return None


def get_user_forums(_user_id: str) -> Optional[list[dict[str, Any]]]:
    """
    Get user's forum activity.
    """
    return None


def get_form_thread(_parent_asin: str) -> Optional[dict[str, Any]]:
    """
    Get forum thread for a book.
    """
    return None


# ---------------------------------------------------------------------------
# Storage adapter: get_storage() returns LocalStorage or CloudStorage so the app
# can use the same interface for local vs AWS. Book recommender fallback uses
# get_top50_review_books() from here (local file vs S3).
# ---------------------------------------------------------------------------

def get_storage():
    """Return LocalStorage or CloudStorage based on APP_ENV (use cloud when APP_ENV=aws)."""
    if getattr(_config, "IS_AWS", False):
        return CloudStorage()
    return LocalStorage()


def _default_books_record():
    """Build a default user-books payload.

    Returns:
        dict: User books record with empty shelves and no genre preferences.
    """
    return {"library": {"in_progress": [], "saved": [], "finished": []}, "genre_preferences": []}


class LocalStorage:
    """Local file-backed storage. Delegates to user_store and forum_store for file I/O."""

    _cache: dict[str, Any] = {}

    def _load_json_file(self, path, *, cache_key: str) -> Any:
        """Load JSON from disk with a simple in-process cache."""
        if cache_key in self._cache:
            return self._cache[cache_key]
        try:
            if not path or not path.exists():
                self._cache[cache_key] = None
                return None
            with open(path, "r", encoding="utf-8") as f:
                data = json.load(f)
            self._cache[cache_key] = data
            return data
        except (OSError, ValueError, TypeError):
            self._cache[cache_key] = None
            return None

    def get_top50_review_books(self):
        """Return list of book dicts from reviews_top50_books.json (local path)."""
        path = globals().get("REVIEWS_TOP50_BOOKS_LOCAL_PATH") or getattr(
            _config, "REVIEWS_TOP50_BOOKS_LOCAL_PATH", None
        )
        # Fall back to the smaller reviews_top25_books.json if top-50 isn't present locally.
        if not path or not path.exists():
            path = getattr(_config, "PROCESSED_DIR", None) / "reviews_top25_books.json"
        data = self._load_json_file(path, cache_key=f"reviews::{str(path)}")
        if not data:
            return []
        if isinstance(data, list):
            return data
        if isinstance(data, dict) and "books" in data:
            return data["books"]
        return []

    def load_user_store(self, _email=None):
        """Load full user store from local JSON (email ignored; all users in files)."""
        return load_user_store()

    def save_user_books(self, user_id_or_store, rec=None):
        """Persist user books to local JSON storage.

        Args:
            user_id_or_store: Either a user ID (when `rec` is provided) or a full
                store dict containing `books`.
            rec: Optional per-user books record to save.

        Returns:
            None.

        Exceptions:
            None. Downstream write failures are handled in user_store helpers.
        """
        if rec is not None:
            store = self.load_user_store()
            store.setdefault("books", {})[str(user_id_or_store).strip().lower()] = rec
            save_user_books(store)
        else:
            save_user_books(user_id_or_store)

    def save_user_clubs(self, store):
        """Persist all users' saved clubs from a combined store.

        Args:
            store: Combined store dict containing a `clubs` mapping.

        Returns:
            None.

        Exceptions:
            None. Downstream write failures are handled in user_store helpers.
        """
        save_user_clubs(store)

    def save_user_forum(self, store):
        """Persist all users' forum metadata from a combined store.

        Args:
            store: Combined store dict containing a `forum` mapping.

        Returns:
            None.

        Exceptions:
            None. Downstream write failures are handled in user_store helpers.
        """
        save_user_forum(store)

    def get_user_account(self, user_id):
        """Fetch a local user account by ID/email.

        Args:
            user_id: User identifier (typically normalized email).

        Returns:
            dict | None: Account record when found; otherwise None.

        Exceptions:
            None.
        """
        if not user_id:
            return None
        store = self.load_user_store()
        return ((store.get("accounts") or {}).get("users") or {}).get(str(user_id).strip().lower())

    def get_user_books(self, user_id):
        """Fetch local user books/preferences, returning defaults when missing.

        Args:
            user_id: User identifier (typically normalized email).

        Returns:
            dict | None: User books record, default record if absent, or None for
            empty user IDs.

        Exceptions:
            None.
        """
        if not user_id:
            return None
        store = self.load_user_store()
        return (store.get("books") or {}).get(str(user_id).strip().lower()) or _default_books_record()

    def save_user_account(self, record):
        """Save a single local user account record.

        Args:
            record: Account payload containing at least user identifier fields.

        Returns:
            None.

        Exceptions:
            None. Downstream write failures are handled in user_store helpers.
        """
        store = self.load_user_store()
        users = store.setdefault("accounts", {}).setdefault("users", {})
        uid = record.get("user_id") or record.get("email", "").strip().lower()
        users[uid] = record
        save_user_accounts(store)

    def get_user_events(self, user_id):
        """Fetch saved event IDs for a local user.

        Args:
            user_id: User identifier (typically normalized email).

        Returns:
            dict | None: `{\"events\": [...]}` payload or None for empty user IDs.

        Exceptions:
            None.
        """
        if not user_id:
            return None
        store = self.load_user_store()
        clubs = (store.get("clubs") or {}).get(str(user_id).strip().lower()) or {}
        return {"events": clubs.get("club_ids", [])}

    def save_user_events(self, user_id, data):
        """Persist saved event IDs for a local user.

        Args:
            user_id: User identifier (typically normalized email).
            data: Mapping that may contain an `events` list.

        Returns:
            None.

        Exceptions:
            None. Invalid values are normalized/skipped.
        """
        if not user_id:
            return
        store = self.load_user_store()
        uid = str(user_id).strip().lower()
        # Keep event identifiers as strings (event_id) so they can be joined to events.
        raw = data.get("events", [])
        events: list[str] = []
        for x in raw or []:
            if x is None:
                continue
            s = str(x).strip()
            if s:
                events.append(s)
        store.setdefault("clubs", {})[uid] = {"club_ids": events}
        save_user_clubs(store)

    def get_user_forums(self, user_id):
        """Fetch local forum metadata for a user.

        Args:
            user_id: User identifier (typically normalized email).

        Returns:
            dict | None: User forum metadata or None for empty user IDs.

        Exceptions:
            None.
        """
        if not user_id:
            return None
        store = self.load_user_store()
        return (store.get("forum") or {}).get(str(user_id).strip().lower()) or {}

    def save_user_forums(self, user_id, data):
        """Persist local forum metadata for a user.

        Args:
            user_id: User identifier (typically normalized email).
            data: Forum metadata payload to store.

        Returns:
            None.

        Exceptions:
            None. Downstream write failures are handled in user_store helpers.
        """
        if not user_id:
            return
        store = self.load_user_store()
        store.setdefault("forum", {})[str(user_id).strip().lower()] = data
        save_user_forum(store)

    def load_forum_db(self):
        """Load the forum database payload from local storage.

        Returns:
            dict: Forum database with `posts` and `next_post_id`.

        Exceptions:
            None. Store helpers provide safe defaults.
        """
        return load_forum_store([])

    def save_forum_db(self, db):
        """Persist the forum database payload to local storage.

        Args:
            db: Forum database dict to persist.

        Returns:
            None.

        Exceptions:
            None. Empty payloads are ignored.
        """
        if not db:
            return
        save_forum_store(db)

    def get_user_recommendations(self, user_id):
        """Fetch locally cached recommendations for one user.

        Args:
            user_id: User identifier (typically normalized email).

        Returns:
            dict | None: Cached recommendation payload if available.

        Exceptions:
            None. Read/parse errors return None.
        """
        if not user_id:
            return None
        path = getattr(_config, "USER_RECOMMENDATIONS_PATH", None)
        if not path:
            return None
        uid = str(user_id).strip().lower()
        try:
            if not path.exists():
                return None
            with open(path, "r", encoding="utf-8") as f:
                data = json.load(f) or {}
            if not isinstance(data, dict):
                return None
            rec = data.get(uid)
            return rec if isinstance(rec, dict) else None
        except (OSError, ValueError, TypeError):
            return None

    def save_user_recommendations(self, user_id, rec):
        """Persist locally cached recommendations for one user.

        Args:
            user_id: User identifier (typically normalized email).
            rec: Recommendation payload to write.

        Returns:
            None.

        Exceptions:
            None. Write/parse errors are swallowed to keep UI resilient.
        """
        if not user_id:
            return
        path = getattr(_config, "USER_RECOMMENDATIONS_PATH", None)
        if not path:
            return
        uid = str(user_id).strip().lower()
        try:
            # Ensure directory exists.
            path.parent.mkdir(parents=True, exist_ok=True)
            data: dict = {}
            if path.exists():
                try:
                    with open(path, "r", encoding="utf-8") as f:
                        loaded = json.load(f) or {}
                    if isinstance(loaded, dict):
                        data = loaded
                except (OSError, ValueError, TypeError):
                    data = {}
            data[uid] = rec or {}
            with open(path, "w", encoding="utf-8") as f:
                json.dump(data, f, indent=2)
        except (OSError, ValueError, TypeError):
            return

    def get_soonest_events(self, limit=10):
        """Return soonest local events sorted by ttl/expiry.

        Args:
            limit: Maximum number of events to return.

        Returns:
            list: Sorted event dicts (possibly empty).

        Exceptions:
            None. Invalid/missing data files return an empty list.
        """
        path = getattr(_config, "PROCESSED_DIR", None) / "book_events_clean.json"
        data = self._load_json_file(path, cache_key="book_events_clean")
        if not isinstance(data, list):
            return []
        # Match CloudStorage semantics: return soonest by ttl (ascending).
        items = sorted(
            (e for e in data if isinstance(e, dict)),
            key=lambda x: int(x.get("ttl") or x.get("expiry") or 0),
        )
        return items[: max(0, int(limit))]

    def get_book_metadata(self, parent_asin):
        """Resolve local book metadata by parent ASIN/source ID.

        Args:
            parent_asin: Parent ASIN (or source ID) to look up.

        Returns:
            dict | None: Matching metadata payload when found.

        Exceptions:
            None.
        """
        # Local metadata comes from the curated review list(s).
        pid = str(parent_asin or "").strip()
        if not pid:
            return None
        # Try reviews list first.
        for b in (self.get_top50_review_books() or []):
            if isinstance(b, dict) and str(b.get("parent_asin") or b.get("source_id") or "").strip() == pid:
                return dict(b)
        # Try SPL trending list (if available locally).
        for b in (self.get_spl_top50_checkout_books() or []):
            if isinstance(b, dict) and str(b.get("parent_asin") or b.get("source_id") or "").strip() == pid:
                return dict(b)
        return None

    def get_books_metadata_batch(self, parent_asins: list[str]) -> dict[str, dict]:
        """Resolve local metadata for many parent ASINs.

        Args:
            parent_asins: Parent ASIN list to fetch.

        Returns:
            dict[str, dict]: Mapping of parent ASIN to metadata payload.

        Exceptions:
            None.
        """
        ids = [str(x).strip() for x in (parent_asins or []) if str(x).strip()]
        if not ids:
            return {}
        wanted = set(ids)
        out: dict[str, dict] = {}
        for b in (self.get_top50_review_books() or []):
            if not isinstance(b, dict):
                continue
            pid = str(b.get("parent_asin") or b.get("source_id") or "").strip()
            if pid and pid in wanted and pid not in out:
                out[pid] = dict(b)
        for b in (self.get_spl_top50_checkout_books() or []):
            if not isinstance(b, dict):
                continue
            pid = str(b.get("parent_asin") or b.get("source_id") or "").strip()
            if pid and pid in wanted and pid not in out:
                out[pid] = dict(b)
        return out

    def get_book_details(self, parent_asin):
        """Resolve local book details by delegating to metadata lookup.

        Args:
            parent_asin: Parent ASIN (or source ID) to look up.

        Returns:
            dict | None: Matching metadata/details payload.

        Exceptions:
            None.
        """
        # Local details are limited; return metadata (no S3/parquet reads).
        return self.get_book_metadata(parent_asin)

    def get_event_details(self, event_id):
        """Resolve one local event by event ID.

        Args:
            event_id: Event identifier.

        Returns:
            dict | None: Matching event payload when found.

        Exceptions:
            None.
        """
        eid = str(event_id or "").strip()
        if not eid:
            return None
        for e in self.get_soonest_events(500) or []:
            if str(e.get("event_id") or "").strip() == eid:
                return dict(e)
        return None

    def get_events_by_city(self, city_state):
        """Filter local events by exact `city_state`.

        Args:
            city_state: City/state string to match.

        Returns:
            list: Matching event payloads.

        Exceptions:
            None.
        """
        city_state = str(city_state or "").strip()
        if not city_state:
            return []
        events = [
            e
            for e in (self.get_soonest_events(500) or [])
            if str(e.get("city_state") or "").strip() == city_state
        ]
        return events

    def get_forum_post(self, _post_id):
        """Return one forum post by ID in local mode.

        Args:
            post_id: Forum post identifier.

        Returns:
            None: Local mode currently does not implement this lookup.

        Exceptions:
            None.
        """
        return None

    def update_forum_post(self, _post_id, _post):
        """Update one forum post in local mode.

        Args:
            post_id: Forum post identifier.
            post: Updated post payload.

        Returns:
            None.

        Exceptions:
            None. This local placeholder is currently a no-op.
        """
        return None

    def get_spl_top50_checkout_books(self):
        """Load locally cached SPL top-checkout books.

        Returns:
            list: Book metadata list from `spl_top50_checkouts_in_books.json`
            (or equivalent dict wrapper).

        Exceptions:
            None. Missing/invalid files return an empty list.
        """
        path = getattr(_config, "PROCESSED_DIR", None) / "spl_top50_checkouts_in_books.json"
        data = self._load_json_file(path, cache_key="spl_top50_checkouts_in_books")
        if not data:
            return []
        if isinstance(data, list):
            return data
        if isinstance(data, dict):
            return data.get("books") or data.get("items") or []
        return []

    def get_forum_thread_for_book(self, _parent_asin):
        """Return forum posts linked to a book in local mode.

        Args:
            parent_asin: Parent ASIN identifier.

        Returns:
            list: Empty list (not implemented in local mode).

        Exceptions:
            None.
        """
        return []

    def get_forum_thread(self, _parent_asin):
        """Return forum thread wrapper for a book in local mode.

        Args:
            parent_asin: Parent ASIN identifier.

        Returns:
            dict | None: None (not implemented in local mode).

        Exceptions:
            None.
        """
        return None

    def get_events_for_book(self, parent_asin, limit=10):
        """Return local events associated with a specific book.

        Args:
            parent_asin: Parent ASIN identifier.
            limit: Maximum number of events to return.

        Returns:
            list: Matching event payloads, truncated to `limit`.

        Exceptions:
            None.
        """
        pid = str(parent_asin or "").strip()
        if not pid:
            return []
        events = [
            e
            for e in (self.get_soonest_events(500) or [])
            if str(e.get("parent_asin") or "").strip() == pid
        ]
        return events[: max(0, int(limit))]


class CloudStorage:
    """AWS-backed storage (S3, DynamoDB). Use APP_ENV=aws to develop against AWS."""

    def _dynamo(self):
        """Create a DynamoDB resource client pinned to configured region.

        Returns:
            boto3.resources.base.ServiceResource: DynamoDB resource handle.

        Exceptions:
            boto3/botocore exceptions may be raised by client initialization.
        """
        # Always pin region so local dev doesn't depend on AWS CLI default region.
        return boto3.resource("dynamodb", region_name=getattr(_config, "AWS_REGION", None))

    def _s3(self):
        """Create an S3 client pinned to configured region.

        Returns:
            botocore.client.BaseClient: S3 client handle.

        Exceptions:
            boto3/botocore exceptions may be raised by client initialization.
        """
        # Always pin region (bucket is regional and credentials may have default region elsewhere).
        return boto3.client("s3", region_name=getattr(_config, "AWS_REGION", None))

    def _table(self, config_attr: str, env_fallback: str):
        """Resolve and return a DynamoDB table object.

        Args:
            config_attr: Config attribute name containing the table name.
            env_fallback: Fallback table name when config/env is unset.

        Returns:
            boto3.resources.factory.dynamodb.Table: DynamoDB table resource.

        Exceptions:
            boto3/botocore exceptions may be raised when creating the table handle.
        """
        name = getattr(_config, config_attr, None) or os.getenv(config_attr, env_fallback)
        return self._dynamo().Table(name)

    def get_top50_review_books(self):
        """Return list of book dicts from S3 (reviews_top50_books.json)."""
        bucket = getattr(_config, "DATA_BUCKET", None) or os.getenv("DATA_BUCKET")
        key = getattr(_config, "REVIEWS_TOP50_BOOKS_S3_KEY", None) or os.getenv(
            "REVIEWS_TOP50_BOOKS_S3_KEY", "books/reviews_top50_books.json"
        )
        if not bucket:
            return []
        try:
            resp = self._s3().get_object(Bucket=bucket, Key=key)
            data = json.loads(resp["Body"].read().decode("utf-8"))
        except Exception as e:
            logging.warning("get_top50_review_books (S3) failed: %s", e)
            return []
        if isinstance(data, list):
            return data
        if isinstance(data, dict) and "books" in data:
            return data["books"]
        return []

    def load_user_store(self, email=None):
        """Build a store-like dict for one user from DynamoDB (for frontend compatibility)."""
        email = (email or "").strip().lower()
        if not email:
            return {"accounts": {"users": {}}, "books": {}, "clubs": {}, "forum": {}}
        acc = self.get_user_account(email)
        if not acc:
            return {"accounts": {"users": {}}, "books": {}, "clubs": {}, "forum": {}}
        books_rec = self.get_user_books(email) or _default_books_record()
        events_rec = self.get_user_events(email) or {}
        raw_club_ids = events_rec.get("events", events_rec.get("club_ids", []))
        # Keep saved events as string event_ids.
        club_ids: list[str] = []
        for x in raw_club_ids or []:
            if x is None:
                continue
            s = str(x).strip()
            if not s:
                continue
            club_ids.append(s)
        forum_rec = self.get_user_forums(email) or {}
        acc_ui = dict(acc)
        acc_ui.setdefault("password", "")
        return {
            "accounts": {"users": {email: acc_ui}},
            "books": {email: books_rec},
            "clubs": {email: {"club_ids": list(club_ids)}},
            "forum": {email: forum_rec},
        }

    def get_user_books(self, user_id: str) -> Optional[dict]:
        """Get user library + genre_preferences from DynamoDB user_books table."""
        if not user_id:
            return None
        user_id = str(user_id).strip().lower()
        pk = getattr(_config, "USER_BOOKS_PK", "user_email").strip() or "user_email"
        try:
            table = self._table("USER_BOOKS_TABLE", "user_books")
            resp = table.get_item(Key={pk: user_id}, ConsistentRead=True)
            item = resp.get("Item")
            if not item:
                return None
            out = _from_dynamo(item)
            # Normalize library shelf lists: keep original tokens as strings so we don't
            # lose parent_asin-like IDs that happen to be numeric. Numeric IDs are
            # still resolvable by casting back to int in the UI when needed.
            if isinstance(out, dict) and "library" in out and isinstance(out["library"], dict):
                for shelf in ("in_progress", "saved", "finished"):
                    raw = out["library"].get(shelf)
                    if raw is not None and isinstance(raw, list):
                        tokens: list[str] = []
                        for x in raw:
                            if x is None:
                                continue
                            s = str(x).strip()
                            if not s:
                                continue
                            tokens.append(s)
                        out["library"][shelf] = tokens
            return out
        except Exception as e:
            logging.warning("get_user_books failed for %s: %s", user_id, e)
            return None

    def save_user_books(self, user_id_or_store, rec=None) -> None:
        """Persist one user's books (user_id, rec) or full store['books'] dict."""
        pk = getattr(_config, "USER_BOOKS_PK", "user_email").strip() or "user_email"
        if rec is not None:
            user_id = str(user_id_or_store).strip().lower()
            if not user_id:
                return
            try:
                table = self._table("USER_BOOKS_TABLE", "user_books")
                item = dict(rec)
                item[pk] = user_id
                # Ensure library and genre_preferences exist and are serializable.
                item.setdefault("genre_preferences", [])
                if "library" not in item or not isinstance(item["library"], dict):
                    item["library"] = {"in_progress": [], "saved": [], "finished": []}
                for shelf in ("in_progress", "saved", "finished"):
                    raw = item["library"].get(shelf)
                    if raw is not None:
                        tokens: list[str] = []
                        for x in raw:
                            if x is None:
                                continue
                            s = str(x).strip()
                            if not s:
                                continue
                            tokens.append(s)
                        item["library"][shelf] = tokens
                    else:
                        item["library"][shelf] = []
                table.put_item(Item=item)
            except Exception as e:
                logging.warning("save_user_books failed for %s: %s", user_id, e)
        else:
            for uid, r in (user_id_or_store.get("books") or {}).items():
                if uid:
                    self.save_user_books(uid, r)

    def save_user_clubs(self, store) -> None:
        """Persist store['clubs'] to DynamoDB user_events (events = list of event_id strings)."""
        for uid, rec in (store.get("clubs") or {}).items():
            if uid:
                events = [
                    str(e).strip()
                    for e in rec.get("club_ids", []) or []
                    if str(e).strip()
                ]
                self.save_user_events(str(uid).strip().lower(), {"events": events})

    def save_user_forum(self, store) -> None:
        """Persist store['forum'] to DynamoDB user_forums."""
        for uid, data in (store.get("forum") or {}).items():
            if uid:
                self.save_user_forums(str(uid).strip().lower(), data)

    def get_user_recommendations(self, user_id: str) -> Optional[dict]:
        """Fetch a user's recommendation payload from DynamoDB.

        Args:
            user_id: User email/ID key used in `user_recommendations`.

        Returns:
            dict | None: Recommendation payload or None when missing/error.

        Exceptions:
            None. Errors are logged and converted to None.
        """
        if not user_id:
            return None
        user_id = str(user_id).strip().lower()
        try:
            table = self._table("USER_RECOMMENDATIONS_TABLE", "user_recommendations")
            resp = table.get_item(Key={"user_email": user_id})
            item = resp.get("Item")
            return _from_dynamo(item) if item else None
        except Exception as e:
            logging.warning("get_user_recommendations failed for %s: %s", user_id, e)
            return None

    def save_user_recommendations(self, user_id: str, rec: dict) -> None:
        """Persist a user's recommendation payload to DynamoDB.

        Args:
            user_id: User email/ID key used in `user_recommendations`.
            rec: Recommendation payload to store.

        Returns:
            None.

        Exceptions:
            None. Errors are logged and swallowed.
        """
        if not user_id:
            return
        user_id = str(user_id).strip().lower()
        try:
            table = self._table("USER_RECOMMENDATIONS_TABLE", "user_recommendations")
            item = {"user_email": user_id, **rec}
            table.put_item(Item=_to_dynamo(item))
        except Exception as e:
            logging.warning("save_user_recommendations failed for %s: %s", user_id, e)

    def get_soonest_events(self, limit: int = 10) -> list:
        """Return soonest-upcoming events (by ttl).

        Tries the configured EVENTS_GSI query first, and falls back to a table
        scan when GSI query is unavailable (for example due to IAM/index config).
        """
        gsi = getattr(_config, "EVENTS_GSI", None) or os.getenv("EVENTS_GSI", "").strip() or None
        try:
            table = self._table("EVENTS_TABLE", "events")
            if gsi:
                try:
                    resp = table.query(
                        IndexName=gsi,
                        KeyConditionExpression=Key("type").eq("event"),
                        Limit=limit,
                        ScanIndexForward=True,
                    )
                    return _from_dynamo(resp.get("Items", []))
                except Exception:
                    # Fallback keeps Explore Events usable even when Query on GSI is denied.
                    pass
            resp = table.scan(Limit=min(limit * 3, 200))
            items = resp.get("Items", [])
            items = sorted(
                items,
                key=lambda x: int(x.get("ttl") or x.get("expiry") or 0),
            )[:limit]
            return _from_dynamo(items)
        except Exception:
            return []

    def get_book_metadata(self, parent_asin: str):
        """Fetch book metadata from the shared DynamoDB metadata helper.

        Args:
            parent_asin: Parent ASIN to look up.

        Returns:
            dict | None: Book metadata record when found.

        Exceptions:
            None. The delegated helper returns None on failures.
        """
        return get_book_metadata(parent_asin)
    def get_books_metadata_batch(self, parent_asins: list[str]) -> dict[str, dict]:
        """Batch fetch book metadata from DynamoDB by parent_asin.

        Returns mapping parent_asin -> metadata dict for items that exist.
        Uses BatchGetItem (up to 100 keys/request).
        """
        ids = [str(x).strip() for x in (parent_asins or []) if str(x).strip()]
        if not ids:
            return {}
        # Deduplicate while preserving order (stable).
        ids = list(dict.fromkeys(ids))
        out: dict[str, dict] = {}
        try:
            client = boto3.client(
                "dynamodb", region_name=getattr(_config, "AWS_REGION", None)
            )
            table = self._table("BOOKS_TABLE", "books").name
            # DynamoDB BatchGetItem limit: 100 keys.
            for i in range(0, len(ids), 100):
                chunk = ids[i : i + 100]
                req = {
                    table: {
                        "Keys": [{"parent_asin": {"S": pid}} for pid in chunk]
                    }
                }
                resp = client.batch_get_item(RequestItems=req)
                items = (resp.get("Responses") or {}).get(table) or []
                # items are in Dynamo wire format; reuse boto3 TypeDeserializer via resource
                # by round-tripping through Table? We'll do lightweight manual decode via _from_dynamo
                # by first converting attribute values with boto3.dynamodb.types.TypeDeserializer if available.
                try:
                    deser = TypeDeserializer()

                    def _decode_item(it: dict, _deser: TypeDeserializer = deser) -> dict:
                        """Decode one DynamoDB wire-format item to plain Python values."""
                        return {k: _deser.deserialize(v) for k, v in it.items()}

                    decoded = [_decode_item(it) for it in items]
                except Exception:
                    decoded = items  # best-effort; may already be plain dicts in some envs
                for it in decoded:
                    try:
                        meta = _from_dynamo(it)
                    except Exception:
                        meta = it
                    pid = str((meta or {}).get("parent_asin") or "").strip()
                    if pid:
                        out[pid] = dict(meta)
        except Exception as e:
            logging.warning("get_books_metadata_batch failed: %s", e)
        return out
    def get_book_details(self, parent_asin: str):
        """Fetch detailed book data from shared detail helper.

        Args:
            parent_asin: Parent ASIN to look up.

        Returns:
            dict | None: Book detail payload when found.

        Exceptions:
            RuntimeError: Propagated when required S3 configuration is missing.
        """
        return get_book_details(parent_asin)

    def get_event_details(self, event_id: str):
        """Fetch one event record by ID from shared helper.

        Args:
            event_id: Event identifier.

        Returns:
            dict | None: Event payload when found.

        Exceptions:
            None. The delegated helper returns None on failures.
        """
        return get_event_details(event_id)

    def get_events_by_city(self, city_state: str) -> list:
        """Query events by city_state using EVENTS_CITY_STATE_GSI if set."""
        gsi = getattr(_config, "EVENTS_CITY_STATE_GSI", None) or os.getenv("EVENTS_CITY_STATE_GSI", "").strip() or None
        if not gsi:
            return []
        try:
            table = self._table("EVENTS_TABLE", "events")
            resp = table.query(
                IndexName=gsi,
                KeyConditionExpression=Key("city_state").eq(city_state),
                Limit=100,
                ScanIndexForward=True,
            )
            return _from_dynamo(resp.get("Items", []))
        except Exception:
            return []

    def get_user_account(self, user_id: str) -> Optional[dict]:
        """Fetch one user account record from DynamoDB.

        Args:
            user_id: User identifier/email matching configured account PK.

        Returns:
            dict | None: Account record when found.

        Exceptions:
            None. Errors return None.
        """
        if not user_id:
            return None
        user_id = str(user_id).strip().lower()
        try:
            pk = getattr(_config, "USER_ACCOUNTS_PK", "user_id")
            table = self._table("USER_ACCOUNTS_TABLE", "user_accounts")
            resp = table.get_item(Key={pk: user_id})
            item = resp.get("Item")
            return _from_dynamo(item) if item else None
        except Exception:
            return None

    def save_user_account(self, record: dict) -> None:
        """Persist one user account record to DynamoDB.

        Args:
            record: Account payload to store.

        Returns:
            None.

        Exceptions:
            None. Errors are swallowed to keep writes non-fatal.
        """
        if not record:
            return
        try:
            pk = getattr(_config, "USER_ACCOUNTS_PK", "user_id")
            table = self._table("USER_ACCOUNTS_TABLE", "user_accounts")
            item = dict(record)
            # Ensure partition key is in the item (table may use user_id or user_email).
            if pk not in item:
                item[pk] = record.get("email") or record.get("user_id") or ""
            table.put_item(Item=item)
        except Exception:
            pass

    def get_user_events(self, user_id: str) -> Optional[dict]:
        """Fetch saved events payload for one user from DynamoDB.

        Args:
            user_id: User identifier/email matching configured events PK.

        Returns:
            dict | None: Event payload when found.

        Exceptions:
            None. Errors return None.
        """
        if not user_id:
            return None
        user_id = str(user_id).strip().lower()
        try:
            pk = getattr(_config, "USER_EVENTS_PK", "user_id")
            table = self._table("USER_EVENTS_TABLE", "user_events")
            resp = table.get_item(Key={pk: user_id}, ConsistentRead=True)
            item = resp.get("Item")
            return _from_dynamo(item) if item else None
        except Exception:
            return None

    def save_user_events(self, user_id: str, data: dict) -> None:
        """Persist saved events payload for one user to DynamoDB.

        Args:
            user_id: User identifier/email matching configured events PK.
            data: Payload that may include an `events` collection.

        Returns:
            None.

        Exceptions:
            None. Errors are logged and swallowed.
        """
        if not user_id:
            return
        user_id = str(user_id).strip().lower()
        try:
            pk = getattr(_config, "USER_EVENTS_PK", "user_email")
            table = self._table("USER_EVENTS_TABLE", "user_events")
            item = dict(data)
            if pk not in item:
                item[pk] = user_id
            # Ensure events list is JSON/DynamoDB-serializable (list of event_id strings).
            if "events" in item and item["events"] is not None:
                cleaned: list[str] = []
                for x in item["events"] or []:
                    if x is None:
                        continue
                    s = str(x).strip()
                    if s:
                        cleaned.append(s)
                item["events"] = cleaned
            table.put_item(Item=item)
        except Exception as e:
            logging.warning("save_user_events failed: %s", e)

    def load_forum_db(self) -> list:
        """Forum posts from DynamoDB. Reads next_post_id from META row; posts from pk=POST items."""
        pk = getattr(_config, "FORUM_POSTS_PK", "pk")
        sk_name = getattr(_config, "FORUM_POSTS_SK", "sk")
        pk_value = getattr(_config, "FORUM_POSTS_PK_VALUE", "POST")
        meta_pk = getattr(_config, "FORUM_POSTS_META_PK", "META")
        next_sk = getattr(_config, "FORUM_POSTS_NEXT_ID_SK", "next_post_id")
        try:
            table = self._table("FORUM_POSTS_TABLE", "forum_posts")
            # Get next_post_id from the counter row (pk=META, sk=next_post_id).
            next_id = 1
            try:
                meta_resp = table.get_item(
                    Key={pk: str(meta_pk), sk_name: str(next_sk)},
                    ConsistentRead=True,
                )
                meta = _from_dynamo(meta_resp.get("Item"))
                if meta:
                    next_id = int(meta.get("next_post_id") or meta.get("value") or 1)
            except Exception:
                pass
            resp = table.scan(Limit=500, ConsistentRead=True)
            raw = _from_dynamo(resp.get("Items", []))
            items = [p for p in raw if str(p.get(pk)) == str(pk_value)]
            for p in items:
                post_id = p.get(sk_name) or p.get("id") or p.get("post_id")
                try:
                    p["id"] = int(post_id) if post_id is not None else 0
                except (TypeError, ValueError):
                    p["id"] = 0
                p["post_id"] = p["id"]
            return {"posts": items, "next_post_id": next_id}
        except Exception:
            return {"posts": [], "next_post_id": 1}

    def save_forum_db(self, db: dict) -> None:
        """Persist forum state: write each post and update the next_post_id counter row."""
        if not db:
            return
        pk = getattr(_config, "FORUM_POSTS_PK", "pk")
        sk = getattr(_config, "FORUM_POSTS_SK", "sk")
        pk_value = getattr(_config, "FORUM_POSTS_PK_VALUE", "POST")
        meta_pk = getattr(_config, "FORUM_POSTS_META_PK", "META")
        next_sk = getattr(_config, "FORUM_POSTS_NEXT_ID_SK", "next_post_id")
        try:
            table = self._table("FORUM_POSTS_TABLE", "forum_posts")
            for post in (db.get("posts") or [])[:500]:
                item = _forum_post_to_item(post, pk, sk, pk_value)
                table.put_item(Item=item)
            # Persist next_post_id as a row (pk=META, sk=next_post_id).
            next_id = db.get("next_post_id")
            if next_id is not None:
                table.put_item(
                    Item={
                        pk: str(meta_pk),
                        sk: str(next_sk),
                        "next_post_id": int(next_id),
                    }
                )
        except Exception as e:
            logging.warning("save_forum_db failed: %s", e)

    def get_forum_post(self, post_id) -> Optional[dict]:
        """Fetch one forum post from DynamoDB by post ID.

        Args:
            post_id: Post identifier convertible to int.

        Returns:
            dict | None: Forum post payload when found.

        Exceptions:
            None. Errors return None.
        """
        try:
            pk = getattr(_config, "FORUM_POSTS_PK", "pk")
            sk = getattr(_config, "FORUM_POSTS_SK", "sk")
            pk_value = getattr(_config, "FORUM_POSTS_PK_VALUE", "POST")
            table = self._table("FORUM_POSTS_TABLE", "forum_posts")
            resp = table.get_item(Key={pk: str(pk_value), sk: str(int(post_id))})
            item = resp.get("Item")
            if item:
                item = _from_dynamo(item)
                item.setdefault("id", item.get("post_id") or item.get("sk"))
                item.setdefault("post_id", item.get("id"))
            return item
        except Exception:
            return None

    def update_forum_post(self, post_id, post: dict) -> None:
        """Upsert one forum post in DynamoDB.

        Args:
            post_id: Post identifier convertible to int.
            post: Post payload to persist.

        Returns:
            None.

        Exceptions:
            None. Errors are logged and swallowed.
        """
        try:
            pk = getattr(_config, "FORUM_POSTS_PK", "pk")
            sk = getattr(_config, "FORUM_POSTS_SK", "sk")
            pk_value = getattr(_config, "FORUM_POSTS_PK_VALUE", "POST")
            pid = int(post_id)
            post["id"] = pid
            post["post_id"] = pid
            table = self._table("FORUM_POSTS_TABLE", "forum_posts")
            table.put_item(Item=_forum_post_to_item(post, pk, sk, pk_value))
        except Exception as e:
            logging.warning("update_forum_post failed: %s", e)

    def get_user_forums(self, user_id: str) -> Optional[dict]:
        """Fetch user forum metadata from DynamoDB.

        Args:
            user_id: User identifier/email.

        Returns:
            dict | None: User forum metadata when found.

        Exceptions:
            None. Errors return None.
        """
        if not user_id:
            return None
        user_id = str(user_id).strip().lower()
        try:
            table = self._table("USER_FORUMS_TABLE", "user_forums")
            resp = table.get_item(Key={"user_email": user_id})
            item = resp.get("Item")
            return _from_dynamo(item) if item else None
        except Exception:
            return None

    def save_user_forums(self, user_id: str, data: dict) -> None:
        """Persist user forum metadata to DynamoDB.

        Args:
            user_id: User identifier/email.
            data: Forum metadata payload to store.

        Returns:
            None.

        Exceptions:
            None. Errors are swallowed.
        """
        if not user_id:
            return
        user_id = str(user_id).strip().lower()
        try:
            table = self._table("USER_FORUMS_TABLE", "user_forums")
            table.put_item(Item={"user_email": user_id, **data})
        except Exception:
            pass

    def get_spl_top50_checkout_books(self) -> list:
        """Return SPL top-50 checkouts list from S3 (fallback for trending)."""
        bucket = getattr(_config, "DATA_BUCKET", None) or os.getenv("DATA_BUCKET")
        key = getattr(_config, "TOP50_BOOKS_S3_KEY", None) or os.getenv(
            "TOP50_BOOKS_S3_KEY",
            "books/spl_top50_checkouts_in_books.json",
        )
        if not bucket:
            return []
        try:
            s3 = boto3.client("s3")
            resp = s3.get_object(Bucket=bucket, Key=key)
            data = json.loads(resp["Body"].read().decode("utf-8"))
        except Exception:
            return []
        return data if isinstance(data, list) else data.get("books", data.get("items", []))

    def get_forum_thread_for_book(self, parent_asin: str) -> list:
        """Fetch forum posts for a given book using the configured GSI.

        Args:
            parent_asin: Parent ASIN identifier.

        Returns:
            list: Matching forum post payloads (possibly empty).

        Exceptions:
            None. Missing GSI/errors return an empty list.
        """
        gsi = getattr(_config, "FORUM_POSTS_GSI", None) or os.getenv("FORUM_POSTS_GSI", "").strip() or None
        if not gsi:
            return []
        try:
            table = self._table("FORUM_POSTS_TABLE", "forum_posts")
            resp = table.query(
                IndexName=gsi,
                KeyConditionExpression=Key("parent_asin").eq(parent_asin),
                Limit=50,
            )
            return _from_dynamo(resp.get("Items", []))
        except Exception:
            return []

    def get_forum_thread(self, parent_asin: str) -> Optional[dict]:
        """Fetch a forum thread wrapper for one book.

        Args:
            parent_asin: Parent ASIN identifier.

        Returns:
            dict | None: `{\"posts\": [...]}` when posts exist, otherwise None.

        Exceptions:
            None.
        """
        posts = self.get_forum_thread_for_book(parent_asin)
        return {"posts": posts} if posts else None

    def get_events_for_book(self, parent_asin: str, limit: int = 10) -> list:
        """Fetch upcoming events related to a book via events GSI.

        Args:
            parent_asin: Parent ASIN identifier.
            limit: Maximum number of events to return.

        Returns:
            list: Matching event payloads (possibly empty).

        Exceptions:
            None. Missing GSI/errors return an empty list.
        """
        gsi = (
            getattr(_config, "EVENTS_PARENT_ASIN_GSI", None)
            or os.getenv("EVENTS_PARENT_ASIN_GSI", "").strip()
            or None
        )
        if not gsi:
            return []
        try:
            table = self._table("EVENTS_TABLE", "events")
            resp = table.query(
                IndexName=gsi,
                KeyConditionExpression=Key("parent_asin").eq(parent_asin),
                Limit=limit,
                ScanIndexForward=True,
            )
            return _from_dynamo(resp.get("Items", []))
        except Exception:
            return []
