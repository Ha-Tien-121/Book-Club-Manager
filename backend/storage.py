"""Read and write storage helpers for books, events, users, catalogs, and forums."""

import json
import os
from decimal import Decimal
from typing import Any, Optional

import boto3
from boto3.dynamodb.conditions import Key
import pandas as pd


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


def get_book_details(parent_asin: str, local_dir: Optional[str] = None, engine: str = "pyarrow") -> Optional[dict[str, Any]]:
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
            from backend import config
            bucket = getattr(config, "DATA_BUCKET", None) or os.getenv("DATA_BUCKET")
        except Exception:
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


# ---------------------------------------------------------------------------
# Storage adapter: get_storage() returns LocalStorage or CloudStorage so the app
# can use the same interface for local vs AWS. Book recommender fallback uses
# get_top50_review_books() from here (local file vs S3).
# ---------------------------------------------------------------------------

def get_storage():
    """Return LocalStorage or CloudStorage based on APP_ENV (use cloud when APP_ENV=aws)."""
    from backend import config
    if getattr(config, "IS_AWS", False):
        return CloudStorage()
    return LocalStorage()


def _default_books_record():
    return {"library": {"in_progress": [], "saved": [], "finished": []}, "genre_preferences": []}


class LocalStorage:
    """Local file-backed storage. Delegates to user_store and forum_store for file I/O."""

    def get_top50_review_books(self):
        """Return list of book dicts from reviews_top50_books.json (local path)."""
        import sys
        from backend import config
        mod = sys.modules.get("backend.storage")
        path = getattr(mod, "REVIEWS_TOP50_BOOKS_LOCAL_PATH", None) if mod else None
        if path is None:
            path = getattr(config, "REVIEWS_TOP50_BOOKS_LOCAL_PATH", None)
        if not path or not path.exists():
            return []
        import json
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        if isinstance(data, list):
            return data
        if isinstance(data, dict) and "books" in data:
            return data["books"]
        return []

    def load_user_store(self, email=None):
        """Load full user store from local JSON (email ignored; all users in files)."""
        from backend.user_store import load_user_store as _load
        return _load()

    def save_user_books(self, user_id_or_store, rec=None):
        if rec is not None:
            store = self.load_user_store()
            store.setdefault("books", {})[str(user_id_or_store).strip().lower()] = rec
            from backend.user_store import save_user_books as _save
            _save(store)
        else:
            from backend.user_store import save_user_books as _save
            _save(user_id_or_store)

    def save_user_clubs(self, store):
        from backend.user_store import save_user_clubs as _save
        _save(store)

    def save_user_forum(self, store):
        from backend.user_store import save_user_forum as _save
        _save(store)

    def get_user_account(self, user_id):
        if not user_id:
            return None
        store = self.load_user_store()
        return ((store.get("accounts") or {}).get("users") or {}).get(str(user_id).strip().lower())

    def get_user_books(self, user_id):
        if not user_id:
            return None
        store = self.load_user_store()
        return (store.get("books") or {}).get(str(user_id).strip().lower()) or _default_books_record()

    def save_user_account(self, record):
        store = self.load_user_store()
        store.setdefault("accounts", {}).setdefault("users", {})[record.get("user_id") or record.get("email", "").strip().lower()] = record
        from backend.user_store import save_user_accounts
        save_user_accounts(store)

    def get_user_events(self, user_id):
        if not user_id:
            return None
        store = self.load_user_store()
        clubs = (store.get("clubs") or {}).get(str(user_id).strip().lower()) or {}
        return {"events": clubs.get("club_ids", [])}

    def save_user_events(self, user_id, data):
        if not user_id:
            return
        store = self.load_user_store()
        uid = str(user_id).strip().lower()
        store.setdefault("clubs", {})[uid] = {"club_ids": data.get("events", [])}
        from backend.user_store import save_user_clubs as _save
        _save(store)

    def get_user_forums(self, user_id):
        if not user_id:
            return None
        store = self.load_user_store()
        return (store.get("forum") or {}).get(str(user_id).strip().lower()) or {}

    def save_user_forums(self, user_id, data):
        if not user_id:
            return
        store = self.load_user_store()
        store.setdefault("forum", {})[str(user_id).strip().lower()] = data
        from backend.user_store import save_user_forum as _save
        _save(store)

    def load_forum_db(self):
        from backend.forum_store import load_forum_store
        return load_forum_store([])

    def save_forum_db(self, db):
        if not db:
            return
        from backend.forum_store import save_forum_store
        save_forum_store(db)

    def get_user_recommendations(self, user_id):
        return None  # local: no table; implement if needed

    def save_user_recommendations(self, user_id, rec):
        pass

    def get_soonest_events(self, limit=10):
        return []

    def get_book_metadata(self, parent_asin):
        return get_book_metadata(parent_asin)

    def get_book_details(self, parent_asin):
        return get_book_details(parent_asin)

    def get_event_details(self, event_id):
        return get_event_details(event_id)

    def get_events_by_city(self, city_state):
        return []

    def get_forum_post(self, post_id):
        return None

    def update_forum_post(self, post_id, post):
        pass

    def get_spl_top50_checkout_books(self):
        return []

    def get_forum_thread_for_book(self, parent_asin):
        return []

    def get_forum_thread(self, parent_asin):
        return None

    def get_events_for_book(self, parent_asin, limit=10):
        return []


class CloudStorage:
    """AWS-backed storage (S3, DynamoDB). Use APP_ENV=aws to develop against AWS."""

    def _dynamo(self):
        return boto3.resource("dynamodb")

    def _table(self, config_attr: str, env_fallback: str):
        from backend import config
        name = getattr(config, config_attr, None) or os.getenv(config_attr, env_fallback)
        return self._dynamo().Table(name)

    def get_top50_review_books(self):
        """Return list of book dicts from S3 (reviews_top50_books.json)."""
        from backend import config
        bucket = getattr(config, "DATA_BUCKET", None) or os.getenv("DATA_BUCKET")
        key = getattr(config, "REVIEWS_TOP50_BOOKS_S3_KEY", None) or os.getenv("REVIEWS_TOP50_BOOKS_S3_KEY", "books/reviews_top50_books.json")
        if not bucket:
            return []
        try:
            s3 = boto3.client("s3")
            resp = s3.get_object(Bucket=bucket, Key=key)
            data = json.loads(resp["Body"].read().decode("utf-8"))
        except Exception:
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
        # Normalize to ints so UI comparison (club["id"] in club_ids) works after DynamoDB round-trip.
        club_ids = []
        for x in raw_club_ids or []:
            if x is None:
                continue
            try:
                club_ids.append(int(x))
            except (TypeError, ValueError):
                pass
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
        try:
            table = self._table("USER_BOOKS_TABLE", "user_books")
            resp = table.get_item(Key={"user_id": user_id})
            item = resp.get("Item")
            return _from_dynamo(item) if item else None
        except Exception:
            return None

    def save_user_books(self, user_id_or_store, rec=None) -> None:
        """Persist one user's books (user_id, rec) or full store['books'] dict."""
        if rec is not None:
            user_id = str(user_id_or_store).strip().lower()
            if not user_id:
                return
            try:
                table = self._table("USER_BOOKS_TABLE", "user_books")
                table.put_item(Item={"user_id": user_id, **rec})
            except Exception:
                pass
        else:
            for uid, r in (user_id_or_store.get("books") or {}).items():
                if uid:
                    self.save_user_books(uid, r)

    def save_user_clubs(self, store) -> None:
        """Persist store['clubs'] to DynamoDB user_events (events = club_ids)."""
        for uid, rec in (store.get("clubs") or {}).items():
            if uid:
                self.save_user_events(str(uid).strip().lower(), {"events": rec.get("club_ids", [])})

    def save_user_forum(self, store) -> None:
        """Persist store['forum'] to DynamoDB user_forums."""
        for uid, data in (store.get("forum") or {}).items():
            if uid:
                self.save_user_forums(str(uid).strip().lower(), data)

    def get_user_recommendations(self, user_id: str) -> Optional[dict]:
        if not user_id:
            return None
        user_id = str(user_id).strip().lower()
        try:
            table = self._table("USER_RECOMMENDATIONS_TABLE", "user_recommendations")
            resp = table.get_item(Key={"user_email": user_id})
            item = resp.get("Item")
            return _from_dynamo(item) if item else None
        except Exception:
            return None

    def save_user_recommendations(self, user_id: str, rec: dict) -> None:
        if not user_id:
            return
        user_id = str(user_id).strip().lower()
        try:
            table = self._table("USER_RECOMMENDATIONS_TABLE", "user_recommendations")
            table.put_item(Item={"user_email": user_id, **rec})
        except Exception:
            pass

    def get_soonest_events(self, limit: int = 10) -> list:
        """Return soonest-upcoming events (by ttl).

        Tries the configured EVENTS_GSI query first, and falls back to a table
        scan when GSI query is unavailable (for example due to IAM/index config).
        """
        from backend import config
        gsi = getattr(config, "EVENTS_GSI", None) or os.getenv("EVENTS_GSI", "").strip() or None
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

    def get_book_metadata(self, parent_asin: str): return get_book_metadata(parent_asin)
    def get_book_details(self, parent_asin: str): return get_book_details(parent_asin)
    def get_event_details(self, event_id: str): return get_event_details(event_id)

    def get_events_by_city(self, city_state: str) -> list:
        """Query events by city_state using EVENTS_CITY_STATE_GSI if set."""
        from backend import config
        gsi = getattr(config, "EVENTS_CITY_STATE_GSI", None) or os.getenv("EVENTS_CITY_STATE_GSI", "").strip() or None
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
        if not user_id:
            return None
        user_id = str(user_id).strip().lower()
        try:
            from backend import config
            pk = getattr(config, "USER_ACCOUNTS_PK", "user_id")
            table = self._table("USER_ACCOUNTS_TABLE", "user_accounts")
            resp = table.get_item(Key={pk: user_id})
            item = resp.get("Item")
            return _from_dynamo(item) if item else None
        except Exception:
            return None

    def save_user_account(self, record: dict) -> None:
        if not record:
            return
        try:
            from backend import config
            pk = getattr(config, "USER_ACCOUNTS_PK", "user_id")
            table = self._table("USER_ACCOUNTS_TABLE", "user_accounts")
            item = dict(record)
            # Ensure partition key is in the item (table may use user_id or user_email).
            if pk not in item:
                item[pk] = record.get("email") or record.get("user_id") or ""
            table.put_item(Item=item)
        except Exception:
            pass

    def get_user_events(self, user_id: str) -> Optional[dict]:
        if not user_id:
            return None
        user_id = str(user_id).strip().lower()
        try:
            from backend import config
            pk = getattr(config, "USER_EVENTS_PK", "user_id")
            table = self._table("USER_EVENTS_TABLE", "user_events")
            resp = table.get_item(Key={pk: user_id}, ConsistentRead=True)
            item = resp.get("Item")
            return _from_dynamo(item) if item else None
        except Exception:
            return None

    def save_user_events(self, user_id: str, data: dict) -> None:
        if not user_id:
            return
        user_id = str(user_id).strip().lower()
        try:
            from backend import config
            pk = getattr(config, "USER_EVENTS_PK", "user_email")
            table = self._table("USER_EVENTS_TABLE", "user_events")
            item = dict(data)
            if pk not in item:
                item[pk] = user_id
            # Ensure events list is JSON/DynamoDB-serializable (list of ints).
            if "events" in item and item["events"] is not None:
                item["events"] = [int(x) for x in item["events"] if x is not None]
            table.put_item(Item=item)
        except Exception as e:
            import logging
            logging.warning("save_user_events failed: %s", e)

    def load_forum_db(self) -> list:
        """Forum posts from DynamoDB. Reads next_post_id from META row; posts from pk=POST items."""
        from backend import config
        pk = getattr(config, "FORUM_POSTS_PK", "pk")
        sk_name = getattr(config, "FORUM_POSTS_SK", "sk")
        pk_value = getattr(config, "FORUM_POSTS_PK_VALUE", "POST")
        meta_pk = getattr(config, "FORUM_POSTS_META_PK", "META")
        next_sk = getattr(config, "FORUM_POSTS_NEXT_ID_SK", "next_post_id")
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
        from backend import config
        pk = getattr(config, "FORUM_POSTS_PK", "pk")
        sk = getattr(config, "FORUM_POSTS_SK", "sk")
        pk_value = getattr(config, "FORUM_POSTS_PK_VALUE", "POST")
        meta_pk = getattr(config, "FORUM_POSTS_META_PK", "META")
        next_sk = getattr(config, "FORUM_POSTS_NEXT_ID_SK", "next_post_id")
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
            import logging
            logging.warning("save_forum_db failed: %s", e)

    def get_forum_post(self, post_id) -> Optional[dict]:
        try:
            from backend import config
            pk = getattr(config, "FORUM_POSTS_PK", "pk")
            sk = getattr(config, "FORUM_POSTS_SK", "sk")
            pk_value = getattr(config, "FORUM_POSTS_PK_VALUE", "POST")
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
        try:
            from backend import config
            pk = getattr(config, "FORUM_POSTS_PK", "pk")
            sk = getattr(config, "FORUM_POSTS_SK", "sk")
            pk_value = getattr(config, "FORUM_POSTS_PK_VALUE", "POST")
            pid = int(post_id)
            post["id"] = pid
            post["post_id"] = pid
            table = self._table("FORUM_POSTS_TABLE", "forum_posts")
            table.put_item(Item=_forum_post_to_item(post, pk, sk, pk_value))
        except Exception as e:
            import logging
            logging.warning("update_forum_post failed: %s", e)

    def get_user_forums(self, user_id: str) -> Optional[dict]:
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
        from backend import config
        bucket = getattr(config, "DATA_BUCKET", None) or os.getenv("DATA_BUCKET")
        key = getattr(config, "TOP50_BOOKS_S3_KEY", None) or os.getenv("TOP50_BOOKS_S3_KEY", "books/spl_top50_checkouts_in_books.json")
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
        from backend import config
        gsi = getattr(config, "FORUM_POSTS_GSI", None) or os.getenv("FORUM_POSTS_GSI", "").strip() or None
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
        posts = self.get_forum_thread_for_book(parent_asin)
        return {"posts": posts} if posts else None

    def get_events_for_book(self, parent_asin: str, limit: int = 10) -> list:
        from backend import config
        gsi = getattr(config, "EVENTS_PARENT_ASIN_GSI", None) or os.getenv("EVENTS_PARENT_ASIN_GSI", "").strip() or None
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
