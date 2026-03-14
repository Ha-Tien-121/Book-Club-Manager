"""Storage access layer for Bookish."""

from __future__ import annotations

import json
import os
from functools import lru_cache
from pathlib import Path
from typing import Any, Protocol

import boto3
from boto3.dynamodb.conditions import Key
import pandas as pd

from backend.config import (
    # Local JSON files
    FORUM_DB_PATH,
    PROCESSED_DIR,
    USER_ACCOUNTS_PATH,
    USER_BOOKS_PATH,
    USER_CLUBS_PATH,
    USER_FORUM_PATH,
    USER_EVENTS_PATH,
    # AWS configuration
    AWS_REGION,
    USER_ACCOUNTS_TABLE,
    USER_BOOKS_TABLE,
    USER_EVENTS_TABLE,
    FORUM_POSTS_TABLE,
    BOOKS_TABLE,
    EVENTS_TABLE,
    DATA_BUCKET,
    CDN_BASE_URL,
    DEFAULT_BOOK_IMAGE_KEY,
    TOP50_BOOKS_S3_KEY,
)
from backend.data_loader import load_data as _load_ui_data


def _read_json(path: Path, default: Any) -> Any:
    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
    if not path.exists():
        return default
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return default


def _write_json(path: Path, data: Any) -> None:
    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, indent=2), encoding="utf-8")


# Sharding configuration for S3 parquet book data
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


def build_image_url(key: str) -> str:
    """
    Build a public URL for an image key using CDN_BASE_URL.

    The key should be a relative S3 key like "images/book_icon.png".
    """
    return f"{CDN_BASE_URL.rstrip('/')}/{key.lstrip('/')}"


class Storage(Protocol):
    """
    Interface for user/forum storage backends.

    Local and cloud implementations should provide these methods.
    """

    # User accounts
    def get_user_account(self, user_id: str) -> dict: ...
    def save_user_account(self, item: dict) -> None: ...

    # User books
    def get_user_books(self, user_id: str) -> dict: ...
    def save_user_books(self, user_id: str, item: dict) -> None: ...

    # User clubs/forums/events
    def get_user_clubs(self, user_id: str) -> dict: ...
    def save_user_clubs(self, user_id: str, item: dict) -> None: ...

    def get_user_forums(self, user_id: str) -> dict: ...
    def save_user_forums(self, user_id: str, item: dict) -> None: ...

    def get_user_events(self, user_id: str) -> dict: ...
    def save_user_events(self, user_id: str, item: dict) -> None: ...

    # Forum posts
    def load_forum_db(self) -> dict: ...
    def save_forum_db(self, db: dict) -> None: ...
    def get_forum_thread(self, parent_asin: str) -> list[dict]: ...

    # Books & events (main content)
    def get_book_details(self, parent_asin: str) -> dict | None: ...
    def get_book_metadata(self, parent_asin: str) -> dict | None: ...
    def get_book_by_title_author_key(self, title_author_key: str) -> dict | None: ...
    def get_event_details(self, event_id: str) -> dict | None: ...
    def get_top50_books(self) -> list[dict]: ...


class LocalStorage:
    """
    File-based storage backend using JSON files under data/.

    This wraps the existing helper functions so callers can share a common
    interface with future DynamoDB-backed storage.
    """

    def get_user_account(self, user_id: str) -> dict:
        data = _read_json(USER_ACCOUNTS_PATH, {"users": {}})
        return dict((data.get("users") or {}).get(user_id) or {})

    def save_user_account(self, item: dict) -> None:
        data = _read_json(USER_ACCOUNTS_PATH, {"users": {}})
        users = data.setdefault("users", {})
        user_id = str(item.get("user_id") or item.get("email") or "").strip().lower()
        if not user_id:
            return
        users[user_id] = dict(item)
        _write_json(USER_ACCOUNTS_PATH, data)

    def get_user_books(self, user_id: str) -> dict:
        data = _read_json(USER_BOOKS_PATH, {})
        return dict(data.get(user_id) or {})

    def save_user_books(self, user_id: str, item: dict) -> None:
        data = _read_json(USER_BOOKS_PATH, {})
        data[user_id] = dict(item)
        _write_json(USER_BOOKS_PATH, data)

    def get_user_clubs(self, user_id: str) -> dict:
        data = _read_json(USER_CLUBS_PATH, {})
        return dict(data.get(user_id) or {})

    def save_user_clubs(self, user_id: str, item: dict) -> None:
        data = _read_json(USER_CLUBS_PATH, {})
        data[user_id] = dict(item)
        _write_json(USER_CLUBS_PATH, data)

    def get_user_forums(self, user_id: str) -> dict:
        data = _read_json(USER_FORUM_PATH, {})
        return dict(data.get(user_id) or {})

    def save_user_forums(self, user_id: str, item: dict) -> None:
        data = _read_json(USER_FORUM_PATH, {})
        data[user_id] = dict(item)
        _write_json(USER_FORUM_PATH, data)

    def get_user_events(self, user_id: str) -> dict:
        data = _read_json(USER_EVENTS_PATH, {})
        return dict(data.get(user_id) or {})

    def save_user_events(self, user_id: str, item: dict) -> None:
        data = _read_json(USER_EVENTS_PATH, {})
        data[user_id] = dict(item)
        _write_json(USER_EVENTS_PATH, data)

    def load_forum_db(self) -> dict:
        return _read_json(FORUM_DB_PATH, {"next_post_id": 1, "posts": []})

    def save_forum_db(self, db: dict) -> None:
        _write_json(FORUM_DB_PATH, db)

    def get_forum_thread(self, parent_asin: str) -> list[dict]:
        posts = (self.load_forum_db().get("posts") or [])
        target = str(parent_asin).strip().lower()
        out: list[dict] = []
        for post in posts:
            tags = post.get("tags") or []
            if any(str(t).strip().lower() == target for t in tags):
                out.append(post)
                continue
            if str(post.get("book_title") or "").strip().lower() == target:
                out.append(post)
        return out

    # ------------------------------------------------------------------
    # Books & events (local dev) - TODO: wire to real sources if needed
    # ------------------------------------------------------------------

    def get_book_details(self, parent_asin: str) -> dict | None:
        """
        TODO: implement a full local book-details lookup.

        For now, this returns metadata from the preloaded UI catalog, which
        may not include full description text.
        """
        data = _catalog_cache()
        return dict(
            data.get("books_by_source_id", {}).get(str(parent_asin)) or {}
        ) or None

    def get_book_metadata(self, parent_asin: str) -> dict | None:
        """
        TODO: refine this for local dev if needed.

        Currently mirrors get_book_details but strips description field.
        """
        item = self.get_book_details(parent_asin)
        if not item:
            return None
        item = dict(item)
        item.pop("description", None)
        return item

    def get_book_by_title_author_key(self, title_author_key: str) -> dict | None:
        """
        TODO: implement local lookup by title_author_key if needed.
        """
        _ = title_author_key
        return None

    def get_event_details(self, event_id: str) -> dict | None:
        """
        TODO: implement local event-details lookup if/when there is a local
        events JSON source for development.
        """
        _ = event_id
        return None

    def get_top50_books(self) -> list[dict]:
        """
        TODO: implement local get_top50_books (e.g. from local JSON) if needed.
        CloudStorage fetches from S3; local returns empty list.
        """
        return []

    # ------------------------------------------------------------------
    # TODO convenience helpers for local dev
    # ------------------------------------------------------------------

    def get_user_library(self, user_id: str) -> dict:
        """
        TODO: implement local get_user_library if needed.

        For now this is a placeholder so that the interface matches CloudStorage.
        """
        _ = user_id
        return {"in_progress": [], "saved": [], "finished": []}

    def get_user_genre_preferences(self, user_id: str) -> list[str]:
        """
        TODO: implement local get_user_genre_preferences if needed.
        """
        _ = user_id
        return []

    def add_book_to_shelf(self, user_id: str, shelf: str, parent_asin: str) -> dict:
        """
        TODO: implement local add_book_to_shelf if needed.
        """
        _ = (user_id, shelf, parent_asin)
        return {"in_progress": [], "saved": [], "finished": []}

    def remove_book_from_shelf(self, user_id: str, shelf: str, parent_asin: str) -> dict:
        """
        TODO: implement local remove_book_from_shelf if needed.
        """
        _ = (user_id, shelf, parent_asin)
        return {"in_progress": [], "saved": [], "finished": []}

    def set_user_genre_preferences(self, user_id: str, genres: list[str]) -> list[str]:
        """
        TODO: implement local set_user_genre_preferences if needed.
        """
        _ = (user_id, genres)
        return []

    def get_user_events(self, user_id: str) -> dict:
        """
        TODO: implement local get_user_events for saved events if needed.
        """
        _ = user_id
        return {"events": []}

    def save_user_events(self, user_id: str, item: dict) -> None:
        """
        TODO: implement local save_user_events if needed.
        """
        _ = (user_id, item)

    def add_event_for_user(self, user_id: str, event_id: str) -> list[str]:
        """
        TODO: implement local add_event_for_user if needed.
        """
        _ = (user_id, event_id)
        return []

    def remove_event_for_user(self, user_id: str, event_id: str) -> list[str]:
        """
        TODO: implement local remove_event_for_user if needed.
        """
        _ = (user_id, event_id)
        return []


class CloudStorage:
    """
    DynamoDBbacked storage backend.

    Mirrors the LocalStorageBackend interface, but persists data to DynamoDB and S3
    tables instead of local JSON files. This class is not wired in yet; when
    you're ready, you can instantiate it based on APP_ENV.
    """

    def __init__(self) -> None:
        dynamodb = boto3.resource("dynamodb", region_name=AWS_REGION)
        # Per-user tables
        self._accounts = dynamodb.Table(USER_ACCOUNTS_TABLE)
        self._books = dynamodb.Table(USER_BOOKS_TABLE)
        self._events = dynamodb.Table(USER_EVENTS_TABLE)
        self._forum = dynamodb.Table(FORUM_POSTS_TABLE)
        # Main content tables
        self._books_main = dynamodb.Table(BOOKS_TABLE)
        self._events_main = dynamodb.Table(EVENTS_TABLE)

    # ------------------------------------------------------------------
    # Helpers for normalizing DynamoDB user_books schema
    # ------------------------------------------------------------------

    @staticmethod
    def _normalize_string_list(value: Any) -> list[str]:
        """
        Normalize a DynamoDB string-list style value into a plain list[str].

        Handles both the high-level resource format (['a', 'b']) and the low-level
        attribute value format ({'L': [{'S': 'a'}, ...]}).
        """
        if value is None:
            return []
        if isinstance(value, list):
            return [str(v) for v in value]
        if isinstance(value, dict) and "L" in value:
            out: list[str] = []
            for entry in value.get("L") or []:
                if isinstance(entry, dict) and "S" in entry:
                    s = str(entry.get("S") or "").strip()
                    if s:
                        out.append(s)
            return out
        return [str(value)]

    @staticmethod
    def _normalize_library_map(value: Any) -> dict:
        """
        Normalize the DynamoDB library map into:
        { 'in_progress': [str], 'saved': [str], 'finished': [str] }.
        """
        default = {"in_progress": [], "saved": [], "finished": []}
        if not isinstance(value, dict):
            return {k: [] for k in default}
        lib: dict[str, list[str]] = {}
        for shelf in default:
            raw = value.get(shelf)
            lib[shelf] = CloudStorage._normalize_string_list(raw)
        return lib

    # --------------------------------------------------------------
    # User accounts
    # --------------------------------------------------------------
    def get_user_account(self, user_id: str) -> dict:
        """
        Fetch a user account record from the DynamoDB user accounts table.

        The primary key is `user_email` (lowercased). Returns an empty dict
        if the user does not exist.
        """
        key = {"user_email": str(user_id).strip().lower()}
        resp = self._accounts.get_item(Key=key)
        return dict(resp.get("Item") or {})

    def save_user_account(self, item: dict) -> None:
        """
        Upsert a user account record into the DynamoDB user accounts table.

        Expects `item` to contain at least `user_id` or `email`; it will be
        normalized into a `user_email` primary key.
        """
        user_id = str(item.get("user_id") or item.get("email") or "").strip().lower()
        if not user_id:
            return
        to_save = dict(item)
        to_save.setdefault("user_email", user_id)
        self._accounts.put_item(Item=to_save)

    # --------------------------------------------------------------
    # User books (library & genre preferences)
    # --------------------------------------------------------------
    def get_user_books(self, user_id: str) -> dict:
        """
        Return the user's books record in the same shape as LocalStorage:

        {
            "library": {"in_progress": [...], "saved": [...], "finished": [...]},
            "genre_preferences": [...],
        }
        """
        key = {"user_email": str(user_id).strip().lower()}
        resp = self._books.get_item(Key=key)
        item = resp.get("Item") or {}
        if not isinstance(item, dict):
            return {
                "library": {"in_progress": [], "saved": [], "finished": []},
                "genre_preferences": [],
            }

        library = self._normalize_library_map(item.get("library"))
        genre_preferences = self._normalize_string_list(item.get("genre_preferences"))
        return {
            "library": library,
            "genre_preferences": genre_preferences,
        }
    
    def get_user_library(self, user_id: str) -> dict:
        """Convenience: return just the library shelves for a user."""
        record = self.get_user_books(user_id)
        return record.get("library") or {"in_progress": [], "saved": [], "finished": []}

    def get_user_genre_preferences(self, user_id: str) -> list[str]:
        """Convenience: return just the genre_preferences list for a user."""
        record = self.get_user_books(user_id)
        prefs = record.get("genre_preferences") or []
        return [str(g) for g in prefs]

    def save_user_books(self, user_id: str, item: dict) -> None:
        """
        Persist the user's books record to DynamoDB.

        Expects `item` in the same shape returned by get_user_books.
        """
        user_id = str(user_id).strip().lower()
        library = item.get("library") or {"in_progress": [], "saved": [], "finished": []}
        genre_preferences = item.get("genre_preferences") or []

        to_save = {
            "user_email": user_id,
            "library": {
                "in_progress": [str(b) for b in library.get("in_progress") or []],
                "saved": [str(b) for b in library.get("saved") or []],
                "finished": [str(b) for b in library.get("finished") or []],
            },
            "genre_preferences": [str(g) for g in genre_preferences],
        }
        self._books.put_item(Item=to_save)

    def add_book_to_shelf(self, user_id: str, shelf: str, parent_asin: str) -> dict:
        """
        Add a book to a specific shelf, ensuring it is removed from the others.
        Returns the updated library dict.
        """
        shelf = str(shelf).strip()
        parent_asin = str(parent_asin).strip()
        if shelf not in {"in_progress", "saved", "finished"}:
            raise ValueError(f"Invalid shelf {shelf!r}; must be one of in_progress, saved, finished.")
        if not parent_asin:
            raise ValueError("parent_asin is required to add a book to a shelf.")

        # Require that the user already has a record in user_books and update it
        # with a single read–modify–write cycle.
        key = {"user_email": str(user_id).strip().lower()}
        existing = self._books.get_item(Key=key).get("Item") or {}
        if not isinstance(existing, dict) or not existing:
            raise ValueError(
                f"user_books record does not exist for user {user_id!r}; "
                "initialize it before adding books to shelves."
            )

        # Normalize existing record into app shape without doing a second
        # network round-trip through get_user_books.
        record = {
            "library": self._normalize_library_map(existing.get("library")),
            "genre_preferences": self._normalize_string_list(existing.get("genre_preferences")),
        }
        library = record.setdefault(
            "library", {"in_progress": [], "saved": [], "finished": []}
        )
        # Remove from all shelves
        for name in ("in_progress", "saved", "finished"):
            items = [str(b) for b in library.get(name) or [] if str(b) != parent_asin]
            library[name] = items
        # Add to target shelf
        library[shelf].append(parent_asin)
        self.save_user_books(user_id, record)
        return library

    def remove_book_from_shelf(self, user_id: str, shelf: str, parent_asin: str) -> dict:
        """
        Remove a book from a single shelf. Returns the updated library dict.
        """
        shelf = str(shelf).strip()
        parent_asin = str(parent_asin).strip()
        if shelf not in {"in_progress", "saved", "finished"} or not parent_asin:
            raise ValueError(
                "remove_book_from_shelf requires a valid shelf "
                "(in_progress, saved, finished) and non-empty parent_asin."
            )

        # Require that the user already has a record in user_books and update it
        # with a single read–modify–write cycle.
        key = {"user_email": str(user_id).strip().lower()}
        existing = self._books.get_item(Key=key).get("Item") or {}
        if not isinstance(existing, dict) or not existing:
            raise ValueError(
                f"user_books record does not exist for user {user_id!r}; "
                "initialize it before removing books from shelves."
            )

        record = {
            "library": self._normalize_library_map(existing.get("library")),
            "genre_preferences": self._normalize_string_list(existing.get("genre_preferences")),
        }
        library = record.setdefault(
            "library", {"in_progress": [], "saved": [], "finished": []}
        )
        items = [str(b) for b in library.get(shelf) or [] if str(b) != parent_asin]
        library[shelf] = items
        self.save_user_books(user_id, record)
        return library

    def set_user_genre_preferences(self, user_id: str, genres: list[str]) -> list[str]:
        """
        Overwrite the user's genre_preferences list. Returns the updated list.
        """
        record = self.get_user_books(user_id)
        record["genre_preferences"] = [str(g) for g in (genres or [])]
        self.save_user_books(user_id, record)
        return record["genre_preferences"]
    
    # --------------------------------------------------------------
    # User events
    # --------------------------------------------------------------

    def get_user_events(self, user_id: str) -> dict:
        """
        Return the user's saved events record.

        Shape:
            { "events": [event_id1, event_id2, ...] }
        """
        key = {"user_email": str(user_id).strip().lower()}
        resp = self._events.get_item(Key=key)
        item = resp.get("Item") or {}
        if not isinstance(item, dict):
            return {"events": []}
        events = self._normalize_string_list(item.get("events"))
        return {"events": events}

    def save_user_events(self, user_id: str, item: dict) -> None:
        """
        Upsert the user's events record into DynamoDB.

        Expects `item` in the shape returned by get_user_events:
            { "events": [event_id1, ...] }
        """
        user_id = str(user_id).strip().lower()
        if not user_id:
            return
        events = item.get("events") or []
        to_save = {
            "user_email": user_id,
            "events": [str(e) for e in events],
        }
        self._events.put_item(Item=to_save)

    def add_event_for_user(self, user_id: str, event_id: str) -> list[str]:
        """
        Add an event_id to the user's saved events list.

        Creates the user_events record if it does not already exist.
        Returns the updated list of event_ids.
        """
        user_id = str(user_id).strip().lower()
        event_id = str(event_id).strip()
        if not user_id:
            raise ValueError("user_id is required to add an event.")
        if not event_id:
            raise ValueError("event_id is required to add an event.")

        current = self.get_user_events(user_id)
        events: list[str] = [str(e) for e in current.get("events") or []]
        if event_id not in events:
            events.append(event_id)
        self.save_user_events(user_id, {"events": events})
        return events

    def remove_event_for_user(self, user_id: str, event_id: str) -> list[str]:
        """
        Remove an event_id from the user's saved events list.

        Returns the updated list of event_ids.
        """
        user_id = str(user_id).strip().lower()
        event_id = str(event_id).strip()
        if not user_id:
            raise ValueError("user_id is required to remove an event.")
        if not event_id:
            raise ValueError("event_id is required to remove an event.")

        current = self.get_user_events(user_id)
        events: list[str] = [
            str(e) for e in current.get("events") or [] if str(e) != event_id
        ]
        self.save_user_events(user_id, {"events": events})
        return events
    
    # --------------------------------------------------------------
    # Forum posts (posts-table pattern: one item per post)
    # --------------------------------------------------------------
    # Table must have partition key "pk" (string) and sort key "sk" (string).
    # - Meta: pk="forum_meta", sk="meta" -> { next_post_id: N }
    # - Post: pk="forum_posts", sk=str(post_id) -> full post dict as attributes
    _FORUM_META_PK = "forum_meta"
    _FORUM_META_SK = "meta"
    _FORUM_POSTS_PK = "forum_posts"

    def load_forum_db(self) -> dict:
        """
        Load the global forum state from DynamoDB (posts-table pattern).

        Returns the same shape as before: { "next_post_id": int, "posts": [ {...}, ... ] }
        so the UI and forum_service remain unchanged.
        """
        # Meta: next_post_id
        meta_resp = self._forum.get_item(
            Key={"pk": self._FORUM_META_PK, "sk": self._FORUM_META_SK}
        )
        meta = meta_resp.get("Item") or {}
        next_post_id = int(meta.get("next_post_id") or 1)

        # All posts: Query partition forum_posts (resource API for Python types)
        posts = []
        kwargs = {
            "KeyConditionExpression": Key("pk").eq(self._FORUM_POSTS_PK),
        }
        while True:
            resp = self._forum.query(**kwargs)
            for item in resp.get("Items") or []:
                post = {k: v for k, v in item.items() if k not in ("pk", "sk")}
                if post:
                    posts.append(post)
            if "LastEvaluatedKey" not in resp:
                break
            kwargs["ExclusiveStartKey"] = resp["LastEvaluatedKey"]

        # Sort by id so order is stable (e.g. newest first is handled by UI)
        posts.sort(key=lambda p: int(p.get("id") or 0))

        return {"next_post_id": next_post_id, "posts": posts}

    def save_forum_db(self, db: dict) -> None:
        """
        Persist the global forum state to DynamoDB (posts-table pattern).

        Accepts the same shape: { "next_post_id": int, "posts": [ {...}, ... ] }.
        Replaces all post items and updates meta.
        """
        next_post_id = int(db.get("next_post_id") or 1)
        posts = db.get("posts") or []

        # Delete existing post items (Query then BatchWriteItem delete)
        to_delete: list[dict] = []
        paginator = self._forum.meta.client.get_paginator("query")
        for page in paginator.paginate(
            TableName=self._forum.name,
            KeyConditionExpression="pk = :pk",
            ExpressionAttributeValues={":pk": self._FORUM_POSTS_PK},
            ProjectionExpression="pk, sk",
        ):
            to_delete.extend(page.get("Items") or [])

        for i in range(0, len(to_delete), 25):
            batch = to_delete[i : i + 25]
            self._forum.meta.client.batch_write_item(
                RequestItems={
                    self._forum.name: [
                        {
                            "DeleteRequest": {
                                "Key": {"pk": x["pk"], "sk": x["sk"]},
                            }
                        }
                        for x in batch
                    ]
                }
            )

        # Write new post items (one PutItem per post; resource handles serialization)
        for post in posts:
            post_id = post.get("id")
            if post_id is None:
                continue
            item = dict(post)
            item["pk"] = self._FORUM_POSTS_PK
            item["sk"] = str(post_id)
            self._forum.put_item(Item=item)

        # Update meta
        self._forum.put_item(
            Item={
                "pk": self._FORUM_META_PK,
                "sk": self._FORUM_META_SK,
                "next_post_id": next_post_id,
            }
        )

    def get_forum_thread(self, parent_asin: str) -> list[dict]:
        """
        Return all posts whose tags or book_title match the given parent_asin.
        Same contract as before; uses posts-table Query + in-memory filter.
        """
        db = self.load_forum_db()
        posts = db.get("posts") or []
        target = str(parent_asin).strip().lower()
        out: list[dict] = []
        for post in posts:
            tags = post.get("tags") or []
            if any(str(t).strip().lower() == target for t in tags):
                out.append(post)
                continue
            if str(post.get("book_title") or "").strip().lower() == target:
                out.append(post)
        return out

    # ------------------------------------------------------------------
    # Main books & events tables 
    # ------------------------------------------------------------------

    def get_top50_books(self) -> list[dict]:
        """
        Fetch the SPL top-50 checkouts book list from S3.

        Reads s3://{DATA_BUCKET}/{TOP50_BOOKS_S3_KEY} (JSON array of book dicts).
        Returns the list, or [] if the bucket/key is missing or read fails.
        """
        if not DATA_BUCKET:
            return []
        try:
            s3 = boto3.client("s3")
            resp = s3.get_object(Bucket=DATA_BUCKET, Key=TOP50_BOOKS_S3_KEY)
            body = resp["Body"].read().decode("utf-8")
            data = json.loads(body)
            if isinstance(data, list):
                return data
            return []
        except Exception:
            return []

    def get_book_metadata(self, parent_asin: str) -> dict | None:
        """
        Get book metadata without description from the main books table.
        """
        try:
            resp = self._books_main.get_item(Key={"parent_asin": parent_asin})
        except Exception:
            return None
        item = resp.get("Item")
        if not item:
            return None
        item["average_rating"] = float(item["average_rating"])
        # Fallback thumbnail URL if images field is missing/empty.
        images = item.get("images")
        # Avoid truth-testing numpy arrays (DeprecationWarning).
        if images is None or (hasattr(images, "__len__") and len(images) == 0):
            item["images"] = build_image_url(DEFAULT_BOOK_IMAGE_KEY)
        return item

    def get_book_by_title_author_key(self, title_author_key: str) -> dict | None:
        """
        Look up a book by title_author_key using the books table GSI.
        Expects a GSI named 'title_author_key' on the main books table.
        Returns { 'parent_asin': ..., 'categories': [...] } or None.
        """
        if not title_author_key or not str(title_author_key).strip():
            return None
        try:
            resp = self._books_main.query(
                IndexName="title_author_key",
                KeyConditionExpression=Key("title_author_key").eq(str(title_author_key).strip()),
                Limit=1,
            )
        except Exception:
            return None
        items = resp.get("Items") or []
        if not items:
            return None
        item = items[0]
        categories = item.get("categories")
        if not isinstance(categories, list):
            categories = []
        return {
            "parent_asin": item.get("parent_asin"),
            "categories": categories,
        }

    def get_event_details(self, event_id: str) -> dict | None:
        """
        Get all event details from the main events table.
        Mirrors the old get_event_details helper.
        """
        try:
            resp = self._events_main.get_item(Key={"event_id": event_id})
        except Exception:
            return None
        item = resp.get("Item")
        if not item:
            return None
        return item

    def get_book_details(self, parent_asin: str) -> dict | None:
        """
        Best-effort full book details (including description) from sharded parquet.

        Reads from s3://{DATA_BUCKET}/books/parent_asin/{shard}.parquet.
        Falls back to None if the parquet file can't be read.
        """
        shard = _get_shard_key(parent_asin)
        if not DATA_BUCKET:
            raise RuntimeError("DATA_BUCKET env not set")
        path = f"s3://{DATA_BUCKET}/books/parent_asin/{shard}.parquet"

        # Try pyarrow; if it fails, give up and let callers fall back.
        try:
            df = pd.read_parquet(path, engine="pyarrow")
        except Exception:
            return None

        if "parent_asin" not in df.columns:
            return None
        match = df[df["parent_asin"] == parent_asin]
        if match.empty:
            return None
        item = match.iloc[0].to_dict()
        item["average_rating"] = float(item["average_rating"])
        # Fallback thumbnail URL if images field is missing/empty.
        images = item.get("images")
        # Avoid truth-testing numpy arrays (DeprecationWarning).
        if images is None or (hasattr(images, "__len__") and len(images) == 0):
            item["images"] = build_image_url(DEFAULT_BOOK_IMAGE_KEY)
        return item


# For now we only declare the local backend class. A Dynamo backend can be
# added later and selected based on APP_ENV; callers currently use the
# function-level helpers below which directly call the JSON helpers.


@lru_cache(maxsize=1)
def _catalog_cache() -> dict:
    return _load_ui_data()


def get_book_detail(parent_asin: str) -> dict:
    return dict(_catalog_cache().get("books_by_source_id", {}).get(str(parent_asin)) or {})


def get_book_metadata(parent_asin: str) -> dict:
    meta = get_book_detail(parent_asin)
    meta.pop("description", None)
    return meta


def get_event_detail(event_id: str) -> dict:
    _ = event_id
    return {}


def get_catalog(parent_asin: str) -> dict:
    return get_book_detail(parent_asin)


def get_user_accounts(user_id: str) -> dict:
    return dict((_read_json(USER_ACCOUNTS_PATH, {"users": {}}).get("users") or {}).get(user_id) or {})


def get_user_books(user_id: str) -> dict:
    return dict((_read_json(USER_BOOKS_PATH, {})).get(user_id) or {})


def get_user_clubs(user_id: str) -> dict:
    return dict((_read_json(USER_CLUBS_PATH, {})).get(user_id) or {})


def get_user_forums(user_id: str) -> dict:
    return dict((_read_json(USER_FORUM_PATH, {})).get(user_id) or {})


def get_form_thread(parent_asin: str) -> list[dict]:
    posts = (_read_json(FORUM_DB_PATH, {"posts": {}}).get("posts") or [])
    target = str(parent_asin).strip().lower()
    out: list[dict] = []
    for post in posts:
        tags = post.get("tags") or []
        if any(str(t).strip().lower() == target for t in tags):
            out.append(post)
            continue
        if str(post.get("book_title") or "").strip().lower() == target:
            out.append(post)
    return out


def _save_user_accounts_all(accounts: dict) -> None:
    _write_json(USER_ACCOUNTS_PATH, accounts)


def _save_user_books_all(books: dict) -> None:
    _write_json(USER_BOOKS_PATH, books)


def _save_user_clubs_all(clubs: dict) -> None:
    _write_json(USER_CLUBS_PATH, clubs)


def _save_user_forums_all(forums: dict) -> None:
    _write_json(USER_FORUM_PATH, forums)


def _load_forum_db() -> dict:
    return _read_json(FORUM_DB_PATH, {"next_post_id": 1, "posts": []})


def _save_forum_db(db: dict) -> None:
    _write_json(FORUM_DB_PATH, db)

