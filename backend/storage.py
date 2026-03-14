"""Storage access layer for Bookish."""

from __future__ import annotations

import json
import os
import time
from decimal import Decimal
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
    USER_RECOMMENDATIONS_PATH,
    RECOMMENDED_BOOKS_SIZE,
    RECOMMENDED_EVENTS_SIZE,
    # AWS configuration
    IS_AWS,
    USER_RECOMMENDATIONS_TABLE,
    AWS_REGION,
    USER_ACCOUNTS_TABLE,
    USER_BOOKS_TABLE,
    USER_EVENTS_TABLE,
    FORUM_POSTS_TABLE,
    USER_FORUMS_TABLE,
    FORUM_POSTS_GSI,
    EVENTS_GSI,
    BOOKS_TABLE,
    EVENTS_TABLE,
    DATA_BUCKET,
    CDN_BASE_URL,
    DEFAULT_BOOK_IMAGE_KEY,
    TOP50_BOOKS_S3_KEY,
    REVIEWS_TOP50_BOOKS_S3_KEY,
    REVIEWS_TOP50_BOOKS_LOCAL_PATH,
)
from backend.data_loader import load_data as _load_ui_data


def _default_user_recommendations() -> dict:
    """Default shape for user_recommendations when missing."""
    return {
        "recommended_books": [],
        "recommended_events": [],
        "book_updated_at": 0,
        "events_soonest_expiry": 0,
        "adds_since_last_book_run": 0,
    }


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
    """Build public URL for an image stored in S3.
    Args:
        key: S3 object key (e.g. images/book_icon.png).
    Returns:
        Full URL string for the image.
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

    def get_user_recommendations(self, user_id: str) -> dict: ...
    def save_user_recommendations(self, user_id: str, item: dict) -> None: ...

    # Forum posts
    def load_forum_db(self) -> dict: ...
    def save_forum_db(self, db: dict) -> None: ...
    def get_forum_thread(self, parent_asin: str) -> list[dict]: ...
    def get_forum_thread_for_book(self, parent_asin: str) -> list[dict]: ...
    def get_forum_post(self, post_id: int) -> dict: ...
    def get_forum_posts_by_tag(self, tag: str) -> list[dict]: ...
    def update_forum_post(self, post_id: int, post: dict) -> None: ...

    # Books & events (main content)
    def get_book_details(self, parent_asin: str) -> dict | None: ...
    def get_book_metadata(self, parent_asin: str) -> dict | None: ...
    def get_book_by_title_author_key(self, title_author_key: str) -> dict | None: ...
    def get_event_details(self, event_id: str) -> dict | None: ...
    def get_spl_top50_checkout_books(self) -> list[dict]: ...
    def get_top50_review_books(self) -> list[dict]: ...
    def get_soonest_events(self, limit: int = 10) -> list[dict]: ...


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
        rec = dict(data.get(user_id) or {})
        return {
            "saved_forum_post_ids": rec.get("saved_forum_post_ids") or [],
            "liked_post_ids": rec.get("liked_post_ids") or [],
            "liked_comment_ids": rec.get("liked_comment_ids") or [],
        }

    def save_user_forums(self, user_id: str, item: dict) -> None:
        data = _read_json(USER_FORUM_PATH, {})
        existing = dict(data.get(user_id) or {})
        existing["saved_forum_post_ids"] = list(item.get("saved_forum_post_ids") or [])
        existing["liked_post_ids"] = list(item.get("liked_post_ids") or [])
        existing["liked_comment_ids"] = list(item.get("liked_comment_ids") or [])
        data[user_id] = existing
        _write_json(USER_FORUM_PATH, data)

    def get_user_events(self, user_id: str) -> dict:
        data = _read_json(USER_EVENTS_PATH, {})
        return dict(data.get(user_id) or {})

    def save_user_events(self, user_id: str, item: dict) -> None:
        data = _read_json(USER_EVENTS_PATH, {})
        data[user_id] = dict(item)
        _write_json(USER_EVENTS_PATH, data)

    def get_user_recommendations(self, user_id: str) -> dict:
        data = _read_json(USER_RECOMMENDATIONS_PATH, {})
        rec = dict(data.get(user_id) or {})
        rec.setdefault("recommended_books", [])
        rec.setdefault("recommended_events", [])
        rec.setdefault("book_updated_at", 0)
        rec.setdefault("events_soonest_expiry", 0)
        rec.setdefault("adds_since_last_book_run", 0)
        return rec

    def save_user_recommendations(self, user_id: str, item: dict) -> None:
        data = _read_json(USER_RECOMMENDATIONS_PATH, {})
        data[user_id] = dict(item)
        _write_json(USER_RECOMMENDATIONS_PATH, data)

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
            if str(post.get("parent_asin") or "").strip().lower() == target:
                out.append(post)
        return out

    def get_forum_thread_for_book(self, parent_asin: str) -> list[dict]:
        """Return the forum thread for a book by parent_asin. Same as get_forum_thread."""
        return self.get_forum_thread(parent_asin)

    def get_forum_post(self, post_id: int) -> dict:
        """Fetch a single forum post by id.
        Args:
            post_id: Post id.
        Returns:
            Post dict, or {} if not found.
        """
        posts = self.load_forum_db().get("posts") or []
        for post in posts:
            if int(post.get("id", -1)) == int(post_id):
                return dict(post)
        return {}

    def get_forum_posts_by_tag(self, tag: str) -> list[dict]:
        """Return posts that have the given tag (case-insensitive).
        Args:
            tag: Tag to filter by; empty string returns all posts.
        Returns:
            List of post dicts.
        """
        tag = str(tag or "").strip().lower()
        posts = self.load_forum_db().get("posts") or []
        if not tag:
            return list(posts)
        out: list[dict] = []
        for post in posts:
            tags = post.get("tags") or []
            if any(str(t).strip().lower() == tag for t in tags):
                out.append(post)
        return out

    def update_forum_post(self, post_id: int, post: dict) -> None:
        """Update a single post (e.g. after adding a comment). Loads db, replaces post, saves."""
        db = self.load_forum_db()
        posts = list(db.get("posts") or [])
        for i, p in enumerate(posts):
            if int(p.get("id", -1)) == int(post_id):
                posts[i] = dict(post)
                posts[i]["id"] = int(post_id)
                db["posts"] = posts
                _write_json(FORUM_DB_PATH, db)
                return
        raise ValueError(f"post not found: {post_id}")

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

    def get_spl_top50_checkout_books(self) -> list[dict]:
        """
        TODO: implement local SPL top-50 checkouts (e.g. from local JSON) if needed.
        CloudStorage fetches from S3; local returns empty list.
        """
        return []

    def get_top50_review_books(self) -> list[dict]:
        """Default/cold-start book recs: top 50 from Amazon reviews JSON. Local: read from REVIEWS_TOP50_BOOKS_LOCAL_PATH if exists."""
        if REVIEWS_TOP50_BOOKS_LOCAL_PATH.exists():
            try:
                data = json.loads(REVIEWS_TOP50_BOOKS_LOCAL_PATH.read_text(encoding="utf-8"))
                return data[:50] if isinstance(data, list) else []
            except Exception:
                return []
        return []

    def get_soonest_events(self, limit: int = 10) -> list[dict]:
        """Default/cold-start event recs: soonest-upcoming events. Local: no events source, return []."""
        _ = limit
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
        self._user_forums = dynamodb.Table(USER_FORUMS_TABLE)
        self._recommendations = dynamodb.Table(USER_RECOMMENDATIONS_TABLE)
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
    def _normalize_decimals(obj: Any) -> Any:
        """Convert DynamoDB Decimal to int in dicts/lists so callers get plain Python types."""
        if isinstance(obj, Decimal):
            return int(obj) if obj % 1 == 0 else float(obj)
        if isinstance(obj, dict):
            return {k: CloudStorage._normalize_decimals(v) for k, v in obj.items()}
        if isinstance(obj, list):
            return [CloudStorage._normalize_decimals(v) for v in obj]
        return obj

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
        """Fetch user account by email.
        Args:
            user_id: User email (used as primary key, lowercased).
        Returns:
            Account dict, or {} if not found.
        """
        key = {"user_email": str(user_id).strip().lower()}
        resp = self._accounts.get_item(Key=key)
        return dict(resp.get("Item") or {})

    def save_user_account(self, item: dict) -> None:
        """Upsert user account to DynamoDB.
        Args:
            item: Account dict; must contain user_id or email (used as primary key).
        Returns:
            None.
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
        """Fetch user library and genre preferences.
        Args:
            user_id: User email (primary key).
        Returns:
            Dict with library (in_progress, saved, finished lists) and genre_preferences; empty defaults if missing.
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
        """Return only the library shelves for a user.
        Args:
            user_id: User email.
        Returns:
            Dict with in_progress, saved, finished (each a list of parent_asin).
        """
        record = self.get_user_books(user_id)
        return record.get("library") or {"in_progress": [], "saved": [], "finished": []}

    def get_user_genre_preferences(self, user_id: str) -> list[str]:
        """Return only the genre preferences for a user.
        Args:
            user_id: User email.
        Returns:
            List of genre strings.
        """
        record = self.get_user_books(user_id)
        prefs = record.get("genre_preferences") or []
        return [str(g) for g in prefs]

    def save_user_books(self, user_id: str, item: dict) -> None:
        """Persist user library and genre preferences to DynamoDB.
        Args:
            user_id: User email.
            item: Dict with library and genre_preferences (same shape as get_user_books).
        Returns:
            None.
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
        """Add a book to one shelf and remove it from the others.
        Args:
            user_id: User email.
            shelf: One of in_progress, saved, finished.
            parent_asin: Book id.
        Returns:
            Updated library dict. Raises ValueError if user_books record does not exist.
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
        try:
            from backend.recommender_service import on_book_added_to_shelf
            on_book_added_to_shelf(str(user_id).strip().lower())
        except Exception:
            pass
        return library

    def remove_book_from_shelf(self, user_id: str, shelf: str, parent_asin: str) -> dict:
        """Remove a book from a single shelf.
        Args:
            user_id: User email.
            shelf: One of in_progress, saved, finished.
            parent_asin: Book id.
        Returns:
            Updated library dict. Raises ValueError if user_books record does not exist.
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
        """Overwrite the user's genre preferences.
        Args:
            user_id: User email.
            genres: List of genre strings to save.
        Returns:
            The saved genre list.
        """
        record = self.get_user_books(user_id)
        record["genre_preferences"] = [str(g) for g in (genres or [])]
        self.save_user_books(user_id, record)
        return record["genre_preferences"]
    
    # --------------------------------------------------------------
    # User events
    # --------------------------------------------------------------

    def get_user_events(self, user_id: str) -> dict:
        """Fetch user's saved event ids.
        Args:
            user_id: User email.
        Returns:
            Dict with key events (list of event_id strings).
        """
        key = {"user_email": str(user_id).strip().lower()}
        resp = self._events.get_item(Key=key)
        item = resp.get("Item") or {}
        if not isinstance(item, dict):
            return {"events": []}
        events = self._normalize_string_list(item.get("events"))
        return {"events": events}

    def save_user_events(self, user_id: str, item: dict) -> None:
        """Upsert user's saved events list to DynamoDB.
        Args:
            user_id: User email.
            item: Dict with key events (list of event_id strings).
        Returns:
            None.
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
        """Add an event to the user's saved list; creates record if missing.
        Args:
            user_id: User email.
            event_id: Event id to save.
        Returns:
            Updated list of saved event_ids.
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
        """Remove an event from the user's saved list.
        Args:
            user_id: User email.
            event_id: Event id to remove.
        Returns:
            Updated list of saved event_ids.
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
    # User recommendations (50 books, 10 events; when to re-run tracked here)
    # --------------------------------------------------------------
    # Partition key: user_email. Attributes: recommended_books, recommended_events, book_updated_at, events_soonest_expiry, adds_since_last_book_run.

    def get_user_recommendations(self, user_id: str) -> dict:
        """Fetch stored recommendations and run-tracking fields."""
        user_id = str(user_id).strip().lower()
        if not user_id:
            return _default_user_recommendations()
        key = {"user_email": user_id}
        try:
            resp = self._recommendations.get_item(Key=key)
        except Exception:
            return _default_user_recommendations()
        item = resp.get("Item") or {}
        if not item:
            return _default_user_recommendations()
        books = item.get("recommended_books")
        events = item.get("recommended_events")
        if not isinstance(books, list):
            books = []
        if not isinstance(events, list):
            events = []
        return {
            "recommended_books": list(books),
            "recommended_events": list(events),
            "book_updated_at": int(item.get("book_updated_at") or 0),
            "events_soonest_expiry": int(item.get("events_soonest_expiry") or 0),
            "adds_since_last_book_run": int(item.get("adds_since_last_book_run") or 0),
        }

    def save_user_recommendations(self, user_id: str, item: dict) -> None:
        """Persist recommendations and run-tracking fields."""
        user_id = str(user_id).strip().lower()
        if not user_id:
            return
        books = item.get("recommended_books") or []
        events = item.get("recommended_events") or []
        if not isinstance(books, list):
            books = []
        if not isinstance(events, list):
            events = []
        to_save = {
            "user_email": user_id,
            "recommended_books": books[:RECOMMENDED_BOOKS_SIZE],
            "recommended_events": events[:RECOMMENDED_EVENTS_SIZE],
            "book_updated_at": int(item.get("book_updated_at") or 0),
            "events_soonest_expiry": int(item.get("events_soonest_expiry") or 0),
            "adds_since_last_book_run": int(item.get("adds_since_last_book_run") or 0),
        }
        self._recommendations.put_item(Item=to_save)

    # --------------------------------------------------------------
    # User forums (saved posts, who liked what - separate from forum posts)
    # --------------------------------------------------------------
    # Table partition key: user_email (string). No sort key. Attributes: saved_forum_post_ids, liked_post_ids, liked_comment_ids.

    def get_user_forums(self, user_id: str) -> dict:
        """Get user forum state: saved_forum_post_ids, liked_post_ids, liked_comment_ids.
        Args:
            user_id: User email.
        Returns:
            Dict with saved_forum_post_ids (list of int), liked_post_ids (list of int), liked_comment_ids (list of str).
        """
        user_id = str(user_id).strip().lower()
        if not user_id:
            return {"saved_forum_post_ids": [], "liked_post_ids": [], "liked_comment_ids": []}
        key = {"user_email": user_id}
        try:
            resp = self._user_forums.get_item(Key=key)
        except Exception:
            return {"saved_forum_post_ids": [], "liked_post_ids": [], "liked_comment_ids": []}
        item = resp.get("Item") or {}
        if not item:
            return {"saved_forum_post_ids": [], "liked_post_ids": [], "liked_comment_ids": []}

        def int_list(v: Any) -> list[int]:
            if v is None:
                return []
            if isinstance(v, list):
                return [int(x) for x in v if x is not None]
            return []

        return {
            "saved_forum_post_ids": int_list(item.get("saved_forum_post_ids")),
            "liked_post_ids": int_list(item.get("liked_post_ids")),
            "liked_comment_ids": self._normalize_string_list(item.get("liked_comment_ids")),
        }

    def save_user_forums(self, user_id: str, item: dict) -> None:
        """Save user forum state to DynamoDB.
        Args:
            user_id: User email.
            item: Dict with saved_forum_post_ids, liked_post_ids, liked_comment_ids (lists).
        """
        user_id = str(user_id).strip().lower()
        if not user_id:
            return
        to_save = {
            "user_email": user_id,
            "saved_forum_post_ids": [int(x) for x in (item.get("saved_forum_post_ids") or [])],
            "liked_post_ids": [int(x) for x in (item.get("liked_post_ids") or [])],
            "liked_comment_ids": [str(x) for x in (item.get("liked_comment_ids") or [])],
        }
        self._user_forums.put_item(Item=to_save)
    
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
        """Load full forum state from DynamoDB.
        Returns:
            Dict with next_post_id (int) and posts (list of post dicts).
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
                    posts.append(self._normalize_decimals(post))
            if "LastEvaluatedKey" not in resp:
                break
            kwargs["ExclusiveStartKey"] = resp["LastEvaluatedKey"]

        # Sort by id so order is stable (e.g. newest first is handled by UI)
        posts.sort(key=lambda p: int(p.get("id") or 0))

        return {"next_post_id": int(next_post_id), "posts": posts}

    def save_forum_db(self, db: dict) -> None:
        """Replace all forum posts and meta in DynamoDB with the given state.
        Args:
            db: Dict with next_post_id (int) and posts (list of post dicts).
        Returns:
            None.
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

        # When post has parent_asin, add book title to tags at write time so users can filter by title.
        for post in posts:
            if post.get("parent_asin"):
                meta = self.get_book_metadata(str(post["parent_asin"]))
                if meta and meta.get("title"):
                    title_str = str(meta["title"]).strip()
                    tags = list(post.get("tags") or [])
                    if title_str and title_str not in tags:
                        tags.append(title_str)
                        post["tags"] = tags

        # Write new post items in batches of 25 (BatchWriteItem limit).
        # Omit parent_asin when None so GSI (parent_asin-index) does not get NULL key.
        with self._forum.batch_writer() as batch:
            for post in posts:
                post_id = post.get("id")
                if post_id is None:
                    continue
                item = dict(post)
                if item.get("parent_asin") is None:
                    item.pop("parent_asin", None)
                item["pk"] = self._FORUM_POSTS_PK
                item["sk"] = str(post_id)
                batch.put_item(Item=item)

        # Update meta
        self._forum.put_item(
            Item={
                "pk": self._FORUM_META_PK,
                "sk": self._FORUM_META_SK,
                "next_post_id": next_post_id,
            }
        )

    def get_forum_thread(self, parent_asin: str) -> list[dict]:
        """Return posts whose parent_asin or tag matches the given parent_asin.
        Uses GSI on parent_asin when FORUM_POSTS_GSI is set (efficient); otherwise loads all and filters.
        Args:
            parent_asin: Book id (matched case-insensitively against post parent_asin and tags).
        Returns:
            List of matching post dicts.
        """
        target = str(parent_asin).strip().lower()
        if not target:
            return []

        if FORUM_POSTS_GSI:
            # Query GSI by parent_asin (partition key); sort key = sk.
            out: list[dict] = []
            kwargs: dict = {
                "IndexName": FORUM_POSTS_GSI,
                "KeyConditionExpression": Key("parent_asin").eq(target),
            }
            while True:
                resp = self._forum.query(**kwargs)
                for item in resp.get("Items") or []:
                    post = {k: v for k, v in item.items() if k not in ("pk", "sk")}
                    if post:
                        out.append(self._normalize_decimals(post))
                if "LastEvaluatedKey" not in resp:
                    break
                kwargs["ExclusiveStartKey"] = resp["LastEvaluatedKey"]
            out.sort(key=lambda p: int(p.get("id") or 0))
            return out

        db = self.load_forum_db()
        posts = db.get("posts") or []
        out = []
        for post in posts:
            tags = post.get("tags") or []
            if any(str(t).strip().lower() == target for t in tags):
                out.append(post)
                continue
            if str(post.get("parent_asin") or "").strip().lower() == target:
                out.append(post)
        return out

    def get_forum_thread_for_book(self, parent_asin: str) -> list[dict]:
        """Return the forum thread for a book by parent_asin.
        Uses get_forum_thread; for large forums a GSI on parent_asin would make this O(posts per book).
        Args:
            parent_asin: Book id (matched against post parent_asin and tags).
        Returns:
            List of matching post dicts.
        """
        return self.get_forum_thread(parent_asin)

    def get_forum_post(self, post_id: int) -> dict:
        """Fetch a single forum post by id.
        Args:
            post_id: Post id.
        Returns:
            Post dict, or {} if not found.
        """
        try:
            resp = self._forum.get_item(
                Key={"pk": self._FORUM_POSTS_PK, "sk": str(int(post_id))}
            )
        except Exception:
            return {}
        item = resp.get("Item") or {}
        if not item:
            return {}
        out = {k: v for k, v in item.items() if k not in ("pk", "sk")}
        return self._normalize_decimals(out)

    def get_forum_posts_by_tag(self, tag: str) -> list[dict]:
        """Return posts that have the given tag (case-insensitive).
        Args:
            tag: Tag to filter by; empty string returns all posts.
        Returns:
            List of post dicts.
        """
        tag = str(tag or "").strip().lower()
        db = self.load_forum_db()
        posts = db.get("posts") or []
        if not tag:
            return list(posts)
        out: list[dict] = []
        for post in posts:
            tags = post.get("tags") or []
            if any(str(t).strip().lower() == tag for t in tags):
                out.append(post)
        return out

    def update_forum_post(self, post_id: int, post: dict) -> None:
        """Update a single post (e.g. after adding a comment). Single PutItem; no full load/save.
        Args:
            post_id: Post id.
            post: Full post dict (as from get_forum_post); must include id.
        Returns:
            None. Raises ValueError if post_id does not match post.get("id").
        """
        post_id = int(post_id)
        if int(post.get("id", -1)) != post_id:
            raise ValueError("post_id does not match post['id']")
        # When post has parent_asin, add book title to tags at write time so users can filter by title.
        if post.get("parent_asin"):
            meta = self.get_book_metadata(str(post["parent_asin"]))
            if meta and meta.get("title"):
                title_str = str(meta["title"]).strip()
                tags = list(post.get("tags") or [])
                if title_str and title_str not in tags:
                    tags.append(title_str)
                    post = dict(post)
                    post["tags"] = tags
        item = dict(post)
        if item.get("parent_asin") is None:
            item.pop("parent_asin", None)
        item["pk"] = self._FORUM_POSTS_PK
        item["sk"] = str(post_id)
        self._forum.put_item(Item=item)

    # ------------------------------------------------------------------
    # Main books & events tables 
    # ------------------------------------------------------------------

    def get_spl_top50_checkout_books(self) -> list[dict]:
        """Fetch SPL top-50 checkouts book list from S3.
        Returns:
            List of book dicts, or [] on missing bucket/key or error.
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

    def get_top50_review_books(self) -> list[dict]:
        """Default/cold-start book recs: top 50 from Amazon reviews JSON in S3 (books/reviews_top50_books.json)."""
        if not DATA_BUCKET:
            return []
        try:
            s3 = boto3.client("s3")
            resp = s3.get_object(Bucket=DATA_BUCKET, Key=REVIEWS_TOP50_BOOKS_S3_KEY)
            body = resp["Body"].read().decode("utf-8")
            data = json.loads(body)
            return data[:50] if isinstance(data, list) else []
        except Exception:
            return []

    def get_soonest_events(self, limit: int = 10) -> list[dict]:
        """Default/cold-start event recs: up to limit events sorted by soonest ttl. Uses GSI (type, ttl) when EVENTS_GSI set; else table scan."""
        if EVENTS_GSI:
            try:
                now = int(time.time())
                resp = self._events_main.query(
                    IndexName=EVENTS_GSI,
                    KeyConditionExpression=Key("type").eq("event") & Key("ttl").gte(now),
                    Limit=limit,
                    ScanIndexForward=True,
                )
                return list(resp.get("Items") or [])[:limit]
            except Exception:
                pass
        try:
            scan = self._events_main.scan()
            items = list(scan.get("Items") or [])
            while "LastEvaluatedKey" in scan:
                scan = self._events_main.scan(ExclusiveStartKey=scan["LastEvaluatedKey"])
                items.extend(scan.get("Items") or [])
            def sort_key(e: dict) -> int:
                t = e.get("ttl") or e.get("expiry") or e.get("start_time")
                if t is None:
                    return 0
                try:
                    return int(t)
                except (TypeError, ValueError):
                    return 0
            items.sort(key=sort_key)
            return items[:limit]
        except Exception:
            return []

    def get_book_metadata(self, parent_asin: str) -> dict | None:
        """Fetch book metadata (no description) from main books table.
        Args:
            parent_asin: Book id.
        Returns:
            Book metadata dict, or None if not found.
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
        """Look up book by title_author_key (GSI on main books table).
        Args:
            title_author_key: Lookup key.
        Returns:
            Dict with parent_asin and categories, or None if not found.
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
        """Fetch event by id from main events table.
        Args:
            event_id: Event id.
        Returns:
            Event dict, or None if not found.
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
        """Fetch full book record including description from S3 parquet.
        Args:
            parent_asin: Book id.
        Returns:
            Full book dict, or None if not found or read fails.
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


def get_storage() -> LocalStorage | CloudStorage:
    """Return the storage backend for the current environment (local or AWS)."""
    if IS_AWS:
        return CloudStorage()
    return LocalStorage()


# Module-level helpers below; callers can also use get_storage() for backend-agnostic access.


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
        if str(post.get("parent_asin") or "").strip().lower() == target:
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

