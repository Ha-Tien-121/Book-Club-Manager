"""User stores: accounts, books, clubs, forum."""

from __future__ import annotations

import json
from pathlib import Path

from backend.config import (
    PROCESSED_DIR,
    USER_ACCOUNTS_PATH,
    USER_BOOKS_PATH,
    USER_CLUBS_PATH,
    USER_FORUM_PATH,
)


def _load_json_store(path: Path, default: dict) -> dict:
    """Load a JSON file or return default if missing/invalid."""
    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
    if not path.exists():
        return default
    with path.open("r", encoding="utf-8") as file_obj:
        try:
            return json.load(file_obj)
        except json.JSONDecodeError:
            return default


def _save_json_store(path: Path, data: dict) -> None:
    """Write a dict to JSON file."""
    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, indent=2), encoding="utf-8")


def _default_books_record() -> dict:
    return {
        "library": {"in_progress": [], "saved": [], "finished": []},
        "genre_preferences": [],
    }


def _default_clubs_record() -> dict:
    return {"club_ids": []}


def _default_forum_record() -> dict:
    return {"forum_posts": [], "saved_forum_post_ids": []}


def ensure_user_account_schema(record: dict) -> dict:
    """Ensure account record has user_id, email, name, password."""
    record.setdefault("user_id", record.get("email", ""))
    record.setdefault("email", "")
    record.setdefault("name", "")
    record.setdefault("password", "")
    return record


def ensure_user_books_schema(record: dict) -> dict:
    """Ensure books record has library and genre_preferences."""
    record.setdefault("library", {"in_progress": [], "saved": [], "finished": []})
    record.setdefault("genre_preferences", [])
    return record


def ensure_user_clubs_schema(record: dict) -> dict:
    """Ensure clubs record has club_ids."""
    record.setdefault("club_ids", [])
    return record


def ensure_user_forum_schema(record: dict) -> dict:
    """Ensure forum record has forum_posts and saved_forum_post_ids."""
    record.setdefault("forum_posts", [])
    record.setdefault("saved_forum_post_ids", [])
    return record


def _migrate_legacy_user_accounts(accounts_data: dict) -> dict | None:
    """Migrate legacy single-file user format into split stores."""
    users = accounts_data.get("users") or {}
    if not users:
        return None
    first = next(iter(users.values()), {})
    if "library" not in first and "club_ids" not in first:
        return None

    accounts = {"users": {}}
    books: dict = {}
    clubs: dict = {}
    forum: dict = {}
    for email, u in users.items():
        accounts["users"][email] = ensure_user_account_schema(
            {
                "user_id": email,
                "email": email,
                "name": u.get("name", email.split("@")[0]),
                "password": u.get("password", ""),
            }
        )
        books[email] = {
            "library": u.get("library")
            or {"in_progress": [], "saved": [], "finished": []},
            "genre_preferences": u.get("genre_preferences") or [],
        }
        clubs[email] = {"club_ids": u.get("club_ids") or []}
        forum[email] = {
            "forum_posts": u.get("forum_posts") or [],
            "saved_forum_post_ids": u.get("saved_forum_post_ids") or [],
        }

    _save_json_store(USER_ACCOUNTS_PATH, accounts)
    _save_json_store(USER_BOOKS_PATH, books)
    _save_json_store(USER_CLUBS_PATH, clubs)
    _save_json_store(USER_FORUM_PATH, forum)
    return {"accounts": accounts, "books": books, "clubs": clubs, "forum": forum}


def load_user_store() -> dict:
    """Load all user-related stores into one dict: accounts, books, clubs, forum."""
    accounts = _load_json_store(USER_ACCOUNTS_PATH, {"users": {}})
    if "users" not in accounts or not isinstance(accounts["users"], dict):
        accounts = {"users": {}}
    books = _load_json_store(USER_BOOKS_PATH, {})
    clubs = _load_json_store(USER_CLUBS_PATH, {})
    forum = _load_json_store(USER_FORUM_PATH, {})
    migrated = _migrate_legacy_user_accounts(accounts)
    if migrated is not None:
        return migrated
    return {"accounts": accounts, "books": books, "clubs": clubs, "forum": forum}


def save_user_accounts(store: dict) -> None:
    _save_json_store(USER_ACCOUNTS_PATH, store["accounts"])


def save_user_books(store: dict) -> None:
    _save_json_store(USER_BOOKS_PATH, store["books"])


def save_user_clubs(store: dict) -> None:
    _save_json_store(USER_CLUBS_PATH, store["clubs"])


def save_user_forum(store: dict) -> None:
    _save_json_store(USER_FORUM_PATH, store["forum"])


def get_current_user(store: dict, email: str) -> dict | None:
    """Return merged user dict from accounts/books/clubs/forum."""
    users = store["accounts"].get("users") or {}
    acc = users.get(email)
    if not acc:
        return None
    books_rec = store["books"].setdefault(email, _default_books_record())
    clubs_rec = store["clubs"].setdefault(email, _default_clubs_record())
    forum_rec = store["forum"].setdefault(email, _default_forum_record())
    ensure_user_books_schema(books_rec)
    ensure_user_clubs_schema(clubs_rec)
    ensure_user_forum_schema(forum_rec)
    return {
        "user_id": email,
        "email": acc.get("email", email),
        "name": acc.get("name", email.split("@")[0]),
        "password": acc.get("password", ""),
        "library": books_rec["library"],
        "genre_preferences": books_rec["genre_preferences"],
        "club_ids": clubs_rec["club_ids"],
        "forum_posts": forum_rec["forum_posts"],
        "saved_forum_post_ids": forum_rec["saved_forum_post_ids"],
    }


def create_user(store: dict, email: str, password: str) -> dict:
    """Create user across the four stores and persist them."""
    users = store["accounts"].setdefault("users", {})
    users[email] = ensure_user_account_schema(
        {
            "user_id": email,
            "email": email,
            "name": email.split("@")[0],
            "password": password,
        }
    )
    store["books"][email] = _default_books_record()
    store["clubs"][email] = _default_clubs_record()
    store["forum"][email] = _default_forum_record()
    save_user_accounts(store)
    save_user_books(store)
    save_user_clubs(store)
    save_user_forum(store)
    return users[email]

