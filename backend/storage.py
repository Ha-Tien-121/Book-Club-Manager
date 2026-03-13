"""Storage access layer for Bookish."""

from __future__ import annotations

import json
from functools import lru_cache
from pathlib import Path
from typing import Any

from backend.config import FORUM_DB_PATH, PROCESSED_DIR, USER_ACCOUNTS_PATH, USER_BOOKS_PATH, USER_CLUBS_PATH, USER_FORUM_PATH
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

