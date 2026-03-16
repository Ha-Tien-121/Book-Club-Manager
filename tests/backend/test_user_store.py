"""
Tests for Book-Club-Manager.backend.user_store.

Covers:
- _load_json_store / _save_json_store: happy path, missing file, invalid JSON.
- ensure_*_schema helpers: defaulting of required fields.
- _migrate_legacy_user_accounts: legacy users -> split stores and return structure.
- load_user_store: normal load vs legacy migration.
- save_user_* helpers: delegate to _save_json_store with correct paths.
- get_current_user: merges account/books/clubs/forum into a single dict.
- create_user: populates all four stores and calls save_* helpers.
"""

import json
import sys
from pathlib import Path
from typing import Any, Dict
from unittest.mock import MagicMock, patch


def _ensure_inner_project_on_path() -> None:
    """Ensure inner Book-Club-Manager root is on sys.path when running from outer root."""
    tests_dir = Path(__file__).resolve().parents[1]
    repo_root = tests_dir.parent
    inner_root = repo_root / "Book-Club-Manager"
    if inner_root.is_dir() and str(inner_root) not in sys.path:
        sys.path.insert(0, str(inner_root))


_ensure_inner_project_on_path()


import backend.user_store as user_store  # noqa: E402


def test_load_json_store_missing_file_returns_default(tmp_path: Path) -> None:
    default = {"x": 1}
    # Patch PROCESSED_DIR to our tmp dir so mkdir points there.
    with patch.object(user_store, "PROCESSED_DIR", tmp_path):
        result = user_store._load_json_store(tmp_path / "missing.json", default)

    assert result == default


def test_load_json_store_invalid_json_returns_default(tmp_path: Path) -> None:
    target = tmp_path / "store.json"
    target.write_text("{not: valid json}", encoding="utf-8")
    default = {"ok": True}

    with patch.object(user_store, "PROCESSED_DIR", tmp_path):
        result = user_store._load_json_store(target, default)

    assert result == default


def test_save_json_store_writes_pretty_json(tmp_path: Path) -> None:
    target = tmp_path / "store.json"
    data = {"a": 1}

    with patch.object(user_store, "PROCESSED_DIR", tmp_path):
        user_store._save_json_store(target, data)

    loaded = json.loads(target.read_text(encoding="utf-8"))
    assert loaded == data


def test_ensure_user_account_schema_sets_defaults() -> None:
    rec: Dict[str, Any] = {"email": "u@example.com"}
    out = user_store.ensure_user_account_schema(rec)
    assert out["user_id"] == "u@example.com"
    assert out["email"] == "u@example.com"
    assert "name" in out and "password" in out


def test_ensure_user_books_schema_sets_library_and_genres() -> None:
    rec: Dict[str, Any] = {}
    out = user_store.ensure_user_books_schema(rec)
    assert "library" in out
    assert set(out["library"].keys()) == {"in_progress", "saved", "finished"}
    assert out["genre_preferences"] == []


def test_ensure_user_clubs_schema_sets_club_ids() -> None:
    rec: Dict[str, Any] = {}
    out = user_store.ensure_user_clubs_schema(rec)
    assert out["club_ids"] == []


def test_ensure_user_forum_schema_sets_forum_fields() -> None:
    rec: Dict[str, Any] = {}
    out = user_store.ensure_user_forum_schema(rec)
    assert out["forum_posts"] == []
    assert out["saved_forum_post_ids"] == []


def test_migrate_legacy_user_accounts_returns_none_when_no_users() -> None:
    data: Dict[str, Any] = {}
    assert user_store._migrate_legacy_user_accounts(data) is None


def test_migrate_legacy_user_accounts_splits_into_four_stores(tmp_path: Path) -> None:
    # Legacy structure with library/club_ids/forum_posts.
    legacy = {
        "users": {
            "u@example.com": {
                "name": "User",
                "password": "pw",
                "library": {"saved": ["b1"]},
                "genre_preferences": ["Fantasy"],
                "club_ids": [1, 2],
                "forum_posts": [{"id": 1}],
                "saved_forum_post_ids": [1],
            }
        }
    }

    # Redirect paths to tmp files so we don't touch real data.
    accounts_path = tmp_path / "accounts.json"
    books_path = tmp_path / "books.json"
    clubs_path = tmp_path / "clubs.json"
    forum_path = tmp_path / "forum.json"
    with patch.object(user_store, "USER_ACCOUNTS_PATH", accounts_path), patch.object(
        user_store, "USER_BOOKS_PATH", books_path
    ), patch.object(user_store, "USER_CLUBS_PATH", clubs_path), patch.object(
        user_store, "USER_FORUM_PATH", forum_path
    ), patch.object(
        user_store, "PROCESSED_DIR", tmp_path
    ):
        out = user_store._migrate_legacy_user_accounts(legacy)

    assert out is not None
    assert "accounts" in out and "books" in out and "clubs" in out and "forum" in out
    # Accounts must contain normalized user entry.
    acc_users = out["accounts"]["users"]
    assert "u@example.com" in acc_users
    # Files should be written.
    assert accounts_path.exists()
    assert books_path.exists()
    assert clubs_path.exists()
    assert forum_path.exists()


def test_load_user_store_uses_migrated_when_available(tmp_path: Path) -> None:
    # Start with legacy accounts in USER_ACCOUNTS_PATH.
    legacy = {
        "users": {
            "u@example.com": {
                "library": {"saved": []},
                "club_ids": [],
                "forum_posts": [],
                "saved_forum_post_ids": [],
            }
        }
    }
    accounts_path = tmp_path / "accounts.json"
    accounts_path.write_text(json.dumps(legacy), encoding="utf-8")
    with patch.object(user_store, "USER_ACCOUNTS_PATH", accounts_path), patch.object(
        user_store, "USER_BOOKS_PATH", tmp_path / "books.json"
    ), patch.object(
        user_store, "USER_CLUBS_PATH", tmp_path / "clubs.json"
    ), patch.object(
        user_store, "USER_FORUM_PATH", tmp_path / "forum.json"
    ), patch.object(
        user_store, "PROCESSED_DIR", tmp_path
    ):
        store = user_store.load_user_store()

    # Because migration occurs, top-level keys should be accounts/books/clubs/forum.
    assert {"accounts", "books", "clubs", "forum"} <= set(store.keys())


def test_save_user_helpers_delegate_to_save_json_store(tmp_path: Path) -> None:
    store = {
        "accounts": {"users": {}},
        "books": {},
        "clubs": {},
        "forum": {},
    }
    with patch.object(user_store, "_save_json_store") as m_save, patch.object(
        user_store, "USER_ACCOUNTS_PATH", tmp_path / "a.json"
    ), patch.object(
        user_store, "USER_BOOKS_PATH", tmp_path / "b.json"
    ), patch.object(
        user_store, "USER_CLUBS_PATH", tmp_path / "c.json"
    ), patch.object(
        user_store, "USER_FORUM_PATH", tmp_path / "d.json"
    ):
        user_store.save_user_accounts(store)
        user_store.save_user_books(store)
        user_store.save_user_clubs(store)
        user_store.save_user_forum(store)

    # Called four times with each corresponding path and sub-dict.
    paths = [call_args[0][0] for call_args in m_save.call_args_list]
    assert len(paths) == 4


def test_get_current_user_merges_multiple_stores_and_sets_defaults() -> None:
    store = {
        "accounts": {
            "users": {
                "u@example.com": {
                    "email": "u@example.com",
                    "name": "User",
                    "password": "pw",
                }
            }
        },
        "books": {},
        "clubs": {},
        "forum": {},
    }

    user = user_store.get_current_user(store, "u@example.com")

    assert user is not None
    assert user["email"] == "u@example.com"
    assert user["name"] == "User"
    assert set(user["library"].keys()) == {"in_progress", "saved", "finished"}
    assert user["genre_preferences"] == []
    assert user["club_ids"] == []
    assert user["forum_posts"] == []
    assert user["saved_forum_post_ids"] == []


def test_get_current_user_returns_none_for_missing() -> None:
    store = {"accounts": {"users": {}}, "books": {}, "clubs": {}, "forum": {}}
    assert user_store.get_current_user(store, "missing@example.com") is None


def test_create_user_populates_stores_and_calls_saves(tmp_path: Path) -> None:
    store: Dict[str, Any] = {
        "accounts": {"users": {}},
        "books": {},
        "clubs": {},
        "forum": {},
    }
    with patch.object(user_store, "save_user_accounts") as m_acc, patch.object(
        user_store, "save_user_books"
    ) as m_books, patch.object(
        user_store, "save_user_clubs"
    ) as m_clubs, patch.object(
        user_store, "save_user_forum"
    ) as m_forum:
        created = user_store.create_user(store, "u@example.com", "pw")

    assert created["email"] == "u@example.com"
    assert "u@example.com" in store["books"]
    assert "u@example.com" in store["clubs"]
    assert "u@example.com" in store["forum"]
    m_acc.assert_called_once_with(store)
    m_books.assert_called_once_with(store)
    m_clubs.assert_called_once_with(store)
    m_forum.assert_called_once_with(store)

