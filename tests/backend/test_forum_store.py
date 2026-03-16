"""
Tests for Book-Club-Manager.backend.forum_store.

Covers:
- load_forum_store when the forum DB file does not exist: seeds from seed_posts
  and writes a normalized store to the configured FORUM_DB_PATH.
- load_forum_store when the forum DB file exists with valid JSON: returns the
  loaded structure and normalizes missing fields on posts/comments.
- load_forum_store when the file contains invalid JSON: falls back to an empty
  store with sane defaults.
- save_forum_store: writes the provided store to FORUM_DB_PATH with JSON.
"""

import json
import sys
from pathlib import Path
from typing import List, Dict, Any


def _ensure_inner_project_on_path() -> None:
    """Ensure inner Book-Club-Manager root is on sys.path when running from outer root."""
    tests_dir = Path(__file__).resolve().parents[1]
    repo_root = tests_dir.parent
    inner_root = repo_root / "Book-Club-Manager"
    if inner_root.is_dir() and str(inner_root) not in sys.path:
        sys.path.insert(0, str(inner_root))


_ensure_inner_project_on_path()


import backend.forum_store as forum_store  # noqa: E402


def test_load_forum_store_seeds_when_file_missing(tmp_path: Path) -> None:
    """If FORUM_DB_PATH does not exist, load_forum_store seeds from seed_posts."""
    seed_posts: List[Dict[str, Any]] = [
        {"title": "First", "author": "u@x.com", "preview": "Hello", "replies": 2, "likes": 3, "created_at": 123}
    ]
    forum_path = tmp_path / "forum.json"

    # Redirect config paths to tmp so we don't touch real data.
    # We patch module-level constants directly.
    forum_store.FORUM_DB_PATH = forum_path  # type: ignore[attr-defined]
    forum_store.PROCESSED_DIR = tmp_path  # type: ignore[attr-defined]

    store = forum_store.load_forum_store(seed_posts)

    assert forum_path.exists()
    assert store["next_post_id"] == 2
    assert len(store["posts"]) == 1
    post = store["posts"][0]
    assert post["id"] == 1
    assert post["title"] == "First"
    assert post["author"] == "u@x.com"
    assert post["preview"] == "Hello"
    assert post["replies"] == 2
    assert post["likes"] == 3
    # Seeded posts should have normalized fields.
    assert post["parent_asin"] is None
    assert post["book_title"] is None
    assert post["tags"] == []
    assert post["liked_by"] == []
    assert post["comments"] == []


def test_load_forum_store_reads_existing_file_and_normalizes(tmp_path: Path) -> None:
    """Existing JSON is loaded and missing fields normalized."""
    forum_path = tmp_path / "forum.json"
    raw_store = {
        # next_post_id missing on purpose to hit normalization
        "posts": [
            {
                "id": 5,
                "title": "Existing",
                "author": "u@x.com",
                "preview": "Hi",
                # missing liked_by, comments, parent_asin, book_title, tags
            }
        ]
    }
    forum_path.write_text(json.dumps(raw_store), encoding="utf-8")
    forum_store.FORUM_DB_PATH = forum_path  # type: ignore[attr-defined]
    forum_store.PROCESSED_DIR = tmp_path  # type: ignore[attr-defined]

    store = forum_store.load_forum_store(seed_posts=[])

    assert store["next_post_id"] == len(store["posts"]) + 1
    assert len(store["posts"]) == 1
    post = store["posts"][0]
    assert post["id"] == 5
    assert post["liked_by"] == []
    assert post["comments"] == []
    assert post["parent_asin"] is None
    assert post["book_title"] is None
    assert post["tags"] == []


def test_load_forum_store_invalid_json_falls_back_to_empty_store(tmp_path: Path) -> None:
    """Invalid JSON yields a default empty store."""
    forum_path = tmp_path / "forum.json"
    forum_path.write_text("{not: valid json}", encoding="utf-8")
    forum_store.FORUM_DB_PATH = forum_path  # type: ignore[attr-defined]
    forum_store.PROCESSED_DIR = tmp_path  # type: ignore[attr-defined]

    store = forum_store.load_forum_store(seed_posts=[])

    assert store["next_post_id"] == 1
    assert store["posts"] == []


def test_save_forum_store_writes_json(tmp_path: Path) -> None:
    forum_path = tmp_path / "forum.json"
    forum_store.FORUM_DB_PATH = forum_path  # type: ignore[attr-defined]
    forum_store.PROCESSED_DIR = tmp_path  # type: ignore[attr-defined]

    input_store = {"next_post_id": 3, "posts": [{"id": 1}, {"id": 2}]}

    forum_store.save_forum_store(input_store)

    assert forum_path.exists()
    loaded = json.loads(forum_path.read_text(encoding="utf-8"))
    assert loaded == input_store

