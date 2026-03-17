"""
Tests for Book-Club-Manager.backend.local_storage.

`backend.local_storage.LocalStorage` extends the base LocalStorage from
`backend.storage` and adds SQLite-backed helpers for book metadata/details.

These tests verify:
- The subclass relationship to the base LocalStorage.
- Reading book metadata/details from a local SQLite `books.db`.
- Fallback behavior to the base implementation when no row is found.
"""

import json
import sqlite3
import sys
import types
from pathlib import Path
from typing import Any, Dict, Optional
from unittest.mock import MagicMock, patch


def _ensure_inner_project_on_path() -> None:
    """Ensure inner Book-Club-Manager root is on sys.path when running from outer root."""
    tests_dir = Path(__file__).resolve().parents[1]
    repo_root = tests_dir.parent
    inner_root = repo_root / "Book-Club-Manager"
    if inner_root.is_dir() and str(inner_root) not in sys.path:
        sys.path.insert(0, str(inner_root))


_ensure_inner_project_on_path()


def _stub_boto3() -> None:
    """Install a lightweight boto3 stub so backend.storage can import cleanly."""
    if "boto3" in sys.modules:
        return
    _boto3 = types.ModuleType("boto3")
    _conditions = types.ModuleType("boto3.dynamodb.conditions")
    setattr(_conditions, "Attr", object())
    setattr(_conditions, "Key", object())
    setattr(_boto3, "dynamodb", types.SimpleNamespace(conditions=_conditions))
    sys.modules["boto3"] = _boto3
    sys.modules["boto3.dynamodb"] = types.ModuleType("boto3.dynamodb")
    sys.modules["boto3.dynamodb.conditions"] = _conditions


def _make_local_storage() -> "Any":
    """Return a LocalStorage instance with boto3 stubbed."""
    _stub_boto3()
    import backend.local_storage as local_mod  # type: ignore  # noqa: E402

    return local_mod.LocalStorage()


def test_local_storage_subclasses_base_local_storage() -> None:
    """backend.local_storage.LocalStorage should extend backend.storage.LocalStorage."""
    _stub_boto3()
    import backend.local_storage as local_mod  # type: ignore  # noqa: E402
    import backend.storage as storage_mod  # type: ignore  # noqa: E402

    assert hasattr(local_mod, "LocalStorage")
    assert issubclass(local_mod.LocalStorage, storage_mod.LocalStorage)


def test_books_db_path_uses_config_processed_dir(tmp_path: Path) -> None:
    """_books_db_path should use backend.config.PROCESSED_DIR when present."""
    storage = _make_local_storage()
    # Patch backend.config.PROCESSED_DIR so _books_db_path points at tmp_path/books.db.
    with patch("backend.config.PROCESSED_DIR", tmp_path):
        out = storage._books_db_path()
    assert out == tmp_path / "books.db"


def test_fetch_book_row_returns_none_when_db_missing(tmp_path: Path) -> None:
    """_fetch_book_row returns None if the db file is missing."""
    storage = _make_local_storage()
    missing_db = tmp_path / "books.db"
    with patch.object(storage, "_books_db_path", return_value=missing_db):
        assert storage._fetch_book_row("P1") is None


def test_fetch_book_row_handles_sqlite_error(tmp_path: Path) -> None:
    """_fetch_book_row returns None on sqlite3.Error."""
    storage = _make_local_storage()
    db_path = tmp_path / "books.db"
    db_path.write_text("", encoding="utf-8")
    with patch.object(storage, "_books_db_path", return_value=db_path), patch(
        "backend.local_storage.sqlite3.connect", side_effect=sqlite3.Error("boom")
    ):
        assert storage._fetch_book_row("P1") is None


def _create_books_db(db_path: Path, rows: list[tuple[Any, ...]]) -> None:
    """Create a minimal books.db with the schema expected by LocalStorage."""
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    cur.execute(
        """
        CREATE TABLE books (
            parent_asin TEXT PRIMARY KEY,
            title TEXT,
            author_name TEXT,
            average_rating REAL,
            rating_number INTEGER,
            description TEXT,
            images TEXT,
            categories TEXT,
            title_author_key TEXT
        )
        """
    )
    cur.executemany(
        """
        INSERT INTO books (
            parent_asin, title, author_name, average_rating, rating_number,
            description, images, categories, title_author_key
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        rows,
    )
    conn.commit()
    conn.close()


def test_get_book_metadata_reads_from_local_sqlite(tmp_path: Path) -> None:
    """get_book_metadata should return a dict from local books.db without description."""
    db_path = tmp_path / "books.db"
    _create_books_db(
        db_path,
        [
            (
                "P1",
                "Title",
                "Author",
                4.5,
                10,
                json.dumps(["Long description"]),
                "img.png",
                json.dumps(["Fantasy", "Sci-Fi"]),
                "title|author",
            )
        ],
    )
    storage = _make_local_storage()

    with patch.object(storage, "_books_db_path", return_value=db_path):
        meta = storage.get_book_metadata("P1")

    assert meta is not None
    assert meta["parent_asin"] == "P1"
    assert meta["title"] == "Title"
    assert meta["author_name"] == "Author"
    assert meta["average_rating"] == 4.5
    assert meta["rating_number"] == 10
    assert meta["images"] == "img.png"
    assert meta["categories"] == ["Fantasy", "Sci-Fi"]
    # Metadata call should not include description.
    assert "description" not in meta


def test_row_to_book_dict_loads_or_empty_branches() -> None:
    """Cover _row_to_book_dict parsing branches for categories/description."""
    storage = _make_local_storage()

    class FakeRow:
        def __init__(self, mapping: Dict[str, Any]):
            "Support __init__ for test doubles."
            self._m = mapping

        def __getitem__(self, k: str) -> Any:
            "Support __getitem__ for test doubles."
            return self._m[k]

    row = FakeRow(
        {
            "parent_asin": "P9",
            "title": "T",
            "author_name": "A",
            "average_rating": None,
            "rating_number": None,
            "images": None,
            # list branch
            "categories": ["X", 1],
            # json.loads non-list branch (dict -> ["{'a': 1}"])
            "description": json.dumps({"a": 1}),
            "title_author_key": "",
        }
    )
    out = storage._row_to_book_dict(row)  # type: ignore[arg-type]
    assert out["categories"] == ["X", "1"]
    assert out["description"] == ["{'a': 1}"]

    # json.loads failure branch -> returns [s]
    row2 = FakeRow(
        {
            "parent_asin": "P10",
            "title": "",
            "author_name": "",
            "average_rating": 0,
            "rating_number": 0,
            "images": "",
            "categories": "not-json",
            "description": "not-json",
            "title_author_key": "",
        }
    )
    out2 = storage._row_to_book_dict(row2)  # type: ignore[arg-type]
    assert out2["categories"] == ["not-json"]
    assert out2["description"] == ["not-json"]

    # empty / whitespace branches -> []
    row3 = FakeRow(
        {
            "parent_asin": "P11",
            "title": "",
            "author_name": "",
            "average_rating": 0,
            "rating_number": 0,
            "images": "",
            "categories": None,
            "description": "   ",
            "title_author_key": "",
        }
    )
    out3 = storage._row_to_book_dict(row3)  # type: ignore[arg-type]
    assert out3["categories"] == []
    assert out3["description"] == []


def test_get_book_details_reads_full_details_from_local_sqlite(tmp_path: Path) -> None:
    """get_book_details should return full details (including description) from books.db."""
    db_path = tmp_path / "books.db"
    _create_books_db(
        db_path,
        [
            (
                "P2",
                "T2",
                "A2",
                3.0,
                5,
                json.dumps(["Desc1", "Desc2"]),
                "",
                "Fantasy",
                "t2|a2",
            )
        ],
    )
    storage = _make_local_storage()

    with patch.object(storage, "_books_db_path", return_value=db_path):
        details = storage.get_book_details("P2")

    assert details is not None
    assert details["parent_asin"] == "P2"
    # Description should be a list (from _row_to_book_dict).
    assert details["description"] == ["Desc1", "Desc2"]
    # Categories string should be normalized to list.
    assert details["categories"] == ["Fantasy"]


def test_get_book_metadata_and_details_return_none_for_empty_parent_asin() -> None:
    "Test get book metadata and details return none for empty parent asin."
    storage = _make_local_storage()
    assert storage.get_book_metadata("") is None
    assert storage.get_book_metadata("   ") is None
    assert storage.get_book_details("") is None
    assert storage.get_book_details("   ") is None


def test_get_book_metadata_falls_back_to_base_when_no_row() -> None:
    """If books.db has no row, LocalStorage should call the base implementation."""
    storage = _make_local_storage()

    with patch.object(storage, "_fetch_book_row", return_value=None), patch(
        "backend.storage.LocalStorage.get_book_metadata", return_value={"from": "base"}
    ) as m_base:
        out = storage.get_book_metadata("P3")

    m_base.assert_called_once_with("P3")
    assert out == {"from": "base"}


def test_get_book_details_falls_back_to_base_when_no_row() -> None:
    """If books.db has no row for details, LocalStorage should call the base implementation."""
    storage = _make_local_storage()

    with patch.object(storage, "_fetch_book_row", return_value=None), patch(
        "backend.storage.LocalStorage.get_book_details", return_value={"from": "base"}
    ) as m_base:
        out = storage.get_book_details("P4")

    m_base.assert_called_once_with("P4")
    assert out == {"from": "base"}


