"""
Tests for backend.services.library_service.

Covers add_book_to_library, remove_book_from_library, get_user_library,
get_shelf_for_book, is_book_in_library, get_library_with_details,
get_user_preferences, update_user_preferences, update_book_status, remove_book_from_shelf.
"""
from __future__ import annotations

import sys
import types
import unittest
from unittest.mock import MagicMock, patch

# Avoid loading real boto3 when backend.storage is imported.
if "boto3" not in sys.modules:
    _boto3 = MagicMock()
    _conditions = types.ModuleType("boto3.dynamodb.conditions")
    _conditions.Attr = MagicMock()
    _conditions.Key = MagicMock()
    _boto3.dynamodb.conditions = _conditions
    sys.modules["boto3"] = _boto3
    sys.modules["boto3.dynamodb"] = MagicMock()
    sys.modules["boto3.dynamodb.conditions"] = _conditions

# library_service imports on_book_added_to_shelf from backend.recommender_service.
if "backend.recommender_service" not in sys.modules:
    _rec_mod = types.ModuleType("backend.recommender_service")
    _rec_mod.on_book_added_to_shelf = MagicMock()
    sys.modules["backend.recommender_service"] = _rec_mod

from backend.services import library_service  # noqa: E402


def _make_rec(library=None, genre_preferences=None):
    lib = library or {"in_progress": [], "saved": [], "finished": []}
    rec = {"library": lib, "genre_preferences": genre_preferences or []}
    return rec


@patch("backend.services.library_service.get_storage")
class TestAddBookToLibrary(unittest.TestCase):
    """Tests for add_book_to_library."""

    def test_adds_book_to_shelf_and_saves(
        self, mock_get_storage: MagicMock
    ) -> None:
        store = MagicMock()
        store.get_user_books.return_value = _make_rec()
        store.get_book_metadata.return_value = None
        mock_get_storage.return_value = store

        result = library_service.add_book_to_library("u@x.com", "B1", "saved")

        self.assertIn("B1", result["library"]["saved"])
        self.assertEqual(result["library"]["in_progress"], [])
        self.assertEqual(result["library"]["finished"], [])
        store.save_user_books.assert_called_once()
        import backend.recommender_service as _rec
        _rec.on_book_added_to_shelf.assert_called_once_with("u@x.com")

    def test_invalid_shelf_raises(self, mock_get_storage: MagicMock) -> None:
        mock_get_storage.return_value = MagicMock()
        with self.assertRaises(ValueError) as ctx:
            library_service.add_book_to_library("u@x.com", "B1", "invalid")
        self.assertIn("invalid shelf", str(ctx.exception))

    def test_noop_when_book_already_only_on_shelf(
        self, mock_get_storage: MagicMock
    ) -> None:
        import backend.recommender_service as _rec
        _rec.on_book_added_to_shelf.reset_mock()

        store = MagicMock()
        store.get_user_books.return_value = _make_rec(
            library={"in_progress": [], "saved": ["B1"], "finished": []}
        )
        mock_get_storage.return_value = store

        result = library_service.add_book_to_library("u@x.com", "B1", "saved")

        store.save_user_books.assert_not_called()
        _rec.on_book_added_to_shelf.assert_not_called()
        # No-op: book was already only on this shelf; code mutates lib then returns without persisting
        self.assertIn("library", result)

    def test_merges_book_categories_into_preferences(
        self, mock_get_storage: MagicMock
    ) -> None:
        store = MagicMock()
        store.get_user_books.return_value = _make_rec()
        store.get_book_metadata.return_value = {"categories": ["Fantasy", "Adventure"]}
        mock_get_storage.return_value = store

        result = library_service.add_book_to_library("u@x.com", "B2", "in_progress")

        self.assertIn("Fantasy", result["genre_preferences"])
        self.assertIn("Adventure", result["genre_preferences"])
        store.save_user_books.assert_called_once()


@patch("backend.services.library_service.get_storage")
class TestRemoveBookFromLibrary(unittest.TestCase):
    """Tests for remove_book_from_library."""

    def test_removes_book_from_all_shelves(
        self, mock_get_storage: MagicMock
    ) -> None:
        store = MagicMock()
        store.get_user_books.return_value = _make_rec(
            library={"in_progress": ["B1"], "saved": [], "finished": []}
        )
        store.get_book_metadata.return_value = {"categories": ["Fantasy"]}
        mock_get_storage.return_value = store

        result = library_service.remove_book_from_library("u@x.com", "B1")

        self.assertEqual(result["library"]["in_progress"], [])
        store.save_user_books.assert_called_once()


@patch("backend.services.library_service.get_storage")
class TestGetUserLibrary(unittest.TestCase):
    """Tests for get_user_library."""

    def test_returns_library_shelves(self, mock_get_storage: MagicMock) -> None:
        store = MagicMock()
        store.get_user_books.return_value = _make_rec(
            library={"in_progress": ["A"], "saved": ["B"], "finished": ["C"]}
        )
        mock_get_storage.return_value = store

        result = library_service.get_user_library("u@x.com")

        self.assertEqual(result["in_progress"], ["A"])
        self.assertEqual(result["saved"], ["B"])
        self.assertEqual(result["finished"], ["C"])


@patch("backend.services.library_service.get_storage")
class TestGetShelfForBook(unittest.TestCase):
    """Tests for get_shelf_for_book."""

    def test_returns_shelf_when_found(self, mock_get_storage: MagicMock) -> None:
        store = MagicMock()
        store.get_user_books.return_value = _make_rec(
            library={"in_progress": [], "saved": ["B1"], "finished": []}
        )
        mock_get_storage.return_value = store

        self.assertEqual(
            library_service.get_shelf_for_book("u@x.com", "B1"),
            "saved",
        )

    def test_returns_none_when_not_in_library(
        self, mock_get_storage: MagicMock
    ) -> None:
        store = MagicMock()
        store.get_user_books.return_value = _make_rec()
        mock_get_storage.return_value = store

        self.assertIsNone(library_service.get_shelf_for_book("u@x.com", "X99"))

    def test_returns_none_for_empty_user_id(
        self, mock_get_storage: MagicMock
    ) -> None:
        mock_get_storage.return_value = MagicMock()
        self.assertIsNone(library_service.get_shelf_for_book("", "B1"))

    def test_returns_none_for_empty_book_id(
        self, mock_get_storage: MagicMock
    ) -> None:
        mock_get_storage.return_value = MagicMock()
        self.assertIsNone(library_service.get_shelf_for_book("u@x.com", ""))


@patch("backend.services.library_service.get_storage")
class TestIsBookInLibrary(unittest.TestCase):
    """Tests for is_book_in_library."""

    def test_returns_true_when_on_shelf(self, mock_get_storage: MagicMock) -> None:
        store = MagicMock()
        store.get_user_books.return_value = _make_rec(
            library={"in_progress": ["B1"], "saved": [], "finished": []}
        )
        mock_get_storage.return_value = store

        self.assertTrue(library_service.is_book_in_library("u@x.com", "B1"))

    def test_returns_false_when_not_in_library(
        self, mock_get_storage: MagicMock
    ) -> None:
        store = MagicMock()
        store.get_user_books.return_value = _make_rec()
        mock_get_storage.return_value = store

        self.assertFalse(library_service.is_book_in_library("u@x.com", "B99"))


@patch("backend.services.books_service.get_book_detail")
@patch("backend.services.library_service.get_storage")
class TestGetLibraryWithDetails(unittest.TestCase):
    """Tests for get_library_with_details (it imports get_book_detail from books_service)."""

    def test_returns_shelves_with_book_details(
        self, mock_get_storage: MagicMock, mock_get_book_detail: MagicMock
    ) -> None:
        store = MagicMock()
        store.get_user_books.return_value = _make_rec(
            library={"in_progress": ["B1"], "saved": [], "finished": []}
        )
        mock_get_storage.return_value = store
        mock_get_book_detail.return_value = {"parent_asin": "B1", "title": "Book One"}

        result = library_service.get_library_with_details("u@x.com")

        self.assertEqual(len(result["in_progress"]), 1)
        self.assertEqual(result["in_progress"][0]["title"], "Book One")
        mock_get_book_detail.assert_called_with("B1")


@patch("backend.services.library_service.get_storage")
class TestGetUserPreferences(unittest.TestCase):
    """Tests for get_user_preferences."""

    def test_returns_genre_list(self, mock_get_storage: MagicMock) -> None:
        store = MagicMock()
        store.get_user_books.return_value = {
            "library": _make_rec()["library"],
            "genre_preferences": ["Fantasy", "Sci-Fi"],
        }
        mock_get_storage.return_value = store

        result = library_service.get_user_preferences("u@x.com")
        self.assertEqual(result, ["Fantasy", "Sci-Fi"])


@patch("backend.services.library_service.get_storage")
class TestUpdateUserPreferences(unittest.TestCase):
    """Tests for update_user_preferences."""

    def test_overwrites_preferences_and_clears_genre_counts(
        self, mock_get_storage: MagicMock
    ) -> None:
        store = MagicMock()
        store.get_user_books.return_value = {
            "library": _make_rec()["library"],
            "genre_preferences": ["Old"],
            "genre_counts": {"old": 2},
        }
        mock_get_storage.return_value = store

        result = library_service.update_user_preferences(
            "u@x.com", ["Fantasy", "Mystery"]
        )

        self.assertEqual(result["genre_preferences"], ["Fantasy", "Mystery"])
        self.assertEqual(result["genre_counts"], {})
        store.save_user_books.assert_called_once()


@patch("backend.services.library_service.get_storage")
class TestUpdateBookStatus(unittest.TestCase):
    """Tests for update_book_status."""

    def test_moves_book_to_shelf(self, mock_get_storage: MagicMock) -> None:
        store = MagicMock()
        store.get_user_books.return_value = _make_rec(
            library={"in_progress": ["B1"], "saved": [], "finished": []}
        )
        mock_get_storage.return_value = store

        result = library_service.update_book_status("u@x.com", "B1", "finished")

        self.assertEqual(result["library"]["in_progress"], [])
        self.assertEqual(result["library"]["finished"], ["B1"])
        store.save_user_books.assert_called_once()

    def test_invalid_shelf_raises(self, mock_get_storage: MagicMock) -> None:
        mock_get_storage.return_value = MagicMock()
        with self.assertRaises(ValueError) as ctx:
            library_service.update_book_status("u@x.com", "B1", "bad")
        self.assertIn("invalid shelf", str(ctx.exception))

@patch("backend.services.library_service.get_storage")
class TestRemoveBookFromShelf(unittest.TestCase):
    """Tests for remove_book_from_shelf."""

    def test_removes_book_from_given_shelf(
        self, mock_get_storage: MagicMock
    ) -> None:
        store = MagicMock()
        store.get_user_books.return_value = _make_rec(
            library={"in_progress": ["B1"], "saved": [], "finished": []}
        )
        store.get_book_metadata.return_value = {"categories": ["Fantasy"]}
        mock_get_storage.return_value = store

        result = library_service.remove_book_from_shelf(
            "u@x.com", "in_progress", "B1"
        )

        self.assertEqual(result["library"]["in_progress"], [])
        store.save_user_books.assert_called_once()

    def test_invalid_shelf_raises(self, mock_get_storage: MagicMock) -> None:
        mock_get_storage.return_value = MagicMock()
        with self.assertRaises(ValueError) as ctx:
            library_service.remove_book_from_shelf("u@x.com", "bad", "B1")
        self.assertIn("invalid shelf", str(ctx.exception))

    def test_empty_user_or_parent_asin_returns_record_without_save(
        self, mock_get_storage: MagicMock
    ) -> None:
        store = MagicMock()
        store.get_user_books.return_value = _make_rec()
        mock_get_storage.return_value = store

        result = library_service.remove_book_from_shelf("", "saved", "B1")
        self.assertIn("library", result)
        store.save_user_books.assert_not_called()
        
        
if __name__ == "__main__":
    unittest.main()
