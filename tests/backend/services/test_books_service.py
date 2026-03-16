"""
Tests for backend.services.books_service.

- get_trending_books_spl: success, fallback to review books, exception returns []
- get_trending_books_reviews: success, exception returns []
- get_book_detail: success from details, fallback to metadata, empty parent_asin, exception
- get_book_forum_thread: success, empty parent_asin, fallback to get_forum_thread, exceptions
- get_book_hub: combines book, forum_thread, related_events
- get_book_related_events: success, empty parent_asin, limit<=0, exception returns []
"""

import sys
import types
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

# Ensure inner Book-Club-Manager backend package is importable when running from outer repo root.
_TESTS_DIR = Path(__file__).resolve().parents[2]
_REPO_ROOT = _TESTS_DIR.parent
_INNER_ROOT = _REPO_ROOT / "Book-Club-Manager"
if _INNER_ROOT.is_dir() and str(_INNER_ROOT) not in sys.path:
    sys.path.insert(0, str(_INNER_ROOT))

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

from backend.services import books_service  # noqa: E402


@patch("backend.services.books_service.get_storage")
class TestGetTrendingBooksSpl(unittest.TestCase):
    """Tests for get_trending_books_spl."""

    def test_returns_spl_books_up_to_limit(self, mock_get_storage: MagicMock) -> None:
        store = MagicMock()
        store.get_spl_top50_checkout_books.return_value = [
            {"parent_asin": "A1", "title": "Book 1"},
            {"parent_asin": "A2", "title": "Book 2"},
            {"parent_asin": "A3", "title": "Book 3"},
        ]
        mock_get_storage.return_value = store

        result = books_service.get_trending_books_spl(limit=2)
        self.assertEqual(len(result), 2)
        self.assertEqual(result[0]["parent_asin"], "A1")
        store.get_spl_top50_checkout_books.assert_called_once()

    def test_fallback_to_review_books_on_exception(self, mock_get_storage: MagicMock) -> None:
        store = MagicMock()
        store.get_spl_top50_checkout_books.side_effect = OSError("network")
        store.get_top50_review_books.return_value = [{"parent_asin": "B1", "title": "Review 1"}]
        mock_get_storage.return_value = store

        result = books_service.get_trending_books_spl(limit=10)
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]["parent_asin"], "B1")
        store.get_top50_review_books.assert_called_once()

    def test_returns_empty_list_when_both_raise(self, mock_get_storage: MagicMock) -> None:
        store = MagicMock()
        store.get_spl_top50_checkout_books.side_effect = ValueError("bad")
        store.get_top50_review_books.side_effect = TypeError("bad")
        mock_get_storage.return_value = store

        result = books_service.get_trending_books_spl(limit=50)
        self.assertEqual(result, [])

    def test_none_spl_returns_empty_list(self, mock_get_storage: MagicMock) -> None:
        store = MagicMock()
        store.get_spl_top50_checkout_books.return_value = None
        mock_get_storage.return_value = store

        result = books_service.get_trending_books_spl(limit=5)
        self.assertEqual(result, [])
        store.get_spl_top50_checkout_books.assert_called_once()


@patch("backend.services.books_service.get_storage")
class TestGetTrendingBooksReviews(unittest.TestCase):
    """Tests for get_trending_books_reviews."""

    def test_returns_review_books_up_to_limit(self, mock_get_storage: MagicMock) -> None:
        store = MagicMock()
        store.get_top50_review_books.return_value = [
            {"parent_asin": "R1"},
            {"parent_asin": "R2"},
        ]
        mock_get_storage.return_value = store

        result = books_service.get_trending_books_reviews(limit=1)
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]["parent_asin"], "R1")

    def test_exception_returns_empty_list(self, mock_get_storage: MagicMock) -> None:
        store = MagicMock()
        store.get_top50_review_books.side_effect = OSError("network error")
        mock_get_storage.return_value = store

        result = books_service.get_trending_books_reviews(limit=50)
        self.assertEqual(result, [])


@patch("backend.services.books_service.get_storage")
class TestGetBookDetail(unittest.TestCase):
    """Tests for get_book_detail."""

    def test_returns_full_details_when_available(self, mock_get_storage: MagicMock) -> None:
        store = MagicMock()
        store.get_book_details.return_value = {"parent_asin": "X1", "title": "Full", "description": "Long"}
        mock_get_storage.return_value = store

        result = books_service.get_book_detail("X1")
        self.assertEqual(result["parent_asin"], "X1")
        self.assertEqual(result["title"], "Full")
        store.get_book_details.assert_called_once_with("X1")

    def test_fallback_to_metadata_when_details_empty(self, mock_get_storage: MagicMock) -> None:
        store = MagicMock()
        store.get_book_details.return_value = None
        store.get_book_metadata.return_value = {"parent_asin": "Y1", "title": "Meta"}
        mock_get_storage.return_value = store

        result = books_service.get_book_detail("Y1")
        self.assertEqual(result["parent_asin"], "Y1")
        self.assertEqual(result["title"], "Meta")
        store.get_book_metadata.assert_called_once_with("Y1")

    def test_empty_parent_asin_returns_empty_dict(self, mock_get_storage: MagicMock) -> None:
        mock_get_storage.return_value = MagicMock()

        result = books_service.get_book_detail("")
        self.assertEqual(result, {})
        result2 = books_service.get_book_detail("   ")
        self.assertEqual(result2, {})

    def test_exception_returns_empty_dict(self, mock_get_storage: MagicMock) -> None:
        store = MagicMock()
        store.get_book_details.side_effect = OSError("fail")
        store.get_book_metadata.side_effect = ValueError("fail")
        mock_get_storage.return_value = store

        result = books_service.get_book_detail("Z1")
        self.assertEqual(result, {})


@patch("backend.services.books_service.get_storage")
class TestGetBookForumThread(unittest.TestCase):
    """Tests for get_book_forum_thread."""

    def test_returns_thread_from_get_forum_thread_for_book(self, mock_get_storage: MagicMock) -> None:
        store = MagicMock()
        store.get_forum_thread_for_book.return_value = [{"id": 1, "body": "Post 1"}]
        mock_get_storage.return_value = store

        result = books_service.get_book_forum_thread("B123")
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]["body"], "Post 1")
        store.get_forum_thread_for_book.assert_called_once_with("B123")

    def test_empty_parent_asin_returns_empty_list(self, mock_get_storage: MagicMock) -> None:
        mock_get_storage.return_value = MagicMock()

        result = books_service.get_book_forum_thread("")
        self.assertEqual(result, [])
        result2 = books_service.get_book_forum_thread("   ")
        self.assertEqual(result2, [])

    def test_fallback_to_get_forum_thread_on_exception(self, mock_get_storage: MagicMock) -> None:
        store = MagicMock()
        store.get_forum_thread_for_book.side_effect = OSError("fail")
        store.get_forum_thread.return_value = [{"id": 2, "body": "Fallback"}]
        mock_get_storage.return_value = store

        result = books_service.get_book_forum_thread("B456")
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]["body"], "Fallback")
        store.get_forum_thread.assert_called_once_with("B456")

    def test_both_exceptions_returns_empty_list(self, mock_get_storage: MagicMock) -> None:
        store = MagicMock()
        store.get_forum_thread_for_book.side_effect = ValueError("a")
        store.get_forum_thread.side_effect = TypeError("b")
        mock_get_storage.return_value = store

        result = books_service.get_book_forum_thread("B789")
        self.assertEqual(result, [])


@patch("backend.services.books_service.get_book_related_events")
@patch("backend.services.books_service.get_book_forum_thread")
@patch("backend.services.books_service.get_book_detail")
class TestGetBookHub(unittest.TestCase):
    """Tests for get_book_hub."""

    def test_returns_combined_payload(
        self,
        mock_get_detail: MagicMock,
        mock_get_forum: MagicMock,
        mock_get_events: MagicMock,
    ) -> None:
        mock_get_detail.return_value = {"parent_asin": "H1", "title": "Hub Book"}
        mock_get_forum.return_value = [{"id": 1}]
        mock_get_events.return_value = [{"event_id": "e1"}]

        result = books_service.get_book_hub("H1")

        self.assertIn("book", result)
        self.assertIn("forum_thread", result)
        self.assertIn("related_events", result)
        self.assertEqual(result["book"]["parent_asin"], "H1")
        self.assertEqual(len(result["forum_thread"]), 1)
        self.assertEqual(len(result["related_events"]), 1)
        mock_get_detail.assert_called_once_with("H1")
        mock_get_forum.assert_called_once_with("H1")
        mock_get_events.assert_called_once_with("H1")


@patch("backend.services.books_service.get_storage")

class TestGetBookRelatedEvents(unittest.TestCase):
    """Tests for get_book_related_events."""
    def test_returns_events_up_to_limit(self, mock_get_storage: MagicMock) -> None:
        store = MagicMock()
        store.get_events_for_book.return_value = [
            {"event_id": "ev1"},
            {"event_id": "ev2"},

        ]
        mock_get_storage.return_value = store
        result = books_service.get_book_related_events("P1", limit=3)
        self.assertEqual(len(result), 2)
        store.get_events_for_book.assert_called_once_with("P1", limit=3)

    def test_empty_parent_asin_returns_empty_list(self, mock_get_storage: MagicMock) -> None:
        mock_get_storage.return_value = MagicMock()
        result = books_service.get_book_related_events("", limit=5)
        self.assertEqual(result, [])
        result2 = books_service.get_book_related_events("   ", limit=5)
        self.assertEqual(result2, [])

    def test_limit_zero_or_negative_returns_empty_list(self, mock_get_storage: MagicMock) -> None:
        mock_get_storage.return_value = MagicMock()
        result = books_service.get_book_related_events("P1", limit=0)
        self.assertEqual(result, [])
        result2 = books_service.get_book_related_events("P1", limit=-1)
        self.assertEqual(result2, [])

    def test_exception_returns_empty_list(self, mock_get_storage: MagicMock) -> None:
        store = MagicMock()
        store.get_events_for_book.side_effect = OSError("fail")
        mock_get_storage.return_value = store
        result = books_service.get_book_related_events("P2", limit=5)
        self.assertEqual(result, [])

    def test_none_result_returns_empty_list(self, mock_get_storage: MagicMock) -> None:
        store = MagicMock()
        store.get_events_for_book.return_value = None
        mock_get_storage.return_value = store
        result = books_service.get_book_related_events("P3", limit=5)
        self.assertEqual(result, [])

if __name__ == "__main__":
    unittest.main()
