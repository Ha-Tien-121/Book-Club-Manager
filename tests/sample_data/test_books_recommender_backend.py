"""
Tests for BookRecommender in book_recommender_backend.py.

Covers:
    - recommend returns top_k results
    - library books are excluded from recommendations
    - empty library falls back to popularity-only scoring
    - unknown book ids in user library are skipped
    - higher scored books rank first
    - fetch_books returns empty list for no book_ids
    
Usage:
    Run tests from the project root using:
    python -m unittest tests.sample_data.test_books_recommender_backend
"""

import unittest
import numpy as np
from scipy.sparse import eye, csr_matrix
from unittest.mock import MagicMock, patch


from backend.recommender.book_recommender_backend import BookRecommender


def _make_recommender(n_books=20, beta=(1.0, 1.0, 1.0), library_book_ids=None):
    """
    Build a BookRecommender with all file I/O mocked out.

    Args:
        n_books: number of books in the fake catalogue
        beta: model coefficients
        library_book_ids: list of string book IDs the user owns (default: none)
    """
    rec = BookRecommender.__new__(BookRecommender)

    rec.beta_scaled = np.array(beta, dtype=np.float32)
    rec.book_similarity = eye(n_books, format="csr", dtype=np.float32)
    rec.popularity_score = np.ones(n_books, dtype=np.float32)

    rec.book_id_to_idx = {f"book_{i}": i for i in range(n_books)}
    rec.idx_to_book_id = {i: f"book_{i}" for i in range(n_books)}

    storage = MagicMock()
    storage.get_user_books.return_value = library_book_ids or []
    rec.storage = storage

    return rec


class TestRecommendOutputShape(unittest.TestCase):

    def test_returns_top_k_results(self):
        rec = _make_recommender(n_books=50)
        with patch.object(rec, "fetch_books", return_value=[{"id": i} for i in range(10)]):
            results = rec.recommend("user_1", top_k=10)
        self.assertEqual(len(results), 10)

    def test_top_k_larger_than_available_raises(self):
        """top_k >= n_books should raise ValueError."""
        rec = _make_recommender(n_books=10)
        with patch.object(rec, "fetch_books", return_value=[]):
            with self.assertRaises(ValueError):
                rec.recommend("user_1", top_k=10)


class TestLibraryMasking(unittest.TestCase):

    def test_library_books_excluded_from_recommendations(self):
        n_books = 20
        owned = ["book_0", "book_1", "book_2"]
        rec = _make_recommender(n_books=n_books, library_book_ids=owned)

        recommended_indices = []

        def capture(book_ids):
            recommended_indices.extend(book_ids)
            return []

        with patch.object(rec, "fetch_books", side_effect=capture):
            rec.recommend("user_1", top_k=5)

        for b in owned:
            self.assertNotIn(b, recommended_indices)

    def test_empty_library_returns_results(self):
        rec = _make_recommender(n_books=20, library_book_ids=[])
        with patch.object(rec, "fetch_books", return_value=[{"id": i} for i in range(5)]):
            results = rec.recommend("user_1", top_k=5)
        self.assertEqual(len(results), 5)


class TestUnknownBookIds(unittest.TestCase):

    def test_unknown_book_ids_are_skipped(self):
        """Books not in book_id_to_idx should be silently ignored."""
        rec = _make_recommender(n_books=20, library_book_ids=["unknown_book"])
        with patch.object(rec, "fetch_books", return_value=[]):
            rec.recommend("user_1", top_k=5)


class TestScoreOrdering(unittest.TestCase):

    def test_highest_scored_book_is_first(self):
        """
        Give book_5 a much higher popularity score than all others.
        It should appear first in recommendations (assuming empty library).
        """
        n_books = 20
        rec = _make_recommender(n_books=n_books, library_book_ids=[])
        rec.popularity_score = np.zeros(n_books, dtype=np.float32)
        rec.popularity_score[5] = 100.0

        recommended = []

        def capture(book_ids):
            recommended.extend(book_ids)
            return []

        with patch.object(rec, "fetch_books", side_effect=capture):
            rec.recommend("user_1", top_k=5)

        self.assertEqual(recommended[0], "book_5")


class TestFetchBooks(unittest.TestCase):

    def test_fetch_books_empty_input_returns_empty_list(self):
        rec = _make_recommender()
        result = rec.fetch_books([])
        self.assertEqual(result, [])


if __name__ == "__main__":
    unittest.main()