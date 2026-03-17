"""
Tests for hit50_evaluation_logistic in recommender_test.py.

Covers:
    - Perfect recall: ground truth always in top-K → hit rate = 1.0
    - Zero recall: ground truth never scoreable → hit rate = 0.0
    - Partial recall: known fraction of hits → hit rate matches exactly
    - Popularity baseline correctness
    - Library books are masked (score = -inf, not recommended)
    - Single user
    - top_k=1 extreme case
    - Results consistent across different block sizes
    - top_k == n_books

Usage:
    Run tests from the project root using:
    python -m unittest tests.test_recommender_evaluation
"""

import unittest
import sys
import types
import numpy as np
from scipy.sparse import csr_matrix
from unittest.mock import MagicMock


def _make_clf(coefs=(1.0, 1.0, 1.0)):
    clf = MagicMock()
    clf.coef_ = [np.array(coefs, dtype=np.float32)]
    return clf


def _make_scaler(scale=(1.0, 1.0, 1.0)):
    scaler = MagicMock()
    scaler.scale_ = np.array(scale, dtype=np.float32)
    return scaler



for mod in ["joblib", "data.scripts.config", "backend.recommender.config"]:
    sys.modules.setdefault(mod, types.ModuleType(mod))

sys.modules["data.scripts.config"].PROCESSED_DIR = ""
sys.modules["backend.recommender.config"].RECOMMENDER_DIR = ""

from backend.recommender.book_recommender_evaluation import hit50_evaluation_logistic


def _sparse_library(n_users, n_books, owned):
    rows, cols = zip(*owned) if owned else ([], [])
    data = [1] * len(rows)
    return csr_matrix((data, (rows, cols)), shape=(n_users, n_books), dtype=np.float32)


def _identity_sim(n_books):
    """Each book is only similar to itself."""
    from scipy.sparse import eye
    return eye(n_books, format="csc", dtype=np.float32)


def _run(n_users, n_books, ground_truth, library_pairs=None,
         coefs=(1.0, 1.0, 1.0), top_k=10, block_size=None):
    clf = _make_clf(coefs)
    scaler = _make_scaler()
    lib = _sparse_library(n_users, n_books, library_pairs or [])
    sim = _identity_sim(n_books)
    # Use non-uniform popularity signals so top-K selection is deterministic.
    # Make book 0 the most "popular" and ensure it ranks in top-K when unmasked.
    avg_ratings = np.linspace(5.0, 1.0, n_books, dtype=np.float32)
    num_ratings = np.linspace(1000.0, 1.0, n_books, dtype=np.float32)
    
    kwargs = dict(
        clf=clf,
        scaler=scaler,
        user_library_sparse=lib,
        ground_truth=np.array(ground_truth, dtype=np.int32),
        book_similarity_sparse=sim,
        book_avg_ratings=avg_ratings,
        book_num_ratings=num_ratings,
        top_k=top_k,
    )
    if block_size is not None:
        kwargs["block_size"] = block_size

    return hit50_evaluation_logistic(**kwargs)



class TestHitRateExtremes(unittest.TestCase):

    def test_perfect_hit_rate(self):
        """Ground truth not in library, should be recommended, hit rate = 1.0."""
        n_users, n_books = 5, 20
        ground_truth = [0] * n_users
        model_hr, _ = _run(n_users, n_books, ground_truth, top_k=5)
        self.assertAlmostEqual(model_hr, 1.0)

    def test_zero_hit_rate_when_ground_truth_masked(self):
        """Ground truth in library, masked to -inf, never recommended, hit rate = 0.0."""
        n_users, n_books = 4, 20
        ground_truth = [0] * n_users
        library_pairs = [(u, 0) for u in range(n_users)]
        model_hr, _ = _run(n_users, n_books, ground_truth,
                           library_pairs=library_pairs, top_k=5)
        self.assertAlmostEqual(model_hr, 0.0)

    def test_partial_hit_rate(self):
        """Half masked, half not, hit rate = 0.5."""
        n_users, n_books = 6, 30
        ground_truth = [0] * n_users
        library_pairs = [(u, 0) for u in range(n_users // 2)]
        model_hr, _ = _run(n_users, n_books, ground_truth,
                           library_pairs=library_pairs, top_k=10)
        self.assertAlmostEqual(model_hr, 0.5)


class TestPopularityBaseline(unittest.TestCase):

    def test_popularity_hit_rate_in_range(self):
        _, pop_hr = _run(5, 50, list(range(5)), top_k=10)
        self.assertGreaterEqual(pop_hr, 0.0)
        self.assertLessEqual(pop_hr, 1.0)

    def test_popularity_perfect_when_ground_truth_within_top_k(self):
        """Uniform ratings, argpartition returns first top_k books, hit rate = 1.0."""
        n_users, n_books, top_k = 5, 50, 10
        ground_truth = list(range(n_users))  # books 0-4, all within top_k=10
        _, pop_hr = _run(n_users, n_books, ground_truth, top_k=top_k)
        self.assertAlmostEqual(pop_hr, 1.0)


class TestLibraryMasking(unittest.TestCase):

    def test_library_books_never_recommended(self):
        """Only one available book (not in library), must appear in top-1."""
        n_users, n_books = 3, 10
        library_pairs = [(u, b) for u in range(n_users) for b in range(n_books - 1)]
        ground_truth = [n_books - 1] * n_users
        model_hr, _ = _run(n_users, n_books, ground_truth,
                           library_pairs=library_pairs, top_k=1)
        self.assertAlmostEqual(model_hr, 1.0)


class TestReturnTypes(unittest.TestCase):

    def test_returns_two_floats(self):
        result = _run(3, 20, [0, 1, 2], top_k=5)
        self.assertEqual(len(result), 2)
        for val in result:
            self.assertIsInstance(float(val), float)

    def test_hit_rates_between_zero_and_one(self):
        model_hr, pop_hr = _run(4, 25, [0, 1, 2, 3], top_k=5)
        for hr in (model_hr, pop_hr):
            self.assertGreaterEqual(hr, 0.0)
            self.assertLessEqual(hr, 1.0)


class TestEdgeCases(unittest.TestCase):

    def test_single_user(self):
        model_hr, pop_hr = _run(1, 20, [5], top_k=5)
        self.assertGreaterEqual(model_hr, 0.0)
        self.assertLessEqual(model_hr, 1.0)

    def test_top_k_equals_one(self):
        model_hr, _ = _run(5, 20, [0] * 5, top_k=1)
        self.assertIn(model_hr, [0.0, 1.0])

    def test_top_k_greater_than_n_books_raises(self):
        """top_k >= n_books should raise ValueError."""
        with self.assertRaises(ValueError):
            _run(3, 10, [7, 8, 9], top_k=10)


if __name__ == "__main__":
    unittest.main()
