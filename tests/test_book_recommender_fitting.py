"""
Tests for book_recommender_fitting.py: sample_negative_books,
build_training_set, and train_logistic_model

These tests cover:
- One-shot behavior tests for normal negative sampling and training set construction
- Output schema and type checks for saved model and scaler pkl files
- Feature correctness checks (similarity normalisation, popularity, interaction term)
- Edge-case tests for boundary conditions in negative sampling and feature computation

Usage:
    Run all tests from the project root using:
        python -m unittest tests.test_book_recommender_fitting
"""

import shutil
import unittest
import uuid
from pathlib import Path

import joblib
import numpy as np
from scipy.sparse import csc_matrix, csr_matrix
from sklearn.linear_model import LogisticRegression
from sklearn.preprocessing import StandardScaler

from backend.recommender.book_recommender_fitting import (
    build_training_set,
    sample_negative_books,
    train_logistic_model,
)


def _make_temp_dir() -> Path:
    "Helper for  make temp dir."
    base = Path(__file__).resolve().parent / "_tmp"
    base.mkdir(exist_ok=True)
    temp = base / f"tmp_{uuid.uuid4().hex}"
    temp.mkdir()
    return temp


def _make_user_library(n_users: int, n_books: int, density: float = 0.3, seed: int = 0) -> csr_matrix:
    """Return a random binary CSR user-book interaction matrix."""
    rng = np.random.default_rng(seed)
    data = (rng.random((n_users, n_books)) < density).astype(np.float32)
    return csr_matrix(data)


def _make_similarity_matrix(n_books: int, seed: int = 1) -> csc_matrix:
    """Return a symmetric random CSC similarity matrix with values in [0, 1]."""
    rng = np.random.default_rng(seed)
    raw = rng.random((n_books, n_books)).astype(np.float32)
    sym = (raw + raw.T) / 2
    np.fill_diagonal(sym, 1.0)
    return csc_matrix(sym)


class BookRecommenderFittingTestHelpers(unittest.TestCase):
    """
    Shared fixtures and convenience wrappers for the recommender fitting tests.

    Builds a small but realistic in-memory dataset:
        - 20 users, 15 books
        - sparse user-library matrix (~30% density)
        - symmetric book-similarity matrix
        - per-book average rating and number-of-ratings vectors
        - ground-truth vector (first 3 users have no held-out book)
    """

    N_USERS = 20
    N_BOOKS = 15

    @classmethod
    def setUpClass(cls):
        "Helper for setUpClass."
        cls.user_library = _make_user_library(cls.N_USERS, cls.N_BOOKS, density=0.3)
        cls.similarity_matrix = _make_similarity_matrix(cls.N_BOOKS)
        cls.avg_ratings = (
            np.random.default_rng(2).random(cls.N_BOOKS).astype(np.float32) * 4 + 1
        )
        cls.num_ratings = (
            np.random.default_rng(3).random(cls.N_BOOKS).astype(np.float32) * 1000 + 1
        )
        rng = np.random.default_rng(4)
        cls.ground_truth = rng.integers(0, cls.N_BOOKS, size=cls.N_USERS).astype(np.int32)
        cls.ground_truth[:3] = -1   # first 3 users have no held-out book

    def _build(self, n_neg=3, batch_size=5, ground_truth=None):
        """Convenience wrapper around build_training_set."""
        return build_training_set(
            ground_truth=self.ground_truth if ground_truth is None else ground_truth,
            user_library_matrix=self.user_library,
            book_similarity_matrix=self.similarity_matrix,
            book_avg_ratings_vector=self.avg_ratings,
            book_number_ratings_vector=self.num_ratings,
            n_neg=n_neg,
            batch_size=batch_size,
        )

    def _train(self, temp_dir: Path, n_neg=3):
        """Convenience wrapper around train_logistic_model."""
        model_path = str(temp_dir / "model.pkl")
        scaler_path = str(temp_dir / "scaler.pkl")
        clf, scaler = train_logistic_model(
            ground_truth=self.ground_truth,
            output_model_file=model_path,
            output_scaler_file=scaler_path,
            user_library_matrix=self.user_library,
            book_similarity_matrix=self.similarity_matrix,
            book_avg_ratings_vector=self.avg_ratings,
            book_number_ratings_vector=self.num_ratings,
            batch_size=5,
            n_neg=n_neg,
        )
        return clf, scaler, model_path, scaler_path


class OneShotTestsSampleNegativeBooks(unittest.TestCase):
    """One-shot tests for normal expected behaviour of `sample_negative_books`."""

    N_BOOKS = 20

    def test_returns_array_of_correct_shape(self):
        """
        The returned array should have shape (len(users), n_neg).
        """
        library = _make_user_library(5, self.N_BOOKS)
        result = sample_negative_books(
            users=np.arange(5),
            ground_truth=np.zeros(5, dtype=np.int32),
            user_library_matrix=library,
            n_books=self.N_BOOKS,
            n_neg=4,
        )
        self.assertEqual(result.shape, (5, 4))

    def test_sampled_indices_are_within_valid_book_range(self):
        """
        All sampled book indices must lie in [0, n_books).
        """
        library = _make_user_library(10, self.N_BOOKS)
        result = sample_negative_books(
            users=np.arange(10),
            ground_truth=np.full(10, -1, dtype=np.int32),
            user_library_matrix=library,
            n_books=self.N_BOOKS,
            n_neg=5,
        )
        self.assertTrue(np.all(result >= 0))
        self.assertTrue(np.all(result < self.N_BOOKS))

    def test_sampled_books_are_not_in_user_library(self):
        """
        Negative samples must not include books already in the user's library.
        """
        library = _make_user_library(8, self.N_BOOKS, density=0.5)
        result = sample_negative_books(
            users=np.arange(8),
            ground_truth=np.full(8, -1, dtype=np.int32),
            user_library_matrix=library,
            n_books=self.N_BOOKS,
            n_neg=3,
        )
        for i in range(8):
            library_books = set(library[i].indices)
            for neg in result[i]:
                self.assertNotIn(neg, library_books)

    def test_works_with_a_single_user(self):
        """
        Sampling for a single user should return an array of shape (1, n_neg).
        """
        library = _make_user_library(1, self.N_BOOKS, density=0.2)
        result = sample_negative_books(
            users=np.array([0]),
            ground_truth=np.array([-1]),
            user_library_matrix=library,
            n_books=self.N_BOOKS,
            n_neg=3,
        )
        self.assertEqual(result.shape, (1, 3))


class OneShotTestsBuildTrainingSet(BookRecommenderFittingTestHelpers):
    """One-shot tests for normal expected behaviour of `build_training_set`."""

    def test_returns_feature_matrix_and_label_vector(self):
        """
        `build_training_set` should return a 2-D ndarray X and a 1-D ndarray y.
        """
        X, y = self._build()
        self.assertIsInstance(X, np.ndarray)
        self.assertIsInstance(y, np.ndarray)
        self.assertEqual(X.ndim, 2)
        self.assertEqual(y.ndim, 1)

    def test_feature_matrix_has_three_columns(self):
        """
        X must have exactly three columns: similarity, popularity, interaction term.
        """
        X, _ = self._build()
        self.assertEqual(X.shape[1], 3)

    def test_row_count_equals_positives_plus_negatives(self):
        """
        Total rows should equal n_valid_users * (1 + n_neg).
        """
        n_neg = 4
        X, y = self._build(n_neg=n_neg)
        n_valid = int(np.sum(self.ground_truth != -1))
        self.assertEqual(X.shape[0], n_valid * (1 + n_neg))
        self.assertEqual(len(y), n_valid * (1 + n_neg))

    def test_labels_are_binary(self):
        """
        y must contain only the values 0 and 1.
        """
        _, y = self._build()
        self.assertTrue(set(y.tolist()).issubset({0, 1}))

    def test_all_feature_values_are_finite(self):
        """
        No feature value in X should be NaN or infinite.
        """
        X, _ = self._build()
        self.assertTrue(np.all(np.isfinite(X)))

    def test_similarity_and_popularity_features_are_non_negative(self):
        """
        Similarity (col 0) and popularity (col 1) are log-transformed and
        must be >= 0 for non-negative inputs.
        """
        X, _ = self._build()
        self.assertTrue(np.all(X[:, 0] >= 0))
        self.assertTrue(np.all(X[:, 1] >= 0))

    def test_popularity_column_matches_log1p_avg_times_num_ratings(self):
        """
        Column 1 for positive examples should equal
        log1p(avg_rating[book] * num_ratings[book]).
        """
        n_books = 8
        library = _make_user_library(4, n_books, density=0.3, seed=10)
        sim = _make_similarity_matrix(n_books)
        avg_r = np.array([3.5, 4.0, 2.5, 4.5, 3.0, 4.2, 3.8, 4.1], dtype=np.float32)
        num_r = np.array([100, 200, 50, 300, 80, 150, 120, 90], dtype=np.float32)
        gt = np.array([7, 6, 5, 4], dtype=np.int32)

        X, y = build_training_set(
            ground_truth=gt,
            user_library_matrix=library,
            book_similarity_matrix=sim,
            book_avg_ratings_vector=avg_r,
            book_number_ratings_vector=num_r,
            n_neg=2,
            batch_size=4,
        )

        for i, row_idx in enumerate(np.where(y == 1)[0]):
            book = gt[i]
            expected = float(np.log1p(avg_r[book] * num_r[book]))
            self.assertAlmostEqual(float(X[row_idx, 1]), expected, places=4)


class OneShotTestsTrainLogisticModel(BookRecommenderFittingTestHelpers):
    """One-shot tests for normal expected behaviour of `train_logistic_model`."""

    def test_returns_logistic_regression_and_standard_scaler(self):
        """
        The function should return a fitted LogisticRegression and StandardScaler.
        """
        temp = _make_temp_dir()
        self.addCleanup(shutil.rmtree, temp, ignore_errors=True)
        clf, scaler, _, _ = self._train(temp)

        self.assertIsInstance(clf, LogisticRegression)
        self.assertIsInstance(scaler, StandardScaler)

    def test_saves_model_and_scaler_pkl_files_to_disk(self):
        """
        Both pkl files should exist on disk after training completes.
        """
        temp = _make_temp_dir()
        self.addCleanup(shutil.rmtree, temp, ignore_errors=True)
        _, _, model_path, scaler_path = self._train(temp)

        self.assertTrue(Path(model_path).exists())
        self.assertTrue(Path(scaler_path).exists())

    def test_saved_model_loads_and_predicts_valid_probabilities(self):
        """
        The joblib-saved model should load and produce probabilities in [0, 1]
        that sum to 1 across classes.
        """
        temp = _make_temp_dir()
        self.addCleanup(shutil.rmtree, temp, ignore_errors=True)
        _, _, model_path, scaler_path = self._train(temp)

        clf = joblib.load(model_path)
        scaler = joblib.load(scaler_path)

        sample = np.array([[0.5, 3.2, 0.8]], dtype=np.float32)
        proba = clf.predict_proba(scaler.transform(sample))

        self.assertEqual(proba.shape, (1, 2))
        self.assertAlmostEqual(float(proba.sum()), 1.0, places=5)
        self.assertTrue(np.all(proba >= 0) and np.all(proba <= 1))

    def test_model_has_one_coefficient_per_feature(self):
        """
        The coefficient array should have shape (1, 3) for three input features.
        """
        temp = _make_temp_dir()
        self.addCleanup(shutil.rmtree, temp, ignore_errors=True)
        clf, _, _, _ = self._train(temp)

        self.assertEqual(clf.coef_.shape, (1, 3))


class EdgeCaseTestsSampleNegativeBooks(unittest.TestCase):
    """Edge-case tests for `sample_negative_books`."""

    def test_empty_library_user_receives_valid_samples(self):
        """
        A user with an empty library should still receive n_neg valid samples.
        """
        n_books = 10
        library = csr_matrix((1, n_books), dtype=np.float32)
        result = sample_negative_books(
            users=np.array([0]),
            ground_truth=np.array([-1]),
            user_library_matrix=library,
            n_books=n_books,
            n_neg=5,
        )
        self.assertEqual(result.shape, (1, 5))
        self.assertTrue(np.all(result >= 0))
        self.assertTrue(np.all(result < n_books))

    def test_nearly_full_library_still_yields_n_neg_samples(self):
        """
        Even when the user's library covers most books, n_neg valid samples
        should be returned as long as enough books remain outside the library.
        """
        n_books = 50
        row = np.zeros(45, dtype=np.int32)
        col = np.arange(45, dtype=np.int32)
        library = csr_matrix(
            (np.ones(45, dtype=np.float32), (row, col)), shape=(1, n_books)
        )
        result = sample_negative_books(
            users=np.array([0]),
            ground_truth=np.array([-1]),
            user_library_matrix=library,
            n_books=n_books,
            n_neg=4,
        )
        self.assertEqual(result.shape, (1, 4))
        for neg in result[0]:
            self.assertGreaterEqual(neg, 45)


class EdgeCaseTestsBuildTrainingSet(BookRecommenderFittingTestHelpers):
    """Edge-case tests for `build_training_set`."""

    def test_all_ground_truth_negative_one_returns_empty_arrays(self):
        """
        When every user has ground_truth == -1 the returned X and y should
        be empty (zero rows).
        """
        gt = np.full(self.N_USERS, -1, dtype=np.int32)
        X, y = self._build(ground_truth=gt)

        self.assertEqual(X.shape[0], 0)
        self.assertEqual(len(y), 0)

    def test_single_valid_user_produces_correct_number_of_rows(self):
        """
        With exactly one valid user the dataset should contain 1 + n_neg rows,
        with exactly one positive label.
        """
        n_neg = 3
        gt = np.full(self.N_USERS, -1, dtype=np.int32)
        gt[5] = 7

        X, y = self._build(n_neg=n_neg, ground_truth=gt)

        self.assertEqual(X.shape[0], 1 + n_neg)
        self.assertEqual(int(y.sum()), 1)

    def test_similarity_score_is_normalised_by_library_size(self):
        """
        The similarity score for a user is divided by their library size.
        Two users with different library sizes should therefore yield different
        column-0 values for the same ground-truth book, confirming normalisation.
        """
        n_books = 10
        lib_data = np.zeros((2, n_books), dtype=np.float32)
        lib_data[0, 0] = 1
        lib_data[1, :5] = 1
        library = csr_matrix(lib_data)

        sim = _make_similarity_matrix(n_books)
        avg_r = np.ones(n_books, dtype=np.float32) * 4.0
        num_r = np.ones(n_books, dtype=np.float32) * 100.0
        gt = np.array([7, 7], dtype=np.int32)

        X, y = build_training_set(
            ground_truth=gt,
            user_library_matrix=library,
            book_similarity_matrix=sim,
            book_avg_ratings_vector=avg_r,
            book_number_ratings_vector=num_r,
            n_neg=1,
            batch_size=2,
        )

        pos_rows = np.where(y == 1)[0]
        sim_user0 = float(X[pos_rows[0], 0])
        sim_user1 = float(X[pos_rows[1], 0])
        self.assertNotAlmostEqual(sim_user0, sim_user1, places=6)


class EdgeCaseTestsTrainLogisticModel(BookRecommenderFittingTestHelpers):
    """Edge-case tests for `train_logistic_model`."""

    def test_coefficient_shape_is_invariant_to_n_neg(self):
        """
        Varying n_neg should not affect the shape of the learned coefficient vector.
        """
        temp = _make_temp_dir()
        self.addCleanup(shutil.rmtree, temp, ignore_errors=True)

        for n_neg in (2, 5, 10):
            clf, _, _, _ = self._train(temp, n_neg=n_neg)
            self.assertEqual(clf.coef_.shape, (1, 3))

    def test_overwriting_existing_pkl_files_does_not_raise(self):
        """
        Calling train twice with the same output paths should silently overwrite
        both files without raising an exception.
        """
        temp = _make_temp_dir()
        self.addCleanup(shutil.rmtree, temp, ignore_errors=True)

        self._train(temp)
        try:
            self._train(temp)
        except Exception as exc:
            self.fail(f"Second call raised an unexpected exception: {exc}")


if __name__ == "__main__":
    unittest.main()
