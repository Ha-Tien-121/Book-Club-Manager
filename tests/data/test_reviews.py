"""
Tests for `reviews.py` main and `create_leave_n_out_split`.

These tests cover:
- One-shot behavior tests for normal review loading and matrix creation
- Edge-case tests for filtering, held-out logic, and invalid review rows
- Output schema and type checks for saved NPZ and NPY files
- Mapping consistency between review parent_asins and `book_id_to_idx.json`

Usage:
    Run all tests from the project root using:
        python -m unittest tests.test_reviews
"""

import tempfile
import unittest
import shutil
import uuid
from pathlib import Path

import numpy as np
import pandas as pd
from scipy.sparse import csr_matrix, load_npz
import pytest

DATA_DIR = Path(__file__).resolve().parent / "sample_data"
SAMPLE_REVIEWS = DATA_DIR / "reviews_sample.jsonl"
SAMPLE_IDX = DATA_DIR / "book_id_to_idx_sample.json"

try:
    from data.scripts.amazon_books_data.reviews import (
        create_leave_n_out_split,
        main,
    )
except Exception:
    # Data pipeline scripts and their configuration (RAW_DIR/PROCESSED_DIR) are
    # optional in this environment. If they are not importable, skip this suite
    # so that core backend coverage can still run.
    pytest.skip("reviews data pipeline not available", allow_module_level=True)

if not SAMPLE_REVIEWS.exists() or not SAMPLE_IDX.exists():
    pytest.skip("reviews sample data not available", allow_module_level=True)

def _make_temp_dir():
    base = Path(__file__).resolve().parent / "_tmp"
    base.mkdir(exist_ok=True)
    temp_path = base / f"tmp_{uuid.uuid4().hex}"
    temp_path.mkdir()
    return temp_path



class BooksReviewsDataTestHelpers(unittest.TestCase):
    """
    Helper base class that provides shared setup, temporary output paths,
    and artifact loading utilities for the books reviews data test suite.
    """

    @classmethod
    def setUpClass(cls):
        """
        Set the paths to the shared sample review JSONL and book-id mapping JSON.
        """
        cls.sample_input_path = (
            Path(__file__).resolve().parent / "sample_data" / "reviews_sample.jsonl"
        )
        cls.sample_book_mapping_path = (
            Path(__file__).resolve().parent / "sample_data" / "book_id_to_idx_sample.json"
        )

    def run_main(self):
        """
        Run `main` against the sample review JSONL and mapping JSON, writing all
        outputs to a temporary directory.

        Returns:
        tuple[str, str, str, str, str]
            Paths to train_matrix.npz, test_matrix.npz, book_similarity.npz,
            train_ground_truth.npy, and test_ground_truth.npy.
        """
        temp_path = _make_temp_dir()
        self.addCleanup(shutil.rmtree, temp_path, ignore_errors=True)

        output_train = str(temp_path / "train_matrix.npz")
        output_test = str(temp_path / "test_matrix.npz")
        output_similarity = str(temp_path / "book_similarity.npz")
        output_train_gt = str(temp_path / "train_ground_truth.npy")
        output_test_gt = str(temp_path / "test_ground_truth.npy")

        main(
            input_file=str(self.sample_input_path),
            book_id_to_idx=str(self.sample_book_mapping_path),
            output_file_train_matrix=output_train,
            output_file_test_matrix=output_test,
            output_file_book_similarity=output_similarity,
            output_file_train_ground_truth=output_train_gt,
            output_file_test_ground_truth=output_test_gt,
        )
        return output_train, output_test, output_similarity, output_train_gt, output_test_gt

    def load_sparse_matrix(self, path):
        """
        Load a sparse matrix saved as NPZ.

        Args:
        path : str
            Path to the NPZ file.

        Returns:
        scipy.sparse.spmatrix
            Loaded sparse matrix.
        """
        return load_npz(path)

    def load_numpy_array(self, path):
        """
        Load a dense NumPy array saved as NPY.

        Args:
        path : str
            Path to the NPY file.

        Returns:
        numpy.ndarray
            Loaded array.
        """
        return np.load(path)


class OneShotTestsCreateLeaveNOutSplit(unittest.TestCase):
    """
    One-shot tests for `create_leave_n_out_split`.
    """

    def test_returns_sparse_matrix_ground_truth_and_compliment(self):
        """
            The helper should return a CSR sparse matrix, a dense integer ground
            truth array, and a DataFrame compliment split.
        """
        candidate_df = pd.DataFrame(
            {
                "user_idx": [0, 0, 1, 1, 2, 2],
                "book_idx": [0, 1, 1, 2, 2, 3],
            }
        )

        split_matrix, ground_truth, split_compliment = create_leave_n_out_split(
            candidate_df=candidate_df,
            user_idx_col_name="user_idx",
            book_idx_col_name="book_idx",
            split_proportion=1,
            total_n_books=5,
            total_n_users=3,
            ground_truth_set_size=1,
        )

        self.assertIsInstance(split_matrix, csr_matrix)
        self.assertIsInstance(ground_truth, np.ndarray)
        self.assertIsInstance(split_compliment, pd.DataFrame)
        self.assertEqual(split_matrix.shape, (3, 5))
        self.assertEqual(ground_truth.shape, (3, 1))

    def test_users_with_one_or_fewer_books_are_not_held_out(self):
        """
            If a user has fewer than or equal to `ground_truth_set_size`
            interactions, their ground truth should remain -1.
        """
        candidate_df = pd.DataFrame(
            {
                "user_idx": [0, 1, 1],
                "book_idx": [0, 1, 2],
            }
        )

        _, ground_truth, _ = create_leave_n_out_split(
            candidate_df=candidate_df,
            user_idx_col_name="user_idx",
            book_idx_col_name="book_idx",
            split_proportion=1,
            total_n_books=3,
            total_n_users=2,
            ground_truth_set_size=1,
        )

        self.assertEqual(ground_truth[0, 0], -1)
        self.assertIn(ground_truth[1, 0], [1, 2])

    def test_split_proportion_zero_creates_empty_split(self):
        """
            If `split_proportion` is 0, no users should be selected into the
            split matrix and all ground truth values should remain -1.
        """
        candidate_df = pd.DataFrame(
            {
                "user_idx": [0, 0, 1, 1],
                "book_idx": [0, 1, 1, 2],
            }
        )

        split_matrix, ground_truth, split_compliment = create_leave_n_out_split(
            candidate_df=candidate_df,
            user_idx_col_name="user_idx",
            book_idx_col_name="book_idx",
            split_proportion=0,
            total_n_books=3,
            total_n_users=2,
            ground_truth_set_size=1,
        )

        self.assertEqual(split_matrix.nnz, 0)
        self.assertTrue(np.all(ground_truth == -1))
        self.assertEqual(len(split_compliment), len(candidate_df))

    def test_ground_truth_values_are_valid_book_indices_or_negative_one(self):
        """
            Ground truth entries should be either -1 or one of the book indices
            present for the corresponding user.
        """
        candidate_df = pd.DataFrame(
            {
                "user_idx": [0, 0, 1, 1, 1],
                "book_idx": [2, 3, 0, 1, 4],
            }
        )

        _, ground_truth, _ = create_leave_n_out_split(
            candidate_df=candidate_df,
            user_idx_col_name="user_idx",
            book_idx_col_name="book_idx",
            split_proportion=1,
            total_n_books=5,
            total_n_users=2,
            ground_truth_set_size=1,
        )

        self.assertIn(ground_truth[0, 0], [2, 3])
        self.assertIn(ground_truth[1, 0], [0, 1, 4])


class OneShotTestsBooksReviewsDataMain(BooksReviewsDataTestHelpers):
    """
    One-shot pattern tests for normal expected behavior of the Amazon book
    reviews processing pipeline.
    """

    def test_creates_all_output_files(self):
        """
            Running `main` should create all expected NPZ and NPY output files.
        """
        output_train, output_test, output_similarity, output_train_gt, output_test_gt = self.run_main()

        self.assertTrue(Path(output_train).exists())
        self.assertTrue(Path(output_test).exists())
        self.assertTrue(Path(output_similarity).exists())
        self.assertTrue(Path(output_train_gt).exists())
        self.assertTrue(Path(output_test_gt).exists())

    def test_output_types_and_shapes_are_expected(self):
        """
            Saved train/test matrices and ground truth arrays should have the
            expected types and compatible shapes.
        """
        output_train, output_test, output_similarity, output_train_gt, output_test_gt = self.run_main()

        train = self.load_sparse_matrix(output_train)
        test = self.load_sparse_matrix(output_test)
        similarity = self.load_sparse_matrix(output_similarity)
        train_gt = self.load_numpy_array(output_train_gt)
        test_gt = self.load_numpy_array(output_test_gt)

        self.assertIsInstance(train, csr_matrix)
        self.assertIsInstance(test, csr_matrix)
        self.assertIsInstance(train_gt, np.ndarray)
        self.assertIsInstance(test_gt, np.ndarray)

        self.assertEqual(train.shape, (4, 5))
        self.assertEqual(test.shape, (4, 5))
        self.assertEqual(similarity.shape, (5, 5))
        self.assertEqual(train_gt.shape, (4, 1))
        self.assertEqual(test_gt.shape, (4, 1))

    def test_only_reviews_with_rating_at_least_three_and_known_parent_asin_are_used(self):
        """
            Reviews with rating < 3 or parent_asin not in the book mapping
            should be excluded from the matrices.
        """
        output_train, output_test, _, _, _ = self.run_main()
        train = self.load_sparse_matrix(output_train).toarray()
        test = self.load_sparse_matrix(output_test).toarray()
        combined = train + test

        self.assertEqual(combined.shape, (4, 5))
        self.assertEqual(int(combined.sum()), 5)

    def test_book_similarity_matrix_is_square_and_symmetric(self):
        """
            The saved cosine similarity matrix should be square and symmetric.
        """
        _, _, output_similarity, _, _ = self.run_main()
        similarity = self.load_sparse_matrix(output_similarity).toarray()

        self.assertEqual(similarity.shape, (5, 5))
        self.assertTrue(np.allclose(similarity, similarity.T))

    def test_ground_truth_arrays_contain_valid_indices_or_negative_one(self):
        """
            Ground truth arrays should contain only valid book indices from the
            mapping or -1 where no book was held out.
        """
        _, _, _, output_train_gt, output_test_gt = self.run_main()
        train_gt = self.load_numpy_array(output_train_gt)
        test_gt = self.load_numpy_array(output_test_gt)

        for value in train_gt.flatten():
            self.assertIn(value, [-1, 0, 1, 2, 3, 4])

        for value in test_gt.flatten():
            self.assertIn(value, [-1, 0, 1, 2, 3, 4])


class EdgeCaseTestsBooksReviewsDataMain(BooksReviewsDataTestHelpers):
    """
    Edge-case tests for filtering, empty outcomes, and invalid inputs.
    """

    def test_unknown_parent_asins_are_dropped(self):
        """
            Reviews whose `parent_asin` is not in `book_id_to_idx.json` should
            not contribute to train or test matrices.
        """
        output_train, output_test, _, _, _ = self.run_main()
        combined = self.load_sparse_matrix(output_train).toarray() + self.load_sparse_matrix(output_test).toarray()

        self.assertEqual(combined.shape[1], 5)
        self.assertEqual(int(combined.sum()), 5)

    def test_users_are_factorized_to_consecutive_row_indices(self):
        """
            Output matrices should have one row per distinct valid user and
            therefore use consecutive row indices from 0 to n_users - 1.
        """
        output_train, output_test, _, output_train_gt, _ = self.run_main()
        train = self.load_sparse_matrix(output_train)
        test = self.load_sparse_matrix(output_test)
        train_gt = self.load_numpy_array(output_train_gt)

        self.assertEqual(train.shape[0], 4)
        self.assertEqual(test.shape[0], 4)
        self.assertEqual(train_gt.shape[0], 4)

    def test_missing_mapping_file_raises_file_not_found_error(self):
        """
            If the mapping JSON file does not exist, `main` should raise
            `FileNotFoundError`.
        """
        temp_path = _make_temp_dir()
        self.addCleanup(shutil.rmtree, temp_path, ignore_errors=True)

        with self.assertRaises(FileNotFoundError):
            main(
                input_file=str(self.sample_input_path),
                book_id_to_idx=str(temp_path / "does_not_exist.json"),
                output_file_train_matrix=str(temp_path / "train_matrix.npz"),
                output_file_test_matrix=str(temp_path / "test_matrix.npz"),
                output_file_book_similarity=str(temp_path / "book_similarity.npz"),
                output_file_train_ground_truth=str(temp_path / "train_ground_truth.npy"),
                output_file_test_ground_truth=str(temp_path / "test_ground_truth.npy"),
            )

    def test_missing_input_reviews_file_raises_file_not_found_error(self):
        """
            If the reviews JSONL file does not exist, `main` should raise
            `ValueError`.
        """
        temp_path = _make_temp_dir()
        self.addCleanup(shutil.rmtree, temp_path, ignore_errors=True)

        with self.assertRaises(FileNotFoundError):
            main(
                input_file=str(temp_path / "does_not_exist.jsonl"),
                book_id_to_idx=str(self.sample_book_mapping_path),
                output_file_train_matrix=str(temp_path / "train_matrix.npz"),
                output_file_test_matrix=str(temp_path / "test_matrix.npz"),
                output_file_book_similarity=str(temp_path / "book_similarity.npz"),
                output_file_train_ground_truth=str(temp_path / "train_ground_truth.npy"),
                output_file_test_ground_truth=str(temp_path / "test_ground_truth.npy"),
            )


if __name__ == "__main__":
    unittest.main()

