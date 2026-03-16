"""
Tests for the `main` function in `spl_checkout_data`.

These tests focus on:
- Aggregation by ISBN and checkout summing
- Pagination handling
- Error behavior on empty API responses
- Writing the top-50 SPL checkouts that exist in DynamoDB to a single JSON file

Usage:
    python -m unittest tests.test_spl_checkout_data
"""

import json
import tempfile
import unittest
from unittest.mock import MagicMock, patch

from data.scripts.spl_data.spl_checkout_data import main
from tests.sample_data.isbn_constants import VALID_ISBN10


class SPLCheckoutsTestHelpers(unittest.TestCase):
    """Shared helpers for SPL checkouts tests."""

    def setUp(self):
        """Create temporary file for JSON output and close immediately to avoid ResourceWarnings."""
        self.temp_top50 = tempfile.NamedTemporaryFile(delete=False)
        self.temp_top50.close()

    def build_mock_client(self, responses):
        """Return a mocked Socrata client with sequential responses."""
        client = MagicMock()
        client.get.side_effect = responses
        return client


class OneShotTestsSPLCheckouts(SPLCheckoutsTestHelpers):
    """Pattern tests for SPL checkouts aggregation pipeline."""

    @patch("data.scripts.spl_data.spl_checkout_data._batch_get_books")
    @patch("data.scripts.spl_data.spl_checkout_data._get_top_existing_isbns_in_dynamo")
    def test_checkouts_grouped_by_isbn(
        self,
        mock_get_top_existing_isbns,
        mock_batch_get_books,
    ):
        """Checkouts for rows with the same ISBN are summed into a single record."""
        isbn = VALID_ISBN10[0]
        catalog_rows = [
            {"Title": "Book A", "Creator": "Author A", "Checkouts": "2", "ISBN": isbn},
            {"Title": "Book A", "Creator": "Author A", "Checkouts": "3", "ISBN": isbn},
        ]
        client = self.build_mock_client([catalog_rows, []])

        # Pretend DynamoDB contains this ISBN and returns a simple book item.
        mock_get_top_existing_isbns.return_value = [isbn]
        mock_batch_get_books.return_value = {
            isbn: {"parent_asin": isbn, "title": "Book A", "author": "Author A"}
        }

        main(
            output_top50_in_books=self.temp_top50.name,
            client=client,
        )

        with open(self.temp_top50.name, encoding="utf-8") as f:
            data = json.load(f)

        self.assertEqual(len(data), 1)
        book = data[0]
        self.assertEqual(book["parent_asin"], isbn)
        self.assertEqual(book["checkouts"], 5)

    @patch("data.scripts.spl_data.spl_checkout_data._batch_get_books")
    @patch("data.scripts.spl_data.spl_checkout_data._get_top_existing_isbns_in_dynamo")
    def test_pagination_combines_chunks(
        self,
        mock_get_top_existing_isbns,
        mock_batch_get_books,
    ):
        """Results from multiple paginated API responses are combined correctly."""
        isbn1 = VALID_ISBN10[0]
        isbn2 = VALID_ISBN10[1]
        chunk1 = [
            {"Title": "Book E", "Creator": "Author E", "Checkouts": "1", "ISBN": isbn1}
        ]
        chunk2 = [
            {"Title": "Book F", "Creator": "Author F", "Checkouts": "2", "ISBN": isbn2}
        ]
        client = self.build_mock_client([chunk1, chunk2, []])

        mock_get_top_existing_isbns.return_value = [isbn1, isbn2]
        mock_batch_get_books.return_value = {
            isbn1: {"parent_asin": isbn1, "title": "Book E", "author": "Author E"},
            isbn2: {"parent_asin": isbn2, "title": "Book F", "author": "Author F"},
        }

        main(
            output_top50_in_books=self.temp_top50.name,
            client=client,
        )

        with open(self.temp_top50.name, encoding="utf-8") as f:
            data = json.load(f)

        parent_asins = {b["parent_asin"] for b in data}
        self.assertIn(isbn1, parent_asins)
        self.assertIn(isbn2, parent_asins)


class EdgeCaseTestsSPLCheckouts(SPLCheckoutsTestHelpers):
    """Edge-case tests for SPL checkouts aggregation pipeline."""

    def test_empty_api_returns_raise_value_error(self):
        """Empty API responses produce Raises ValueError."""
        client = self.build_mock_client([[], []])
        with self.assertRaises(ValueError):
            main(
                output_top50_in_books=self.temp_top50.name,
                client=client,
            )


if __name__ == "__main__":
    unittest.main()

