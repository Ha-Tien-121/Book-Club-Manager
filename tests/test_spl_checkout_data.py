"""
Tests for the `main` function in `spl_checkouts_last_year`.

Covers:
- Aggregation by ISBN or Title/Author
- Checkouts summing
- Fake ISBN generation for missing ISBNs or non-convertible ISBN-13
- JSON output structure
- Pagination handling

Usage:
    python -m unittest tests.test_spl_checkout_data
"""

import unittest
import tempfile
import json
from unittest.mock import MagicMock

from data.scripts.spl_data.spl_checkout_data import main
from tests.sample_data.isbn_constants import (
    VALID_ISBN10,
    VALID_ISBN13_NO_10,
    INVALID_ISBNS
)


class SPLCheckoutsTestHelpers(unittest.TestCase):
    """Shared helpers for SPL checkouts tests."""

    def setUp(self):
        """Create temporary files for JSON output and close immediately to avoid ResourceWarnings."""
        self.temp_isbn = tempfile.NamedTemporaryFile(delete=False)
        self.temp_title_author = tempfile.NamedTemporaryFile(delete=False)
        self.temp_isbn.close()
        self.temp_title_author.close()

    def build_mock_client(self, responses):
        """Return a mocked Socrata client with sequential responses."""
        client = MagicMock()
        client.get.side_effect = responses
        return client


class OneShotTestsSPLCheckouts(SPLCheckoutsTestHelpers):
    """Pattern tests for SPL checkouts aggregation pipeline."""

    def test_checkouts_grouped_by_isbn(self):
        """Checkouts for rows with the same ISBN are summed into a single record."""
        isbn = VALID_ISBN10[0]
        catalog_rows = [
            {"Title": "Book A", "Creator": "Author A", "Checkouts": "2", "ISBN": isbn},
            {"Title": "Book A", "Creator": "Author A", "Checkouts": "3", "ISBN": isbn},
        ]
        client = self.build_mock_client([catalog_rows, []])

        main(
            output_isbn_index=self.temp_isbn.name,
            output_title_author_index=self.temp_title_author.name,
            client=client
        )

        with open(self.temp_isbn.name) as f:
            data = json.load(f)

        self.assertEqual(data[isbn]["Checkouts"], 5)
        self.assertEqual(data[isbn]["Title"], "Book A")
        self.assertEqual(data[isbn]["Author"], "Author A")

    def test_checkouts_grouped_by_title_author_when_no_isbn(self):
        """Rows without ISBNs are aggregated by normalized title and author."""
        catalog_rows = [
            {"Title": "Book B", "Creator": "Author B", "Checkouts": "1", "ISBN": None},
            {"Title": "Book B", "Creator": "Author B", "Checkouts": "2", "ISBN": None},
        ]
        client = self.build_mock_client([catalog_rows, []])

        main(
            output_isbn_index=self.temp_isbn.name,
            output_title_author_index=self.temp_title_author.name,
            client=client
        )

        with open(self.temp_title_author.name) as f:
            data = json.load(f)

        key = "book b|author b"
        self.assertTrue(data[key].startswith("FAKE"))  # points to fake ISBN


class EdgeCaseTestsSPLCheckouts(SPLCheckoutsTestHelpers):
    """Edge-case tests for SPL checkouts aggregation pipeline."""

    def test_fake_isbn_generated_for_missing_isbn(self):
        """Rows with missing ISBNs generate a fake ISBN identifier."""
        catalog_rows = [
            {"Title": "Book C", "Creator": "Author C", "Checkouts": "1", "ISBN": None}
        ]
        client = self.build_mock_client([catalog_rows, []])

        main(
            output_isbn_index=self.temp_isbn.name,
            output_title_author_index=self.temp_title_author.name,
            client=client
        )

        with open(self.temp_isbn.name) as f:
            data = json.load(f)

        fake_keys = [k for k in data.keys() if k.startswith("FAKE")]
        self.assertEqual(len(fake_keys), 1)

    def test_isbn13_without_10_generates_fake(self):
        """ISBN-13 values without a convertible ISBN-10 result in a fake ISBN."""
        catalog_rows = [
            {"Title": "Book D", "Creator": "Author D", "Checkouts": "1",
             "ISBN": VALID_ISBN13_NO_10[0]}
        ]
        client = self.build_mock_client([catalog_rows, []])

        main(
            output_isbn_index=self.temp_isbn.name,
            output_title_author_index=self.temp_title_author.name,
            client=client
        )

        with open(self.temp_isbn.name) as f:
            data = json.load(f)

        fake_keys = [k for k in data.keys() if k.startswith("FAKE")]
        self.assertEqual(len(fake_keys), 1)

    def test_empty_api_returns_raise_value_error(self):
        """Empty API responses produce Raises ValueError."""
        client = self.build_mock_client([[], []])
        with self.assertRaises(ValueError):
            main(
                output_isbn_index=self.temp_isbn.name,
                output_title_author_index=self.temp_title_author.name,
                client=client
            )

    def test_pagination_combines_chunks(self):
        """Results from multiple paginated API responses are combined correctly."""
        chunk1 = [
            {"Title": "Book E", "Creator": "Author E", "Checkouts": "1", "ISBN": VALID_ISBN10[0]}
        ]
        chunk2 = [
            {"Title": "Book F", "Creator": "Author F", "Checkouts": "2", "ISBN": VALID_ISBN10[1]}
        ]
        client = self.build_mock_client([chunk1, chunk2, []])

        main(
            output_isbn_index=self.temp_isbn.name,
            output_title_author_index=self.temp_title_author.name,
            client=client
        )

        with open(self.temp_isbn.name) as f:
            data = json.load(f)
        self.assertIn(VALID_ISBN10[0], data)
        self.assertIn(VALID_ISBN10[1], data)


if __name__ == "__main__":
    unittest.main()
