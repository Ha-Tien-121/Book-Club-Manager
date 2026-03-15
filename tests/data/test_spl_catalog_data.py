"""
Tests for SPL catalog pipeline and ISBN helper function.

This suite separates tests for:
- `extract_isbn10` helper function
- `main` function for cleaning and indexing SPL catalog

The Socrata API is mocked so tests run without a token.

Usage:
    python -m unittest tests.test_spl_catalog_data
"""

import unittest
import tempfile
import json
from unittest.mock import MagicMock

from data.scripts.spl_data.spl_catalog_data import main
from data.scripts.spl_data.spl_helper_functions.extract_10_digit_isbn import extract_isbn10
from tests.sample_data.isbn_constants import (
    VALID_ISBN10,
    VALID_ISBN13,
    VALID_ISBN13_NO_10,
    INVALID_ISBNS
)


class SPLCatalogTestHelpers(unittest.TestCase):
    """
    Helper base class providing shared setup utilities and
    common ISBN constants for SPL catalog tests.
    """

    def setUp(self):
        """Create temporary files for output JSONs."""
        self.temp_isbn = tempfile.NamedTemporaryFile(delete=False)
        self.temp_title_author = tempfile.NamedTemporaryFile(delete=False)
        self.temp_isbn.close()
        self.temp_title_author.close()
        self.VALID_ISBN10 = VALID_ISBN10
        self.VALID_ISBN13 = VALID_ISBN13
        self.VALID_ISBN13_NO_10 = VALID_ISBN13_NO_10
        self.INVALID_ISBNS = INVALID_ISBNS   

    def build_mock_client(self, responses):
        """
        Helper function to create a mocked Socrata client.

        Args:
        responses : list
            Values returned sequentially by client.get()
        """
        client = MagicMock()
        client.get.side_effect = responses
        return client



class OneShotTestsSPLCatalog(SPLCatalogTestHelpers):
    """Pattern tests for the SPL catalog cleaning pipeline."""

    def test_books_grouped_into_branch_counts(self):
        """ 
            Books with same ISBN, Title, and Author are grouped by branch counts.
        """
        isbn = self.VALID_ISBN10[0]
        catalog_rows = [
            {"Title": "Test Book", "Author": "John Doe", "ItemLocation": "BranchA",
             "ItemCount": "2", "ISBN": isbn},
            {"Title": "Test Book", "Author": "John Doe", "ItemLocation": "BranchB",
             "ItemCount": "3", "ISBN": isbn}
        ]

        client = self.build_mock_client([
            [{"max_reportdate": "2024-01-01"}],
            catalog_rows,
            []
        ])

        main(output_isbn_index=self.temp_isbn.name,
             output_title_author_index=self.temp_title_author.name,
             client=client)

        with open(self.temp_isbn.name) as f:
            data = json.load(f)
        book = data[isbn]
        self.assertEqual(book["branch_counts"]["BranchA"], 2)
        self.assertEqual(book["branch_counts"]["BranchB"], 3)

    def test_title_author_lookup_created(self):
        """
        A normalized title|author key is created in the lookup dictionary 
        when both fields exist.
        """
        isbn = self.VALID_ISBN10[0]
        catalog_rows = [
            {"Title": "Test Book", "Author": "Jane Smith", "ItemLocation": "Central",
             "ItemCount": "1", "ISBN": isbn}
        ]

        client = self.build_mock_client([
            [{"max_reportdate": "2024-01-01"}],
            catalog_rows,
            []
        ])

        main(output_isbn_index=self.temp_isbn.name,
             output_title_author_index=self.temp_title_author.name,
             client=client)

        with open(self.temp_title_author.name) as f:
            data = json.load(f)

        self.assertIn("test book|jane smith", data)
        self.assertEqual(data["test book|jane smith"], isbn)

    def test_itemcount_converted_to_numeric(self):
        """ItemCount values returned as strings are converted to numeric before aggregation."""
        isbn = self.VALID_ISBN10[0]
        catalog_rows = [
            {"Title": "Book", "Author": "Author", "ItemLocation": "Central",
             "ItemCount": "5", "ISBN": isbn}
        ]

        client = self.build_mock_client([
            [{"max_reportdate": "2024-01-01"}],
            catalog_rows,
            []
        ])

        main(output_isbn_index=self.temp_isbn.name,
             output_title_author_index=self.temp_title_author.name,
             client=client)

        with open(self.temp_isbn.name) as f:
            data = json.load(f)

        self.assertEqual(data[isbn]["branch_counts"]["Central"], 5)


class EdgeCaseTestsSPLCatalog(SPLCatalogTestHelpers):
    """
    Edge-case tests for SPL catalog pipeline.
    """

    def test_fake_isbn_generated_when_invalid(self):
        """Invalid ISBN values result in generation of a fake ISBN identifier."""
        catalog_rows = [
            {"Title": "Unknown ISBN Book", "Author": "Anon", "ItemLocation": "Central",
             "ItemCount": "1", "ISBN": self.INVALID_ISBNS[0]}
        ]

        client = self.build_mock_client([
            [{"max_reportdate": "2024-01-01"}],
            catalog_rows,
            []
        ])

        main(output_isbn_index=self.temp_isbn.name,
             output_title_author_index=self.temp_title_author.name,
             client=client)

        with open(self.temp_isbn.name) as f:
            data = json.load(f)

        fake_keys = [k for k in data.keys() if k.startswith("FAKE")]
        self.assertEqual(len(fake_keys), 1)

    def test_isbn13_without_isbn10_generates_fake(self):
        """ISBN-13 values without a valid ISBN-10 counterpart generate a fake ISBN."""
        catalog_rows = [
            {"Title": "Non Convertible ISBN13", "Author": "Test Author",
             "ItemLocation": "Central", "ItemCount": "1",
             "ISBN": self.VALID_ISBN13_NO_10[0]}
        ]

        client = self.build_mock_client([
            [{"max_reportdate": "2024-01-01"}],
            catalog_rows,
            []
        ])

        main(output_isbn_index=self.temp_isbn.name,
             output_title_author_index=self.temp_title_author.name,
             client=client)

        with open(self.temp_isbn.name) as f:
            data = json.load(f)

        fake_keys = [k for k in data.keys() if k.startswith("FAKE")]
        self.assertEqual(len(fake_keys), 1)

    def test_empty_api_results_raises_error(self):
        """Raises ValueError when the API returns no catalog data."""
        client = self.build_mock_client([
            [{"max_reportdate": "2024-01-01"}],
            []
        ])

        with self.assertRaises(ValueError):
            main(output_isbn_index=self.temp_isbn.name,
                 output_title_author_index=self.temp_title_author.name,
                 client=client)

    def test_pagination_multiple_chunks(self):
        """Catalog records from multiple paginated API responses are combined correctly."""
        chunk1 = [
            {"Title": "Book A", "Author": "Author A", "ItemLocation": "Central",
             "ItemCount": "1", "ISBN": self.VALID_ISBN10[0]}
        ]

        chunk2 = [
            {"Title": "Book B", "Author": "Author B", "ItemLocation": "Ballard",
             "ItemCount": "2", "ISBN": self.VALID_ISBN10[1]}
        ]

        client = self.build_mock_client([
            [{"max_reportdate": "2024-01-01"}],
            chunk1,
            chunk2,
            []
        ])

        main(output_isbn_index=self.temp_isbn.name,
             output_title_author_index=self.temp_title_author.name,
             client=client)

        with open(self.temp_isbn.name) as f:
            data = json.load(f)

        self.assertIn(self.VALID_ISBN10[0], data)
        self.assertIn(self.VALID_ISBN10[1], data)


if __name__ == "__main__":
    unittest.main()


