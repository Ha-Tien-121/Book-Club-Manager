"""
Tests for `extract_10_digit_isbn.py` helper function.

These tests cover:
- Single valid ISBN-10 inputs
- ISBN-13 inputs convertible to ISBN-10
- Invalid ISBNs and missing values
- Lists of ISBNs, ensuring the first valid ISBN is returned
- Whitespace handling in ISBN fields

Usage:
    Run all tests from the project root using:
        python -m unittest tests.test_extract_isbn10_helper
"""


from data.scripts.spl_data.spl_helper_functions.extract_10_digit_isbn import extract_isbn10
from tests.test_spl_catalog_data import SPLCatalogTestHelpers
from tests.sample_data.isbn_constants import (
    VALID_ISBN10,
    VALID_ISBN13,
    VALID_ISBN13_NO_10,
    INVALID_ISBNS
)


class ExtractISBN10Tests(SPLCatalogTestHelpers):
    """
    Tests specifically for the extract_isbn10 helper function.
    """

    def test_valid_isbn10(self):
        """Tests single valid 10 digit ISBN are returned."""
        for isbn in self.VALID_ISBN10:
            self.assertEqual(extract_isbn10(isbn), isbn)

    def test_valid_isbn13_converts_to_isbn10(self):
        """Tests single valid 13 digit ISBN with valid 10 digit ISBN represeatation is returned."""
        self.assertEqual(extract_isbn10(self.VALID_ISBN13[0]), self.VALID_ISBN10[0])

    def test_invalid_isbns_return_none(self):
        """Tests None is returned for single invalid ISBN."""
        for invalid in self.INVALID_ISBNS:
            self.assertIsNone(extract_isbn10(invalid))

    def test_valid_isbn_with_spaces(self):
        """Tests valid ISBN with trailing white returns the valid ISBN."""
        isbn = f"  {self.VALID_ISBN10[0]}  "
        self.assertEqual(extract_isbn10(isbn), self.VALID_ISBN10[0])

    def test_multiple_isbn_returns_first_valid(self):
        """Tests list of ISBNs returns first valid ISBN."""
        field = f"{self.INVALID_ISBNS[1]},{self.VALID_ISBN13[0]},{self.VALID_ISBN10[1]}"
        self.assertEqual(extract_isbn10(field), self.VALID_ISBN10[0])

    def test_all_invalid_multiple_isbn(self):
        """Tests list of invalid ISBNs returns None."""
        field = ",".join(self.INVALID_ISBNS)
        self.assertIsNone(extract_isbn10(field))

    def test_no_isbn_returns_none(self):
        """Tests ISBN with value None, "nan", or empty string return None."""
        self.assertIsNone(extract_isbn10(None))
        self.assertIsNone(extract_isbn10("nan"))
        self.assertIsNone(extract_isbn10(""))
        self.assertIsNone(extract_isbn10("   "))
