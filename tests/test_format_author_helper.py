"""
Tests for the `format_author` helper function.

These tests cover:
- Standard author formatting behavior
- Parenthetical removal
- Handling of multiple comma-separated parts
- Handling of pandas Series input
- Edge cases such as whitespace and invalid types

Usage:
    Run all tests from the project root using:
        python -m unittest tests.test_format_author_helper
"""

import unittest
import pandas as pd

from data.scripts.helper_functions.format_author import format_author


class OneShotTestsFormatAuthor(unittest.TestCase):
    """
    Pattern tests for expected author formatting behavior.
    """

    def test_last_first_reversed(self):
        """
            'Last, First' should become 'First Last'.
        """
        self.assertEqual(format_author("Smith, John"), "John Smith")

    def test_parenthetical_removed(self):
        """
            Parenthetical information should be removed.
        """
        self.assertEqual(format_author("Smith, John (Editor)"), "John Smith")

    def test_extra_comma_parts_ignored(self):
        """
            Only the first two comma-separated parts should be used.
        """
        self.assertEqual(format_author("Smith, John, Jr."), "John Smith")

    def test_already_first_last(self):
        """
            Names already in 'First Last' format should remain unchanged.
        """
        self.assertEqual(format_author("John Smith"), "John Smith")


class EdgeCaseTestsFormatAuthor(unittest.TestCase):
    """
    Edge case tests for format_author.
    """

    def test_whitespace_cleaned(self):
        """
            Extra whitespace should be normalized.
        """
        self.assertEqual(format_author("  Smith ,   John   "), "John Smith")

    def test_series_input(self):
        """
            Pandas Series input should return a Series with formatted values.
        """
        authors = pd.Series(["Smith, John", "Doe, Jane"])
        result = format_author(authors)

        self.assertIsInstance(result, pd.Series)
        self.assertEqual(result.iloc[0], "John Smith")
        self.assertEqual(result.iloc[1], "Jane Doe")

    def test_series_with_parenthetical(self):
        """
            Parenthetical information should be removed for Series input.
        """
        authors = pd.Series(["Smith, John (Editor)", "Doe, Jane (Translator)"])
        result = format_author(authors)

        self.assertEqual(result.iloc[0], "John Smith")
        self.assertEqual(result.iloc[1], "Jane Doe")

    def test_invalid_input_type(self):
        """
        Validation test:
            Non-string and non-Series inputs should return None.
        """
        self.assertIsNone(format_author(123))
        self.assertIsNone(format_author(["Smith, John"]))


if __name__ == "__main__":
    unittest.main()