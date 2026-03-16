"""
Tests for the `format_title` helper function.

These tests cover:
- Standard formatting rules
- Removal of punctuation sections (: ; /)
- Removal of edition information
- Quote removal
- Whitespace normalization
- Pandas Series input handling
- Edge cases and invalid inputs

Usage:
    Run all tests from the project root using:
        python -m unittest tests.test_format_title_helper
"""

import unittest
import pandas as pd

from data.scripts.helper_functions.format_title import format_title


class OneShotTestsFormatTitle(unittest.TestCase):
    """
    Pattern tests validating expected title formatting behavior.
    """

    def test_remove_quotes(self):
        """
            Quotes should be removed from titles.
        """
        self.assertEqual(format_title('"The Great Book"'), "The Great Book")

    def test_remove_slash_section(self):
        """
            Text after a slash should be removed.
        """
        self.assertEqual(
            format_title("The Great Book / John Smith"),
            "The Great Book"
        )

    def test_remove_colon_section(self):
        """
            Text after a colon should be removed.
        """
        self.assertEqual(
            format_title("The Great Book: A Novel"),
            "The Great Book"
        )

    def test_remove_semicolon_section(self):
        """
        Pattern test:
            Text after a semicolon should be removed.
        """
        self.assertEqual(
            format_title("The Great Book; Special Edition"),
            "The Great Book"
        )

    def test_remove_parenthetical_edition(self):
        """
            Edition information in parentheses should be removed.
        """
        self.assertEqual(
            format_title("The Great Book (2nd edition)"),
            "The Great Book"
        )

    def test_remove_comma_edition(self):
        """
            Edition information after a comma should be removed.
        """
        self.assertEqual(
            format_title("The Great Book, 3rd edition"),
            "The Great Book"
        )

    def test_series_input(self):
        """
            Pandas Series input should return a Series with formatted titles.
        """
        titles = pd.Series([
            "The Great Book: A Novel",
            "Another Title (2nd edition)"
        ])

        result = format_title(titles)

        self.assertIsInstance(result, pd.Series)
        self.assertEqual(result.iloc[0], "The Great Book")
        self.assertEqual(result.iloc[1], "Another Title")
    


class EdgeCaseTestsFormatTitle(unittest.TestCase):
    """
    Edge case tests for format_title.
    """

    def test_strip_and_normalize_whitespace(self):
        """
            Leading/trailing spaces and multiple spaces should be normalized.
        """
        self.assertEqual(
            format_title("   The   Great   Book   "),
            "The Great Book"
        )

    def test_remove_trailing_punctuation(self):
        """
            Trailing periods or commas should be removed.
        """
        self.assertEqual(format_title("The Great Book."), "The Great Book")
        self.assertEqual(format_title("The Great Book,"), "The Great Book")

    def test_series_whitespace_and_quotes(self):
        """
            Series titles with quotes and whitespace should be cleaned.
        """
        titles = pd.Series([
            '  "The Book"  ',
            "Another Book, 2nd edition"
        ])

        result = format_title(titles)

        self.assertEqual(result.iloc[0], "The Book")
        self.assertEqual(result.iloc[1], "Another Book")

    def test_invalid_input_type(self):
        """
            Non-string and non-Series inputs should return None.
        """
        self.assertIsNone(format_title(123))
        self.assertIsNone(format_title(["Book Title"]))


if __name__ == "__main__":
    unittest.main()