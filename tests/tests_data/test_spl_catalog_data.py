"""
Shared helpers for SPL ISBN/catalog-related tests.

This module previously contained tests for the SPL catalog pipeline, which has
since been replaced by the new book-events and SPL checkouts pipelines.

We keep the helper base class so other tests (e.g. extract_isbn10) can continue
to reuse the common ISBN constants and temporary-file setup, but the catalog
pipeline tests themselves have been removed.
"""

import unittest
import tempfile

from tests.sample_data.isbn_constants import (
    VALID_ISBN10,
    VALID_ISBN13,
    VALID_ISBN13_NO_10,
    INVALID_ISBNS,
)


class SPLCatalogTestHelpers(unittest.TestCase):
    """
    Helper base class providing shared setup utilities and
    common ISBN constants for SPL-related tests.
    """

    def setUp(self):
        """Create temporary files for output JSONs (kept for backwards-compat)."""
        self.temp_isbn = tempfile.NamedTemporaryFile(delete=False)
        self.temp_title_author = tempfile.NamedTemporaryFile(delete=False)
        self.temp_isbn.close()
        self.temp_title_author.close()
        self.VALID_ISBN10 = VALID_ISBN10
        self.VALID_ISBN13 = VALID_ISBN13
        self.VALID_ISBN13_NO_10 = VALID_ISBN13_NO_10
        self.INVALID_ISBNS = INVALID_ISBNS


if __name__ == "__main__":
    unittest.main()
