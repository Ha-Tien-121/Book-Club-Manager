"""
Tests for `amazon_books_data.py` main.

These tests cover:
- One-shot behavior tests for normal loading and cleaning
- Edge-case tests for invalid input rows and missing fields
- Duplicate-handling tests for `parent_asin` and `title_author_key`
- Output schema and type checks for values stored in SQLite

Usage:
    Run all tests from the project root using:
        python -m unittest tests.test_books_meta_data
"""

import json
import sqlite3
import tempfile
import unittest
import shutil
import uuid
from pathlib import Path

from data.scripts.amazon_books_data.books_meta_data import main


def _make_temp_dir():
    "Helper for  make temp dir."
    base = Path(__file__).resolve().parent / "_tmp"
    base.mkdir(exist_ok=True)
    temp_path = base / f"tmp_{uuid.uuid4().hex}"
    temp_path.mkdir()
    return temp_path


genres = {
    "Literature & Fiction", "Children's Books", "Mystery, Thriller & Suspense", 
    "Arts & Photography", "History", "Biographies & Memoirs", "Crafts, Hobbies & Home",
    "Business & Money", "Politics & Social Sciences", "Growing Up & Facts of Life",
    "Romance", "Science & Math", "Teen & Young Adult", "Cookbooks, Food & Wine",
    "Religion & Spirituality", "Poetry", "Comics & Graphic Novels", "Travel", "Fantasy",
    "Action & Adventure", "Self-Help", "Science Fiction", "Sports & Outdoors", 
    "Classics", "LGBTQ+ Books"
}

class BooksMetaDataTestHelpers(unittest.TestCase):
    """
    Helper class that provides shared setup and database access utilities
    for the books metadata test suite.
    """

    @classmethod
    def setUpClass(cls):
        """
        Set the path to the shared sample input JSONL used by all tests.
        """
        cls.sample_input_path = Path("tests/sample_data/meta_Books_sample.jsonl")

    def run_main(self, categories=None):
        """
        Run `main` on the sample JSONL and return output paths.
        
        Args:
        categories : set or None
            Allowed categories passed to `main`. If None, uses the module's default `genres` set.
            
        Returns:
        tuple[str, str]
            Paths to the temporary SQLite database and JSON index file.
        """
        temp_path = _make_temp_dir()
        self.addCleanup(shutil.rmtree, temp_path, ignore_errors=True)
        output_db = str(temp_path / "books.db")
        output_json_books_idx = str(temp_path / "book_id_to_idx.json")
        main(
            input_file=str(self.sample_input_path),
            categories=categories if categories is not None else genres,
            output_db=output_db,
            output_json_books_idx=output_json_books_idx,
            )
        return output_db, output_json_books_idx

    def fetch_all_rows_by_parent(self, output_db):
        """
        Read all rows from the SQLite `books` table ordered by `parent_asin`.

        Args:
        output_db : str
            Path to the SQLite database.

        Returns:
        list[sqlite3.Row]
            All rows from the `books` table.
        """
        conn = sqlite3.connect(output_db)
        conn.row_factory = sqlite3.Row
        rows = conn.execute(
            """
            SELECT parent_asin, title, author_name, average_rating, rating_number,
                   description, images, categories, title_author_key
            FROM books
            ORDER BY parent_asin
            """
        ).fetchall()
        conn.close()
        return rows

    def fetch_row_by_parent(self, output_db, parent_asin):
        """
        Read a single row from the SQLite `books` table by `parent_asin`.

        Args:
        output_db : str
            Path to the SQLite database.
        parent_asin : str
            Parent ASIN to query.

        Returns:
        sqlite3.Row or None
            Matching row if found, otherwise None.
        """
        conn = sqlite3.connect(output_db)
        conn.row_factory = sqlite3.Row
        row = conn.execute(
            """
            SELECT parent_asin, title, author_name, average_rating, rating_number,
                   description, images, categories, title_author_key
            FROM books
            WHERE parent_asin = ?
            """,
            (parent_asin,),
        ).fetchone()
        conn.close()
        return row
    
    def fetch_row_by_title_author_key(self, output_db, title_author_key):
        """
        Read a single row from the SQLite `books` table by `title_author_key`.

        Arg:
        output_db : str
            Path to the SQLite database.
        title_author_key : str
            Lowercase lookup key in the form `title|author`.

        Returns:
        sqlite3.Row or None
            First matching row if found, otherwise None.
        """
        conn = sqlite3.connect(output_db)
        conn.row_factory = sqlite3.Row
        row = conn.execute(
        """
        SELECT parent_asin, title, author_name, average_rating, rating_number,
               description, images, categories, title_author_key
        FROM books
        WHERE title_author_key = ?
        """,
        (title_author_key,),
        ).fetchone()
        conn.close()
        return row
    
    def fetch_all_rows_by_title_author_key(self, output_db, title_author_key):
        """
        Read all rows from the SQLite `books` table by `title_author_key`.

        Args:
        output_db : str
            Path to the SQLite databaset
        title_author_key : str
            Lowercase lookup key in the form `title|author`.
            
        Returns:
        list[sqlite3.Row]
                All matching rows.
        """
        conn = sqlite3.connect(output_db)
        conn.row_factory = sqlite3.Row
        rows = conn.execute(
            """
            SELECT parent_asin, title, author_name, average_rating, rating_number,
            description, images, categories, title_author_key
            FROM books
            WHERE title_author_key = ?
            ORDER BY parent_asin
            """,
            (title_author_key,),
        ).fetchall()
        conn.close()
        return rows

    def load_books_idx_json(self, output_json_path):
        """
        Read the JSON file mapping parent ASINs to integer row indices.

        Args:
        output_json_path : str
            Path to the output JSON file.
        
        Returns:
        dict
            Mapping from parent ASIN string to integer index.
        """
        with open(output_json_path, "r", encoding="utf-8") as fp:
            return json.load(fp)



class OneShotTestsBooksMetaData(BooksMetaDataTestHelpers):
    """
    One-shot pattern tests for normal expected behavior of the Amazon books
    metadata loading pipeline.
    """

    def test_creates_sqlite_database(self):
        """
            Running `main` should create the output SQLite database file.
        """
        output_db, _ = self.run_main()
        self.assertTrue(Path(output_db).exists(), msg="Expected SQLite database file to be created")

    def test_creates_books_table_and_title_author_index(self):
        """
            Running `main` should create the `books` table and the
            `idx_title_author` index.
        """
        output_db, _ = self.run_main()
        conn = sqlite3.connect(output_db)

        tables = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='books'"
        ).fetchall()
        indexes = conn.execute("PRAGMA index_list(books)").fetchall()

        conn.close()

        self.assertEqual(tables, [("books",)], msg="Expected `books` table to exist")
        self.assertIn(
            "idx_title_author",
            {index[1] for index in indexes},
            msg="Expected `idx_title_author` index to exist",
        )

    def test_stores_expected_cleaned_values_for_valid_row(self):
        """
            A valid row should be retrieved with the expected stored values,
            including serialized JSON fields and cleaned lookup key.
        """
        output_db, _ = self.run_main()
        row = self.fetch_row_by_parent(output_db, "1111111111")

        self.assertEqual(row["title"], "The Great Book")
        self.assertEqual(row["author_name"], "Jane A. Doe")
        self.assertAlmostEqual(row["average_rating"], 4.8)
        self.assertEqual(row["rating_number"], 321)
        self.assertEqual(json.loads(row["description"]), ["Line one", "Line two"])
        self.assertEqual(row["images"], "http://example.com/great-large.jpg")
        self.assertEqual(json.loads(row["categories"]), ["Literature & Fiction", "LGBTQ+"])
        self.assertEqual(row["title_author_key"], "the great book|jane a doe")

    def test_row_is_queryable_by_title_author_key(self):
        """
        A valid row should also be queryable by `title_author_key`, since the
        database stores and indexes that lookup field.
        """
        output_db, _ = self.run_main()
        row = self.fetch_row_by_title_author_key(output_db, "the great book|jane a doe")
        
        self.assertIsNotNone(row, msg="Expected row to be queryable by title_author_key")
        self.assertEqual(row["parent_asin"], "1111111111")
        self.assertEqual(row["title"], "The Great Book")
        self.assertEqual(row["author_name"], "Jane A. Doe")

    def test_column_value_types_after_loading(self):
        """
            Stored SQLite values should have expected Python types after reading
            them back from the database.
        """
        output_db, _ = self.run_main()
        row = self.fetch_row_by_parent(output_db, "1111111111")

        self.assertIsInstance(row["parent_asin"], str)
        self.assertIsInstance(row["title"], str)
        self.assertIsInstance(row["author_name"], str)
        self.assertIsInstance(row["average_rating"], float)
        self.assertIsInstance(row["rating_number"], int)
        self.assertIsInstance(row["description"], str)
        self.assertIsInstance(row["categories"], str)
        self.assertIsInstance(row["title_author_key"], str)
        self.assertIsInstance(json.loads(row["description"]), list)
        self.assertIsInstance(json.loads(row["categories"]), list)

    def test_extracts_cover_image_only_when_large_url_is_valid_http(self):
        """
            The stored cover image should be the first image object's `large`
            value only when it starts with `http`; otherwise it should be None.
        """
        output_db, _ = self.run_main()

        self.assertEqual(
            self.fetch_row_by_parent(output_db, "1111111111")["images"],
            "http://example.com/great-large.jpg",
            msg="Expected valid HTTP large image URL to be stored",
        )
        self.assertIsNone(
            self.fetch_row_by_parent(output_db, "2222222222")["images"],
            msg="Expected invalid non-HTTP large image URL to be ignored",
        )
        self.assertIsNone(
            self.fetch_row_by_parent(output_db, "5555555555")["images"],
            msg="Expected empty image list to result in None",
        )
        self.assertIsNone(
            self.fetch_row_by_parent(output_db, "7777777777")["images"],
            msg="Expected non-HTTP scheme image URL to result in None",
        )

    def test_keeps_only_categories_specified_in_genres_input(self):
        """
            Only categories present in the supplied `categories` input should be
            kept in the stored `categories` field.
        """
        output_db, _ = self.run_main(categories={"Fantasy", "History"})

        self.assertEqual(json.loads(self.fetch_row_by_parent(output_db, "1111111111")["categories"]), [])
        self.assertEqual(json.loads(self.fetch_row_by_parent(output_db, "2222222222")["categories"]), ["History"])
        self.assertEqual(json.loads(self.fetch_row_by_parent(output_db, "5555555555")["categories"]), ["Fantasy"])

    def test_maps_lgbtq_books_to_lgbtq(self):
        """
            The category `LGBTQ+ Books` should be stored as `LGBTQ+`.
        """
        output_db, _ = self.run_main()
        categories_value = json.loads(self.fetch_row_by_parent(output_db, "1111111111")["categories"])

        self.assertEqual(
            categories_value,
            ["Literature & Fiction", "LGBTQ+"],
            msg="Expected `LGBTQ+ Books` to be mapped to `LGBTQ+`",
        )

    def test_title_author_key_is_lowercase_and_author_has_no_periods(self):
        """
            The `title_author_key` should be lowercase, and the author portion
            should have periods removed.
        """
        output_db, _ = self.run_main()
        row = self.fetch_row_by_parent(output_db, "1111111111")

        self.assertEqual(
            row["title_author_key"],
            "the great book|jane a doe",
            msg="Expected lowercase title-author key with author periods removed",
        )

    def test_parent_asin_is_stored_as_string(self):
        """
            `parent_asin` should be stored as a string in SQLite output.
        """
        output_db, _ = self.run_main()
        row = self.fetch_row_by_parent(output_db, "2222222222")

        self.assertEqual(row["parent_asin"], "2222222222")
        self.assertIsInstance(row["parent_asin"], str)

    def test_description_is_stored_as_json_string(self):
        """
            The `description` field should be stored as a JSON-encoded string
            representing a list.
        """
        output_db, _ = self.run_main()
        row = self.fetch_row_by_parent(output_db, "5555555555")

        self.assertEqual(json.loads(row["description"]), ["Standalone"])


class EdgeCaseTestsBooksMetaData(BooksMetaDataTestHelpers):
    """
    Edge-case tests for invalid rows, missing fields, and fallback behavior.
    """

    def test_skips_rows_with_invalid_title_or_missing_parent_asin(self):
        """
            Rows with title equal to `nan` or missing `parent_asin` should be
            skipped and not inserted into the database.
        """
        output_db, _ = self.run_main()
        rows = self.fetch_all_rows_by_parent(output_db)

        self.assertEqual(
            [row["parent_asin"] for row in rows],
            ["1111111111", "2222222222", "5555555555", "6666666666", "7777777777", "8888888888", "9999999999"],
            msg="Expected rows with invalid title or missing parent_asin to be skipped",
        )

    def test_invalid_author_structure_results_in_none_author_fields(self):
        """
            If `author` is not a dict with a valid `name`, then `author_name`
            and `title_author_key` should both be None.
        """
        output_db, _ = self.run_main()

        row_invalid_author_type = self.fetch_row_by_parent(output_db, "5555555555")
        row_null_author_name = self.fetch_row_by_parent(output_db, "7777777777")

        self.assertIsNone(row_invalid_author_type["author_name"])
        self.assertIsNone(row_invalid_author_type["title_author_key"])
        self.assertIsNone(row_null_author_name["author_name"])
        self.assertIsNone(row_null_author_name["title_author_key"])

    def test_missing_categories_field_defaults_to_empty_list(self):
        """
            If the input row does not contain `categories`, the stored categories
            should be an empty list.
        """
        output_db, _ = self.run_main()
        row = self.fetch_row_by_parent(output_db, "7777777777")

        self.assertEqual(
            json.loads(row["categories"]),
            [],
            msg="Expected missing categories field to be stored as an empty list",
        )

    def test_empty_images_list_results_in_none(self):
        """
            If `images` is an empty list, the stored image should be None.
        """
        output_db, _ = self.run_main()
        row = self.fetch_row_by_parent(output_db, "5555555555")

        self.assertIsNone(row["images"], msg="Expected empty images list to result in None")


class DuplicateHandlingTestsBooksMetaData(BooksMetaDataTestHelpers):
    """
    Tests for duplicate behavior involving primary keys and non-unique lookup keys.
    """

    def test_duplicate_parent_asin_replaces_existing_row(self):
        """
            Because the table uses `parent_asin` as the primary key and inserts
            with `INSERT OR REPLACE`, a later duplicate `parent_asin` row should
            replace the earlier one.
        """
        output_db, _ = self.run_main()
        row = self.fetch_row_by_parent(output_db, "6666666666")

        self.assertEqual(row["title"], "Duplicate Winner")
        self.assertEqual(row["author_name"], "Second Author")
        self.assertAlmostEqual(row["average_rating"], 4.9)
        self.assertEqual(row["rating_number"], 50)
        self.assertEqual(json.loads(row["description"]), ["second version"])
        self.assertEqual(row["images"], "http://example.com/dup-second.jpg")
        self.assertEqual(json.loads(row["categories"]), ["History"])
        self.assertEqual(row["title_author_key"], "duplicate winner|second author")
    
    def test_duplicate_title_author_key_is_allowed_for_different_parent_asins(self):
        """
        Duplicate `title_author_key` values should be allowed when the
        `parent_asin` values differ, because the key is indexed but not
        declared UNIQUE.
        """
        output_db, _ = self.run_main()
        rows = self.fetch_all_rows_by_title_author_key(output_db, "shared title|shared author")
        self.assertEqual(
            [row["parent_asin"] for row in rows],
            ["8888888888", "9999999999"],
            msg="Expected duplicate title_author_key values to be allowed for different parent_asin values",
            )

class JsonIndexTestsBooksMetaData(BooksMetaDataTestHelpers):
    """
    Tests for the JSON file that maps valid parent ASINs to row indices.
    """

    def test_writes_books_idx_json_file(self):
        """
            Running `main` should create the JSON file that maps parent ASINs
            to integer indices.
        """
        _, output_json_books_idx = self.run_main()
        self.assertTrue(
            Path(output_json_books_idx).exists(),
            msg="Expected parent ASIN index JSON file to be created",
        )

    def test_json_contains_only_parent_asins_present_in_sqlite(self):
        """
            The JSON keys should match exactly the parent ASINs stored in the
            SQLite database.
        """
        output_db, output_json_books_idx = self.run_main()

        rows = self.fetch_all_rows_by_parent(output_db)
        expected_parent_asins = [row["parent_asin"] for row in rows]
        books_idx = self.load_books_idx_json(output_json_books_idx)

        self.assertEqual(
            sorted(books_idx.keys()),
            expected_parent_asins,
            msg="Expected JSON keys to match the parent ASINs stored in SQLite",
        )

    def test_json_values_are_zero_based_consecutive_integers(self):
        """
            The JSON values should be consecutive zero-based integer indices in
            the same order as the sorted parent ASINs from SQLite.
        """
        output_db, output_json_books_idx = self.run_main()

        rows = self.fetch_all_rows_by_parent(output_db)
        expected_parent_asins = [row["parent_asin"] for row in rows]
        books_idx = self.load_books_idx_json(output_json_books_idx)

        expected_mapping = {
            parent_asin: i for i, parent_asin in enumerate(expected_parent_asins)
        }

        self.assertEqual(
            books_idx,
            expected_mapping,
            msg="Expected JSON mapping to match sorted SQLite parent ASIN order",
        )


if __name__ == "__main__":
    unittest.main()


