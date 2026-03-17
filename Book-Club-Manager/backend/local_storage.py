"""Local storage implementation.

Extends the base LocalStorage from backend.storage to use the local
`data/processed/books.db` SQLite database for book metadata and details.

- Local (APP_ENV != aws):
    - `get_book_metadata` and `get_book_details` read from `books.db`,
      which includes `description`, so the Book Detail page can show full
      descriptions without hitting S3.
- Cloud (APP_ENV=aws):
    - Uses CloudStorage (in backend.storage) which reads metadata from
      DynamoDB and details from Parquet shards on S3.

All other methods delegate to the base LocalStorage from backend.storage.
"""

from __future__ import annotations

import json
import sqlite3
from pathlib import Path
from typing import Any, Optional

from backend.storage import LocalStorage as _BaseLocalStorage


class LocalStorage(_BaseLocalStorage):
    """Local storage with SQLite-backed books table for metadata/details."""

    def _books_db_path(self) -> Path:
        """Return path to local books.db (data/processed/books.db by default)."""
        try:
            from backend import config

            processed_dir = getattr(config, "PROCESSED_DIR", None)
            if processed_dir is not None:
                return Path(processed_dir) / "books.db"
        except Exception:
            pass
        return Path("data") / "processed" / "books.db"

    def _fetch_book_row(self, parent_asin: str) -> Optional[sqlite3.Row]:
        """Fetch a single row from books.db for the given parent_asin."""
        db_path = self._books_db_path()
        if not db_path.exists():
            return None
        try:
            conn = sqlite3.connect(db_path)
            conn.row_factory = sqlite3.Row
            cur = conn.cursor()
            cur.execute(
                """
                SELECT parent_asin, title, author_name, average_rating, rating_number,
                       description, images, categories, title_author_key
                FROM books
                WHERE parent_asin = ?
                LIMIT 1
                """,
                (str(parent_asin),),
            )
            row = cur.fetchone()
            conn.close()
            return row
        except sqlite3.Error:
            return None

    def _row_to_book_dict(self, row: sqlite3.Row) -> dict[str, Any]:
        """Convert a SQLite books row into a JSON-ready dict."""

        def _loads_or_empty(val):
            """Parse serialized SQLite JSON/text list values into list[str].

            Args:
                val: SQLite field value that may be JSON text, iterable, or scalar.

            Returns:
                list[str]: Parsed values, or an empty list when parsing fails/empty.

            Exceptions:
                None. JSON parsing failures are handled internally.
            """
            if not val:
                return []
            if isinstance(val, (list, tuple)):
                return [str(x) for x in val]
            s = str(val).strip()
            if not s:
                return []
            try:
                parsed = json.loads(s)
                if isinstance(parsed, list):
                    return [str(x) for x in parsed]
                return [str(parsed)]
            except (ValueError, TypeError):
                return [s]

        out: dict[str, Any] = {
            "parent_asin": str(row["parent_asin"]),
            "title": str(row["title"] or ""),
            "author_name": str(row["author_name"] or ""),
            "average_rating": float(row["average_rating"] or 0.0),
            "rating_number": int(row["rating_number"] or 0),
            "images": str(row["images"] or ""),
            "categories": _loads_or_empty(row["categories"]),
            "title_author_key": str(row["title_author_key"] or ""),
        }
        # description is only used in "details" view
        out["description"] = _loads_or_empty(row["description"])
        return out

    def get_book_metadata(self, parent_asin: str) -> Optional[dict[str, Any]]:
        """Return lightweight book metadata from local books.db when available."""
        parent_asin = str(parent_asin or "").strip()
        if not parent_asin:
            return None
        row = self._fetch_book_row(parent_asin)
        if row is not None:
            book = self._row_to_book_dict(row)
            # Strip heavy description for metadata call
            book.pop("description", None)
            return book
        # Fallback to base implementation (which may use S3 or other sources)
        return super().get_book_metadata(parent_asin)

    def get_book_details(self, parent_asin: str) -> Optional[dict[str, Any]]:
        """Return full book details (including description) from local books.db when available."""
        parent_asin = str(parent_asin or "").strip()
        if not parent_asin:
            return None
        row = self._fetch_book_row(parent_asin)
        if row is not None:
            return self._row_to_book_dict(row)
        # Fallback to base implementation (S3 Parquet, etc.)
        return super().get_book_details(parent_asin)

