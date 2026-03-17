"""
Cleans the Amazon books meta data by
(1) takes only the author name from author dictionary and strips extra whitespace
(2) takes only the large image url from images list
(3) only keeping subset of most popular categories (genres)

Args:
    Books.jsonl : The input dataset of Amazon books metadata in JSON Lines format.

Returns:
    books.db : SQLite database containing cleaned book metadata.

    Table: books
        parent_asin (TEXT, PRIMARY KEY)
        title (TEXT)
        author_name (TEXT or NULL)
        average_rating (REAL)
        rating_number (INTEGER)
        description (TEXT)        # JSON-encoded list of strings
        images (TEXT or NULL)     # URL string
        categories (TEXT)         # JSON-encoded list of genre labels
        title_author_key (TEXT)   # normalized "title|author" lookup key

    Indexes:
        idx_title_author on (title_author_key)
    
    book_id_to_idx.json : A JSON file mapping parent_asin to integer index for use in 
    reviews processing.

Notes:
    - `title_author_key` stores the normalized lowercase key
      `"title|author"` used for fast lookup of books by title and author.

Usage:
    Run script from the project root using:
    python -m data.scripts.amazon_books_data.books_meta_data
    
Time: ~9 minutes to run
"""


import json
import os

import sqlite3

from data.scripts.helper_functions.format_title import format_title
from data.scripts.helper_functions.format_author import format_author
from data.scripts.config import RAW_DIR, PROCESSED_DIR

INPUT_FILE = os.path.join(RAW_DIR, 'meta_Books.jsonl')
OUTPUT_DB = os.path.join(PROCESSED_DIR, 'books.db')
OUTPUT_JSON_BOOKS_IDX = os.path.join(PROCESSED_DIR, "book_id_to_idx.json")
# pylint: disable=too-many-locals
# This single-pass ingestion keeps intermediate variables explicit for readability.
def main(categories, input_file=INPUT_FILE, output_db=OUTPUT_DB,
         output_json_books_idx = OUTPUT_JSON_BOOKS_IDX):
    """Run the Amazon books metadata cleaning pipeline and create output files."""

    conn = sqlite3.connect(output_db)
    cur = conn.cursor()
    cur.execute("""
    CREATE TABLE IF NOT EXISTS books (
        parent_asin TEXT PRIMARY KEY,
        title TEXT,
        author_name TEXT,
        average_rating REAL,
        rating_number INTEGER,
        description TEXT,
        images TEXT,
        categories TEXT,
        title_author_key TEXT
    )
    """)
    cur.execute("CREATE INDEX IF NOT EXISTS idx_title_author ON books(title_author_key)")
    conn.commit()

    with open(input_file, 'r', encoding='utf-8') as fp:
        for line in fp:
            book = json.loads(line)
            title = format_title(book.get("title"))
            author = book.get("author")
            parent_asin = book.get("parent_asin")
            if (not title or title.lower() == "nan" or not parent_asin
                or str(parent_asin).lower() == "nan"):
                continue
            if isinstance(author, dict) and author.get("name"):
                author_name = format_author(author.get("name"))
            else:
                author_name = None
            images = book.get("images")
            if isinstance(images, list) and images:
                first = images[0]
                large = first.get("large") if isinstance(first, dict) else None
                cover_image = large if large and large.startswith("http") else None
            else:
                cover_image = None
            cats = book.get("categories", [])
            book_genres = [
                'LGBTQ+' if cat == 'LGBTQ+ Books' else cat
                for cat in cats if cat in categories
                ]
            title_clean = title.lower()
            author_clean = author_name.lower().replace(".", "").strip() if author_name else None
            title_author_key = f"{title_clean}|{author_clean}" if author_clean else None
            cur.execute("""
            INSERT OR REPLACE INTO books
            (parent_asin, title, author_name, average_rating, rating_number, description, images, 
                        categories, title_author_key)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                str(parent_asin),
                title,
                author_name,
                book.get("average_rating"),
                book.get("rating_number"),
                json.dumps(book.get("description", [])),
                cover_image,
                json.dumps(book_genres),
                title_author_key
            ))

    conn.commit()
    conn.close()
    print(f"SQLite database created at {output_db}")

    conn = sqlite3.connect(output_db)
    cur = conn.cursor()
    cur.execute("SELECT parent_asin FROM books ORDER BY parent_asin")
    parent_asins = [row[0] for row in cur.fetchall()]
    conn.close()
    book_id_to_idx = {asin: i for i, asin in enumerate(parent_asins)}
    with open(output_json_books_idx, "w", encoding="utf-8") as f:
        json.dump(book_id_to_idx, f)

genres = {
    "Literature & Fiction", "Children's Books", "Mystery, Thriller & Suspense", 
    "Arts & Photography", "History", "Biographies & Memoirs", "Crafts, Hobbies & Home",
    "Business & Money", "Politics & Social Sciences", "Growing Up & Facts of Life",
    "Romance", "Science & Math", "Teen & Young Adult", "Cookbooks, Food & Wine",
    "Religion & Spirituality", "Poetry", "Comics & Graphic Novels", "Travel", "Fantasy",
    "Action & Adventure", "Self-Help", "Science Fiction", "Sports & Outdoors", 
    "Classics", "LGBTQ+ Books"
}

if __name__ == "__main__":
    main(categories=genres)
