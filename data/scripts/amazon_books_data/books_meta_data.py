"""
Cleans the Amazon books meta data by
(3) takes only the author name from "Author" column and reformats to "last, first"
(4) takes only the large image url from "images" column
(2) only keeping subset of most popular categories (genres)
(4) create a minimal dictionary of relevant columns for each book, and write to output files
(5) adds keys for JSONL indexing by parent_asin and JSONL title|author

Args:
    Books.jsonl : The input dataset of Amazon books metadata in JSON Lines format.

Returns:
    by_parent_asin.jsonl : dictionary indexed by parent_asin
        Columns : title (str), average_rating (float), rating_number (int),
                  description (list of str), images (str: url), categories (list of str),
                  parent_asin (str: 10 digits), author_name (str: "last, first")
    by_title_author.jsonl : a lookup dictionary indexed by title|author
        Columns : parent_asin (str: 10 digits)

Time: ~6 minutes to run
"""


import json
import sys
import os

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))

from scripts.helper_functions.format_title import format_title # pylint: disable=wrong-import-position
from scripts.helper_functions.format_author import format_author # pylint: disable=wrong-import-position


BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', '..'))
INPUT_FILE = os.path.join(BASE_DIR, 'data', 'raw', 'meta_Books.jsonl')
OUTPUT_PARENT_ASIN_INDEX = os.path.join(BASE_DIR, 'data', 'processed', 'books_by_parent_asin.jsonl')
OUTPUT_TITLE_AUTHOR_INDEX = os.path.join(BASE_DIR, 'data', 'processed',
                                         'books_by_title_author.jsonl')

genres = {
    "Literature & Fiction", "Children's Books", "Mystery, Thriller & Suspense", 
    "Arts & Photography", "History", "Biographies & Memoirs", "Crafts, Hobbies & Home",
    "Business & Money", "Politics & Social Sciences", "Growing Up & Facts of Life",
    "Romance", "Science & Math", "Teen & Young Adult", "Cookbooks, Food & Wine",
    "Religion & Spirituality", "Poetry", "Comics & Graphic Novels", "Travel", "Fantasy",
    "Action & Adventure", "Self-Help", "Science Fiction", "Sports & Outdoors", 
    "Classics", "LGBTQ+ Books"
}

with open(INPUT_FILE, 'r', encoding='utf-8') as fp, \
     open(OUTPUT_PARENT_ASIN_INDEX, 'w', encoding='utf-8') as f_parent, \
     open(OUTPUT_TITLE_AUTHOR_INDEX, 'w', encoding='utf-8') as f_title:

    for line in fp:
        book = json.loads(line)
        title = format_title(book.get("title"))
        author = book.get("author")
        parent_asin = book.get("parent_asin")

        if (not title or title.lower() == "nan" or not parent_asin
            or str(parent_asin).lower() == "nan"):
            continue
        if isinstance(author, dict) and author.get("name"):
            name = author["name"].strip()
            parts = name.split()
            if len(parts) >= 2:
                author_name = f"{parts[-1]}, {' '.join(parts[:-1])}"
                author_name = format_author(author_name)
            else:
                author_name = format_author(name)
        else:
            author_name = None

        images = book.get("images")
        if isinstance(images, list) and images:
            first = images[0]
            COVER_IMAGE = first.get("large") if isinstance(first, dict) else None
        else:
            COVER_IMAGE = None

        cats = book.get("categories", [])
        book_genres = [
            'LGBTQ+' if cat == 'LGBTQ+ Books' else cat
            for cat in cats if cat in genres
        ]

        book_minimal = {
            "title": title,
            "author_name": author_name,
            "average_rating": book.get("average_rating"),
            "rating_number": book.get("rating_number"),
            "description": book.get("description", []),
            "images": COVER_IMAGE,
            "categories": book_genres,
            "parent_asin": str(parent_asin)
        }

        parent = book_minimal["parent_asin"]

        title_clean = title.lower()
        author_clean = author_name.lower().replace(".", "").strip() if author_name else None
        (f_parent.write(json.dumps({parent: book_minimal}, ensure_ascii=False)
                        .replace('\\/', '/') + "\n"))
        if author_clean:
            title_author_key = f"{title_clean}|{author_clean}"
            f_title.write(json.dumps({title_author_key: parent}, ensure_ascii=False) + "\n")
