"""
Cleans the SPL physical books catalog data by 
(1) only retrieving data from the most recent report date
(2) only retrieving data of items with ItemType ending in "bk" (books)
(3) only including necessary columns
(4) formats title, author columns, and converts ItemCount to numeric
(5) extracts a 10-digit ISBN from ISBN column if one exists
(6) reformats data so there is one row for each book and all library branches with that book are in 
a single column (branch_counts = dict{branch: count})
(7) creates unique fake indexes for books without ISBNs (FAKE000001, FAKE000002, etc.)
(8) indexes output jsons by ISBN/fake ISBN and normalized title|author (if they exist)

Args:
   Library Collection Inventory JSON : Dataset of Seattle Public Library's collection, retrieved via 
   the SODA3 API.

Returns:
    spl_catalog_by_isbn.json : The cleaned dataset indexed by ISBN (or fake ISBN) as JSON.
        Columns : Title (str), Author (str: "last, first"), ISBN (str: 10 digits), 
                  branch_counts (dict{branch: count})
    spl_catalog_by_title_author.json : A lookup dictionary indexed by normalized title|author.
        Columns : ISBN (str: 10 digits)

Time: ~5 minutes to run due to API.
"""

import json
import os
import sys

import pandas as pd
from sodapy import Socrata

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))
from scripts.helper_functions.format_author import format_author # pylint: disable=wrong-import-position
from scripts.helper_functions.format_title import format_title # pylint: disable=wrong-import-position

from scripts.spl_data.spl_helper_functions.extract_10_digit_isbn import extract_isbn10 # pylint: disable=wrong-import-position

BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', '..'))
OUTPUT_ISBN_INDEX = os.path.join(BASE_DIR, 'data', 'processed', 'spl_catalog_by_isbn.json')
OUTPUT_TITLE_AUTHOR_INDEX = os.path.join(BASE_DIR, 'data', 'processed',
                                         'spl_catalog_by_title_author.json')

APP_TOKEN = os.getenv("SPL_TOKEN")
client = Socrata("data.seattle.gov", APP_TOKEN, timeout=120)

latest_date = client.get(
           "6vkj-f5xf",
           select="max(reportdate)",
            )

latest_date = latest_date[0]["max_reportdate"]
print("Latest reportdate:", latest_date)

offset = 0
chunks = []
while True:
    catalog = client.get(
           "6vkj-f5xf",
           select="Title, Author, ItemLocation, ItemCount, ISBN",
           where=f"reportdate = '{latest_date}' AND lower(ItemType) LIKE '%bk'",
           limit=10000,
           offset=offset
            )
    if not isinstance(catalog, list) or len(catalog) == 0:
        break
    catalog_df = pd.DataFrame(catalog)
    catalog_df["Title"] = format_title(catalog_df["Title"])
    catalog_df["Author"] = format_author(catalog_df["Author"])
    catalog_df["ItemCount"] = pd.to_numeric(catalog_df["ItemCount"], errors="coerce")
    catalog_df["ISBN"] = catalog_df["ISBN"].apply(extract_isbn10)
    chunks.append(catalog_df)
    offset += 10000

catalog_df_all = pd.concat(chunks, axis=0, ignore_index=True)
# rows = (Title, Author, ISBN), columns = branches, values = sum(ItemCount)
pivot_catalog = catalog_df_all.pivot_table(
    index=['Title', 'Author', 'ISBN'],
    columns='ItemLocation',
    values='ItemCount',
    aggfunc='sum',
    fill_value=0
)
pivot_catalog['branch_counts'] = pivot_catalog.apply(
    lambda row: {branch: count for branch, count in row.to_dict().items() if count > 0},
    axis=1
)
pivot_catalog.reset_index(inplace=True)
pivot_catalog = pivot_catalog.where(pd.notna(pivot_catalog), None)
catalog_json = pivot_catalog[['Title', 'Author', 'ISBN', 'branch_counts']].to_dict(orient='records')

count = 0
MAX_FAKE = 999999
books_by_isbn = {}
books_by_title_author = {}
for book in catalog_json:
    isbn = book.get("ISBN")
    title_normalized = book.get("Title").lower() if book.get("Title") else None
    author_normalized = (book.get("Author").lower().replace(".", "").strip()
                         if book.get("Author") else None)
    if isbn:
        books_by_isbn[isbn] = book
    else:
        if count > MAX_FAKE:
            print("Warning: too many fake ISBNs! Stopping further fake generation.")
            break
        isbn = f"FAKE{count:06}"
        count += 1
        books_by_isbn[isbn] = book
    if title_normalized and author_normalized:
        key = f"{title_normalized}|{author_normalized}"
        books_by_title_author[key] = isbn
with open(OUTPUT_ISBN_INDEX, "w", encoding="utf-8") as f:
    json.dump(books_by_isbn, f, ensure_ascii=False, indent=2)

with open(OUTPUT_TITLE_AUTHOR_INDEX, "w", encoding="utf-8") as f:
    json.dump(books_by_title_author, f, ensure_ascii=False, indent=2)
