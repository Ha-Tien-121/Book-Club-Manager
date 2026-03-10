"""
Cleans the SPL physical books catalog data by 
(1) only retrieving data from the most recent report date
(2) only retrieving data of items with ItemType ending in "bk" (books)
(3) only including necessary columns
(4) formats title, author columns, and converts ItemCount to numeric
(5) extracts a 10-digit ISBN from ISBN column if one exists
(6) reformats data so there is one row for each book and all library branches with that book are in 
a single column (branch_counts = dict{branch: count})
(7) creates unique fake indexes for books without ISBNs (FAKE000000, FAKE000001, etc.)
(8) indexes output jsons by ISBN/fake ISBN and normalized title|author (if they exist)

Args:
   Library Collection Inventory JSON : Dataset of Seattle Public Library's collection, retrieved via 
   the SODA3 API.

Returns:
    spl_catalog_by_isbn.json : The cleaned dataset indexed by ISBN (or fake ISBN) as JSON.
        Columns : Title (str), Author (str: "first last"), ISBN (str: 10 digits), 
                  branch_counts (dict{branch: count})
    spl_catalog_by_title_author.json : A lookup dictionary indexed by normalized title|author.
        Columns : ISBN (str: 10 digits)

Time: ~5 minutes to run due to API.
"""

import json
import os

import pandas as pd
from sodapy import Socrata

from data.scripts.helper_functions.format_author import format_author
from data.scripts.helper_functions.format_title import format_title

from data.scripts.spl_data.spl_helper_functions.extract_10_digit_isbn import extract_isbn10

BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', '..'))
OUTPUT_ISBN_INDEX = os.path.join(BASE_DIR, 'data', 'processed', 'spl_catalog_by_isbn.json')
OUTPUT_TITLE_AUTHOR_INDEX = os.path.join(BASE_DIR, 'data', 'processed',
                                         'spl_catalog_by_title_author.json')

APP_TOKEN = os.getenv("SPL_TOKEN")
def main(output_isbn_index=OUTPUT_ISBN_INDEX,
         output_title_author_index=OUTPUT_TITLE_AUTHOR_INDEX,
         client=None):
    """
    Fetches and cleans the latest SPL book inventory, aggregates branch counts,
    generates missing ISBNs, and writes two JSON indexes (by ISBN and by title|author).
    """

    if client is None:
        if not APP_TOKEN:
            raise ValueError("SPL_TOKEN environment variable is required.")
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
    if not chunks:
        raise ValueError("No data retrieved from SPL API. Please check the API query and " \
        "parameters.")
    catalog_df_all = pd.concat(chunks, axis=0, ignore_index=True)
    catalog_df_all["ISBN"] = catalog_df_all["ISBN"].apply(lambda x: x if x else "__MISSING_ISBN__")
    # rows = (Title, Author, ISBN), columns = branches, values = sum(ItemCount)
    pivot_catalog = catalog_df_all.pivot_table(
        index=['Title', 'Author', 'ISBN'], columns='ItemLocation', values='ItemCount',
        aggfunc='sum', fill_value=0)
    pivot_catalog['branch_counts'] = pivot_catalog.apply(
        lambda row: {branch: count for branch, count in row.to_dict().items() if count > 0},
        axis=1)
    pivot_catalog.reset_index(inplace=True)
    pivot_catalog = pivot_catalog.where(pd.notna(pivot_catalog), None)
    catalog_json = (pivot_catalog[['Title', 'Author', 'ISBN', 'branch_counts']]
                    .to_dict(orient='records'))
    count = 0
    max_fake = 999999
    books_by_isbn = {}
    books_by_title_author = {}
    for book in catalog_json:
        isbn = book.get("ISBN")
        title_normalized = book.get("Title").lower() if book.get("Title") else None
        author_normalized = (book.get("Author").lower().replace(".", "").strip() 
                             if book.get("Author") else None)
        if isbn != "__MISSING_ISBN__":
            books_by_isbn[isbn] = book
        else:
            if count > max_fake:
                print("Warning: too many fake ISBNs! Stopping further fake generation.")
                break
            isbn = f"FAKE{count:06}"
            count += 1
            books_by_isbn[isbn] = book
        if title_normalized and author_normalized:
            key = f"{title_normalized}|{author_normalized}"
            books_by_title_author[key] = isbn
    with open(output_isbn_index, "w", encoding="utf-8") as f:
        json.dump(books_by_isbn, f, ensure_ascii=False, indent=2)
    with open(output_title_author_index, "w", encoding="utf-8") as f:
        json.dump(books_by_title_author, f, ensure_ascii=False, indent=2)

if __name__ == "__main__":
    main()
