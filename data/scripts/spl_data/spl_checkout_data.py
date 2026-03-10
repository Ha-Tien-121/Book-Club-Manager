"""
Cleans and aggregates the SPL books checkout data by 
(1) only retrieving data from checkouts from last year (to the month)
(2) only retrieving items with MaterialType of BOOK, EBOOK, or EAUDIOBOOK
(3) only including necessary columns
(3) cleans title, creator columns, and converts Checkouts to numeric
(4) extracts a 10-digit ISBN from ISBN column if one exists
(5) aggregates checkouts by ISBN (if available) or by Title and Author (if no ISBN)
(6) indexes output json by ISBN if it exists, otherwise by Title and Author

Args:
    Checkouts by Title JSON : Dataset of Seattle Public Library's checkouts, retrieved via the SODA3
    API.

Returns:
    seattle_library_checkouts_last_year.csv : The cleaned dataset as CSV. 
    Columns : Title (str), Author (str: "first last"), ISBN (str: 10 digits), Checkouts (int)
    
    * Note if ISBN exists, Title and Author will be None (this is fine since we can link to the 
    Amazon books dataset by ISBN). If no ISBN, then we keep Title and Author and set ISBN to None.

    Time: ~7 minutes to run
"""

import json
import os

from datetime import datetime
import pandas as pd
from sodapy import Socrata

from data.scripts.helper_functions.format_title import format_title
from data.scripts.helper_functions.format_author import format_author

from data.scripts.spl_data.spl_helper_functions.extract_10_digit_isbn import extract_isbn10


BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', '..'))
OUTPUT_ISBN_INDEX = os.path.join(BASE_DIR, 'data', 'processed', 'spl_checkouts_by_isbn.json')
OUTPUT_TITLE_AUTHOR_INDEX = os.path.join(BASE_DIR, 'data', 'processed',
                                         'spl_checkouts_by_title_author.json')

APP_TOKEN = os.getenv("SPL_TOKEN")
def main(output_isbn_index=OUTPUT_ISBN_INDEX,
         output_title_author_index=OUTPUT_TITLE_AUTHOR_INDEX,
         client=None):
    """
    Fetches, cleans, and aggregates SPL book checkout data from the past year,
    summing checkouts by ISBN (if available) or by Title and Author, and writes
    two JSON indexes for lookup by ISBN and by normalized title|author.
    """
    if client is None:
        if not APP_TOKEN:
            raise ValueError("SPL_TOKEN environment variable is required.")
        client = Socrata("data.seattle.gov", APP_TOKEN, timeout=120)
    current_year = datetime.now().year
    current_month = datetime.now().month
    material_types = ("BOOK", "EBOOK", "EAUDIOBOOK")
    chunks = []
    offset = 0
    while True:
        where_clause = (
            f"((CheckoutYear = {current_year - 1} "
            f"AND CheckoutMonth >= {current_month}) "
            f"OR (CheckoutYear = {current_year})) " 
            f"AND MaterialType IN {material_types}"
            )
        checkouts = client.get(
            "tmmm-ytt6",
            select="Title, Creator, Checkouts, ISBN",
            where=where_clause,
            limit=10000,
            offset=offset)
        if not isinstance(checkouts, list) or len(checkouts) == 0:
            break
        checkouts_df = pd.DataFrame(checkouts)
        checkouts_df["Title"] = format_title(checkouts_df["Title"])
        checkouts_df["Creator"] = format_author(checkouts_df["Creator"])
        checkouts_df["Checkouts"] = pd.to_numeric(checkouts_df["Checkouts"],
                                        errors='coerce').fillna(0).astype(int)
        checkouts_df["ISBN"] = checkouts_df["ISBN"].apply(extract_isbn10)
        chunks.append(checkouts_df)
        offset += 10000
    if not chunks:
        raise ValueError("No data retrieved from SPL API. Please check the API query and " \
        "parameters.") 
    checkouts_df_all = pd.concat(chunks, axis=0, ignore_index=True)
    checkouts_df_all.rename(columns={'Creator': 'Author'}, inplace=True)
    checkouts_with_isbn = checkouts_df_all[pd.notna(checkouts_df_all["ISBN"])].copy()
    checkouts_no_isbn = checkouts_df_all[pd.isna(checkouts_df_all["ISBN"])].copy()
    grouped_isbn = (
        checkouts_with_isbn.groupby('ISBN', as_index=False)['Checkouts'].sum()
        .merge(checkouts_with_isbn.groupby('ISBN')['Title'].first(), on='ISBN', how='left')
        .merge(checkouts_with_isbn.groupby('ISBN')['Author'].first(), on='ISBN', how='left')
        )
    grouped_no_isbn = (
        checkouts_no_isbn.groupby(['Title', 'Author'], as_index=False)
        ['Checkouts'].sum()
        )
    grouped_all = pd.concat([grouped_isbn, grouped_no_isbn], axis=0, ignore_index=True)
    grouped_all = grouped_all.where(pd.notnull(grouped_all), None)
    checkouts_json = grouped_all.to_dict(orient='records')
    books_by_isbn = {}
    books_by_title_author = {}
    count = 0
    max_fake = 999999
    for row in checkouts_json:
        isbn = row.get("ISBN")
        normalized_title = row.get("Title").lower() if row.get("Title") else None
        normalized_author = (row.get("Author").lower().replace(".", "").strip()
                            if row.get("Author") else None)
        if not isbn:
            if count > max_fake:
                print("Warning: too many fake ISBNs! Stopping further fake generation.")
                break
            isbn = f"FAKE{count:06}"
            count += 1
        books_by_isbn[isbn] = {"Title": row.get("Title"),
        "Author": row.get("Author"),
        "ISBN": isbn,
        "Checkouts": int(row["Checkouts"])}
        if normalized_title and normalized_author:
            key = f"{normalized_title}|{normalized_author}"
            books_by_title_author[key] = isbn
    with open(output_isbn_index, 'w', encoding='utf-8') as f:
        json.dump(books_by_isbn, f, ensure_ascii=False, indent=2)
    with open(output_title_author_index, 'w', encoding='utf-8') as f:
        json.dump(books_by_title_author, f, ensure_ascii=False, indent=2)

if __name__ == "__main__":
    main()
