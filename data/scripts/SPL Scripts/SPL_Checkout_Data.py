"""
Cleans and aggregates the SPL books checkout data by 
(1) only retrieving data from checkouts from last year (to the month)
(2) only retrieving items with MaterialType of BOOK, EBOOK, or EAUDIOBOOK
(3) only including necessary columns
(3) cleans title, creator columns, and converts Checkouts to numeric
(4) extracts a 10-digit ISBN from ISBN column if one exists
(5) aggregates checkouts by ISBN (if available) or by Title and Author (if no ISBN)

Args:
    Checkouts by Title JSON : Dataset of Seattle Public Library's checkouts, retrieved via the SODA3
    API.

Returns:
    seattle_library_checkouts_last_year.csv : The cleaned dataset as CSV. 
    Columns : Title (str), Author (str: "last, first"), ISBN (str: 10 digits), Checkouts (int)
    
    * Note if ISBN exists, Title and Author will be None (this is fine since we can link to the 
    Amazon books dataset by ISBN). If no ISBN, then we keep Title and Author and set ISBN to None.

    Time: ~7 minutes to run
"""

import os

from datetime import datetime
import pandas as pd
from sodapy import Socrata

from Helper_Func_Clean_Text import clean_text


OUTPUT_FILE = "seattle_library_checkouts_last_year.csv"

APP_TOKEN = os.getenv("SPL_TOKEN")
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
    checkouts_df["Title"] = clean_text(checkouts_df["Title"])
    checkouts_df["Creator"] = clean_text(checkouts_df["Creator"])
    checkouts_df["Checkouts"] = pd.to_numeric(checkouts_df["Checkouts"],
                                               errors='coerce').fillna(0).astype(int)
    checkouts_df["ISBN"] = (
        checkouts_df["ISBN"].apply(
            lambda x: next((isbn.strip() for isbn in str(x).split(",")
                            if len(isbn.strip()) == 10), None))
            )
    chunks.append(checkouts_df)
    offset += 10000

checkouts_df_all = pd.concat(chunks, axis=0, ignore_index=True)
checkouts_df_all.rename(columns={'Creator': 'Author'}, inplace=True)

grouped_isbn = (
    checkouts_df_all[pd.notna(checkouts_df_all["ISBN"])]
    .groupby(['ISBN'], as_index=False)['Checkouts']
    .sum()
    )
grouped_no_isbn = (
    checkouts_df_all[pd.isna(checkouts_df_all["ISBN"])]
    .groupby(['Title', 'Author'],as_index=False)['Checkouts']
    .sum()
    )
grouped_all = pd.concat([grouped_isbn, grouped_no_isbn], axis=0, ignore_index=True)
grouped_all.to_csv(OUTPUT_FILE, mode="w", header=True, index=False)
