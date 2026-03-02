"""
Cleans the SPL physical books catalog data by 
(1) only retrieving data from the most recent report date
(2) only retrieving data of items with ItemType ending in "bk" (books)
(3) only including necessary columns
(3) cleans title, author columns, and converts ItemCount to numeric
(4) extracts a 10-digit ISBN from ISBN column if one exists
(5) reformats data so there is one row for each book and all library branches with that book are in 
a single column (branch_counts = dict{branch: count})

Args:
   Library Collection Inventory JSON : Dataset of Seattle Public Library's collection, retrieved via 
   the SODA3 API.

Returns:
    seattle_library_catalog.csv : The cleaned dataset as CSV. 
    Columns : Title (str), Author (str: "last, first"), ISBN (str: 10 digits), 
              branch_counts (str: JSON dict{branch: count})

Time: ~5 minutes to run due to API.
"""

import json
import os

import pandas as pd
from sodapy import Socrata

from Helper_Func_Clean_Text import clean_text


OUTPUT_FILE = "seattle_library_catalog.csv"

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
    catalog_df["Title"] = clean_text(catalog_df["Title"])
    catalog_df["Author"] = clean_text(catalog_df["Author"])
    catalog_df["ItemCount"] = pd.to_numeric(catalog_df["ItemCount"], errors="coerce")
    catalog_df["ISBN"] = (
        catalog_df["ISBN"].apply(
            lambda x: next(
                    (isbn.strip() for isbn in str(x).split(",")
                     if len(isbn.strip()) == 10), None))
                    )
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
    lambda row: json.dumps({branch: count for branch, count in row.to_dict().items() if count > 0}),
    axis=1
)

pivot_catalog.reset_index(inplace=True)

pivot_catalog[['Title', 'Author', 'ISBN', 'branch_counts']].to_csv(OUTPUT_FILE, index=False)
