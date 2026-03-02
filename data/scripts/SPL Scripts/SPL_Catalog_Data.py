"""
Cleans the SPL physical books catalog data by 
(1) only looking at the most recent report date (most recent snapshot of branch inventories)
(2) only looking at items with ItemType ending in "bk" (books)
(3) excluding unnecessary columns
(3) cleans title, author columns, and converts ItemCount to numeric
(4) extracts a 10-digit ISBN from ISBN column if one exists
(5) reformats data so there is one row for each book and all library branches with that book are in 
a single column (branch_counts = dict{branch: count})

Args:
    Library Collection Inventory json: The input dataset of Seattle Public Library collection 
    inventory json API call (SODA2).

Returns:
    seattle_library_catalog.csv: The cleaned dataset as csv. 
    Columns: Title (str), Author (str: "last, first"), ISBN (int: 10 digits), 
    branch_counts (str: json dict{branch: count})
"""

import pandas as pd
import requests
import json
import os
from Helper_Func_Clean_Text import clean_text


output_file = "seattle_library_catalog.csv"

APP_TOKEN = os.getenv("SPL_TOKEN")
BASE_URL = "https://data.seattle.gov/resource/6vkj-f5xf.json"
headers = {
    "X-App-Token": APP_TOKEN
}

response = requests.get(
    BASE_URL,
    headers=headers,
    params={"$select": "max(reportdate)"}
)
response.raise_for_status()
latest_date = response.json()
latest_date = latest_date[0]["max_reportdate"]
print("Latest reportdate:", latest_date)

offset = 0
chunks = []

while True:

    params = {
        "$select": "Title, Author, ItemLocation, ItemCount, ISBN",
        "$where": f"reportdate = '{latest_date}' AND lower(ItemType) LIKE '%bk'",
        "$limit": 10000,
        "$offset": offset,
    }

    response = requests.get(
        BASE_URL,
        params=params,
        headers=headers,
        timeout=120
    )
    response.raise_for_status()

    catalog = response.json()
    if not isinstance(catalog, list) or len(catalog) == 0:
          break
    
    catalog_df = pd.DataFrame(catalog)
    catalog_df["Title"] = clean_text(catalog_df["Title"])
    catalog_df["Author"] = clean_text(catalog_df["Author"])
    catalog_df["ItemCount"] = pd.to_numeric(catalog_df["ItemCount"], errors="coerce")
    catalog_df["ISBN"] = catalog_df["ISBN"].apply(
                lambda x: next(
                    (isbn.strip() for isbn in str(x).split(",") 
                     if len(isbn.strip()) == 10),
                     None
                     )
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

pivot_catalog[['Title', 'Author', 'ISBN', 'branch_counts']].to_csv(output_file, index=False)