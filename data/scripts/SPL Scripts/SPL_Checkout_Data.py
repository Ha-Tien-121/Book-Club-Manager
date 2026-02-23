import pandas as pd
import requests
import os
from datetime import datetime
from Helper_Func_Clean_Text import clean_text

output_file = "seattle_library_checkouts_last_year.csv"

APP_TOKEN = os.getenv("SPL_TOKEN")
BASE_URL = "https://data.seattle.gov/resource/tmmm-ytt6.json"
headers = {
    "X-App-Token": APP_TOKEN
}

current_year = datetime.now().year
current_month = datetime.now().month

chunks = []

for year in [current_year - 1, current_year]:
    for month in range(1, 13):

        if year == current_year - 1 and month < current_month:
            continue

        material_types = ['BOOK', 'EBOOK', 'EAUDIOBOOK']
        for mtype in material_types:
            offset = 0
           
            while True:
                where_clause = f"""
                  CheckoutYear = {year}
                  AND CheckoutMonth = {month}
                  AND MaterialType = '{mtype}'
                  """
                params = {
                    "$select": "Title, Creator, Checkouts, ISBN",
                    "$where": where_clause,
                    "$limit": 10000,
                    "$offset": offset
                    }
                response = requests.get(
                    BASE_URL,
                    params=params,
                    headers=headers,
                    timeout=120
                    )
                response.raise_for_status()
                
                checkouts = response.json()
                if not isinstance(checkouts, list) or len(checkouts) == 0:
                    break
                
                checkouts_df = pd.DataFrame(checkouts)
                checkouts_df["Title"] = clean_text(checkouts_df["Title"])
                checkouts_df["Creator"] = clean_text(checkouts_df["Creator"])
                checkouts_df["Checkouts"] = pd.to_numeric(checkouts_df["Checkouts"], errors='coerce').fillna(0).astype(int)
                checkouts_df["ISBN"] = checkouts_df["ISBN"].apply(
                lambda x: next(
                    (isbn.strip() for isbn in str(x).split(",") 
                     if len(isbn.strip()) == 10),
                     None
                     )
                     )
                chunks.append(checkouts_df)
                offset += 10000

checkouts_df_all = pd.concat(chunks, axis=0, ignore_index=True)
checkouts_df_all.rename(columns={'Creator': 'Author'}, inplace=True)

grouped_isbn = checkouts_df_all.groupby(['ISBN', 'Title', 'Author'], as_index=False)['Checkouts'].sum()
grouped_no_isbn = checkouts_df_all[pd.isna(checkouts_df_all["ISBN"])].groupby(['Title', 'Author'], as_index=False)['Checkouts'].sum()


grouped_all = pd.concat([grouped_isbn, grouped_no_isbn], axis=0, ignore_index=True)

grouped_all.to_csv(output_file, mode="w", 
                header=True, 
                index=False)