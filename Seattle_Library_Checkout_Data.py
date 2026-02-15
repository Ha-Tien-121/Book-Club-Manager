import pandas as pd
import os
import time
import requests
from datetime import datetime
from io import StringIO

output_file = "seattle_library_checkouts_last_year.csv"

APP_TOKEN = os.getenv("SPL_TOKEN")
BASE_URL = "https://data.seattle.gov/resource/tmmm-ytt6.csv"
headers = {
    "X-App-Token": APP_TOKEN
}

# Only get data from the last year
now = datetime.now()
current_month = now.month
current_year = now.year

where_clause = f"(CheckoutYear > {current_year-1}) OR (CheckoutYear = {current_year-1} AND CheckoutMonth >= {current_month})"

offset = 0
first_request = True

while True:
  params = {
        "$where": where_clause,
        "$limit": 10000,
        "$offset": offset,
    }

  response = requests.get(
        BASE_URL,
        params=params,
        headers=headers,
        timeout=60
    )
  response.raise_for_status()

  df = pd.read_csv(StringIO(response.text))

  if df.empty:
      print("No more data")
      break
  if len(df) != 10000:
      print(f"Final page.")

  if first_request:
      df.to_csv(output_file, mode="w", 
                header=first_request, 
                index=False)
      first_request = False
  else:
      df.to_csv(output_file, mode="a", 
                header=first_request, 
                index=False) 

  offset += 10000
  time.sleep(0.3)
