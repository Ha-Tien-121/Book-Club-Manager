import pandas as pd
import time
import os
import requests
from io import StringIO

APP_TOKEN = os.getenv("SPL_TOKEN")
BASE_URL = "https://data.seattle.gov/resource/6vkj-f5xf.csv"
headers = {
    "X-App-Token": APP_TOKEN
}

response = requests.get(
    BASE_URL,
    headers=headers,
    params={"$select": "max(reportdate)"}
)
response.raise_for_status()
latest_date = response.text.splitlines()[1]
print("Latest reportdate:", latest_date)


first_request = True

output_file = "seattle_library_catalog_last_year.csv"

offset = 0
while True:

    params = {
        "$where": f"reportdate = {latest_date}",
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