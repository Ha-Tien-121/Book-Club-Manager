import os
import requests
import pandas as pd
import csv
import time

API_KEY = os.getenv("api_key")
url = "https://serpapi.com/search.json"

results = []
start = 0
for i in range(0,10):
    params = {
        "engine": "google", 
        "q": "site:meetup.com \"book club\" \"Seattle\"",  # search meetup.com
        "hl": "en",
        "gl": "us",
        "start": start,
        "api_key": API_KEY
        }
    time.sleep(1) # avoid throttleing
    response = requests.get(url, params=params)
    response.raise_for_status() # check for HTTP errors
    data = response.json()

    if not data.get("organic_results", []): 
        break # exit if no more results

    for r in data.get("organic_results", []):
        results.append({ 
            "title": r.get("title", ""),
            "link": r.get("link", ""),
            "snippet": r.get("snippet", "")
        })

    start = start + 10 # next page



results_data = pd.DataFrame(results)
results_data = results_data.drop_duplicates(subset="link")

results_data.to_csv(
        "bookclubs_seattle.csv",
        index=False,
        encoding="utf-8",
        quoting=csv.QUOTE_ALL
        )

print(f"Saved {len(results)} book clubs to meetup_seattle.csv")