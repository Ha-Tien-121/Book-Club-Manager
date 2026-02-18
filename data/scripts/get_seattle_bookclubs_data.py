import os
import time
import csv
import requests
import pandas as pd
from dotenv import load_dotenv

load_dotenv()

API_KEY = os.getenv("api_key")
if not API_KEY:
    raise SystemExit("Missing SerpAPI key. Set env var api_key.")

SERP_URL = "https://serpapi.com/search.json"

# Event-centric queries for Seattle book clubs
QUERIES = ["book club events Seattle"]
LOCATION = "Seattle, WA"
MAX_REQUESTS = 10  # cap total SerpAPI calls per run
RAW_OUTPUT_PATH = os.getenv(
    "BOOKCLUBS_RAW_PATH", "data/raw/bookclubs_seattle_raw.csv"
)


def extract_dates(ev: dict) -> tuple[str, str, str]:
    """
    Pull when/start_date/end_date from google_events payload.
    google_events often nests this under ev['date'] with start_date/end_date/when.
    """
    when = ev.get("when", "")
    start_date = ev.get("start_date", "")
    end_date = ev.get("end_date", "")
    date_obj = ev.get("date") or {}
    if not when:
        when = date_obj.get("when", "")
    if not start_date:
        start_date = date_obj.get("start_date", "")
    if not end_date:
        end_date = date_obj.get("end_date", "")
    return when, start_date, end_date


def fetch_events(
    queries: list[str] = QUERIES,
    location: str = LOCATION,
    max_requests: int = MAX_REQUESTS,
    sleep_s: float = 1.0,
) -> tuple[pd.DataFrame, int]:
    """
    Fetch book club events via SerpAPI google_events.

    Returns:
        (DataFrame, request_count)
    """
    results = []
    request_count = 0

    for query in queries:
        for start in range(0, 100, 10):  # up to 10 pages per query
            if request_count >= max_requests:
                break
            params = {
                "engine": "google_events",
                "q": query,
                "hl": "en",
                "location": location,
                "api_key": API_KEY,
                "start": start,
            }
            resp = requests.get(SERP_URL, params=params)
            request_count += 1
            resp.raise_for_status()
            payload = resp.json()
            events = payload.get("events_results", []) or []
            if not isinstance(events, list) or not events:
                print(f"[info] No events returned for query '{query}' at start={start}")
                break

            for ev in events:
                when, start_date, end_date = extract_dates(ev)
                results.append(
                    {
                        "query": query,
                        "title": ev.get("title", ""),
                        "link": ev.get("link", ""),
                        "description": ev.get("description", ""),
                        "when": when,
                        "start_date": start_date,
                        "end_date": end_date,
                        "address": ev.get("address", ""),
                        "venue": ev.get("venue", ""),
                        "location": ev.get("where", ""),
                        "thumbnail": ev.get("thumbnail", ""),
                    }
                )
            time.sleep(sleep_s)
        if request_count >= max_requests:
            break

    df = pd.DataFrame(results).drop_duplicates(subset="link")
    return df, request_count


def main():
    df, request_count = fetch_events()
    os.makedirs(os.path.dirname(RAW_OUTPUT_PATH), exist_ok=True)
    df.to_csv(
        RAW_OUTPUT_PATH,
        index=False,
        encoding="utf-8",
        quoting=csv.QUOTE_ALL,
    )
    print(f"Saved {len(df)} book club events to {RAW_OUTPUT_PATH}")
    print(f"SerpAPI requests used: {request_count}")


if __name__ == "__main__":
    main()