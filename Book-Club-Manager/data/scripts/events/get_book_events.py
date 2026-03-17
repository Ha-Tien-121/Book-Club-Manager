"""
Fetch book club events via SerpAPI (Google Events) and save to JSON.
"""

import json
import os
import time
from pathlib import Path

import pandas as pd
import requests
from dotenv import load_dotenv

load_dotenv()

# Data paths (anchor on data/ directory where this script lives)
# __file__ = .../data/scripts/events/get_book_events.py
# parents[2] -> .../data
DATA_DIR = Path(__file__).resolve().parents[2]

API_KEY = os.getenv("api_key")
if not API_KEY:
    raise SystemExit("Missing SerpAPI key. Set env var api_key.")

SERP_URL = "https://serpapi.com/search.json"

QUERIES = ["book club events Seattle"]
LOCATION = "Seattle, WA"
MAX_REQUESTS = 10
RAW_JSON_PATH = os.getenv(
    "BOOK_EVENTS_RAW_PATH",
    str(DATA_DIR / "raw" / "book_events_raw.json"),
)


def extract_dates(ev: dict) -> tuple[str, str, str]:
    """
    Pull when/start_date/end_date from google_events payload.
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
    queries: list[str] | None = None,
    location: str = LOCATION,
    max_requests: int = MAX_REQUESTS,
    sleep_s: float = 1.0,
) -> tuple[pd.DataFrame, int]:
    """
    Fetch book club events via SerpAPI google_events.

    Returns:
        (DataFrame, request_count)
    """
    if queries is None:
        queries = QUERIES
    results = []
    request_count = 0

    for query in queries:
        for start in range(0, 100, 10):
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
            resp = requests.get(SERP_URL, params=params, timeout=30)
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


def main() -> None:
    """Fetch events from SerpAPI and write raw JSON."""
    df, request_count = fetch_events()
    out_dir = os.path.dirname(RAW_JSON_PATH)
    if out_dir:
        os.makedirs(out_dir, exist_ok=True)
    records = df.to_dict(orient="records")
    for rec in records:
        for key, val in list(rec.items()):
            if isinstance(val, dict):
                rec[key] = val
            elif hasattr(val, "tolist"):
                rec[key] = val.tolist()
            elif isinstance(val, list):
                rec[key] = val
            elif val is None or (isinstance(val, float) and pd.isna(val)):
                rec[key] = None
            else:
                rec[key] = str(val)
    with open(RAW_JSON_PATH, "w", encoding="utf-8") as f:
        json.dump(records, f, indent=2, ensure_ascii=False)
    print(f"Saved {len(df)} events to {RAW_JSON_PATH}")
    print(f"SerpAPI requests used: {request_count}")


if __name__ == "__main__":
    main()
