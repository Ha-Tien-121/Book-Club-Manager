import ast
import csv
import os
import re
from datetime import datetime

import pandas as pd

RAW_INPUT_PATH = os.getenv(
    "BOOKCLUBS_RAW_PATH", "data/raw/bookclubs_seattle_raw.csv"
)
CLEAN_OUTPUT_PATH = os.getenv(
    "BOOKCLUBS_CLEAN_PATH", "data/processed/bookclubs_seattle_clean.csv"
)


def clean_events(df: pd.DataFrame) -> pd.DataFrame:
    """
    Minimal cleaning:
    - strip whitespace
    - drop rows missing title or link
    - drop duplicate links
    - expand venue details
    - derive start/end datetimes from "when" + start_date
    - reorder columns
    """
    if df.empty:
        return df

    df = df.copy()
    str_cols = [
        "query",
        "title",
        "link",
        "description",
        "when",
        "address",
        "venue",
        "location",
        "thumbnail",
    ]
    for col in str_cols:
        if col in df.columns:
            df[col] = df[col].fillna("").astype(str).str.strip()

    # derive start/end times purely from the "when" column using simple comma splits
    # examples:
    # "Wed, Feb 18, 7:00 – 8:30 PM"
    # "Mon, Feb 16, 7 PM"
    # "Thu, Apr 1, 7 – 8 PM"
    date_pat = re.compile(r"(?P<month>[A-Za-z]+)\s+(?P<day>\d{1,2})(?:\s+(?P<year>\d{4}))?")
    current_year = datetime.now().year

    def _parse_date_match(match_obj) -> datetime | None:
        if not match_obj:
            return None
        month = match_obj.group("month")
        day = match_obj.group("day")
        year = match_obj.group("year") or str(current_year)
        for fmt in ("%b %d %Y", "%B %d %Y"):
            try:
                return datetime.strptime(f"{month} {day} {year}", fmt)
            except ValueError:
                continue
        return None

    def _parse_date_token(token: str) -> datetime | None:
        token = token.strip()
        if not token:
            return None
        for fmt in ("%b %d %Y", "%B %d %Y", "%b %d", "%B %d"):
            try:
                dt = datetime.strptime(token, fmt)
                if "%Y" not in fmt:
                    dt = dt.replace(year=current_year)
                return dt
            except ValueError:
                continue
        # last resort: regex for month/day
        return _parse_date_match(date_pat.search(token))

    def _parse_time_token(t: str, default_ampm: str | None = None):
        """
        Parse a time token that may be 12h with AM/PM or 24h (e.g., 18:30).
        If AM/PM is missing, we optionally inherit default_ampm.
        """
        t = t.strip()
        if not t:
            return None
        ampm_match = re.search(r"([AP]M)", t, re.IGNORECASE)
        ampm = (ampm_match.group(1).upper() if ampm_match else (default_ampm or "")).strip()
        t_no_ampm = re.sub(r"(?i)\s*[AP]M", "", t).strip()

        # Try 12-hour when ampm is available (explicit or inherited)
        if ampm:
            for fmt in ("%I:%M %p", "%I %p"):
                try:
                    return datetime.strptime(f"{t_no_ampm} {ampm}".strip(), fmt).time()
                except ValueError:
                    continue

        # Try 24-hour
        for fmt in ("%H:%M", "%H"):
            try:
                return datetime.strptime(t_no_ampm, fmt).time()
            except ValueError:
                continue

        # As a last fallback, try 12-hour without AM/PM (ambiguous)
        for fmt in ("%I:%M", "%I"):
            try:
                return datetime.strptime(t_no_ampm, fmt).time()
            except ValueError:
                continue
        return None

    def _extract_times(time_str: str):
        """
        Extract up to two times in order from the time token.
        Handles:
        - "7:00 – 8:30 PM"
        - "7 – 8 PM"
        - "7 PM"
        - "11 PM - Thu, Apr 16, 12 AM"
        - "18:00 - 19:30"
        We scan for time-like tokens and then propagate AM/PM from the
        rightmost token to the left when missing.
        """
        if not time_str:
            return []
        clean = time_str.replace("–", "-").replace("—", "-")
        time_regex = re.compile(r"\b(\d{1,2}(?::\d{2})?)\s*([AP]M)?\b", re.IGNORECASE)
        matches = list(time_regex.finditer(clean))
        if not matches:
            return []

        # determine rightmost explicit AM/PM for inheritance
        right_ampm = None
        for m in reversed(matches):
            if m.group(2):
                right_ampm = m.group(2).upper()
                break

        times = []
        for idx, m in enumerate(matches):
            hhmm = m.group(1)
            ampm = m.group(2).upper() if m.group(2) else None

            # skip obvious date tokens like bare "16" with no colon/ampm and >12
            if not ampm and ":" not in hhmm:
                try:
                    if int(hhmm) > 12:
                        continue
                except ValueError:
                    pass

            inherit_ampm = None
            if not ampm and right_ampm and idx == 0:
                inherit_ampm = right_ampm

            times.append(_parse_time_token(hhmm, default_ampm=ampm or inherit_ampm))
            if len(times) == 2:
                break
        return times

    start_iso = []
    end_iso = []
    start_time_col = []
    end_time_col = []
    start_date_col = []
    end_date_col = []
    day_of_week_start_col = []
    day_of_week_end_col = []

    for when_str in df.get("when", []):
        when_str = str(when_str or "")
        tokens = [t.strip() for t in when_str.split(",") if t.strip()]
        day_of_week_token = tokens[0] if tokens else ""
        date_token = tokens[1] if len(tokens) > 1 else ""
        time_token = ",".join(tokens[2:]) if len(tokens) > 2 else (
            tokens[1] if len(tokens) > 1 and re.search(r"\d", tokens[1]) else ""
        )

        dt_date = _parse_date_token(date_token)
        # detect an explicit end date inside the time token (cross-day ranges)
        end_date_override = _parse_date_match(date_pat.search(time_token))

        times = _extract_times(time_token)

        start_dt_val = end_dt_val = None
        start_t_val = end_t_val = None

        if dt_date and times:
            start_t_val = times[0]
            if start_t_val:
                start_dt_val = datetime.combine(dt_date.date(), start_t_val)
            if len(times) > 1:
                end_t_val = times[1]
                if end_t_val:
                    end_date_dt = end_date_override or dt_date
                    end_dt_val = datetime.combine(end_date_dt.date(), end_t_val)
                    # if no explicit end date and end time is earlier than start, assume next day
                    if end_date_override is None and start_dt_val and end_dt_val <= start_dt_val:
                        end_dt_val = end_dt_val.replace(day=end_dt_val.day + 1)

        # normalized date strings
        start_date_str = dt_date.date().isoformat() if dt_date else ""
        end_date_str = (
            end_dt_val.date().isoformat()
            if end_dt_val
            else start_date_str
        )
        start_date_col.append(start_date_str)
        end_date_col.append(end_date_str)

        day_of_week_start_col.append(dt_date.strftime("%a") if dt_date else "")
        day_of_week_end_col.append(
            end_dt_val.strftime("%a") if end_dt_val else day_of_week_start_col[-1]
        )

        start_iso.append(start_dt_val.isoformat() if start_dt_val else "")
        end_iso.append(end_dt_val.isoformat() if end_dt_val else "")

        def _fmt_12h(tval):
            if not tval:
                return ""
            return tval.strftime("%I:%M %p").lstrip("0")

        start_time_col.append(_fmt_12h(start_t_val))
        end_time_col.append(_fmt_12h(end_t_val))

    if "when" in df.columns:
        df["start_date"] = start_date_col
        df["end_date"] = end_date_col
        df["day_of_week_start"] = day_of_week_start_col
        df["day_of_week_end"] = day_of_week_end_col
        df["start_iso"] = start_iso
        df["end_iso"] = end_iso
        df["start_time"] = start_time_col
        df["end_time"] = end_time_col

    # expand venue dict-like strings into explicit columns
    venue_name = []
    venue_rating = []
    venue_reviews = []
    venue_search_link = []
    for val in df.get("venue", []):
        name = rating = reviews = link = ""
        if isinstance(val, dict):
            name = val.get("name", "") or ""
            rating = val.get("rating", "") or ""
            reviews = val.get("reviews", "") or ""
            link = val.get("link", "") or ""
        elif isinstance(val, str) and val:
            try:
                parsed = ast.literal_eval(val)
                if isinstance(parsed, dict):
                    name = parsed.get("name", "") or ""
                    rating = parsed.get("rating", "") or ""
                    reviews = parsed.get("reviews", "") or ""
                    link = parsed.get("link", "") or ""
            except (ValueError, SyntaxError):
                pass
        venue_name.append(str(name).strip())
        venue_rating.append(str(rating).strip())
        venue_reviews.append(str(reviews).strip())
        venue_search_link.append(str(link).strip())

    if "venue" in df.columns:
        df["venue_name"] = venue_name
        df["venue_rating"] = venue_rating
        df["venue_reviews"] = venue_reviews
        df["venue_search_link"] = venue_search_link

    # flatten address list-like strings: first element as address, second as city/state if present
    flattened_address = []
    city_state = []
    for val in df.get("address", []):
        addr = str(val).strip()
        city = ""
        if isinstance(val, list):
            addr = str(val[0]).strip() if val else ""
            city = str(val[1]).strip() if len(val) > 1 else ""
        elif isinstance(val, str) and val.startswith("["):
            try:
                parsed = ast.literal_eval(val)
                if isinstance(parsed, list) and parsed:
                    addr = str(parsed[0]).strip()
                    city = str(parsed[1]).strip() if len(parsed) > 1 else ""
            except (ValueError, SyntaxError):
                pass
        flattened_address.append(addr)
        city_state.append(city)

    if "address" in df.columns:
        df["address"] = flattened_address
        df["city_state"] = city_state

    # drop low-value/raw columns now that we've expanded what we need
    for col in ("query", "location", "venue"):
        if col in df.columns:
            df = df.drop(columns=[col])

    # keep only rows with a title and link
    df = df[df["title"].astype(bool) & df["link"].astype(bool)]
    df = df.drop_duplicates(subset="link")

    # Reorder/select known columns; keep unknowns at the end
    ordered_cols = [
        c
        for c in (
            "title",
            "link",
            "description",
            "when",
            "day_of_week_start",
            "day_of_week_end",
            "start_date",
            "end_date",
            "start_time",
            "end_time",
            "start_iso",
            "end_iso",
            "address",
            "city_state",
            "venue_name",
            "venue_rating",
            "venue_reviews",
            "venue_search_link",
            "thumbnail",
        )
        if c in df.columns
    ]
    remaining_cols = [c for c in df.columns if c not in ordered_cols]
    df = df[ordered_cols + remaining_cols]
    return df


def main():
    if not os.path.exists(RAW_INPUT_PATH):
        raise SystemExit(f"Raw input not found: {RAW_INPUT_PATH}")

    raw_df = pd.read_csv(RAW_INPUT_PATH)
    clean_df = clean_events(raw_df)

    os.makedirs(os.path.dirname(CLEAN_OUTPUT_PATH), exist_ok=True)
    clean_df.to_csv(
        CLEAN_OUTPUT_PATH,
        index=False,
        encoding="utf-8",
        quoting=csv.QUOTE_ALL,
    )
    print(f"Cleaned {len(clean_df)} events -> {CLEAN_OUTPUT_PATH}")


if __name__ == "__main__":
    main()

