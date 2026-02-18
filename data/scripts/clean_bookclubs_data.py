import ast
import csv
import os
import re
import html
from datetime import datetime

import pandas as pd

RAW_INPUT_PATH = os.getenv(
    "BOOKCLUBS_RAW_PATH", "data/raw/bookclubs_seattle_raw.csv"
)
CLEAN_OUTPUT_PATH = os.getenv(
    "BOOKCLUBS_CLEAN_PATH", "data/processed/bookclubs_seattle_clean.csv"
)

# Keyword map for genre/affinity tagging
TAG_KEYWORDS = {
    "fantasy": ["fantasy", "urban fantasy", "epic fantasy", "sword & sorcery", "sword and sorcery"],
    "sci-fi": ["sci-fi", "science fiction", "sf", "speculative", "time travel", "time-travel", "dystopian"],
    "historical": ["historical", "history", "wwii", "world war", "period"],
    "mystery": ["mystery", "thriller", "suspense", "crime", "whodunit", "detective", "noir"],
    "romance": ["romance", "rom-com", "rom com", "romantic"],
    "horror": ["horror", "gothic", "spooky", "ghost", "haunted"],
    "literary": ["literary", "fiction", "novel"],
    "nonfiction": ["nonfiction", "non-fiction", "nf", "essay", "essays", "biography", "bio"],
    "lgbtq": ["lgbt", "lgbtq", "lgbtq+", "queer", "sapphic", "trans", "nonbinary", "non-binary", "gay", "lesbian"],
    "ya": ["young adult", "teen", "teens", "teenager", "teenagers", "tween", "tweens", "tweenager", "tweenagers", "ya"],
    "kids": ["kids", "kid", "children", "childrens", "children's", "family", "families", "youth", "toddler", "toddlers"],
    "graphic": ["graphic novel", "graphic novels", "comic", "comics", "manga"],
    "poetry": ["poetry", "poem", "poems", "poet"],
    "classics": ["classic", "classics", "canon", "canonical"],
    "finance": ["bookkeeping", "finance", "financial", "budget", "budgeting"],
}


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
    book_title_col = []
    book_author_col = []
    tags_col = []

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

    # Extract book title/author from title then description
    def _strip_emphasis(val: str) -> str:
        return re.sub(r"[*_`]+", "", val or "")

    def _clean_token(val: str) -> str:
        return val.strip(" \t\r\n\"'`*_-–—.,;:").strip()

    def _is_valid(val: str, require_capital: bool = True) -> bool:
        if not val:
            return False
        if "book club" in val.lower():
            # allow if "book club" is inside quotes OR not at the start
            if re.search(r"[\"“”'][^\"“”']*book\s*club[^\"“”']*[\"“”']", val, flags=re.IGNORECASE):
                pass
            elif not re.match(r"(?i)^\s*book\s*club\b", val):
                pass
            else:
                return False
        if require_capital and not re.match(r"^[A-Z]", val):
            return False
        return True

    def _trim_author_tail(val: str) -> str:
        """
        Stop at strong boundaries, allowing hyphens in names.
        """
        val = re.split(r"[!?;,\n]", val, 1)[0]  # keep periods to allow initials (e.g., M.L. Wang)
        # If this looks like initials + surname (e.g., "M.L. Wang"), don't split on the period.
        if not re.match(r"^(?:[A-Z]\.){1,3}\s+[A-Z]", val):
            # split on period+Capital with or without a space (sentence break), preserving initials
            val = re.split(r"\.(?![A-Z]\b)\s*(?=[A-Z])", val, 1)[0]
        val = re.split(
            r"\b(join|welcome|registration|register|discussion|club|reading|book|event|tickets)\b",
            val,
            1,
            flags=re.IGNORECASE,
        )[0]
        return _clean_token(val)

    def _trim_title_tail(val: str) -> str:
        """
        Trim trailing punctuation and, if a suffix has >=3 consecutive lowercase-start
        words, drop that suffix (and anything after it).
        """
        val = val.rstrip("!.?;,:")
        tokens = val.split()
        n = len(tokens)
        cut_idx = None
        for i in range(n - 1, 1, -1):
            # check run ending at i of length 3
            if i - 2 >= 0 and tokens[i][:1].islower() and tokens[i - 1][:1].islower() and tokens[i - 2][:1].islower():
                cut_idx = i - 2  # drop from this run start onward
                break
        if cut_idx is not None:
            tokens = tokens[:cut_idx]
        return " ".join(tokens).strip("!.?;,: ")

    def _strip_book_club_prefix(val: str) -> str:
        """
        If "book club" appears, drop everything up to and including it to keep the actual title.
        Skip stripping if the phrase appears inside quotes.
        """
        # If the phrase is inside quotes, leave it
        if re.search(r"[\"“”']\s*[^\"“”']*book\s*club[^\"“”']*[\"“”']", val, flags=re.IGNORECASE):
            return val
        # Otherwise strip the first unquoted occurrence
        m = re.search(r"(?i)\bbook\s*club\b", val)
        if m and m.end() < len(val):
            stripped = val[m.end():].lstrip(" -:–—").strip(" -:–—\t\r\n")
            stripped = re.sub(r"^[^A-Za-z0-9]+", "", stripped)
            return stripped
        return val

    def _trim_title_lead(val: str) -> str:
        """
        Drop leading filler before the actual title:
        - keep the tail after the last strong separator or cue words
        - then remove leading run of lowercase-start words (e.g., "this month we will be reading")
        """
        # split on common separators (exclude colon to keep subtitles); keep the last chunk
        parts = re.split(
            r"[.!?;]|\b(reading|discussion|discuss|discussing|will discuss|will be discussing|our book|this month|we will be reading|selection|first selection is|our selection is)\b",
            val,
            flags=re.IGNORECASE,
        )
        tail = parts[-1] if parts else val
        tokens = tail.split()
        # remove leading run of lowercase-start tokens
        while tokens and tokens[0][:1].islower():
            tokens.pop(0)
        return " ".join(tokens).strip(" \t\r\n-–—:;,.!")

    def _match_title_author(text: str) -> tuple[str, str]:
        """
        Attempt book title/author extraction using ordered strategies:
        1) by-split
        2) possessive
        3) quoted "Title" by Author
        4) loose possessive fallback (last occurrence)
        """
        if not text:
            return "", ""
        text = _strip_emphasis(text)
        # drop zero-width/non-breaking spaces, normalize whitespace
        text = re.sub(r"[\u200b\u200c\u200d\uFEFF]", "", text)
        text = re.sub(r"\s+", " ", text)

        def _by_split(src: str) -> tuple[str, str]:
            matches = list(re.finditer(r"\sby\s", src, flags=re.IGNORECASE))
            if not matches:
                return "", ""
            for m in matches:  # left-to-right
                left = src[: m.start()].strip()
                right = src[m.end() :].strip()
                q = re.search(r"[\"“”']\s*([^\"“”']{2,200}?)\s*[\"“”']", left)
                if q:
                    left = q.group(1)
                t_raw = _trim_title_tail(_strip_book_club_prefix(_trim_title_lead(_clean_token(left))))
                a_raw = _trim_author_tail(right)
                if "book club" in t_raw.lower() and not q:
                    continue
                if not (t_raw and a_raw):
                    continue
                if not re.match(r"^[A-Z][\w'.-]+(?:\s+[A-Z][\w'.-]+){0,3}$", a_raw):
                    continue
                if _is_valid(t_raw, require_capital=True) and _is_valid(a_raw, require_capital=True):
                    return t_raw, a_raw
            return "", ""

        def _possessive(src: str) -> tuple[str, str]:
            mpos = re.search(
                r"([A-Z][\w'.-]+(?:\s+[A-Z][\w'.-]+){0,3})['’]s\s+([^,:;\n]{2,160})",
                src,
            )
            if not mpos:
                return "", ""
            a_raw = _trim_author_tail(_clean_token(mpos.group(1)))
            t_raw = _trim_title_tail(_strip_book_club_prefix(_trim_title_lead(_clean_token(mpos.group(2)))))
            if "book club" in t_raw.lower():
                return "", ""
            if not (t_raw and a_raw):
                return "", ""
            if not re.match(r"^[A-Z][\w'.-]+(?:\s+[A-Z][\w'.-]+){0,3}$", a_raw):
                return "", ""
            if _is_valid(t_raw, require_capital=True) and _is_valid(a_raw, require_capital=True):
                return t_raw, a_raw
            return "", ""

        def _quoted_by(src: str) -> tuple[str, str]:
            m = re.search(
                r"[\"“”']\s*([^\"“”']{2,160}?)\s*[\"“”']\s+by\s+([A-Z][^\n]{1,80})",
                src,
                flags=re.IGNORECASE,
            )
            if not m:
                return "", ""
            t_raw = _trim_title_tail(_trim_title_lead(_clean_token(m.group(1))))
            a_raw = _trim_author_tail(m.group(2))
            if not (t_raw and a_raw):
                return "", ""
            if not re.match(r"^[A-Z][\w'.-]+(?:\s+[A-Z][\w'.-]+){0,3}$", a_raw):
                return "", ""
            if _is_valid(t_raw, require_capital=True) and _is_valid(a_raw, require_capital=True):
                return t_raw, a_raw
            return "", ""

        def _loose_possessive(src: str) -> tuple[str, str]:
            mpos_all = list(
                re.finditer(
                    r"([A-Z][\w'.-]+(?:\s+[A-Z][\w'.-]+){0,4})['’]s\s+([^.!?;\n]{2,200})",
                    src,
                )
            )
            if not mpos_all:
                return "", ""
            mpos = mpos_all[-1]
            a_raw = _trim_author_tail(_clean_token(mpos.group(1)))
            t_raw = _trim_title_tail(_strip_book_club_prefix(_trim_title_lead(_clean_token(mpos.group(2)))))
            if "book club" in t_raw.lower():
                return "", ""
            if a_raw and t_raw and _is_valid(t_raw, require_capital=True) and _is_valid(a_raw, require_capital=True):
                return t_raw, a_raw
            return "", ""

        for extractor in (_by_split, _possessive, _quoted_by, _loose_possessive):
            t_raw, a_raw = extractor(text)
            if t_raw and a_raw:
                return t_raw, a_raw
        return "", ""

    for title, desc in zip(df.get("title", []), df.get("description", [])):
        t_found = a_found = ""
        title_text = html.unescape(str(title or ""))
        desc_text = html.unescape(str(desc or ""))

        t_found, a_found = _match_title_author(title_text)
        if not (t_found and a_found):
            t_found, a_found = _match_title_author(desc_text)

        book_title_col.append(t_found)
        book_author_col.append(a_found)

        # Tag extraction from title + description
        text_lower = f"{title_text} {desc_text}".lower()

        def _term_hit(term: str, corpus: str) -> bool:
            return bool(re.search(rf"\b{re.escape(term.lower())}\b", corpus))

        row_tags = []
        for tag, terms in TAG_KEYWORDS.items():
            for term in terms:
                if _term_hit(term, text_lower):
                    row_tags.append(tag)
                    break
        tags_col.append(sorted(set(row_tags)))

    if "when" in df.columns:
        df["start_date"] = start_date_col
        df["end_date"] = end_date_col
        df["day_of_week_start"] = day_of_week_start_col
        df["day_of_week_end"] = day_of_week_end_col
        df["start_iso"] = start_iso
        df["end_iso"] = end_iso
        df["start_time"] = start_time_col
        df["end_time"] = end_time_col
        df["book_title"] = book_title_col
        df["book_author"] = book_author_col
        df["tags"] = tags_col

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
            "book_title",
            "book_author",
            "tags",
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

