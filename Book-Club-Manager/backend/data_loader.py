"""Bootstrap UI data: one-time load of books, events (as clubs), and forum for the Streamlit app.

This module does not replace the services—it calls them (books_service, events_service,
storage) and assembles a single dict the frontend expects at startup. So the source of
truth stays in the services; load_data() is just a thin bootstrap layer.
"""

from __future__ import annotations

import ast
import csv
import json
from pathlib import Path

from backend.config import PROCESSED_DIR


def books_to_ui_shape(raw_books: list[dict], max_count: int = 50) -> list[dict]:
    """Convert raw service book dicts to UI shape (id, source_id, title, author, cover, ...)."""
    return _books_from_services_to_ui_shape(raw_books, max_count)


def _books_from_services_to_ui_shape(raw: list[dict], max_count: int = 50) -> list[dict]:
    """Normalize service book dicts (parent_asin, title, author_name, ...) to UI shape (id, source_id, ...)."""
    out: list[dict] = []
    for idx, b in enumerate(raw[:max_count], start=1):
        source_id = str(b.get("parent_asin") or b.get("source_id") or f"_idx_{idx}")
        title = str(b.get("title") or "Untitled")
        author = str(b.get("author_name") or b.get("author") or "Unknown")
        cover = b.get("images") or "https://placehold.co/220x330?text=Book"
        rating = float(b.get("average_rating") or 0)
        rating_count = int(b.get("rating_number") or b.get("rating_count") or 0)
        cats = b.get("categories") or []
        if isinstance(cats, str):
            try:
                cats = ast.literal_eval(cats) if ("[" in cats or "{" in cats) else [cats]
            except (ValueError, SyntaxError):
                cats = [cats] if cats else []
        genres = [str(c).strip() for c in (cats if isinstance(cats, list) else [])[:3]] or ["General"]
        out.append({
            "id": idx,
            "source_id": source_id,
            "title": title,
            "author": author,
            "cover": cover,
            "rating": round(rating, 1),
            "rating_count": rating_count,
            "genres": genres,
            "description": str(b.get("description")
                               or "No description available.").strip() or 
                               "No description available.",
                               "spl_available": False,
        })
    return out


def _events_to_clubs_ui_shape(events: list[dict], books_by_source_id: dict) -> list[dict]:
    """Map event dicts from events_service to the club-like shape the UI expects."""
    clubs: list[dict] = []
    for idx, ev in enumerate(events, start=1):
        pa = ev.get("parent_asin") or ""
        book = books_by_source_id.get(pa) if pa else None
        raw_tags = ev.get("tags") or ev.get("tag_list") or ev.get("categories")
        if isinstance(raw_tags, list):
            tags = [str(t).strip() for t in raw_tags if str(t).strip()]
        elif isinstance(raw_tags, (set, frozenset)):
            tags = [str(t).strip() for t in raw_tags if str(t).strip()]
        elif isinstance(raw_tags, str) and raw_tags.strip():
            tags = [s.strip() for s in raw_tags.replace(";", ",").split(",") if s.strip()]
        else:
            tags = [str(ev.get("genre") or "General")]
        if not tags:
            tags = [str(ev.get("genre") or "General")]
        start_iso = ev.get("start_iso") or ev.get("start_date") or ""
        if start_iso and not isinstance(start_iso, str):
            start_iso = str(start_iso)
        event_id = str(ev.get("event_id") or ev.get("id") or idx)
        clubs.append({
            "id": idx,
            "event_id": event_id,
            "name": str(ev.get("title") or ev.get("event_id") or f"Event {idx}"),
            "description": str(ev.get("description") or "No description.").strip() or "No description.",
            "genre": str(ev.get("genre") or "General"),
            "location": str(ev.get("city_state") or ev.get("location") or "Seattle, WA"),
            "meeting_day": str(ev.get("meeting_day") or "TBD"),
            "meeting_time": str(ev.get("start_time") or ev.get("meeting_time") or "TBD"),
            "start_iso": start_iso.strip() if start_iso else "",
            "tags": tags,
            "current_book_id": book["id"] if book else None,
            "current_book_title": book["title"] if book else "—",
            "thumbnail": str(ev.get("thumbnail") or "https://placehold.co/600x360?text=Club"),
            "is_external": bool(ev.get("link")),
            "external_link": str(ev.get("link") or ""),
        })
    return clubs


def _forum_posts_to_ui_shape(posts: list[dict]) -> list[dict]:
    """Map forum post dicts from storage to the shape the UI expects (title, author, genre, preview, ...)."""
    out: list[dict] = []
    for idx, p in enumerate(posts[:20], start=1):
        tags = p.get("tags")
        genre = str(tags[0]) if isinstance(tags, list) and tags else str(p.get("genre") or "General")
        text = str(p.get("text") or "")[:120]
        preview = (text + "…") if len(text) >= 120 else text or "No preview."
        out.append({
            "id": int(p.get("id") or p.get("post_id") or idx),
            "title": str(p.get("title") or "Post"),
            "author": str(p.get("author") or "Anonymous"),
            "genre": genre,
            "club": None,
            "replies": int(p.get("replies") or 0),
            "likes": int(p.get("likes") or 0),
            "time_ago": "—",
            "preview": preview,
        })
    return out


def _read_jsonl_dict_lines(path: Path) -> list[dict]:
    """Read JSONL file where each line is a dictionary and return parsed rows."""
    rows: list[dict] = []
    if not path.exists():
        return rows
    with path.open("r", encoding="utf-8") as file_obj:
        for line in file_obj:
            line = line.strip()
            if not line:
                continue
            rows.append(json.loads(line))
    return rows


def _read_isbn_index_file(path: Path) -> set[str]:
    """Read indexed ISBN JSON format and return normalized ISBN set."""
    if not path.exists():
        return set()
    with path.open("r", encoding="utf-8") as file_obj:
        data = json.load(file_obj)
    out: set[str] = set()
    for row in data:
        val = str(row.get("0", "")).strip()
        if val:
            out.add(val.upper())
    return out


def _parse_tags(text: str) -> list[str]:
    """Parse serialized tag list string into normalized lowercase tags."""
    if not text:
        return []
    try:
        raw = ast.literal_eval(text)
    except (ValueError, SyntaxError):
        return []
    if isinstance(raw, list):
        return [str(x).strip().lower() for x in raw if str(x).strip()]
    return []


def build_ui_bootstrap(
    raw_books: list[dict],
    events: list[dict],
    forum_posts: list[dict],
) -> dict:
    """Build the UI bootstrap dict from service-shaped data (books, events, forum posts).

    Use this when data comes from books_service, events_service, and storage so the
    frontend gets one consistent dict. Handles empty data with fallbacks.
    """
    books = _books_from_services_to_ui_shape(raw_books, 50)
    books_by_id = {b["id"]: b for b in books}
    books_by_source_id = {str(b["source_id"]): b for b in books}
    clubs = _events_to_clubs_ui_shape(events, books_by_source_id)
    if not clubs:
        first_book = books[0] if books else None
        clubs = [
            {
                "id": 1,
                "event_id": "1",
                "name": "Seattle Readers",
                "description": "Fallback club (no events).",
                "genre": "General",
                "location": "Seattle, WA",
                "meeting_day": "Wed",
                "meeting_time": "7:00 PM",
                "start_iso": "",
                "tags": ["General"],
                "current_book_id": first_book["id"] if first_book else None,
                "current_book_title": first_book["title"] if first_book else "—",
                "thumbnail": "https://placehold.co/600x360?text=Club",
                "is_external": False,
                "external_link": "",
            }
        ]
    user_club_ids = [c["id"] for c in clubs[: min(4, len(clubs))]]
    library = {
        "in_progress": [b["id"] for b in books[0:4]],
        "saved": [b["id"] for b in books[4:8]],
        "finished": [b["id"] for b in books[8:12]],
    }
    forum_posts_ui = _forum_posts_to_ui_shape(forum_posts)
    if not forum_posts_ui and books:
        b0, b1 = books[0], (books[1] if len(books) > 1 else {})
        forum_posts_ui = [
            {
                "id": 1,
                "title": f"What do you think about {b0['title']}?" if b0 else "Join the discussion",
                "author": "Community Mod",
                "genre": b0["genres"][0] if b0 and b0.get("genres") else "General",
                "club": clubs[0]["name"] if clubs else None,
                "replies": 8,
                "likes": 15,
                "time_ago": "2 hours ago",
                "preview": (f"Share your thoughts about {b0['title']} by {b0['author']}."
                            if b0 else "Share your thoughts."),
            },
            {
                "id": 2,
                "title": f"Top picks this week: {b1['title']}" if b1 else "Top picks this week",
                "author": "Bookish Team",
                "genre": b1["genres"][0] if b1 and b1.get("genres") else "General",
                "club": None,
                "replies": 5,
                "likes": 12,
                "time_ago": "1 day ago",
                "preview": f"This week's recommendation highlight is {b1['title']}." if b1 else "Discover highlights.",
            },
        ]
    genres = sorted({g for b in books for g in b["genres"]})
    neighborhoods = sorted(
        {(c["location"].split(",", maxsplit=1)[0]).strip() for c in clubs}
    )
    return {
        "books": books,
        "books_by_id": books_by_id,
        "books_by_source_id": books_by_source_id,
        "clubs": clubs,
        "forum_posts": forum_posts_ui,
        "genres": genres,
        "library": library,
        "neighborhoods": neighborhoods,
        "user_club_ids": user_club_ids,
    }


def load_data() -> dict:
    """Load bootstrap UI data. Local only: reads from processed JSONL/CSV files.

    For AWS, the frontend should call the services directly and then build_ui_bootstrap().
    """
    # Primary local bootstrap: small JSONL excerpt.
    books_parent = _read_jsonl_dict_lines(
        PROCESSED_DIR / "first_100_books_by_parent_asin.jsonl"
    )
    # Fallback: use reviews list (already in repo) when JSONL excerpt is missing.
    if not books_parent:
        reviews_path = PROCESSED_DIR / "reviews_top25_books.json"
        if reviews_path.exists():
            try:
                with reviews_path.open("r", encoding="utf-8") as f:
                    reviews = json.load(f) or []
                if isinstance(reviews, dict) and "books" in reviews:
                    reviews = reviews.get("books") or []
                if isinstance(reviews, list):
                    # Convert list[book_dict] into the JSONL-like [{asin: meta}] rows
                    # expected by the existing parsing logic below.
                    tmp: list[dict] = []
                    for b in reviews:
                        if not isinstance(b, dict):
                            continue
                        asin = str(b.get("parent_asin") or b.get("source_id") or "").strip()
                        if not asin:
                            continue
                        tmp.append({asin: b})
                    books_parent = tmp
            except (AttributeError, TypeError):
                books_parent = []
    catalog_isbns = _read_isbn_index_file(
        PROCESSED_DIR / "first_100_spl_catalog_by_isbn.json"
    )
    checkout_isbns = _read_isbn_index_file(
        PROCESSED_DIR / "first_100_spl_checkouts_by_isbn.json"
    )

    books: list[dict] = []
    for idx, row in enumerate(books_parent, start=1):
        source_id, meta = next(iter(row.items()))
        cats = meta.get("categories") or []
        if isinstance(cats, str):
            try:
                cats = ast.literal_eval(cats) if ("[" in cats or "{" in cats) else [cats]
            except (ValueError, SyntaxError):
                cats = [cats] if cats else []
        genres = [str(c).strip() for c in (cats if isinstance(cats, list) else [])[:3] if str(c).strip()] or ["General"]
        desc = meta.get("description") or []
        if isinstance(desc, list):
            description = " ".join(str(x) for x in desc[:3]).strip()
        else:
            description = str(desc).strip()
        if not description:
            description = "No description available."

        cover = meta.get("images") or "https://placehold.co/220x330?text=Book"
        rating_number = int(meta.get("rating_number") or 0)
        rating = float(meta.get("average_rating") or 0.0)
        in_spl = (
            source_id.upper() in catalog_isbns or source_id.upper() in checkout_isbns
        )
        books.append(
            {
                "id": idx,
                "source_id": source_id,
                "title": str(meta.get("title") or "Untitled"),
                "author": str(meta.get("author_name") or "Unknown"),
                "cover": cover,
                "rating": round(rating, 1),
                "rating_count": rating_number,
                "genres": genres,
                "description": description,
                "spl_available": in_spl,
            }
        )

    books = books[:36]
    books_by_id = {b["id"]: b for b in books}
    books_by_source_id = {str(b["source_id"]): b for b in books}
    title_author_to_id = {
        f"{b['title'].strip().lower()}|{b['author'].strip().lower()}": b["id"]
        for b in books
    }

    clubs: list[dict] = []
    clubs_path = PROCESSED_DIR / "bookclubs_seattle_clean.csv"
    if clubs_path.exists():
        with clubs_path.open("r", encoding="utf-8") as file_obj:
            reader = csv.DictReader(file_obj)
            for idx, row in enumerate(reader, start=1):
                tags = _parse_tags(row.get("tags", ""))
                genre = tags[0].title() if tags else "General"
                key = (
                    f"{(row.get('book_title') or '').strip().lower()}|"
                    f"{(row.get('book_author') or '').strip().lower()}"
                )
                current_book_id = title_author_to_id.get(key)
                current_book_title = (
                    books_by_id[current_book_id]["title"]
                    if current_book_id in books_by_id
                    else "NA"
                )
                clubs.append(
                    {
                        "id": idx,
                        "event_id": str(row.get("event_id") or row.get("id") or idx),
                        "name": row.get("title") or f"Book Club {idx}",
                        "description": (
                            row.get("description") or "No description provided."
                        ).strip(),
                        "genre": genre,
                        "location": row.get("city_state") or "Seattle, WA",
                        "meeting_day": row.get("day_of_week_start") or "TBD",
                        "meeting_time": row.get("start_time") or "TBD",
                        "start_iso": "",  # CSV may not have ISO date
                        "tags": [t.title() for t in tags] if tags else [genre],
                        "current_book_id": current_book_id,
                        "current_book_title": current_book_title,
                        "thumbnail": row.get("thumbnail")
                        or "https://placehold.co/600x360?text=Club",
                        "is_external": True,
                        "external_link": row.get("link") or "",
                    }
                )

    if not clubs:
        first_book = books[0] if books else None
        clubs = [
            {
                "id": 1,
                "event_id": "1",
                "name": "Seattle Readers",
                "description": "Fallback club generated because processed club data is missing.",
                "genre": "General",
                "location": "Seattle, WA",
                "meeting_day": "Wed",
                "meeting_time": "7:00 PM",
                "start_iso": "",
                "tags": ["General"],
                "current_book_id": first_book["id"] if first_book else None,
                "current_book_title": first_book["title"] if first_book else "—",
                "thumbnail": "https://placehold.co/600x360?text=Club",
                "is_external": False,
                "external_link": "",
            }
        ]

    user_club_ids = [c["id"] for c in clubs[: min(4, len(clubs))]]
    library = {
        "in_progress": [b["id"] for b in books[0:4]],
        "saved": [b["id"] for b in books[4:8]],
        "finished": [b["id"] for b in books[8:12]],
    }
    b0, b1 = (books[0] if len(books) > 0 else {}), (books[1] if len(books) > 1 else {})
    forum_posts = [
        {
            "title": f"What do you think about {b0['title']}?" if b0 else "Join the discussion",
            "author": "Community Mod",
            "genre": b0["genres"][0] if b0 and b0.get("genres") else "General",
            "club": clubs[0]["name"] if clubs else None,
            "replies": 8,
            "likes": 15,
            "time_ago": "2 hours ago",
            "preview": (
                f"Share your thoughts about {b0['title']} by {b0['author']}." if b0
                else "Share your thoughts about your current read."
            ),
        },
        {
            "title": f"Top picks this week: {b1['title']}" if b1 else "Top picks this week",
            "author": "Bookish Team",
            "genre": b1["genres"][0] if b1 and b1.get("genres") else "General",
            "club": None,
            "replies": 5,
            "likes": 12,
            "time_ago": "1 day ago",
            "preview": (f"This week's recommendation highlight is {b1['title']}." 
                        if b1 else "Discover this week's highlights."),
        },
    ]
    genres = sorted({g for b in books for g in b["genres"]})
    neighborhoods = sorted(
        {(c["location"].split(",", maxsplit=1)[0]).strip() for c in clubs}
    )

    return {
        "books": books,
        "books_by_id": books_by_id,
        "books_by_source_id": books_by_source_id,
        "clubs": clubs,
        "forum_posts": forum_posts,
        "genres": genres,
        "library": library,
        "neighborhoods": neighborhoods,
        "user_club_ids": user_club_ids,
    }
