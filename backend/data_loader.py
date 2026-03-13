"""Load and transform processed datasets into UI-ready structures."""

from __future__ import annotations

import ast
import csv
import json
from pathlib import Path

from backend.config import PROCESSED_DIR


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


def load_data() -> dict:
    """Load and transform processed datasets into UI-ready structures."""
    books_parent = _read_jsonl_dict_lines(
        PROCESSED_DIR / "first_100_books_by_parent_asin.jsonl"
    )
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
        genres = [str(c) for c in cats[:3]] or ["General"]
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
                        "name": row.get("title") or f"Book Club {idx}",
                        "description": (
                            row.get("description") or "No description provided."
                        ).strip(),
                        "genre": genre,
                        "location": row.get("city_state") or "Seattle, WA",
                        "meeting_day": row.get("day_of_week_start") or "TBD",
                        "meeting_time": row.get("start_time") or "TBD",
                        "current_book_id": current_book_id,
                        "current_book_title": current_book_title,
                        "thumbnail": row.get("thumbnail")
                        or "https://placehold.co/600x360?text=Club",
                        "is_external": True,
                        "external_link": row.get("link") or "",
                    }
                )

    if not clubs:
        clubs = [
            {
                "id": 1,
                "name": "Seattle Readers",
                "description": "Fallback club generated because processed club data is missing.",
                "genre": "General",
                "location": "Seattle, WA",
                "meeting_day": "Wed",
                "meeting_time": "7:00 PM",
                "current_book_id": books[0]["id"],
                "current_book_title": books[0]["title"],
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
    forum_posts = [
        {
            "title": f"What do you think about {books[0]['title']}?",
            "author": "Community Mod",
            "genre": books[0]["genres"][0],
            "club": clubs[0]["name"] if clubs else None,
            "replies": 8,
            "likes": 15,
            "time_ago": "2 hours ago",
            "preview": (
                f"Share your thoughts about {books[0]['title']} by {books[0]['author']}."
            ),
        },
        {
            "title": f"Top picks this week: {books[1]['title']}",
            "author": "Bookish Team",
            "genre": books[1]["genres"][0],
            "club": None,
            "replies": 5,
            "likes": 12,
            "time_ago": "1 day ago",
            "preview": f"This week's recommendation highlight is {books[1]['title']}.",
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

