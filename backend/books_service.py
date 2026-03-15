"""Books service layer."""

from __future__ import annotations

from backend import storage


def get_books() -> list[dict]:
    """Return all books (metadata)."""
    data = storage._catalog_cache()  # pylint: disable=protected-access
    return [storage.get_book_metadata(b["source_id"]) for b in data.get("books", [])]


def get_books_by_genre(genre: str) -> list[dict]:
    """Return books filtered by genre."""
    genre = str(genre or "").strip().lower()
    if not genre:
        return get_books()
    out: list[dict] = []
    for b in get_books():
        if any(str(g).strip().lower() == genre for g in (b.get("genres") or [])):
            out.append(b)
    return out


def get_trending_books() -> list[dict]:
    """Return trending books by rating_count descending."""
    books = get_books()
    return sorted(books, key=lambda b: int(b.get("rating_count") or 0), reverse=True)

