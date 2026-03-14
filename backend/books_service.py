"""Books service layer."""

from __future__ import annotations

from backend import storage


def get_books() -> list[dict]:
    """Return all books (metadata)."""
    data = storage._catalog_cache()  # pylint: disable=protected-access
    return [storage.get_book_metadata(book["source_id"]) for book in data.get("books", [])]


def get_books_by_genre(genre: str) -> list[dict]:
    """Return books filtered by genre."""
    genre = str(genre or "").strip().lower()
    if not genre:
        return get_books()
    out: list[dict] = []
    for book in get_books():
        if any(str(gen).strip().lower() == genre for gen in (book.get("genres") or [])):
            out.append(book)
    return out


def get_trending_books() -> list[dict]:
    """Return trending books by rating_count descending."""
    books = get_books()
    return sorted(books, key=lambda book: int(book.get("rating_count") or 0), reverse=True)


def get_trending_books_spl(limit: int = 50) -> list[dict]:
    """
    Return top books by SPL checkouts from S3.
    Falls back to get_trending_books() if unavailable locally.
    """
    try:
        from backend.storage import CloudStorage  # pylint: disable=import-outside-toplevel
        from backend.config import IS_AWS  # pylint: disable=import-outside-toplevel
        if IS_AWS:
            return CloudStorage().get_top50_books()[:limit]
    except Exception:  # pylint: disable=broad-exception-caught
        pass
    return get_trending_books()[:limit]
