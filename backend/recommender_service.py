"""Recommender service layer."""

from __future__ import annotations

from collections import Counter

from backend import storage
from backend.recommender.service import (
    get_recommendations as _get_recommendations,
    get_top_popular_books as _get_top_popular_books,
)


def _build_user_recommender_inputs(user_id: str) -> tuple[dict, dict, bool]:
    user_id = str(user_id).strip().lower()
    user_books = storage.get_user_books(user_id)
    user_clubs = storage.get_user_clubs(user_id)
    user_forums = storage.get_user_forums(user_id)
    data = storage._catalog_cache()  # pylint: disable=protected-access
    books_by_id = data.get("books_by_id", {})
    clubs = data.get("clubs", [])
    user_books_read_store: dict[str, list[str]] = {}
    user_genres_store: dict[str, list[dict]] = {}
    has_behavior = False
    source_ids: list[str] = []
    library = (user_books.get("library") or {}) if isinstance(user_books, dict) else {}
    for shelf in ("in_progress", "saved", "finished"):
        for bid in library.get(shelf, []) or []:
            book = books_by_id.get(int(bid))
            if book and book.get("source_id"):
                source_ids.append(str(book["source_id"]))
    if source_ids:
        user_books_read_store[user_id] = list(dict.fromkeys(source_ids))
        has_behavior = True
    genre_counts: Counter = Counter()
    joined_ids = {int(cid) for cid in (user_clubs.get("club_ids") or [])}
    for club in clubs:
        if int(club.get("id", -1)) in joined_ids:
            g = str(club.get("genre") or "").strip()
            if g:
                genre_counts[g] += 1
    if genre_counts:
        ranked = [name for name, _ in genre_counts.most_common(3)]
        user_genres_store[user_id] = [
            {"genre": g, "rank": rank} for rank, g in enumerate(ranked, start=1)
        ]
        has_behavior = True
    if user_forums.get("forum_posts") or user_forums.get("saved_forum_post_ids"):
        has_behavior = True
    return user_genres_store, user_books_read_store, has_behavior


def get_book_recommendations(user_id: str) -> list[dict]:
    """Return book recommendations; cold-start users get top popular books."""
    user_id = str(user_id).strip().lower()
    if not user_id:
        return _get_top_popular_books(top_k=10)
    user_genres_store, user_books_store, has_data = _build_user_recommender_inputs(
        user_id
    )
    if not has_data:
        return _get_top_popular_books(top_k=10)
    return _get_recommendations(
        user_id=user_id,
        user_genres_store=user_genres_store,
        user_books_read_store=user_books_store,
        top_k=10,
    )


def get_event_recommendations(user_id: str) -> list[dict]:
    """Return event recommendations (placeholder)."""
    _ = user_id
    return []

