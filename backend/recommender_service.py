"""Recommender service layer.

Recommendations are stored in user_recommendations (not user_books).
- Books: 50 items. Re-run after every ADDS_BEFORE_BOOK_RERUN (3) adds to shelf (see on_book_added_to_shelf).
- Events: 10 items. Re-run when events_soonest_expiry is past (lazy refresh when reading).

Default (anonymous or user with no genre preferences):
- Books: top 50 from static JSON (bookish-data-elsie/books/reviews_top50_books.json).
- Events: top 10 soonest events (by ttl/expiry).
Seeded on account creation via ensure_default_recommendations(); also used for signed-out users.
"""

from __future__ import annotations

import time
from collections import Counter

from backend import storage
from backend.config import (
    ADDS_BEFORE_BOOK_RERUN,
    RECOMMENDED_BOOKS_SIZE,
    RECOMMENDED_EVENTS_SIZE,
)
from backend.recommender.service import (
    get_recommendations as _get_recommendations,
    get_top_popular_books as _get_top_popular_books,
)
from backend.storage import get_storage
from backend.recommender.event_recommender import EventRecommender


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
            genre_name = str(club.get("genre") or "").strip()
            if genre_name:
                genre_counts[genre_name] += 1
    for rank, genre in enumerate(user_books.get("genre_preferences") or [], start=1):
        if genre:
            genre_counts[genre] += (4 - min(rank, 3))
            has_behavior = True
    if genre_counts:
        ranked = [name for name, _ in genre_counts.most_common(3)]
        user_genres_store[user_id] = [
            {"genre": g, "rank": rank} for rank, g in enumerate(ranked, start=1)
        ]
        has_behavior = True
    if user_forums.get("forum_posts") or user_forums.get("saved_forum_post_ids"):
        has_behavior = True
    return user_genres_store, user_books_read_store, has_behavior


def get_book_recommendations(user_id: str, top_k: int | None = None) -> list[dict]:
    """Return book recommendations; cold-start users get top popular books. Default top_k=50."""
    top_k = top_k if top_k is not None else RECOMMENDED_BOOKS_SIZE
    user_id = str(user_id).strip().lower()
    if not user_id:
        return _get_top_popular_books(top_k=10)
    user_genres_store, user_books_store, has_data = _build_user_recommender_inputs(
        user_id
    )
    if not has_data:
        return _get_top_popular_books(top_k=min(top_k, 50))
    return _get_recommendations(
        user_id=user_id,
        user_genres_store=user_genres_store,
        user_books_read_store=user_books_store,
        top_k=top_k,
    )


def get_event_recommendations(user_id: str, top_k: int | None = None) -> list[dict]:
    """Return event recommendations (personalized when possible).

    Logic:
    - If user has genre preferences:
        - Build user tag set from top genres.
        - Pull a pool of upcoming events from storage (get_soonest_events).
        - Score and rank with EventRecommender.
    - If no prefs or no events, returns [] (caller falls back to default soonest events).
    """
    top_k = top_k if top_k is not None else RECOMMENDED_EVENTS_SIZE
    user_id = str(user_id or "").strip().lower()
    if not user_id or top_k <= 0:
        return []

    # Reuse genre-preference aggregation
    user_genres_store, _user_books_store, has_data = _build_user_recommender_inputs(
        user_id
    )
    if not has_data:
        return []
    rows = user_genres_store.get(user_id) or []
    user_tags = [r.get("genre") for r in rows if r.get("genre")]
    if not user_tags:
        return []

    store = get_storage()
    # Pull a reasonably sized pool of upcoming events; recommender will re-rank.
    pool_size = max(top_k * 4, 40)
    events = store.get_soonest_events(pool_size) or []
    if not events:
        return []

    recommender = EventRecommender()
    ranked = recommender.recommend(events, user_tags=user_tags, top_k=top_k)
    return ranked[:top_k]


def _events_soonest_expiry(events: list[dict]) -> int:
    """Return min ttl/expiry (Unix) from event dicts; 0 if none."""
    expiries = []
    for e in events:
        t = e.get("ttl") or e.get("expiry")
        if t is not None:
            try:
                expiries.append(int(t))
            except (TypeError, ValueError):
                pass
    return min(expiries) if expiries else 0


def _user_has_genre_preferences(user_id: str) -> bool:
    """True if user has at least one genre preference."""
    if not user_id:
        return False
    store = get_storage()
    user_books = store.get_user_books(user_id)
    prefs = user_books.get("genre_preferences") or []
    return len(prefs) > 0


def get_recommended_books_for_user(user_id: str | None = None) -> list[dict]:
    """Return book recommendations (50). Anonymous or no genre prefs → top 50 popular (static JSON). Else → stored or run recommender."""
    store = get_storage()
    if not user_id or not str(user_id).strip():
        return store.get_top50_review_books()[:RECOMMENDED_BOOKS_SIZE]
    user_id = str(user_id).strip().lower()
    if not _user_has_genre_preferences(user_id):
        return store.get_top50_review_books()[:RECOMMENDED_BOOKS_SIZE]
    rec = store.get_user_recommendations(user_id)
    books = list(rec.get("recommended_books") or [])
    if not books:
        books = get_book_recommendations(user_id)[:RECOMMENDED_BOOKS_SIZE]
        rec["recommended_books"] = books
        rec["book_updated_at"] = int(time.time())
        store.save_user_recommendations(user_id, rec)
    return books[:RECOMMENDED_BOOKS_SIZE]


def get_recommended_events_for_user(user_id: str | None = None) -> list[dict]:
    """Return event recommendations (10). Anonymous or no genre prefs → top 10 soonest events. Else → stored or refresh on expiry."""
    store = get_storage()
    if not user_id or not str(user_id).strip():
        return store.get_soonest_events(RECOMMENDED_EVENTS_SIZE)[:RECOMMENDED_EVENTS_SIZE]
    user_id = str(user_id).strip().lower()
    if not _user_has_genre_preferences(user_id):
        return store.get_soonest_events(RECOMMENDED_EVENTS_SIZE)[:RECOMMENDED_EVENTS_SIZE]
    rec = store.get_user_recommendations(user_id)
    now = int(time.time())
    soonest = int(rec.get("events_soonest_expiry") or 0)
    if soonest <= 0 or now >= soonest:
        events = get_event_recommendations(user_id)
        rec["recommended_events"] = events[:RECOMMENDED_EVENTS_SIZE]
        rec["events_soonest_expiry"] = _events_soonest_expiry(events)
        store.save_user_recommendations(user_id, rec)
    return list(rec.get("recommended_events") or [])[:RECOMMENDED_EVENTS_SIZE]


def refresh_and_save_recommendations(user_id: str) -> dict:
    """Compute 50 books + 10 events, save to user_recommendations, return that record."""
    user_id = str(user_id).strip().lower()
    if not user_id:
        return {}
    store = get_storage()
    rec = store.get_user_recommendations(user_id)
    books = get_book_recommendations(user_id)
    events = get_event_recommendations(user_id)
    rec["recommended_books"] = books[:RECOMMENDED_BOOKS_SIZE]
    rec["recommended_events"] = events[:RECOMMENDED_EVENTS_SIZE]
    rec["book_updated_at"] = int(time.time())
    rec["events_soonest_expiry"] = _events_soonest_expiry(events)
    store.save_user_recommendations(user_id, rec)
    return rec


def ensure_default_recommendations(user_id: str) -> None:
    """Seed user_recommendations for new users (no genre prefs yet): top 50 popular books + top 10 soonest events. Call on account creation."""
    user_id = str(user_id).strip().lower()
    if not user_id:
        return
    store = get_storage()
    if _user_has_genre_preferences(user_id):
        return
    rec = store.get_user_recommendations(user_id)
    if rec.get("recommended_books") or rec.get("recommended_events"):
        return
    rec["recommended_books"] = store.get_top50_review_books()[:RECOMMENDED_BOOKS_SIZE]
    rec["recommended_events"] = store.get_soonest_events(RECOMMENDED_EVENTS_SIZE)[:RECOMMENDED_EVENTS_SIZE]
    rec["events_soonest_expiry"] = _events_soonest_expiry(rec["recommended_events"])
    store.save_user_recommendations(user_id, rec)


def on_book_added_to_shelf(user_id: str) -> None:
    """Call after a book is added to a shelf. Increments adds_since_last_book_run; if >= ADDS_BEFORE_BOOK_RERUN (3), re-runs book recommender and resets."""
    user_id = str(user_id).strip().lower()
    if not user_id:
        return
    store = get_storage()
    rec = store.get_user_recommendations(user_id)
    adds = int(rec.get("adds_since_last_book_run") or 0) + 1
    rec["adds_since_last_book_run"] = adds
    if adds >= ADDS_BEFORE_BOOK_RERUN:
        books = get_book_recommendations(user_id)
        rec["recommended_books"] = books[:RECOMMENDED_BOOKS_SIZE]
        rec["book_updated_at"] = int(time.time())
        rec["adds_since_last_book_run"] = 0
    store.save_user_recommendations(user_id, rec)
