"""Recommender service layer.

Recommendations are stored in user_recommendations (not user_books).
- Books: 50 items. Re-run after every ADDS_BEFORE_BOOK_RERUN (3) adds to shelf (see on_book_added_to_shelf).
  Book recommender uses only the user's library (books on shelves).
- Events: 10 items. Re-run when events_soonest_expiry is past (lazy refresh when reading).
  Event recommender uses only the user's genre_preferences.

Default (anonymous or user with no library / no genre preferences):
- Books: top 25 from static JSON (reviews_top25_books.json).
- Events: top 10 soonest events (by ttl/expiry).
Seeded on account creation via ensure_default_recommendations(); also used for signed-out users.
"""

from __future__ import annotations

import time

from backend import storage
from backend.config import (
    ADDS_BEFORE_BOOK_RERUN,
    RECOMMENDED_BOOKS_SIZE,
    RECOMMENDED_EVENTS_SIZE,
)
from backend.storage import get_storage
from backend.recommender.book_recommender import BookRecommender
from backend.recommender.event_recommender import EventRecommender


def _build_user_recommender_inputs(user_id: str) -> tuple[dict, dict, bool, bool]:
    """Build inputs for book and event recommenders from storage.

    - Book recommender uses only library (books on shelves → user_books_read_store).
    - Event recommender uses only genre_preferences (user_genres_store).

    Args:
        user_id: Normalized user identifier.

    Returns:
        (user_genres_store, user_books_read_store, has_library, has_genre_prefs).
    """
    user_id = str(user_id).strip().lower()
    store = get_storage()
    user_books = store.get_user_books(user_id)
    user_books_read_store: dict[str, list[str]] = {}
    user_genres_store: dict[str, list[dict]] = {}
    library = (user_books.get("library") or {}) if isinstance(user_books, dict) else {}
    source_ids: list[str] = []
    for shelf in ("in_progress", "saved", "finished"):
        for bid in library.get(shelf, []) or []:
            if isinstance(bid, str) and bid.strip():
                source_ids.append(bid.strip())
            elif bid is not None and str(bid).strip():
                source_ids.append(str(bid).strip())
    if source_ids:
        user_books_read_store[user_id] = list(dict.fromkeys(source_ids))
    has_library = bool(user_books_read_store.get(user_id))
    # Genre preferences only (no clubs / forums).
    prefs = (user_books or {}).get("genre_preferences") or []
    if prefs:
        user_genres_store[user_id] = [
            {"genre": g, "rank": rank}
            for rank, g in enumerate([p for p in prefs if p][:3], start=1)
        ]
    has_genre_prefs = bool(user_genres_store.get(user_id))
    return user_genres_store, user_books_read_store, has_library, has_genre_prefs


def get_book_recommendations(user_id: str, top_k: int | None = None) -> list[dict]:
    """Return book recommendations (same pattern as get_event_recommendations).

    Gets user_book_ids from storage (library), then BookRecommender().recommend(user_book_ids, top_k).
    Cold start: pass [] as user_book_ids; recommender returns popular/catalog results.
    """
    top_k = top_k if top_k is not None else RECOMMENDED_BOOKS_SIZE
    user_id = str(user_id).strip().lower() if user_id else ""
    _, user_books_store, _, _ = _build_user_recommender_inputs(user_id)
    user_book_ids = user_books_store.get(user_id, []) if user_id else []
    recommender = BookRecommender()
    return recommender.recommend(user_book_ids, top_k=top_k)


def get_event_recommendations(user_id: str, top_k: int | None = None) -> list[dict]:
    """Return event recommendations (personalized when possible).

    Event recommender uses only the user's genre_preferences. If none, returns []
    (caller falls back to default soonest events).
    """
    top_k = top_k if top_k is not None else RECOMMENDED_EVENTS_SIZE
    user_id = str(user_id or "").strip().lower()
    if not user_id or top_k <= 0:
        return []

    user_genres_store, _, _has_library, has_genre_prefs = _build_user_recommender_inputs(
        user_id
    )
    if not has_genre_prefs:
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
    """Return the soonest expiry time (min ttl) from event dicts; 0 if none."""
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
    """Return True if the user has at least one genre in genre_preferences."""
    if not user_id:
        return False
    store = get_storage()
    user_books = store.get_user_books(user_id)
    prefs = (user_books or {}).get("genre_preferences") or []
    return len(prefs) > 0


def get_recommended_books_for_user(user_id: str | None = None) -> list[dict]:
    """Return the cached book recommendation list for the UI (up to 50).

    Anonymous or no genre prefs: returns top 25 from reviews JSON (get_top50_review_books).
    Signed-in with genre prefs: returns stored recommended_books, or runs recommender once
    and saves. Use this for homepage/feed; use get_book_recommendations for recomputing only.

    Args:
        user_id: User email/id or None for anonymous.

    Returns:
        List of up to RECOMMENDED_BOOKS_SIZE book dicts (full records for display).
    """
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
    """Return the cached event recommendation list for the UI (up to 10).

    Anonymous or no genre prefs: returns soonest-upcoming events from storage.
    Signed-in with genre prefs: returns stored recommended_events, or re-ranks on expiry.

    Args:
        user_id: User email/id or None for anonymous.

    Returns:
        List of up to RECOMMENDED_EVENTS_SIZE event dicts.
    """
    store = get_storage()
    if not user_id or not str(user_id).strip():
        return store.get_soonest_events(RECOMMENDED_EVENTS_SIZE)[:RECOMMENDED_EVENTS_SIZE]
    user_id = str(user_id).strip().lower()
    if not _user_has_genre_preferences(user_id):
        return store.get_soonest_events(RECOMMENDED_EVENTS_SIZE)[:RECOMMENDED_EVENTS_SIZE]
    rec = store.get_user_recommendations(user_id) or {}
    now = int(time.time())
    soonest = int(rec.get("events_soonest_expiry") or 0)
    if soonest <= 0 or now >= soonest:
        events = get_event_recommendations(user_id)
        rec["recommended_events"] = events[:RECOMMENDED_EVENTS_SIZE]
        rec["events_soonest_expiry"] = _events_soonest_expiry(events)
        store.save_user_recommendations(user_id, rec)
    return list(rec.get("recommended_events") or [])[:RECOMMENDED_EVENTS_SIZE]


def refresh_and_save_recommendations(user_id: str) -> dict:
    """Compute and persist 50 books + 10 events for the user.

    Args:
        user_id: User identifier.

    Returns:
        The user_recommendations record (recommended_books, recommended_events, timestamps).
    """
    user_id = str(user_id).strip().lower()
    if not user_id:
        return {}
    store = get_storage()
    rec = store.get_user_recommendations(user_id) or {}
    books = get_book_recommendations(user_id) or []
    events = get_event_recommendations(user_id) or []
    rec["recommended_books"] = books[:RECOMMENDED_BOOKS_SIZE]
    rec["recommended_events"] = events[:RECOMMENDED_EVENTS_SIZE]
    rec["book_updated_at"] = int(time.time())
    rec["events_soonest_expiry"] = _events_soonest_expiry(events)
    store.save_user_recommendations(user_id, rec)
    return rec


def ensure_default_recommendations(user_id: str) -> None:
    """Seed user_recommendations for new users with no genre prefs.

    Idempotent: no-op if user has genre prefs or already has recommendations.
    Call from auth_service (or equivalent) on account creation.
    """
    user_id = str(user_id).strip().lower()
    if not user_id:
        return
    store = get_storage()
    if _user_has_genre_preferences(user_id):
        return
    rec = store.get_user_recommendations(user_id) or {}
    if rec.get("recommended_books") or rec.get("recommended_events"):
        return
    rec["recommended_books"] = store.get_top50_review_books()[:RECOMMENDED_BOOKS_SIZE]
    rec["recommended_events"] = store.get_soonest_events(RECOMMENDED_EVENTS_SIZE)[:RECOMMENDED_EVENTS_SIZE]
    rec["events_soonest_expiry"] = _events_soonest_expiry(rec["recommended_events"])
    store.save_user_recommendations(user_id, rec)


def on_book_added_to_shelf(user_id: str) -> None:
    """Hook to call after a book is added to a shelf.

    Increments adds_since_last_book_run; when it reaches ADDS_BEFORE_BOOK_RERUN (3),
    re-runs the book recommender and resets the counter. Called from library_service.
    """
    user_id = str(user_id).strip().lower()
    if not user_id:
        return
    store = get_storage()
    rec = store.get_user_recommendations(user_id) or {}
    adds = int(rec.get("adds_since_last_book_run") or 0) + 1
    rec["adds_since_last_book_run"] = adds
    if adds >= ADDS_BEFORE_BOOK_RERUN:
        books = get_book_recommendations(user_id)
        rec["recommended_books"] = books[:RECOMMENDED_BOOKS_SIZE]
        rec["book_updated_at"] = int(time.time())
        rec["adds_since_last_book_run"] = 0
    store.save_user_recommendations(user_id, rec)
