"""Recommender service layer.

Recommendations are stored in user_recommendations (not user_books).
- Books: 50 items. Re-run after every ADDS_BEFORE_BOOK_RERUN (3) adds to shelf (see on_book_added_to_shelf).
  Book recommender uses only the user's library (books on shelves).
- Events: 10 items. Re-run when events_soonest_expiry is past (lazy refresh when reading).
  Event recommender uses only the user's genre_preferences.

Default (anonymous or user with no library / no genre preferences):
- Books: top 50 from static JSON (reviews_top50_books.json).
- Events: top 10 soonest events (by ttl/expiry).
Seeded on account creation via ensure_default_recommendations(); also used for signed-out users.
"""

from __future__ import annotations

import json
import logging
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


def _ui_shape_recommended_books(rows: list[dict]) -> list[dict]:
    """Convert stored/recommender book rows to the UI card shape.

    We intentionally store the UI shape in DynamoDB so the Feed can render quickly
    without resolving IDs via additional DynamoDB/S3 lookups on every rerun.
    """
    out: list[dict] = []
    # Enrich sparse recommendation rows with DynamoDB book metadata in one batch.
    store = get_storage()
    to_enrich: list[str] = []
    for r in rows or []:
        if not isinstance(r, dict):
            continue
        if r.get("cover") and r.get("title") and r.get("author"):
            continue
        asin = str(r.get("parent_asin") or r.get("book_id") or r.get("source_id") or "").strip()
        if asin and (not r.get("title") or not (r.get("author_name") or r.get("author")) or not (r.get("images") or r.get("cover"))):
            to_enrich.append(asin)
    meta_by_asin: dict[str, dict] = {}
    try:
        if to_enrich and hasattr(store, "get_books_metadata_batch"):
            meta_by_asin = store.get_books_metadata_batch(to_enrich) or {}
    except Exception:
        meta_by_asin = {}
    # Note: we intentionally do not fall back to S3-parquet detail fetch here.
    # That path reads large shards and is too slow to run synchronously when
    # saving recommendations (and would make every recommender run feel stuck).

    def _genres_from_raw(raw) -> list[str]:
        """Normalize genre/category values into a clean list of strings."""
        if raw is None:
            return []
        if isinstance(raw, list):
            return [str(x).strip() for x in raw if x is not None and str(x).strip()]
        if isinstance(raw, str):
            s = raw.strip()
            if not s:
                return []
            if s.startswith("[") and s.endswith("]"):
                try:
                    parsed = json.loads(s)
                    if isinstance(parsed, list):
                        return [str(x).strip() for x in parsed if x is not None and str(x).strip()]
                except Exception:
                    pass
            return [s]
        return []

    for r in rows or []:
        if not isinstance(r, dict):
            continue
        # Already UI-shaped.
        if r.get("cover") and r.get("title") and r.get("author"):
            out.append(
                {
                    "id": r.get("id") or 0,
                    "source_id": r.get("source_id") or r.get("parent_asin") or r.get("book_id") or "",
                    "title": r.get("title") or "",
                    "author": r.get("author") or "",
                    "genres": list(r.get("genres") or []),
                    "cover": r.get("cover") or "https://placehold.co/220x330?text=Book",
                    "rating": r.get("rating") or 0,
                    "rating_count": r.get("rating_count") or 0,
                }
            )
            continue

        asin = str(r.get("parent_asin") or r.get("book_id") or r.get("source_id") or "").strip()
        if not asin:
            continue
        if meta_by_asin and asin in meta_by_asin:
            # Don't overwrite explicit values (e.g. ML might provide some fields),
            # but fill in blanks from the canonical books table.
            m = meta_by_asin.get(asin) or {}
            for k, v in m.items():
                if k not in r or r.get(k) in (None, "", [], {}):
                    r[k] = v
        genres = _genres_from_raw(r.get("genres"))
        if not genres:
            genres = _genres_from_raw(r.get("categories"))
        cover = (r.get("images") or r.get("image_url") or r.get("cover") or "").strip() if isinstance(r.get("images") or r.get("image_url") or r.get("cover"), str) else r.get("images") or r.get("image_url") or r.get("cover")
        if not cover:
            cover = "https://placehold.co/220x330?text=Book"
        try:
            rating = float(r.get("average_rating") or r.get("rating") or 0)
        except Exception:
            rating = 0
        try:
            rating_count = int(r.get("rating_number") or r.get("rating_count") or 0)
        except Exception:
            rating_count = 0
        out.append(
            {
                "id": 0,
                "source_id": asin,
                "title": r.get("title") or "",
                "author": r.get("author_name") or r.get("author") or "",
                "genres": genres,
                "cover": cover,
                "rating": rating,
                "rating_count": rating_count,
            }
        )
    return out


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
    """Return book recommendations (content-based or fallback).

    Uses BookRecommender().recommend_for_user() with the user's library and
    genre_preferences. Cold start: recommender returns popular/catalog results.
    """
    top_k = top_k if top_k is not None else RECOMMENDED_BOOKS_SIZE
    user_id = str(user_id).strip().lower() if user_id else ""
    rows, _source, _err = _run_book_recommender(user_id, top_k=top_k)
    return rows


def _run_book_recommender(
    user_id: str,
    *,
    top_k: int,
) -> tuple[list[dict], str, str]:
    """Run content-based recommender; fall back to top-50 on error.

    Returns:
        (rows, source, error_message)
        - source: "content" or "fallback"
        - error_message: "" if none
    """
    user_id = str(user_id or "").strip().lower()
    store = get_storage()
    user_account = store.get_user_books(user_id) if user_id else {}
    if not isinstance(user_account, dict):
        user_account = {}
    user_genres_store, _, _, _ = _build_user_recommender_inputs(user_id)
    user_genres = list(user_genres_store.get(user_id) or [])

    recommender = BookRecommender()
    try:
        rows = recommender.recommend_for_user(
            user_email=user_id,
            user_account=user_account,
            user_genres=user_genres if user_genres else None,
            top_k=top_k,
        )
        return list(rows or []), "content", ""
    except Exception as e:
        logging.exception("BookRecommender failed; falling back. error=%s", e)
        try:
            from backend.recommender.book_recommender import _FallbackBookRecommender

            rows = _FallbackBookRecommender().recommend_for_user(
                user_email=user_id,
                user_account=user_account,
                user_genres=user_genres if user_genres else None,
                top_k=top_k,
            )
            return list(rows or []), "fallback", f"{type(e).__name__}: {e}"
        except Exception as e2:
            logging.exception("FallbackBookRecommender failed. error=%s", e2)
            return [], "fallback", f"{type(e2).__name__}: {e2}"


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

    Anonymous: returns top 50 from reviews JSON (get_top50_review_books).
    Signed-in: returns stored recommended_books, or runs the book recommender and saves.
    The book recommender uses only the user's library; if the library is empty it
    falls back to popular/top-50 results.

    Args:
        user_id: User email/id or None for anonymous.

    Returns:
        List of up to RECOMMENDED_BOOKS_SIZE book dicts (full records for display).
    """
    store = get_storage()
    # Anonymous: always use static top-50 reviews.
    if not user_id or not str(user_id).strip():
        return store.get_top50_review_books()[:RECOMMENDED_BOOKS_SIZE]

    user_id = str(user_id).strip().lower()
    # Validate user exists.
    if not store.get_user_account(user_id):
        raise ValueError("Invalid user_id")

    # No genre preferences: show top 50 reviews books (same as anonymous).
    if not _user_has_genre_preferences(user_id):
        return _ui_shape_recommended_books(
            store.get_top50_review_books()
        )[:RECOMMENDED_BOOKS_SIZE]

    rec = store.get_user_recommendations(user_id) or {}
    books = list(rec.get("recommended_books") or [])

    if not books:
        # Run book recommender once and cache result.
        books = _ui_shape_recommended_books(get_book_recommendations(user_id))[:RECOMMENDED_BOOKS_SIZE]
        rec["recommended_books"] = books
        rec["book_updated_at"] = int(time.time())
        store.save_user_recommendations(user_id, rec)

    if not books:
        # Ultimate fallback: static top-50 when recommender produced nothing.
        return store.get_top50_review_books()[:RECOMMENDED_BOOKS_SIZE]

    # Ensure UI shape (backward compatible with older stored records).
    return _ui_shape_recommended_books(books)[:RECOMMENDED_BOOKS_SIZE]


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
    # Capture source/error so we can tell in Dynamo whether ML ran.
    book_rows, book_source, book_err = _run_book_recommender(
        user_id, top_k=RECOMMENDED_BOOKS_SIZE
    )
    books = _ui_shape_recommended_books(book_rows or [])
    events = get_event_recommendations(user_id) or []
    rec["recommended_books"] = books[:RECOMMENDED_BOOKS_SIZE]
    rec["recommended_events"] = events[:RECOMMENDED_EVENTS_SIZE]
    rec["book_updated_at"] = int(time.time())
    rec["book_recs_source"] = book_source
    if book_err:
        rec["book_recs_error"] = book_err
    else:
        rec.pop("book_recs_error", None)
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
    rec["recommended_books"] = _ui_shape_recommended_books(store.get_top50_review_books())[:RECOMMENDED_BOOKS_SIZE]
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
        # Always refresh the cached recommendations when the threshold is reached.
        # The fallback recommender is still personalized (it excludes owned books),
        # so it's safe and desirable to overwrite existing lists even in fallback.
        book_rows, book_source, book_err = _run_book_recommender(
            user_id, top_k=RECOMMENDED_BOOKS_SIZE
        )
        books = _ui_shape_recommended_books(book_rows or [])
        if books:
            rec["recommended_books"] = books[:RECOMMENDED_BOOKS_SIZE]
        rec["book_updated_at"] = int(time.time())
        rec["book_recs_source"] = book_source
        if book_err:
            rec["book_recs_error"] = book_err
        else:
            rec.pop("book_recs_error", None)
        rec["adds_since_last_book_run"] = 0
    store.save_user_recommendations(user_id, rec)
