"""Library service layer: user shelves and genre preferences.

This module is responsible for:

- Adding, moving, and removing books from a user's shelves (saved, in_progress, finished).
- Reading the user's library (shelf → list of book IDs) and genre preferences.
- UI helpers: is_book_in_library, get_shelf_for_book, get_library_with_details (shelves with full book detail dicts).
- Updating the user's genre preferences (used by the recommender). When a book
  is added to the library, any of that book's categories not already in
  preferences are appended.

All persistence goes through the storage abstraction (`get_storage()`), so the same
logic works in local JSON mode and AWS/DynamoDB mode. The empty library shape is
normally created in `auth_service.create_user`; this module uses a fallback only
when the user_books record is missing or lacks a library key.

Adding a book to a shelf triggers `on_book_added_to_shelf` (recommender_service),
which may run the book recommender after a configured number of adds.
"""

from __future__ import annotations

from typing import Any

from backend.services.books_service import get_book_detail
from backend.storage import get_storage
from backend.services.recommender_service import on_book_added_to_shelf


def _default_library() -> dict[str, list[Any]]:
    """Return empty library shape. Created in auth_service.create_user; used here only as fallback."""
    return {"in_progress": [], "saved": [], "finished": []}


def _merge_genres_into_record(record: dict[str, Any], new_genres: list[str]) -> None:
    """Append new genres to record['genre_preferences'] without duplicates. Mutates record in place.

    Uses a dict (normalized_key -> display string) for O(1) lookup when deduping.
    """
    current = record.get("genre_preferences") or []
    by_key: dict[str, str] = {}
    for g in current:
        s = str(g).strip()
        if s:
            by_key[s.lower()] = s
    for g in new_genres or []:
        s = str(g).strip()
        if s and s.lower() not in by_key:
            by_key[s.lower()] = s
    record["genre_preferences"] = list(by_key.values())


def _merge_book_genres_into_preferences(rec: dict[str, Any], parent_asin: str, store: Any) -> None:
    """Fetch book categories, merge into genre_preferences, and increment genre_counts."""
    try:
        meta = store.get_book_metadata(parent_asin)
    except (RuntimeError, ValueError, TypeError, KeyError):
        return
    if not meta:
        return
    categories = meta.get("categories") or []
    if not isinstance(categories, list):
        categories = [categories] if categories else []
    book_cats = [str(c).strip() for c in categories if str(c).strip()]
    if not book_cats:
        return
    _merge_genres_into_record(rec, book_cats)
    counts = rec.setdefault("genre_counts", {})
    for c in book_cats:
        key = c.lower()
        counts[key] = counts.get(key, 0) + 1


def add_book_to_library(
    user_id: str,
    book_id: int | str,
    shelf: str,
    genres_from_book: list[str] | None = None,
) -> dict[str, Any]:
    """Add a book to one shelf; remove it from the others. Triggers recommender hook.

    Args:
        user_id: User email/identifier.
        book_id: Book identifier (parent_asin or internal id; stored as string).
        shelf: One of saved, in_progress, finished.
        genres_from_book: If provided, merge these genres into genre_preferences
            instead of fetching via get_book_metadata (e.g. from UI when book dict is available).

    Returns:
        Updated user_books record (library + genre_preferences).

    Raises:
        ValueError: If shelf is not one of saved, in_progress, finished.
    """
    store = get_storage()
    user_id = str(user_id).strip().lower()
    shelf = str(shelf).strip().lower()
    if shelf not in {"saved", "in_progress", "finished"}:
        raise ValueError("invalid shelf")

    rec = store.get_user_books(user_id) or {}
    lib = rec.setdefault("library", _default_library())
    rec.setdefault("genre_preferences", [])

    book_key = str(book_id)
    on_target = False
    on_other = False
    for key in ("saved", "in_progress", "finished"):
        shelf_list = lib.get(key) or []
        lib[key] = [bid for bid in shelf_list if str(bid) != book_key]
        if key == shelf:
            on_target = len(lib[key]) < len(shelf_list)
        else:
            on_other = on_other or any(str(bid) == book_key for bid in shelf_list)
    # No-op: book already only on this shelf → skip write and recommender
    if on_target and not on_other:
        return dict(rec)
    lib[shelf].append(book_id)
    # Merge book's categories into genre_preferences
    if genres_from_book:
        book_cats = [str(c).strip() for c in genres_from_book if str(c).strip()]
        if book_cats:
            _merge_genres_into_record(rec, book_cats)
            counts = rec.setdefault("genre_counts", {})
            for c in book_cats:
                key = c.lower()
                counts[key] = counts.get(key, 0) + 1
    else:
        _merge_book_genres_into_preferences(rec, book_key, store)
    store.save_user_books(user_id, rec)
    on_book_added_to_shelf(user_id)
    return dict(rec)


def _drop_genres_only_from_removed_book(
    rec: dict[str, Any], removed_book_key: str, store: Any
) -> None:
    """Decrement genre_counts and remove exhausted preference genres.

    This only touches categories from the removed book and performs O(1)
    metadata reads.
    """
    try:
        meta = store.get_book_metadata(removed_book_key)
    except (RuntimeError, ValueError, TypeError, KeyError):
        return
    if not meta:
        return
    categories = meta.get("categories") or []
    if not isinstance(categories, list):
        categories = [categories] if categories else []
    removed_cats = [str(c).strip().lower() for c in categories if str(c).strip()]
    if not removed_cats:
        return

    counts = rec.setdefault("genre_counts", {})
    to_remove: set[str] = set()
    for key in removed_cats:
        if key not in counts:
            continue
        counts[key] = counts[key] - 1
        if counts[key] <= 0:
            to_remove.add(key)
            del counts[key]
    if not to_remove:
        return
    current = rec.get("genre_preferences") or []
    rec["genre_preferences"] = [
        g for g in current
        if str(g).strip().lower() not in to_remove
    ]


def remove_book_from_library(user_id: str, book_id: int | str) -> dict[str, Any]:
    """Remove a book from every shelf (saved, in_progress, finished).

    If that book was the only one in the library with a given genre, that genre
    is removed from the user's genre_preferences.

    Args:
        user_id: User email/identifier.
        book_id: Book identifier to remove.

    Returns:
        Updated user_books record.
    """
    store = get_storage()
    user_id = str(user_id).strip().lower()
    book_key = str(book_id)

    rec = store.get_user_books(user_id) or {}
    lib = rec.setdefault("library", _default_library())
    for key in ("saved", "in_progress", "finished"):
        lib[key] = [bid for bid in lib.get(key, []) if str(bid) != book_key]
    _drop_genres_only_from_removed_book(rec, book_key, store)
    store.save_user_books(user_id, rec)
    return dict(rec)


def get_user_library(user_id: str) -> dict[str, list[Any]]:
    """Return the user's library (shelves only).

    Args:
        user_id: User email/identifier.

    Returns:
        Dict with keys in_progress, saved, finished; each value is a list of book IDs.
    """
    store = get_storage()
    rec = store.get_user_books(str(user_id).strip().lower())
    library = rec.get("library") or _default_library()
    # Ensure all shelves exist.
    out: dict[str, list[Any]] = {
        "in_progress": list(library.get("in_progress") or []),
        "saved": list(library.get("saved") or []),
        "finished": list(library.get("finished") or []),
    }
    return out


def get_shelf_for_book(user_id: str, book_id: int | str) -> str | None:
    """Return which shelf the book is on, or None if not in library.

    One storage read, single pass with short-circuit. Use this as the primitive
    for shelf lookup; is_book_in_library delegates here.

    Args:
        user_id: User email/identifier.
        book_id: Book identifier (parent_asin or internal id).

    Returns:
        One of \"saved\", \"in_progress\", \"finished\", or None if not in library.
    """
    store = get_storage()
    user_id = str(user_id).strip().lower()
    book_key = str(book_id)
    if not user_id or not book_key:
        return None
    rec = store.get_user_books(user_id) or {}
    lib = rec.get("library") or _default_library()
    for shelf in ("saved", "in_progress", "finished"):
        for bid in lib.get(shelf, []):
            if str(bid) == book_key:
                return shelf
    return None


def is_book_in_library(user_id: str, book_id: int | str) -> bool:
    """Return True if the book is on any shelf in the user's library.

    Delegates to get_shelf_for_book (one read, short-circuit).

    Args:
        user_id: User email/identifier.
        book_id: Book identifier (parent_asin or internal id).

    Returns:
        True if the book is on saved, in_progress, or finished; False otherwise.
    """
    return get_shelf_for_book(user_id, book_id) is not None


def get_library_with_details(user_id: str) -> dict[str, list[dict[str, Any]]]:
    """Return the user's library with full book details per shelf for UI display.

    One user_books read; get_book_detail cached by bid so each book is fetched
    at most once (O(unique books) fetches).

    Args:
        user_id: User email/identifier.

    Returns:
        Dict with keys saved, in_progress, finished; each value is a list of
        book detail dicts (same shape as books_service.get_book_detail).
    """
    store = get_storage()
    user_id = str(user_id).strip().lower()
    rec = store.get_user_books(user_id) or {}
    lib = rec.get("library") or _default_library()
    out: dict[str, list[dict[str, Any]]] = {
        "saved": [],
        "in_progress": [],
        "finished": [],
    }
    detail_cache: dict[str, dict[str, Any]] = {}
    for shelf in ("saved", "in_progress", "finished"):
        for bid in lib.get(shelf, []):
            bid_s = str(bid)
            if bid_s not in detail_cache:
                detail_cache[bid_s] = get_book_detail(bid_s) or {}
            book = detail_cache[bid_s]
            if book:
                out[shelf].append(book)
    return out


def get_user_preferences(user_id: str) -> list[str]:
    """Return the user's genre preferences (for settings/onboarding UI).

    Args:
        user_id: User email/identifier.

    Returns:
        List of genre strings; empty list if none set.
    """
    store = get_storage()
    rec = store.get_user_books(str(user_id).strip().lower()) or {}
    return list(rec.get("genre_preferences") or [])


def update_user_preferences(user_id: str, genres: list[str]) -> dict[str, Any]:
    """Overwrite the user's genre preferences (e.g. onboarding or settings).

    Clears genre_counts so removal logic only uses counts from books in library.

    Args:
        user_id: User email/identifier.
        genres: New list of genre strings; empty list clears preferences.

    Returns:
        Updated user_books record.
    """
    store = get_storage()
    user_id = str(user_id).strip().lower()
    rec = store.get_user_books(user_id) or {}
    rec.setdefault("library", _default_library())
    rec.setdefault("genre_counts", {})
    rec["genre_preferences"] = [str(g).strip() for g in (genres or []) if str(g).strip()]
    rec["genre_counts"] = {}
    store.save_user_books(user_id, rec)
    return dict(rec)


def update_book_status(user_id: str, book_id: int | str, shelf: str) -> dict[str, Any]:
    """Move a book between shelves without triggering the recommender.

    This is intended for status changes (e.g. saved → finished) on books that
    are already in the user's library. It does not call the recommender hook
    and does not modify genre_preferences.

    Args:
        user_id: User email/identifier.
        book_id: Book identifier.
        shelf: One of saved, in_progress, finished.

    Returns:
        Updated user_books record.
    """
    store = get_storage()
    user_id = str(user_id).strip().lower()
    shelf = str(shelf).strip().lower()
    if shelf not in {"saved", "in_progress", "finished"}:
        raise ValueError("invalid shelf")

    rec = store.get_user_books(user_id) or {}
    lib = rec.setdefault("library", _default_library())

    book_key = str(book_id)
    for key in ("saved", "in_progress", "finished"):
        lib[key] = [bid for bid in lib.get(key, []) if str(bid) != book_key]
    lib[shelf].append(book_id)

    store.save_user_books(user_id, rec)
    return dict(rec)


def remove_book_from_shelf(user_id: str, shelf: str, parent_asin: str) -> dict[str, Any]:
    """Remove a book from one shelf only (other shelves unchanged).

    If the book is no longer on any shelf, genres that were only from that book
    are removed from genre_preferences.

    Args:
        user_id: User email/identifier.
        shelf: One of saved, in_progress, finished.
        parent_asin: Book identifier to remove from that shelf.

    Returns:
        Updated user_books record (same shape as remove_book_from_library).

    Raises:
        ValueError: If shelf is invalid.
    """
    store = get_storage()
    user_id = str(user_id).strip().lower()
    shelf = str(shelf).strip().lower()
    parent_asin = str(parent_asin).strip()
    if shelf not in {"saved", "in_progress", "finished"}:
        raise ValueError("invalid shelf")
    if not user_id or not parent_asin:
        rec = store.get_user_books(user_id) or {}
        rec.setdefault("library", _default_library())
        return dict(rec)

    rec = store.get_user_books(user_id) or {}
    lib = rec.setdefault("library", _default_library())
    lib[shelf] = [bid for bid in lib.get(shelf, []) if str(bid) != parent_asin]
    _drop_genres_only_from_removed_book(rec, parent_asin, store)
    store.save_user_books(user_id, rec)
    return dict(rec)
