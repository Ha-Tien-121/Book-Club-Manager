"""Books service layer.

UI-focused book helpers.

Important: the full catalog is very large (~millions of books). This module
must **not** expose functions that load the entire catalog into memory.

Instead, it returns **bounded, curated** lists suitable for:
- Homepage / Explore pages (top lists, recommendations)

For true catalog browsing (search, pagination, arbitrary filters), add a
separate data access path (e.g. OpenSearch, Athena, or DynamoDB indexes).
"""

from __future__ import annotations

from typing import Any

from botocore.exceptions import BotoCoreError, ClientError

from backend.storage import get_storage


def get_trending_books_spl(limit: int = 50) -> list[dict[str, Any]]:
    """
    Return top books by SPL checkouts from storage (e.g. S3 in AWS).
    If unavailable, falls back to the reviews top-50 list.
    """
    store = get_storage()
    try:
        books = store.get_spl_top50_checkout_books() or []
        return books[:limit]
    except (ClientError, BotoCoreError, OSError, ValueError, TypeError):
        pass
    try:
        books = store.get_top50_review_books() or []
        return books[:limit]
    except (ClientError, BotoCoreError, OSError, ValueError, TypeError):
        return []


def get_trending_books_reviews(limit: int = 50) -> list[dict[str, Any]]:
    """Return the reviews-based top books list (bounded)."""
    store = get_storage()
    try:
        books = store.get_top50_review_books() or []
        return books[:limit]
    except (ClientError, BotoCoreError, OSError, ValueError, TypeError):
        return []


def get_book_detail(parent_asin: str) -> dict[str, Any]:
    """Return full book details for a given parent_asin.

    This is intended for the Book Detail page/modal. It tries to load the
    "full" record first (which may include description), and falls back to
    lightweight metadata if needed.

    Args:
        parent_asin: Book identifier (Amazon parent ASIN).

    Returns:
        A dict containing book details, or an empty dict if not found.
    """
    store = get_storage()
    parent_asin = str(parent_asin or "").strip()
    if not parent_asin:
        return {}

    try:
        full = store.get_book_details(parent_asin)
        if full:
            return dict(full)
    except (ClientError, BotoCoreError, OSError, ValueError, TypeError):
        pass

    try:
        meta = store.get_book_metadata(parent_asin)
        return dict(meta) if meta else {}
    except (ClientError, BotoCoreError, OSError, ValueError, TypeError):
        return {}


def get_book_forum_thread(parent_asin: str) -> list[dict[str, Any]]:
    """Return the forum thread for a book (by parent_asin).

    Args:
        parent_asin: Book identifier (Amazon parent ASIN).

    Returns:
        List of forum post dicts related to this book.
    """
    store = get_storage()
    parent_asin = str(parent_asin or "").strip()
    if not parent_asin:
        return []
    try:
        return list(store.get_forum_thread_for_book(parent_asin) or [])
    except (ClientError, BotoCoreError, OSError, ValueError, TypeError):
        try:
            return list(store.get_forum_thread(parent_asin) or [])
        except (ClientError, BotoCoreError, OSError, ValueError, TypeError):
            return []


def get_book_hub(parent_asin: str) -> dict[str, Any]:
    """Return a combined payload for a Book "hub" UI page.

    Includes:
      - Book detail dict
      - Forum thread list
      - Related events reading book

    Args:
        parent_asin: Book identifier (Amazon parent ASIN).

    Returns:
        Dict with keys:
          - "book": dict
          - "forum_thread": list[dict]
    """
    book = get_book_detail(parent_asin)
    forum_thread = get_book_forum_thread(parent_asin)
    related_events = get_book_related_events(parent_asin)
    return {
        "book": book,
        "forum_thread": forum_thread,
        "related_events": related_events,
    }


def get_book_related_events(parent_asin: str, limit: int = 5) -> list[dict[str, Any]]:
    """Return upcoming events related to a given book.

    This is intended for a Book Detail \"Events\" section. It does **not** scan
    the full events table; it relies on the events GSI:
      - Partition key: parent_asin
      - Sort key: ttl

    Args:
        parent_asin: Book identifier (Amazon parent ASIN).
        limit: Max number of related events to return.

    Returns:
        List of event dicts, ordered soonest-first. Returns [] if no matching
        events are found or if the GSI is not configured.
    """
    store = get_storage()
    parent_asin = str(parent_asin or "").strip()
    if not parent_asin or limit <= 0:
        return []

    try:
        events_for_book = store.get_events_for_book(parent_asin, limit=int(limit))  # type: ignore[attr-defined]
    except (ClientError, BotoCoreError, OSError, ValueError, TypeError, AttributeError):
        return []

    return list(events_for_book or [])[:limit]
