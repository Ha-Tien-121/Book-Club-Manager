"""Legacy recommendation service helpers.

This module provides lightweight helper functions that wrap the book
recommender and normalize user-signal stores into DataFrame inputs.
"""

from __future__ import annotations

from typing import Any, Dict, List

import pandas as pd

from backend.recommender.book_recommender import BookRecommender


def get_recommender():
    """Return a book recommender instance (same pattern as event)."""
    return BookRecommender()


def build_user_genres_df(
    user_id: str,
    user_genres_store: Dict[str, List[Dict[str, Any]]],
) -> pd.DataFrame:
    """
    Convert user_genres store to a DataFrame for the recommender.

    user_genres_store[user_id] = [{"genre": str, "rank": int}, ...]
    """
    rows = user_genres_store.get(user_id) or []
    if not rows:
        return pd.DataFrame(columns=["user_id", "genre", "rank"])

    return pd.DataFrame(
        [{"user_id": user_id, "genre": r["genre"], "rank": r["rank"]} for r in rows]
    )


def build_user_books_df(
    user_id: str,
    user_books_read_store: Dict[str, List[str]],
) -> pd.DataFrame:
    """
    Convert user_books_read store to a DataFrame for the recommender.

    user_books_read_store[user_id] = [book_id, ...]
    """
    books_read = user_books_read_store.get(user_id) or []
    return pd.DataFrame([{"user_id": user_id, "books_read": books_read}])


def get_recommendations(
    user_id: str,
    user_genres_store: Dict[str, List[Dict[str, Any]]],
    user_books_read_store: Dict[str, List[str]],
    top_k: int = 5,
) -> List[Dict[str, Any]]:
    """Return top_k book recommendations (same as event: caller passes user signal = book IDs)."""
    _ = user_genres_store
    user_book_ids = user_books_read_store.get(user_id, []) if user_id else []
    return BookRecommender().recommend(user_book_ids, top_k=top_k)


def get_top_popular_books(top_k: int = 10) -> List[Dict[str, Any]]:
    """Return globally popular books (cold start: recommend with empty library)."""
    if top_k <= 0:
        return []
    return BookRecommender().recommend([], top_k=top_k)


def mark_book_as_read(
    user_id: str,
    book_id: str,
    user_genres_store: Dict[str, List[Dict[str, Any]]],
    user_books_read_store: Dict[str, List[str]],
    top_k: int = 5,
) -> List[Dict[str, Any]]:
    """
    Mark a book as read for the user and return updated recommendations.

    - Ensures no duplicate book_id in books_read
    - Updates user_books_read_store in place
    - Recomputes recommendations (read books excluded; genre similarity 1.5x when history exists)
    """
    # Ensure user has an entry in the store
    if user_id not in user_books_read_store:
        user_books_read_store[user_id] = []

    current_list = user_books_read_store[user_id]
    if book_id not in current_list:
        user_books_read_store[user_id] = list(current_list) + [book_id]

    return get_recommendations(
        user_id=user_id,
        user_genres_store=user_genres_store,
        user_books_read_store=user_books_read_store,
        top_k=top_k,
    )


def get_book_details(
    book_id: str,
    books_store: Dict[str, Dict[str, Any]],
) -> Dict[str, Any]:
    """
    Return full book details for UI consumption.

    The books_store schema is:
        books_store[book_id] = {
            "title": str,
            "author": str,
            "genre": List[str],
            "parent_asin": Optional[str],
            "available_libraries": Optional[List[str]],
        }

    This function normalizes output and fills missing optional fields with None.
    """
    raw = books_store.get(book_id, {})

    title = raw.get("title", "")
    author = raw.get("author", "")
    genres = raw.get("genre", []) or []
    parent_asin = raw.get("parent_asin")
    available_libraries = raw.get("available_libraries")

    # Ensure list types where expected.
    if not isinstance(genres, list):
        genres = [str(genres)]
    if available_libraries is not None and not isinstance(available_libraries, list):
        available_libraries = [str(available_libraries)]

    return {
        "book_id": book_id,
        "parent_asin": str(parent_asin) if parent_asin is not None else None,
        "title": title,
        "author": author,
        "genres": [str(g) for g in genres],
        "available_libraries": (
            [str(lib) for lib in available_libraries] if available_libraries else None
        ),
    }
