"""
Important: This is all tentative but recommender counter will be tracked here. 

Library service: business logic for a user's personal library.

This module should be the single entry point for library mutations (save a book,
mark read/reading, remove, etc.). After each mutation, we bump a per-user action
counter. When the counter reaches a threshold (e.g., 3 or 5), callers can trigger
the recommender and then reset the counter.
"""

from typing import Any, Optional

from backend import storage


def save_book_to_library(user_id: str, parent_asin: str) -> dict[str, Any]:
    """
    Skeleton: Save a book to the user's library.

    TODO: implement the actual write to the library table.
    """
    # TODO: storage.add_book_to_user_library(user_id, parent_asin)
    counter = storage.increment_library_actions_since_recs(user_id=user_id, threshold=_threshold())
    return {
        "ok": True,
        "action": "save",
        "user_id": user_id,
        "parent_asin": parent_asin,
        "recommender": counter,
    }


def set_book_status(user_id: str, parent_asin: str, status: str) -> dict[str, Any]:
    """
    Skeleton: Update reading status, e.g. 'saved' | 'reading' | 'read'.

    TODO: implement the actual write to the library table.
    """
    # TODO: storage.set_user_book_status(user_id, parent_asin, status)
    counter = storage.increment_library_actions_since_recs(user_id=user_id, threshold=_threshold())
    return {
        "ok": True,
        "action": "set_status",
        "user_id": user_id,
        "parent_asin": parent_asin,
        "status": status,
        "recommender": counter,
    }


def remove_book_from_library(user_id: str, parent_asin: str) -> dict[str, Any]:
    """
    Skeleton: Remove a book from the user's library.

    TODO: implement the actual write to the library table.
    """
    # TODO: storage.remove_book_from_user_library(user_id, parent_asin)
    counter = storage.increment_library_actions_since_recs(user_id=user_id, threshold=_threshold())
    return {
        "ok": True,
        "action": "remove",
        "user_id": user_id,
        "parent_asin": parent_asin,
        "recommender": counter,
    }


def acknowledge_recommendations_ran(user_id: str) -> bool:
    """
    Call this after you run the recommender for the user to reset the counter.
    """
    return storage.reset_library_actions_since_recs(user_id=user_id)


def _threshold() -> int:
    """
    Threshold for how many library actions should trigger a refresh.

    Kept as a helper so later you can read from env/config without changing call sites.
    """
    return 3

