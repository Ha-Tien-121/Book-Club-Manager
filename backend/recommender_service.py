"""
Recommender service: facade for book and event recommendations.

Sits above the concrete recommenders in `backend.recommender` and provides the
operations the UI / API should call. This keeps callers from importing the
recommenders directly and lets us swap implementations later.
"""

from typing import Any

from backend.recommender import book_recommender, event_recommender


def recommend_books_for_user(user_email: str) -> list[dict[str, Any]]:
    """
    Get recommended books for a user.

    Skeleton: delegates to `book_recommender.recommend_for_user`.
    """
    return book_recommender.recommend_for_user(user_email)


def recommend_events_for_user(user_email: str) -> list[dict[str, Any]]:
    """
    Get recommended events for a user.

    Skeleton: delegates to `event_recommender.recommend_for_user`.
    """
    return event_recommender.recommend_for_user(user_email)


def recommend_all_for_user(user_email: str) -> dict[str, list[dict[str, Any]]]:
    """
    Convenience helper that returns both book and event recommendations.
    """
    return {
        "books": recommend_books_for_user(user_email),
        "events": recommend_events_for_user(user_email),
    }

