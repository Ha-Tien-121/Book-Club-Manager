"""
Books service: business logic for book discovery and details.

Sits above the storage layer and provides the operations needed by the UI and API:
book details (with and without description), trending/popular books in Seattle,
genre-based browsing, and search. Use this module instead of calling storage
directly when you need book data for pages, cards, or recommendations.
"""

from typing import Any, Optional

from backend import storage


def get_trending_books(limit: int = 50) -> list[dict[str, Any]]:
    """
    Return top N most popular books in Seattle.
    Popularity based on SPL checkouts.
    """
    # TODO: implement (use precomputed trending list)
    return []


def get_book_with_description(parent_asin: str) -> Optional[dict[str, Any]]:
    """
    Get full book with description. For book details page.
    """
    return storage.get_book_details(parent_asin)


def get_book_without_description(parent_asin: str) -> Optional[dict[str, Any]]:
    """
    Get book metadata without description. For homepage, library, cards.
    """
    return storage.get_book_metadata(parent_asin)


def get_books_by_genre(genre: str, limit: int = 50) -> list[dict[str, Any]]:
    """
    Get top N books in a given genre/category.
    """
    # TODO: implement (query by category, rank by rating/popularity)
    return []
