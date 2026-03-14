"""
Events service: business logic for book club events and discovery.

Sits above the storage layer and provides the operations needed by the UI and API:
event details, trending/upcoming events, filtering by tag or book, and search.
Use this module instead of calling storage directly when you need event data
for pages, cards, or recommendations.
"""

from typing import Any, Optional

from backend import storage


def get_event_detail(event_id: str) -> Optional[dict[str, Any]]:
    """
    Get full event details. For event details page.
    """
    return storage.get_event_details(event_id)
