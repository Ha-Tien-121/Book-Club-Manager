"""Events service layer.

This module provides a thin, storage-agnostic interface for working with
*event objects themselves* (not per-user data):

- Fetching full event details for a given `event_id`.
- (Future) Querying events by city/genre.

Per-user saved/registered events live in `backend.services.user_events_service`.
"""

from __future__ import annotations

from typing import Any

from backend.config import EVENT_RECOMMENDATION_POOL_SIZE
from backend.storage import get_storage


def get_event_detail(event_id: str) -> dict[str, Any]:
    """Return full event details by `event_id`.

    Args:
        event_id: Identifier of the event to fetch.

    Returns:
        A dict with the event's full schema, or an empty dict if not found.
    """
    store = get_storage()
    ev = store.get_event_details(str(event_id).strip())
    return ev or {}


def get_events_by_city(city_state: str) -> list[dict[str, Any]]:
    """Return events filtered by `city_state` via the storage backend, ordered by soonest start/expiry.

    Args:
        city_state: City/state string to match (exact, e.g. "Seattle, WA").
    Returns:
        List of event dicts in that city/state (may be empty) sorted from soonest to latest.
    """
    store = get_storage()
    return store.get_events_by_city(city_state)


def get_explore_events(limit: int | None = None) -> list[dict[str, Any]]:
    """Return the pool of upcoming events for the Explore page.

    This is intended for the Explore Events UI, which can:
      - Default to showing personalized/default recommendations.
      - Fall back to / switch to this full pool for browsing, genre filters,
        and client-side sorting.

    Args:
        limit: Optional max number of events to return. If not provided,
            the storage backend's default pool size is used.

    Returns:
        List of event dicts, ordered soonest-first.
    """
    store = get_storage()
    if limit is None:
        limit = EVENT_RECOMMENDATION_POOL_SIZE
    return store.get_soonest_events(limit)
