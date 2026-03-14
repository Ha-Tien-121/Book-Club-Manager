"""Events service layer."""

from __future__ import annotations

from backend import storage


def get_event_detail(event_id: str) -> dict:
    """Get full event details by event_id."""
    return storage.get_event_detail(str(event_id).strip()) or {}


def get_events_by_city(city_state: str) -> list[dict]:
    """
    Return events filtered by city_state.
    TODO: implement when storage.get_event_detail supports querying by city.
    """
    _ = city_state
    return []


def get_events_by_genre(genre: str) -> list[dict]:
    """
    Return events filtered by genre.
    TODO: implement when storage.get_event_detail supports querying by genre.
    """
    _ = genre
    return []

#save_events, get_saved_events, save_event
