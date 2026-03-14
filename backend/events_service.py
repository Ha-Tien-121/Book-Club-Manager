"""Events service layer."""

from __future__ import annotations

from backend.config import USER_EVENTS_PATH
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


def get_saved_events(user_id: str) -> list[str]:
    """Return list of event_ids saved by the user."""
    user_id = str(user_id).strip().lower()
    data = storage._read_json(USER_EVENTS_PATH, {})  # pylint: disable=protected-access
    rec = data.get(user_id) or {"events": []}
    return [str(e) for e in rec.get("events") or []]


def save_event(user_id: str, event_id: str) -> list[str]:
    """Add an event to the user's saved events. Returns updated list."""
    user_id = str(user_id).strip().lower()
    event_id = str(event_id).strip()
    data = storage._read_json(USER_EVENTS_PATH, {})  # pylint: disable=protected-access
    rec = data.setdefault(user_id, {"events": []})
    events = [str(e) for e in rec.get("events") or []]
    if event_id and event_id not in events:
        events.append(event_id)
    rec["events"] = events
    storage._write_json(USER_EVENTS_PATH, data)  # pylint: disable=protected-access
    return events


def remove_saved_event(user_id: str, event_id: str) -> list[str]:
    """Remove an event from the user's saved events. Returns updated list."""
    user_id = str(user_id).strip().lower()
    event_id = str(event_id).strip()
    data = storage._read_json(USER_EVENTS_PATH, {})  # pylint: disable=protected-access
    rec = data.setdefault(user_id, {"events": []})
    events = [str(e) for e in rec.get("events") or [] if str(e) != event_id]
    rec["events"] = events
    storage._write_json(USER_EVENTS_PATH, data)  # pylint: disable=protected-access
    return events
