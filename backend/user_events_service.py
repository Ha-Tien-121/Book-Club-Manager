"""User events service layer: saved/registered events per user."""

from __future__ import annotations

from backend.config import USER_EVENTS_PATH
from backend import storage


def get_user_events(user_id: str) -> dict:
    """
    Return the user's saved events.
    Shape: { "events": [event_id1, event_id2, ...] }
    """
    user_id = str(user_id).strip().lower()
    return storage.get_user_events(user_id) or {"events": []}


def add_event_for_user(user_id: str, event_id: str) -> list[str]:
    """
    Add an event to the user's saved events list.
    Returns the updated list of event_ids.
    """
    user_id = str(user_id).strip().lower()
    event_id = str(event_id).strip()
    if not user_id:
        raise ValueError("user_id is required.")
    if not event_id:
        raise ValueError("event_id is required.")
    current = get_user_events(user_id)
    events = [str(e) for e in current.get("events") or []]
    if event_id not in events:
        events.append(event_id)
    storage._write_json(  # pylint: disable=protected-access
        USER_EVENTS_PATH,
        {**storage._read_json(USER_EVENTS_PATH, {}), user_id: {"events": events}},  # pylint: disable=protected-access
    )
    return events


def remove_event_for_user(user_id: str, event_id: str) -> list[str]:
    """
    Remove an event from the user's saved events list.
    Returns the updated list of event_ids.
    """
    user_id = str(user_id).strip().lower()
    event_id = str(event_id).strip()
    if not user_id:
        raise ValueError("user_id is required.")
    if not event_id:
        raise ValueError("event_id is required.")
    current = get_user_events(user_id)
    events = [str(e) for e in current.get("events") or [] if str(e) != event_id]
    storage._write_json(  # pylint: disable=protected-access
        USER_EVENTS_PATH,
        {**storage._read_json(USER_EVENTS_PATH, {}), user_id: {"events": events}},  # pylint: disable=protected-access
    )
    return events