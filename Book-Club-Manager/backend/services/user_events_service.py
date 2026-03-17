"""User events service layer: per-user saved/registered events.

This module is intentionally focused only on *user-related* event data:

- Reading a user's saved events list.
- Adding/removing events from that list.
- UI helpers: check if an event is saved, get saved events with full details.

It delegates all persistence to the storage abstraction (`get_storage()`),
so it works in both local JSON mode and AWS/DynamoDB mode.
"""

from __future__ import annotations

from typing import Any

from backend.services import events_service
from backend.storage import get_storage


def get_user_events(user_id: str) -> dict[str, Any]:
    """Return the user's saved events record.

    Shape:
        { "events": [event_id1, event_id2, ...] }

    Args:
        user_id: User email/identifier.

    Returns:
        Dict with key \"events\" (list of event_id strings). If the user has no
        saved events yet, returns {\"events\": []}.
    """
    store = get_storage()
    user_id = str(user_id).strip().lower()
    rec = store.get_user_events(user_id) or {}
    events = [str(e) for e in (rec.get("events") or [])]
    return {"events": events}


def add_event_for_user(user_id: str, event_id: str) -> list[str]:
    """Add an event to the user's saved events list.

    Args:
        user_id: User email/identifier.
        event_id: Event identifier to add.

    Returns:
        Updated list of saved event_id strings for the user.

    Raises:
        ValueError: If user_id or event_id are empty.
    """
    store = get_storage()
    user_id = str(user_id).strip().lower()
    event_id = str(event_id).strip()
    if not user_id:
        raise ValueError("user_id is required.")
    if not event_id:
        raise ValueError("event_id is required.")

    current = store.get_user_events(user_id) or {}
    events: list[str] = [str(e) for e in (current.get("events") or [])]
    if event_id not in events:
        events.append(event_id)
    store.save_user_events(user_id, {"events": events})
    return events


def remove_event_for_user(user_id: str, event_id: str) -> list[str]:
    """Remove an event from the user's saved events list.

    Args:
        user_id: User email/identifier.
        event_id: Event identifier to remove.

    Returns:
        Updated list of saved event_id strings for the user.

    Raises:
        ValueError: If user_id or event_id are empty.
    """
    store = get_storage()
    user_id = str(user_id).strip().lower()
    event_id = str(event_id).strip()
    if not user_id:
        raise ValueError("user_id is required.")
    if not event_id:
        raise ValueError("event_id is required.")

    current = store.get_user_events(user_id) or {}
    events: list[str] = [
        str(e) for e in (current.get("events") or []) if str(e) != event_id
    ]
    store.save_user_events(user_id, {"events": events})
    return events


def is_event_saved(user_id: str, event_id: str) -> bool:
    """Return True if the user has saved the given event.

    Args:
        user_id: User email/identifier.
        event_id: Event identifier to check.

    Returns:
        True if event_id is in the user's saved list; False otherwise.
    """
    user_id = str(user_id).strip().lower()
    event_id = str(event_id).strip()
    if not user_id or not event_id:
        return False
    rec = get_user_events(user_id)
    return event_id in (rec.get("events") or [])


def get_saved_events_with_details(user_id: str) -> list[dict[str, Any]]:
    """Return the user's saved events as full event dicts for UI display.

    Fetches saved event IDs, then loads each event's full details. Events that
    no longer exist in the catalog are omitted from the result.

    Args:
        user_id: User email/identifier.

    Returns:
        List of event dicts (full schema), in the same order as saved.
    """
    user_id = str(user_id).strip().lower()
    rec = get_user_events(user_id)
    event_ids = rec.get("events") or []
    out: list[dict[str, Any]] = []
    for eid in event_ids:
        ev = events_service.get_event_detail(eid)
        if ev:
            out.append(ev)
    return out
