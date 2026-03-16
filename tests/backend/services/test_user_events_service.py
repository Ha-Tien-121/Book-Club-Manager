"""
Tests for backend.services.user_events_service.

Covers get_user_events, add_event_for_user, remove_event_for_user,
is_event_saved, get_saved_events_with_details.
"""

from __future__ import annotations

import sys
import types
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

# Ensure inner Book-Club-Manager backend package is importable when running from outer repo root.
_TESTS_DIR = Path(__file__).resolve().parents[2]
_REPO_ROOT = _TESTS_DIR.parent
_INNER_ROOT = _REPO_ROOT / "Book-Club-Manager"
if _INNER_ROOT.is_dir() and str(_INNER_ROOT) not in sys.path:
    sys.path.insert(0, str(_INNER_ROOT))

# Avoid loading real boto3 when backend.storage is imported.
if "boto3" not in sys.modules:
    _boto3 = MagicMock()
    _conditions = types.ModuleType("boto3.dynamodb.conditions")
    _conditions.Attr = MagicMock()
    _conditions.Key = MagicMock()
    _boto3.dynamodb.conditions = _conditions
    sys.modules["boto3"] = _boto3
    sys.modules["boto3.dynamodb"] = MagicMock()
    sys.modules["boto3.dynamodb.conditions"] = _conditions

# get_saved_events_with_details imports events_service; avoid loading real one (needs EVENT_RECOMMENDATION_POOL_SIZE).
if "backend.services.events_service" not in sys.modules:
    _ev_mod = types.ModuleType("backend.services.events_service")
    _ev_mod.get_event_detail = MagicMock(return_value={})
    sys.modules["backend.services.events_service"] = _ev_mod

from backend.services import user_events_service  # noqa: E402


@patch("backend.services.user_events_service.get_storage")
class TestGetUserEvents(unittest.TestCase):
    """Tests for get_user_events."""

    def test_returns_events_dict(self, mock_get_storage: MagicMock) -> None:
        store = MagicMock()
        store.get_user_events.return_value = {"events": ["ev1", "ev2"]}
        mock_get_storage.return_value = store

        result = user_events_service.get_user_events("u@x.com")

        self.assertEqual(result["events"], ["ev1", "ev2"])
        store.get_user_events.assert_called_once_with("u@x.com")

    def test_returns_empty_events_when_none(self, mock_get_storage: MagicMock) -> None:
        store = MagicMock()
        store.get_user_events.return_value = None
        mock_get_storage.return_value = store

        result = user_events_service.get_user_events("u@x.com")

        self.assertEqual(result["events"], [])

    def test_normalizes_user_id(self, mock_get_storage: MagicMock) -> None:
        store = MagicMock()
        store.get_user_events.return_value = {"events": []}
        mock_get_storage.return_value = store

        user_events_service.get_user_events("  Alice@Example.COM  ")
        store.get_user_events.assert_called_once_with("alice@example.com")


@patch("backend.services.user_events_service.get_storage")
class TestAddEventForUser(unittest.TestCase):
    """Tests for add_event_for_user."""

    def test_appends_event_and_saves(self, mock_get_storage: MagicMock) -> None:
        store = MagicMock()
        store.get_user_events.return_value = {"events": ["ev1"]}
        mock_get_storage.return_value = store

        result = user_events_service.add_event_for_user("u@x.com", "ev2")

        self.assertEqual(result, ["ev1", "ev2"])
        store.save_user_events.assert_called_once_with("u@x.com", {"events": ["ev1", "ev2"]})

    def test_does_not_duplicate_if_already_saved(self, mock_get_storage: MagicMock) -> None:
        store = MagicMock()
        store.get_user_events.return_value = {"events": ["ev1"]}
        mock_get_storage.return_value = store

        result = user_events_service.add_event_for_user("u@x.com", "ev1")

        self.assertEqual(result, ["ev1"])
        store.save_user_events.assert_called_once_with("u@x.com", {"events": ["ev1"]})

    def test_empty_user_id_raises(self, mock_get_storage: MagicMock) -> None:
        mock_get_storage.return_value = MagicMock()
        with self.assertRaises(ValueError) as ctx:
            user_events_service.add_event_for_user("", "ev1")
        self.assertIn("user_id is required", str(ctx.exception))

    def test_empty_event_id_raises(self, mock_get_storage: MagicMock) -> None:
        mock_get_storage.return_value = MagicMock()
        with self.assertRaises(ValueError) as ctx:
            user_events_service.add_event_for_user("u@x.com", "   ")
        self.assertIn("event_id is required", str(ctx.exception))


@patch("backend.services.user_events_service.get_storage")
class TestRemoveEventForUser(unittest.TestCase):
    """Tests for remove_event_for_user."""

    def test_removes_event_and_saves(self, mock_get_storage: MagicMock) -> None:
        store = MagicMock()
        store.get_user_events.return_value = {"events": ["ev1", "ev2"]}
        mock_get_storage.return_value = store

        result = user_events_service.remove_event_for_user("u@x.com", "ev1")

        self.assertEqual(result, ["ev2"])
        store.save_user_events.assert_called_once_with("u@x.com", {"events": ["ev2"]})

    def test_empty_user_id_raises(self, mock_get_storage: MagicMock) -> None:
        mock_get_storage.return_value = MagicMock()
        with self.assertRaises(ValueError) as ctx:
            user_events_service.remove_event_for_user("", "ev1")
        self.assertIn("user_id is required", str(ctx.exception))

    def test_empty_event_id_raises(self, mock_get_storage: MagicMock) -> None:
        mock_get_storage.return_value = MagicMock()
        with self.assertRaises(ValueError) as ctx:
            user_events_service.remove_event_for_user("u@x.com", "   ")
        self.assertIn("event_id is required", str(ctx.exception))


@patch("backend.services.user_events_service.get_storage")
class TestIsEventSaved(unittest.TestCase):
    """Tests for is_event_saved."""

    def test_returns_true_when_saved(self, mock_get_storage: MagicMock) -> None:
        store = MagicMock()
        store.get_user_events.return_value = {"events": ["ev1", "ev2"]}
        mock_get_storage.return_value = store

        self.assertTrue(user_events_service.is_event_saved("u@x.com", "ev1"))

    def test_returns_false_when_not_saved(self, mock_get_storage: MagicMock) -> None:
        store = MagicMock()
        store.get_user_events.return_value = {"events": ["ev1"]}
        mock_get_storage.return_value = store

        self.assertFalse(user_events_service.is_event_saved("u@x.com", "ev99"))

    def test_returns_false_for_empty_user_id(self, mock_get_storage: MagicMock) -> None:
        mock_get_storage.return_value = MagicMock()
        self.assertFalse(user_events_service.is_event_saved("", "ev1"))

    def test_returns_false_for_empty_event_id(self, mock_get_storage: MagicMock) -> None:
        mock_get_storage.return_value = MagicMock()
        self.assertFalse(user_events_service.is_event_saved("u@x.com", "   "))


@patch("backend.services.user_events_service.get_storage")
class TestGetSavedEventsWithDetails(unittest.TestCase):
    """Tests for get_saved_events_with_details (imports get_event_detail from events_service)."""

    def test_returns_event_details_in_order(self, mock_get_storage: MagicMock) -> None:
        store = MagicMock()
        store.get_user_events.return_value = {"events": ["ev1", "ev2"]}
        mock_get_storage.return_value = store
        ev_mod = sys.modules["backend.services.events_service"]
        ev_mod.get_event_detail.side_effect = lambda eid: (
            {"event_id": eid, "title": f"Event {eid}"}
        )
        result = user_events_service.get_saved_events_with_details("u@x.com")

        self.assertEqual(len(result), 2)
        self.assertEqual(result[0]["event_id"], "ev1")
        self.assertEqual(result[1]["event_id"], "ev2")

    def test_omits_events_with_no_details(self, mock_get_storage: MagicMock) -> None:
        store = MagicMock()
        store.get_user_events.return_value = {"events": ["ev1", "ev2"]}
        mock_get_storage.return_value = store
        ev_mod = sys.modules["backend.services.events_service"]
        ev_mod.get_event_detail.side_effect = lambda eid: (
            {"event_id": eid} if eid == "ev1" else {}
        )
        result = user_events_service.get_saved_events_with_details("u@x.com")

        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]["event_id"], "ev1")

    def test_returns_empty_list_when_no_saved_events(self, mock_get_storage: MagicMock) -> None:
        store = MagicMock()
        store.get_user_events.return_value = {"events": []}
        mock_get_storage.return_value = store

        result = user_events_service.get_saved_events_with_details("u@x.com")

        self.assertEqual(result, [])

if __name__ == "__main__":
    unittest.main()