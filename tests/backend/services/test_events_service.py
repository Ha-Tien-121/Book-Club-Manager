"""
Tests for backend.services.events_service.

Covers:
- get_event_detail: success, not found (None), normalizes event_id
- get_events_by_city: success returns list, empty list
- get_explore_events: with limit, without limit (uses EVENT_RECOMMENDATION_POOL_SIZE)
"""

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

# events_service does "from backend import user_events_service"; provide a mock.
if "backend.user_events_service" not in sys.modules:
    sys.modules["backend.user_events_service"] = types.ModuleType("backend.user_events_service")

# events_service imports EVENT_RECOMMENDATION_POOL_SIZE from backend.config; ensure it exists.
import backend.config as _backend_config  # noqa: E402
if not hasattr(_backend_config, "EVENT_RECOMMENDATION_POOL_SIZE"):
    _backend_config.EVENT_RECOMMENDATION_POOL_SIZE = 200

from backend.services import events_service  # noqa: E402


@patch("backend.services.events_service.get_storage")
class TestGetEventDetail(unittest.TestCase):
    """Tests for get_event_detail."""

    def test_returns_event_when_found(self, mock_get_storage: MagicMock) -> None:
        store = MagicMock()
        store.get_event_details.return_value = {
            "event_id": "ev1",
            "title": "Book Club",
            "city_state": "Seattle, WA",
        }
        mock_get_storage.return_value = store

        result = events_service.get_event_detail("ev1")
        self.assertEqual(result["event_id"], "ev1")
        self.assertEqual(result["title"], "Book Club")
        store.get_event_details.assert_called_once_with("ev1")

    def test_returns_empty_dict_when_not_found(self, mock_get_storage: MagicMock) -> None:
        store = MagicMock()
        store.get_event_details.return_value = None
        mock_get_storage.return_value = store

        result = events_service.get_event_detail("missing")
        self.assertEqual(result, {})

    def test_strips_event_id(self, mock_get_storage: MagicMock) -> None:
        store = MagicMock()
        store.get_event_details.return_value = {"event_id": "ev2"}
        mock_get_storage.return_value = store

        events_service.get_event_detail("  ev2  ")
        store.get_event_details.assert_called_once_with("ev2")


@patch("backend.services.events_service.get_storage")
class TestGetEventsByCity(unittest.TestCase):
    """Tests for get_events_by_city."""

    def test_returns_events_list(self, mock_get_storage: MagicMock) -> None:
        store = MagicMock()
        store.get_events_by_city.return_value = [
            {"event_id": "e1", "city_state": "Seattle, WA"},
            {"event_id": "e2", "city_state": "Seattle, WA"},
        ]
        mock_get_storage.return_value = store

        result = events_service.get_events_by_city("Seattle, WA")
        self.assertEqual(len(result), 2)
        self.assertEqual(result[0]["event_id"], "e1")
        store.get_events_by_city.assert_called_once_with("Seattle, WA")

    def test_returns_empty_list(self, mock_get_storage: MagicMock) -> None:
        store = MagicMock()
        store.get_events_by_city.return_value = []
        mock_get_storage.return_value = store

        result = events_service.get_events_by_city("Nowhere, XX")
        self.assertEqual(result, [])


@patch("backend.services.events_service.get_storage")
class TestGetExploreEvents(unittest.TestCase):
    """Tests for get_explore_events."""

    def test_returns_events_up_to_limit(self, mock_get_storage: MagicMock) -> None:
        store = MagicMock()
        store.get_soonest_events.return_value = [
            {"event_id": "a1"},
            {"event_id": "a2"},
        ]
        mock_get_storage.return_value = store

        result = events_service.get_explore_events(limit=10)
        self.assertEqual(len(result), 2)
        store.get_soonest_events.assert_called_once_with(10)

    def test_uses_pool_size_when_limit_none(self, mock_get_storage: MagicMock) -> None:
        store = MagicMock()
        store.get_soonest_events.return_value = []
        mock_get_storage.return_value = store

        events_service.get_explore_events(limit=None)
        from backend.config import EVENT_RECOMMENDATION_POOL_SIZE
        store.get_soonest_events.assert_called_once_with(EVENT_RECOMMENDATION_POOL_SIZE)

    def test_default_no_args_uses_pool_size(self, mock_get_storage: MagicMock) -> None:
        store = MagicMock()
        store.get_soonest_events.return_value = []
        mock_get_storage.return_value = store

        events_service.get_explore_events()
        from backend.config import EVENT_RECOMMENDATION_POOL_SIZE
        store.get_soonest_events.assert_called_once_with(EVENT_RECOMMENDATION_POOL_SIZE)


if __name__ == "__main__":
    unittest.main()
