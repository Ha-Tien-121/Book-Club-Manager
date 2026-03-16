"""
Tests for Book-Club-Manager.backend.services.recommender_service.

Covers:
- _build_user_recommender_inputs: normalization, library extraction, and genre prefs.
- get_book_recommendations: passes user book IDs to BookRecommender.
- get_event_recommendations: uses genre prefs and event pool, properly handles empty cases.
- _events_soonest_expiry: parses ttl/expiry with error handling.
- _user_has_genre_preferences: checks genre_preferences via storage.
- get_recommended_books_for_user: anonymous/default, no prefs, and recompute path.
- get_recommended_events_for_user: anonymous/default, expired vs cached recommendations.
- refresh_and_save_recommendations: writes both books and events plus timestamps.
- ensure_default_recommendations: idempotent seeding when no prefs or existing recs.
- on_book_added_to_shelf: increments counter and triggers recompute at threshold.
"""

import sys
from pathlib import Path
from typing import Any, Dict, List
from unittest.mock import MagicMock, patch


def _ensure_inner_project_on_path() -> None:
    """Ensure inner Book-Club-Manager root is on sys.path when running from outer root."""
    tests_dir = Path(__file__).resolve().parents[2]
    repo_root = tests_dir.parent
    inner_root = repo_root / "Book-Club-Manager"
    if inner_root.is_dir() and str(inner_root) not in sys.path:
        sys.path.insert(0, str(inner_root))


_ensure_inner_project_on_path()


import backend.services.recommender_service as rs  # noqa: E402


@patch("backend.services.recommender_service.get_storage")
def test_build_user_recommender_inputs_extracts_books_and_genres(mock_get_storage: MagicMock) -> None:
    store = MagicMock()
    store.get_user_books.return_value = {
        "library": {
            "in_progress": ["b1", 2],
            "saved": [],
            "finished": ["  b3  "],
        },
        "genre_preferences": ["Fantasy", "", "Sci-Fi", "Extra"],
    }
    mock_get_storage.return_value = store

    genres_store, books_store, has_library, has_prefs = rs._build_user_recommender_inputs("User@Email.COM")

    user_id = "user@email.com"
    assert has_library is True
    assert has_prefs is True
    assert books_store[user_id] == ["b1", "2", "b3"]
    rows = genres_store[user_id]
    assert [r["genre"] for r in rows] == ["Fantasy", "Sci-Fi", "Extra"]
    assert [r["rank"] for r in rows] == [1, 2, 3]


@patch("backend.services.recommender_service.BookRecommender")
@patch("backend.services.recommender_service.get_storage")
def test_get_book_recommendations_calls_recommender_with_user_books(
    mock_get_storage: MagicMock, mock_book_rec_cls: MagicMock
) -> None:
    store = MagicMock()
    store.get_user_books.return_value = {
        "library": {"in_progress": ["b1"], "saved": [], "finished": []},
        "genre_preferences": [],
    }
    mock_get_storage.return_value = store
    inst = MagicMock()
    mock_book_rec_cls.return_value = inst
    inst.recommend.return_value = [{"book_id": "r1"}]

    out = rs.get_book_recommendations("User@Email.com", top_k=5)

    user_id = "user@email.com"
    inst.recommend.assert_called_once_with(["b1"], top_k=5)
    assert out == [{"book_id": "r1"}]


@patch("backend.services.recommender_service.EventRecommender")
@patch("backend.services.recommender_service.get_storage")
def test_get_event_recommendations_uses_genre_prefs_and_event_pool(
    mock_get_storage: MagicMock, mock_event_rec_cls: MagicMock
) -> None:
    store = MagicMock()
    store.get_user_books.return_value = {
        "library": {},
        "genre_preferences": ["Fantasy"],
    }
    store.get_soonest_events.return_value = [
        {"event_id": "e1", "tags": ["Fantasy"]},
        {"event_id": "e2", "tags": ["Other"]},
    ]
    mock_get_storage.return_value = store
    inst = MagicMock()
    mock_event_rec_cls.return_value = inst
    inst.recommend.return_value = [{"event_id": "e1"}]

    out = rs.get_event_recommendations("User@Email.com", top_k=3)

    inst.recommend.assert_called_once()
    args, kwargs = inst.recommend.call_args
    assert kwargs["top_k"] == 3
    assert out == [{"event_id": "e1"}]


def test_get_event_recommendations_early_return_cases() -> None:
    """Cover early returns for empty user_id, non-positive top_k, no prefs, no tags, and no events."""
    # Empty user_id
    assert rs.get_event_recommendations("", top_k=5) == []
    assert rs.get_event_recommendations(None, top_k=5) == []  # type: ignore[arg-type]
    # Non-positive top_k
    assert rs.get_event_recommendations("u@x.com", top_k=0) == []
    # No genre prefs
    with patch("backend.services.recommender_service._build_user_recommender_inputs") as m_build:
        m_build.return_value = ({}, {}, False, False)
        assert rs.get_event_recommendations("u@x.com", top_k=5) == []
    # Has prefs but no tags
    with patch("backend.services.recommender_service._build_user_recommender_inputs") as m_build:
        m_build.return_value = ({"u@x.com": [{"genre": ""}]}, {}, False, True)
        assert rs.get_event_recommendations("u@x.com", top_k=5) == []
    # Has tags but storage returns no events
    with patch("backend.services.recommender_service._build_user_recommender_inputs") as m_build, patch(
        "backend.services.recommender_service.get_storage"
    ) as m_get_storage:
        m_build.return_value = ({"u@x.com": [{"genre": "Fantasy"}]}, {}, False, True)
        store = MagicMock()
        store.get_soonest_events.return_value = []
        m_get_storage.return_value = store
        assert rs.get_event_recommendations("u@x.com", top_k=5) == []


def test_events_soonest_expiry_parses_ttl_and_expiry() -> None:
    events = [
        {"ttl": "200"},
        {"expiry": 150},
        {"ttl": "not-int"},
        {},
    ]
    assert rs._events_soonest_expiry(events) == 150
    assert rs._events_soonest_expiry([]) == 0


@patch("backend.services.recommender_service.get_storage")
def test_user_has_genre_preferences_reads_from_storage(mock_get_storage: MagicMock) -> None:
    store = MagicMock()
    store.get_user_books.return_value = {"genre_preferences": ["Fantasy"]}
    mock_get_storage.return_value = store

    assert rs._user_has_genre_preferences("u@x.com") is True
    assert rs._user_has_genre_preferences("") is False


@patch("backend.services.recommender_service.get_storage")
def test_get_recommended_books_for_user_anonymous_and_no_prefs(mock_get_storage: MagicMock) -> None:
    store = MagicMock()
    store.get_top50_review_books.return_value = [{"id": i} for i in range(60)]
    # User with no genre prefs
    store.get_user_books.return_value = {"genre_preferences": []}
    store.get_user_recommendations.return_value = {"recommended_books": []}
    mock_get_storage.return_value = store

    anon = rs.get_recommended_books_for_user(None)
    assert len(anon) == rs.RECOMMENDED_BOOKS_SIZE

    # Signed in but no prefs -> same fallback
    user_books = rs.get_recommended_books_for_user("user@example.com")
    assert len(user_books) == rs.RECOMMENDED_BOOKS_SIZE
    store.get_top50_review_books.assert_called()


@patch("backend.services.recommender_service.get_book_recommendations")
@patch("backend.services.recommender_service.get_storage")
def test_get_recommended_books_for_user_recomputes_when_missing(
    mock_get_storage: MagicMock, mock_get_book_recs: MagicMock
) -> None:
    store = MagicMock()
    store.get_top50_review_books.return_value = [{"id": i} for i in range(60)]
    store.get_user_books.return_value = {"genre_preferences": ["Fantasy"]}
    store.get_user_recommendations.return_value = {"recommended_books": []}
    mock_get_storage.return_value = store
    mock_get_book_recs.return_value = [{"id": 1}, {"id": 2}]

    out = rs.get_recommended_books_for_user("user@example.com")

    assert out == [{"id": 1}, {"id": 2}]
    store.save_user_recommendations.assert_called_once()


@patch("backend.services.recommender_service.time.time", return_value=1_700_000_000)
@patch("backend.services.recommender_service.get_event_recommendations")
@patch("backend.services.recommender_service.get_storage")
def test_get_recommended_events_for_user_refreshes_on_expiry(
    mock_get_storage: MagicMock, mock_get_event_recs: MagicMock, mock_time: MagicMock  # noqa: ARG001
) -> None:
    store = MagicMock()
    store.get_user_books.return_value = {"genre_preferences": ["Fantasy"]}
    # Existing rec with expired events_soonest_expiry
    store.get_user_recommendations.return_value = {"events_soonest_expiry": 0}
    mock_get_storage.return_value = store
    mock_get_event_recs.return_value = [{"event_id": "e1"}, {"event_id": "e2"}]

    out = rs.get_recommended_events_for_user("user@example.com")

    assert out == [{"event_id": "e1"}, {"event_id": "e2"}]
    store.save_user_recommendations.assert_called_once()


@patch("backend.services.recommender_service.get_storage")
def test_get_recommended_events_for_user_anonymous_and_no_prefs(mock_get_storage: MagicMock) -> None:
    """Cover anonymous and no-genre-preferences branches."""
    store = MagicMock()
    store.get_soonest_events.return_value = [{"event_id": i} for i in range(20)]
    store.get_user_books.return_value = {"genre_preferences": []}
    mock_get_storage.return_value = store

    # Anonymous
    anon = rs.get_recommended_events_for_user(None)
    assert len(anon) == rs.RECOMMENDED_EVENTS_SIZE

    # Signed in but no prefs
    user_events = rs.get_recommended_events_for_user("user@example.com")
    assert len(user_events) == rs.RECOMMENDED_EVENTS_SIZE


@patch("backend.services.recommender_service.time.time", return_value=1_700_000_000)
@patch("backend.services.recommender_service.get_book_recommendations")
@patch("backend.services.recommender_service.get_event_recommendations")
@patch("backend.services.recommender_service.get_storage")
def test_refresh_and_save_recommendations_writes_books_and_events(
    mock_get_storage: MagicMock,
    mock_get_events: MagicMock,
    mock_get_books: MagicMock,
    mock_time: MagicMock,  # noqa: ARG001
) -> None:
    store = MagicMock()
    store.get_user_recommendations.return_value = {}
    mock_get_storage.return_value = store
    mock_get_books.return_value = [{"id": 1}, {"id": 2}]
    mock_get_events.return_value = [{"event_id": "e1"}]

    rec = rs.refresh_and_save_recommendations("user@example.com")

    assert rec["recommended_books"] == [{"id": 1}, {"id": 2}]
    assert rec["recommended_events"] == [{"event_id": "e1"}]
    assert rec["book_updated_at"] == 1_700_000_000
    store.save_user_recommendations.assert_called_once()


@patch("backend.services.recommender_service.get_storage")
def test_ensure_default_recommendations_seeds_when_no_prefs_or_existing(mock_get_storage: MagicMock) -> None:
    store = MagicMock()
    # No genre prefs
    store.get_user_books.return_value = {"genre_preferences": []}
    # No existing recs
    store.get_user_recommendations.return_value = {}
    store.get_top50_review_books.return_value = [{"id": 1}]
    store.get_soonest_events.return_value = [{"event_id": "e1"}]
    mock_get_storage.return_value = store

    rs.ensure_default_recommendations("user@example.com")

    store.save_user_recommendations.assert_called_once()
    args, kwargs = store.save_user_recommendations.call_args
    user_id, rec = args
    assert user_id == "user@example.com"
    assert rec["recommended_books"] == [{"id": 1}]
    assert rec["recommended_events"] == [{"event_id": "e1"}]


@patch("backend.services.recommender_service.get_storage")
def test_on_book_added_to_shelf_increments_and_triggers_recompute(mock_get_storage: MagicMock) -> None:
    store = MagicMock()
    # First call: below threshold
    store.get_user_recommendations.return_value = {"adds_since_last_book_run": rs.ADDS_BEFORE_BOOK_RERUN - 1}
    mock_get_storage.return_value = store

    with patch("backend.services.recommender_service.get_book_recommendations") as mock_get_books:
        mock_get_books.return_value = [{"id": 1}]
        rs.on_book_added_to_shelf("user@example.com")

    # After threshold, it should have recomputed and reset counter; save_user_recommendations called.
    store.save_user_recommendations.assert_called()

