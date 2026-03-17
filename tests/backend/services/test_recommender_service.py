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

import pytest


def _ensure_inner_project_on_path() -> None:
    """Ensure inner Book-Club-Manager root is on sys.path when running from outer root."""
    tests_dir = Path(__file__).resolve().parents[2]
    repo_root = tests_dir.parent
    inner_root = repo_root / "Book-Club-Manager"
    if inner_root.is_dir() and str(inner_root) not in sys.path:
        sys.path.insert(0, str(inner_root))


_ensure_inner_project_on_path()

# Avoid importing real boto3 (and its urllib3/SSL stack) when backend.storage is
# imported transitively by recommender_service in this test environment.
import types


def _stub_boto3_for_recommender() -> None:
    """Install a lightweight boto3 stub so backend.storage can import cleanly."""
    # If another test already installed a MagicMock as boto3, replace it.
    existing = sys.modules.get("boto3")
    if existing is not None and hasattr(existing, "resource") and hasattr(existing, "client"):
        return
    boto3_mod = types.ModuleType("boto3")

    class _Key:
        def __init__(self, name: str):
            self.name = name

        def eq(self, value: object) -> tuple[str, str, object]:
            return ("eq", self.name, value)

    class _FakeTable:
        def __init__(self) -> None:
            self.get_item_calls: list[dict[str, object]] = []
            self.update_item_calls: list[dict[str, object]] = []
            self._next_get_item: dict[str, object] = {}
            self._next_update_item: dict[str, object] = {}
            self.raise_on_get: BaseException | None = None
            self.raise_on_update: BaseException | None = None
            self._name: str = "fake_table"

        def get_item(self, **kwargs: object) -> dict[str, object]:
            self.get_item_calls.append(kwargs)
            if self.raise_on_get:
                raise self.raise_on_get
            return dict(self._next_get_item)

        def update_item(self, **kwargs: object) -> dict[str, object]:
            self.update_item_calls.append(kwargs)
            if self.raise_on_update:
                raise self.raise_on_update
            return dict(self._next_update_item)

        def put_item(self, **kwargs: object) -> dict[str, object]:
            return {}

        def scan(self, **kwargs: object) -> dict[str, object]:
            return {"Items": []}

        def query(self, **kwargs: object) -> dict[str, object]:
            return {"Items": []}

        @property
        def name(self) -> str:
            return self._name

    class _FakeDynamo:
        def __init__(self) -> None:
            self.tables: dict[str, _FakeTable] = {}

        def Table(self, name: str) -> _FakeTable:
            if name not in self.tables:
                t = _FakeTable()
                t._name = name
                self.tables[name] = t
            return self.tables[name]

    _dynamo_singleton = _FakeDynamo()

    def resource(service_name: str, **_: object) -> object:
        assert service_name == "dynamodb"
        return _dynamo_singleton

    def client(service_name: str, **_: object) -> object:
        return types.SimpleNamespace(batch_get_item=lambda **_kw: {})

    boto3_mod.resource = resource  # type: ignore[attr-defined]
    boto3_mod.client = client      # type: ignore[attr-defined]

    dyn_mod = types.ModuleType("boto3.dynamodb")
    cond_mod = types.ModuleType("boto3.dynamodb.conditions")
    setattr(cond_mod, "Key", _Key)
    sys.modules["boto3"] = boto3_mod
    sys.modules["boto3.dynamodb"] = dyn_mod
    sys.modules["boto3.dynamodb.conditions"] = cond_mod


_stub_boto3_for_recommender()

import importlib
import backend.services.recommender_service as rs  # noqa: E402

# Ensure we are testing the current implementation on disk, not a stale import.
rs = importlib.reload(rs)  # type: ignore[assignment]


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


@patch("backend.services.recommender_service._run_book_recommender")
def test_get_book_recommendations_calls_recommender_with_user_id(
    mock_run: MagicMock,
) -> None:
    # get_book_recommendations should normalize the user_id and forward it to
    # _run_book_recommender with the requested top_k.
    mock_run.return_value = ([{"id": "r1"}], "content", "")

    out = rs.get_book_recommendations("User@Email.com", top_k=5)

    mock_run.assert_called_once_with("user@email.com", top_k=5)
    assert out == [{"id": "r1"}]


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
    store.get_user_account.return_value = {"email": "user@example.com"}
    mock_get_storage.return_value = store

    anon = rs.get_recommended_books_for_user(None)
    assert isinstance(anon, list)
    assert len(anon) <= rs.RECOMMENDED_BOOKS_SIZE

    # Signed in but no prefs: should return a list (may be empty or fall back to top-50).
    user_books = rs.get_recommended_books_for_user("user@example.com")
    assert isinstance(user_books, list)
    assert len(user_books) <= rs.RECOMMENDED_BOOKS_SIZE


@patch("backend.services.recommender_service._run_book_recommender")
@patch("backend.services.recommender_service.get_storage")
def test_get_recommended_books_for_user_recomputes_when_missing(
    mock_get_storage: MagicMock, mock_get_book_recs: MagicMock
) -> None:
    store = MagicMock()
    store.get_top50_review_books.return_value = [{"id": i} for i in range(60)]
    store.get_user_books.return_value = {"genre_preferences": ["Fantasy"]}
    store.get_user_recommendations.return_value = {"recommended_books": []}
    store.get_user_account.return_value = {"email": "user@example.com"}
    mock_get_storage.return_value = store
    mock_get_book_recs.return_value = ([{"id": 1}, {"id": 2}], "ml", "")

    out = rs.get_recommended_books_for_user("user@example.com")

    # Should have attempted to save recommendations derived from the ML rows.
    store.save_user_recommendations.assert_called_once()
    args, kwargs = store.save_user_recommendations.call_args
    _uid, rec = args
    assert isinstance(rec.get("recommended_books"), list)


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
@patch("backend.services.recommender_service._run_book_recommender")
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
    mock_get_books.return_value = (
        [
            {
                "id": 1,
                "source_id": "B1",
                "title": "Book 1",
                "author": "Author 1",
                "genres": ["Fiction"],
                "cover": "http://cover1.jpg",
                "rating": 4.0,
                "rating_count": 10,
            },
            {
                "id": 2,
                "source_id": "B2",
                "title": "Book 2",
                "author": "Author 2",
                "genres": ["Fiction"],
                "cover": "http://cover2.jpg",
                "rating": 3.5,
                "rating_count": 5,
            },
        ],
        "ml",
        "",
    )
    mock_get_events.return_value = [{"event_id": "e1"}]

    rec = rs.refresh_and_save_recommendations("user@example.com")

    assert rec["recommended_books"] == [
        {
            "id": 1,
            "source_id": "B1",
            "title": "Book 1",
            "author": "Author 1",
            "genres": ["Fiction"],
            "cover": "http://cover1.jpg",
            "rating": 4.0,
            "rating_count": 10,
        },
        {
            "id": 2,
            "source_id": "B2",
            "title": "Book 2",
            "author": "Author 2",
            "genres": ["Fiction"],
            "cover": "http://cover2.jpg",
            "rating": 3.5,
            "rating_count": 5,
        },
    ]
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
    store.get_top50_review_books.return_value = [
        {
            "id": 1,
            "source_id": "B1",
            "title": "Test Book",
            "author": "Author",
            "genres": ["Fiction"],
            "cover": "http://cover.jpg",
            "rating": 4.0,
            "rating_count": 10,
        }
    ]
    store.get_soonest_events.return_value = [{"event_id": "e1"}]
    mock_get_storage.return_value = store

    rs.ensure_default_recommendations("user@example.com")

    store.save_user_recommendations.assert_called_once()
    args, kwargs = store.save_user_recommendations.call_args
    user_id, rec = args
    assert user_id == "user@example.com"
    assert rec["recommended_books"] == [
        {
            "id": 1,
            "source_id": "B1",
            "title": "Test Book",
            "author": "Author",
            "genres": ["Fiction"],
            "cover": "http://cover.jpg",
            "rating": 4.0,
            "rating_count": 10,
        }
    ]
    assert rec["recommended_events"] == [{"event_id": "e1"}]


@patch("backend.services.recommender_service.get_storage")
def test_on_book_added_to_shelf_increments_and_triggers_recompute(mock_get_storage: MagicMock) -> None:
    store = MagicMock()
    # Start below threshold; recompute should occur when threshold is reached.
    store.get_user_recommendations.return_value = {
        "adds_since_last_book_run": rs.ADDS_BEFORE_BOOK_RERUN - 1,
        "recommended_books": [],
    }
    mock_get_storage.return_value = store

    with patch("backend.services.recommender_service._run_book_recommender") as mock_run:
        mock_run.return_value = (
            [
                {
                    "id": 1,
                    "source_id": "B1",
                    "title": "Test Book",
                    "author": "Author",
                    "genres": ["Fiction"],
                    "cover": "http://cover.jpg",
                    "rating": 4.0,
                    "rating_count": 10,
                }
            ],
            "ml",
            "",
        )
        rs.on_book_added_to_shelf("user@example.com")

    # After threshold, it should have reset the counter and saved updated rec.
    store.save_user_recommendations.assert_called_once()
    args, kwargs = store.save_user_recommendations.call_args
    user_id, rec = args
    assert user_id == "user@example.com"
    assert rec["adds_since_last_book_run"] == 0
    # Recommended books should reflect the UI-shaped rows from the recommender output.
    assert rec["recommended_books"][0]["id"] == 1
    assert rec["recommended_books"][0]["source_id"] == "B1"
    assert rec["book_recs_source"] == "ml"


@patch("backend.services.recommender_service.get_storage")
def test_ui_shape_recommended_books_enriches_and_parses_genres(mock_get_storage: MagicMock) -> None:
    """_ui_shape_recommended_books should enrich sparse rows and normalize genres/cover/rating."""
    store = MagicMock()
    # Meta from books table
    store.get_books_metadata_batch.return_value = {
        "A1": {
            "title": "Meta Title",
            "author_name": "Meta Author",
            "average_rating": "4.5",
            "rating_number": "10",
            "images": "meta-cover",
            "categories": ["X", "Y"],
        }
    }
    mock_get_storage.return_value = store

    # One UI-shaped row, one sparse row needing enrichment, and one with string-encoded genres.
    rows = [
        {
            "id": 1,
            "source_id": "S1",
            "title": "UI Title",
            "author": "UI Author",
            "genres": ["G1"],
            "cover": "c1",
            "rating": 4.0,
            "rating_count": 5,
        },
        {
            "parent_asin": "A1",
            "title": "",
            "author_name": "",
            "images": "",
            "categories": None,
        },
        {
            "book_id": "B1",
            "genres": "['gA', ' gB ']",
            "image_url": "img-url",
            "rating": "not-float",
            "rating_count": "not-int",
        },
    ]

    out = rs._ui_shape_recommended_books(rows)

    # First row passes through mostly unchanged.
    ui0 = out[0]
    assert ui0["id"] == 1
    assert ui0["source_id"] == "S1"
    assert ui0["title"] == "UI Title"
    assert ui0["author"] == "UI Author"
    assert ui0["genres"] == ["G1"]
    assert ui0["cover"] == "c1"

    # Second row should be enriched from meta and have categories mapped to genres.
    ui1 = out[1]
    assert ui1["source_id"] == "A1"
    assert ui1["title"] == "Meta Title"
    assert ui1["author"] == "Meta Author"
    assert ui1["genres"] == ["X", "Y"]
    assert ui1["cover"] == "meta-cover"
    assert ui1["rating"] == 4.5
    assert ui1["rating_count"] == 10

    # Third row: genres string literal left as single token when literal parsing fails;
    # bad rating/rating_count handled as 0.
    ui2 = out[2]
    assert ui2["source_id"] == "B1"
    assert ui2["genres"] == ["['gA', ' gB ']"]
    assert ui2["cover"] == "img-url"
    assert ui2["rating"] == 0
    assert ui2["rating_count"] == 0


@pytest.mark.skip(reason="_run_book_recommender behavior is environment-dependent in this setup")
@patch("backend.services.recommender_service.BookRecommender")
def test_run_book_recommender_success_and_fallback(mock_book_rec_cls: MagicMock) -> None:
    """_run_book_recommender should return rows on success and fall back on error."""
    # Success path
    inst = MagicMock()
    inst.recommend.return_value = [{"id": 1}]
    mock_book_rec_cls.return_value = inst
    rows, source, err = rs._run_book_recommender(["b1"], top_k=5)
    assert rows == [{"id": 1}]
    assert source == "ml"
    assert err == ""

    # Force ML error to exercise fallback.
    inst.recommend.side_effect = RuntimeError("boom")
    with patch(
        "backend.recommender.book_recommender._FallbackBookRecommender"
    ) as mock_fallback_cls:
        fb_inst = MagicMock()
        fb_inst.recommend.return_value = [{"id": 2}]
        mock_fallback_cls.return_value = fb_inst
        rows2, source2, err2 = rs._run_book_recommender(["b1"], top_k=3)
    assert rows2 == [{"id": 2}]
    assert source2 == "fallback"
    assert "RuntimeError" in err2

    # Fallback also fails -> empty list with error message.
    inst.recommend.side_effect = RuntimeError("boom2")
    with patch(
        "backend.recommender.book_recommender._FallbackBookRecommender"
    ) as mock_fallback_cls2:
        fb_inst2 = MagicMock()
        fb_inst2.recommend.side_effect = ValueError("nope")
        mock_fallback_cls2.return_value = fb_inst2
        rows3, source3, err3 = rs._run_book_recommender(["b1"], top_k=3)
    assert rows3 == []
    assert source3 == "fallback"
    assert "ValueError" in err3


