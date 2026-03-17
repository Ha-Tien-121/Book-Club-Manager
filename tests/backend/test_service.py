"""
Tests for Book-Club-Manager.backend.service.

Covers:
- get_recommender: returns a BookRecommender instance.
- build_user_genres_df: empty and populated cases.
- build_user_books_df: empty and populated cases.
- get_recommendations: passes book IDs and top_k to BookRecommender.
- get_top_popular_books: handles non-positive top_k and normal case.
- mark_book_as_read: updates store without duplicates and delegates to get_recommendations.
- get_book_details: normalizes fields, list coercion, and missing/optional values.
"""

import sys
from pathlib import Path
from typing import Any, Dict, List
from unittest.mock import MagicMock, patch

import pandas as pd


def _ensure_inner_project_on_path() -> None:
    """Ensure inner Book-Club-Manager root is on sys.path when running from outer root."""
    tests_dir = Path(__file__).resolve().parents[1]
    repo_root = tests_dir.parent
    inner_root = repo_root / "Book-Club-Manager"
    if inner_root.is_dir() and str(inner_root) not in sys.path:
        sys.path.insert(0, str(inner_root))


_ensure_inner_project_on_path()


import backend.service as service  # noqa: E402  (import after sys.path tweak)


def test_get_recommender_returns_book_recommender_instance() -> None:
    "Test get recommender returns book recommender instance."
    with patch("backend.service.BookRecommender") as mock_cls:
        inst = MagicMock()
        mock_cls.return_value = inst

        result = service.get_recommender()

        mock_cls.assert_called_once_with()
        assert result is inst


def test_build_user_genres_df_empty_returns_expected_columns() -> None:
    "Test build user genres df empty returns expected columns."
    store: Dict[str, List[Dict[str, Any]]] = {}

    df = service.build_user_genres_df("u1", store)

    assert isinstance(df, pd.DataFrame)
    assert list(df.columns) == ["user_id", "genre", "rank"]
    assert df.empty


def test_build_user_genres_df_populated_creates_rows() -> None:
    "Test build user genres df populated creates rows."
    store = {
        "u1": [
            {"genre": "Fantasy", "rank": 1},
            {"genre": "Sci-Fi", "rank": 2},
        ]
    }

    df = service.build_user_genres_df("u1", store)

    assert len(df) == 2
    assert set(df["genre"]) == {"Fantasy", "Sci-Fi"}
    assert set(df["rank"]) == {1, 2}
    assert set(df["user_id"]) == {"u1"}


def test_build_user_books_df_empty_gives_single_row_with_empty_list() -> None:
    "Test build user books df empty gives single row with empty list."
    store: Dict[str, List[str]] = {}

    df = service.build_user_books_df("u1", store)

    assert len(df) == 1
    row = df.iloc[0]
    assert row["user_id"] == "u1"
    assert row["books_read"] == []


def test_build_user_books_df_populated_uses_list_from_store() -> None:
    "Test build user books df populated uses list from store."
    store = {"u1": ["b1", "b2"]}

    df = service.build_user_books_df("u1", store)

    assert len(df) == 1
    row = df.iloc[0]
    assert row["user_id"] == "u1"
    assert row["books_read"] == ["b1", "b2"]


def test_get_recommendations_passes_ids_and_top_k_to_recommender() -> None:
    "Test get recommendations passes ids and top k to recommender."
    store = {"u1": ["b1", "b2"]}
    with patch("backend.service.BookRecommender") as mock_cls:
        inst = MagicMock()
        mock_cls.return_value = inst
        inst.recommend.return_value = [{"book_id": "r1"}]

        result = service.get_recommendations("u1", user_genres_store={}, user_books_read_store=store, top_k=7)

        mock_cls.assert_called_once_with()
        inst.recommend.assert_called_once_with(["b1", "b2"], top_k=7)
        assert result == [{"book_id": "r1"}]


def test_get_recommendations_empty_user_id_passes_empty_list() -> None:
    "Test get recommendations empty user id passes empty list."
    with patch("backend.service.BookRecommender") as mock_cls:
        inst = MagicMock()
        mock_cls.return_value = inst
        inst.recommend.return_value = []

        result = service.get_recommendations("", user_genres_store={}, user_books_read_store={"u1": ["b1"]}, top_k=3)

        inst.recommend.assert_called_once_with([], top_k=3)
        assert result == []


def test_get_top_popular_books_non_positive_top_k_returns_empty_list() -> None:
    "Test get top popular books non positive top k returns empty list."
    assert service.get_top_popular_books(0) == []
    assert service.get_top_popular_books(-1) == []


def test_get_top_popular_books_calls_recommender_with_empty_library() -> None:
    "Test get top popular books calls recommender with empty library."
    with patch("backend.service.BookRecommender") as mock_cls:
        inst = MagicMock()
        mock_cls.return_value = inst
        inst.recommend.return_value = [{"book_id": "x"}]

        result = service.get_top_popular_books(3)

        inst.recommend.assert_called_once_with([], top_k=3)
        assert result == [{"book_id": "x"}]


def test_mark_book_as_read_creates_entry_and_avoids_duplicates() -> None:
    "Test mark book as read creates entry and avoids duplicates."
    user_genres_store: Dict[str, List[Dict[str, Any]]] = {}
    user_books_read_store: Dict[str, List[str]] = {}

    with patch("backend.service.get_recommendations") as mock_get_recs:
        mock_get_recs.return_value = [{"book_id": "r"}]

        result1 = service.mark_book_as_read("u1", "b1", user_genres_store, user_books_read_store, top_k=5)
        result2 = service.mark_book_as_read("u1", "b1", user_genres_store, user_books_read_store, top_k=5)

        assert user_books_read_store["u1"] == ["b1"]
        assert result1 == [{"book_id": "r"}]
        assert result2 == [{"book_id": "r"}]
        mock_get_recs.assert_called_with(
            user_id="u1",
            user_genres_store=user_genres_store,
            user_books_read_store=user_books_read_store,
            top_k=5,
        )


def test_get_book_details_normalizes_full_record() -> None:
    "Test get book details normalizes full record."
    books_store = {
        "b1": {
            "title": "Title",
            "author": "Author",
            "genre": ["Fantasy", "Sci-Fi"],
            "parent_asin": "P1",
            "available_libraries": ["LibA", "LibB"],
        }
    }

    result = service.get_book_details("b1", books_store)

    assert result["book_id"] == "b1"
    assert result["parent_asin"] == "P1"
    assert result["title"] == "Title"
    assert result["author"] == "Author"
    assert result["genres"] == ["Fantasy", "Sci-Fi"]
    assert result["available_libraries"] == ["LibA", "LibB"]


def test_get_book_details_handles_missing_and_non_list_fields() -> None:
    "Test get book details handles missing and non list fields."
    books_store = {
        "b2": {
            "title": "T2",
            "author": "A2",
            "genre": "Fantasy",
            "available_libraries": "LibX",
        }
    }

    result = service.get_book_details("b2", books_store)

    assert result["book_id"] == "b2"
    # parent_asin not provided -> None
    assert result["parent_asin"] is None
    # genre coerced to list of strings
    assert result["genres"] == ["Fantasy"]
    # available_libraries coerced to list of strings
    assert result["available_libraries"] == ["LibX"]


def test_get_book_details_missing_book_returns_defaults() -> None:
    "Test get book details missing book returns defaults."
    books_store: Dict[str, Dict[str, Any]] = {}

    result = service.get_book_details("missing", books_store)

    assert result["book_id"] == "missing"
    assert result["parent_asin"] is None
    assert result["title"] == ""
    assert result["author"] == ""
    assert result["genres"] == []
    assert result["available_libraries"] is None

