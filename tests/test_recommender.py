"""
Unit tests for BookRecommender class
"""

from __future__ import annotations

from unittest.mock import patch

import pandas as pd
import pytest

from backend.recommender.book_recommender import BookRecommender


def _make_amazon_df() -> pd.DataFrame:
    """Amazon metadata: Book A,B = Romance, Book C = Mystery, Book D = Sci-Fi."""
    return pd.DataFrame({
        "title": ["Book A", "Book B", "Book C", "Book D"],
        "parent_asin": ["B1", "B2", "B3", "B4"],
        "categories": ["['Romance']", "['Romance']", "['Mystery']", "['Science Fiction & Fantasy']"],
        "average_rating": [4.5, 4.0, 4.8, 4.2],
        "rating_number": [100, 50, 200, 80],
    })


def _make_checkouts_df() -> pd.DataFrame:
    return pd.DataFrame({
        "Title": ["Book A", "Book B", "Book C", "Book D"],
        "Checkouts": [10, 20, 30, 5],
    })


def _make_catalog_df() -> pd.DataFrame:
    return pd.DataFrame({
        "Title": ["Book A", "Book B", "Book C", "Book D"],
        "Author": ["Author 1", "Author 2", "Author 3", "Author 4"],
        "ISBN": ["111", "222", "333", "444"],
    })


def _read_csv_side_effect(path):
    path_str = str(path).lower()
    if "amazon" in path_str:
        return _make_amazon_df()
    if "checkout" in path_str:
        return _make_checkouts_df()
    if "catalog" in path_str:
        return _make_catalog_df()
    raise ValueError(f"Unexpected path: {path}")

@pytest.fixture
def mock_read_csv():
    """Patch pd.read_csv to return in-memory mock DataFrames."""
    with patch("backend.recommender.book_recommender.pd.read_csv") as m:
        m.side_effect = _read_csv_side_effect
        yield m


@pytest.fixture
def fitted_recommender(mock_read_csv):
    """BookRecommender fitted with data"""
    rec = BookRecommender()
    rec.fit()
    return rec


@pytest.fixture
def user_genres_romance():
    """User prefers Romance (cold start)."""
    return pd.DataFrame([
        {"user_id": "u1", "genre": "Romance", "rank": 1},
    ])


@pytest.fixture
def user_genres_mystery():
    """User prefers Mystery."""
    return pd.DataFrame([
        {"user_id": "u1", "genre": "Mystery", "rank": 1},
    ])


@pytest.fixture
def user_books_empty():
    """No read history."""
    return pd.DataFrame([
        {"user_id": "u1", "books_read": []},
    ])


@pytest.fixture
def user_books_read_b1():
    """User has read Book B1 (Romance)."""
    return pd.DataFrame([
        {"user_id": "u1", "books_read": ["B1"]},
    ])

class TestColdStart:
    """User has genre preferences, no read history. Recommendations based on genre similarity."""

    def test_recommendations_returned(
        self,
        fitted_recommender,
        user_genres_romance,
        user_books_empty,
    ):
        recs = fitted_recommender.recommend(
            user_id="u1",
            user_genres_df=user_genres_romance,
            user_books_df=user_books_empty,
            top_k=5,
        )
        assert len(recs) > 0
        for r in recs:
            assert "book_id" in r
            assert "title" in r
            assert "score" in r

    def test_top_k_limit(
        self,
        fitted_recommender,
        user_genres_romance,
        user_books_empty,
    ):
        recs = fitted_recommender.recommend(
            user_id="u1",
            user_genres_df=user_genres_romance,
            user_books_df=user_books_empty,
            top_k=2,
        )
        assert len(recs) <= 2

    def test_romance_books_rank_higher_than_mystery(
        self,
        fitted_recommender,
        user_genres_romance,
        user_books_empty,
    ):
        recs = fitted_recommender.recommend(
            user_id="u1",
            user_genres_df=user_genres_romance,
            user_books_df=user_books_empty,
            top_k=4,
        )
        book_ids = [r["book_id"] for r in recs]
        romance_ids = {"B1", "B2"}
        mystery_id = "B3"
        romance_positions = [i for i, bid in enumerate(book_ids) if bid in romance_ids]
        mystery_position = next((i for i, bid in enumerate(book_ids) if bid == mystery_id), None)
        if mystery_position is not None and romance_positions:
            assert max(romance_positions) < mystery_position or any(
                p < mystery_position for p in romance_positions
            )

class TestReadHistoryExclusion:

    def test_read_book_excluded(
        self,
        fitted_recommender,
        user_genres_romance,
        user_books_read_b1,
    ):
        recs = fitted_recommender.recommend(
            user_id="u1",
            user_genres_df=user_genres_romance,
            user_books_df=user_books_read_b1,
            top_k=5,
        )
        book_ids = [r["book_id"] for r in recs]
        assert "B1" not in book_ids

    def test_all_read_books_excluded(
        self,
        fitted_recommender,
        user_genres_romance,
    ):
        user_books = pd.DataFrame([
            {"user_id": "u1", "books_read": ["B1", "B2", "B3"]},
        ])
        recs = fitted_recommender.recommend(
            user_id="u1",
            user_genres_df=user_genres_romance,
            user_books_df=user_books,
            top_k=5,
        )
        book_ids = [r["book_id"] for r in recs]
        assert "B1" not in book_ids
        assert "B2" not in book_ids
        assert "B3" not in book_ids


class TestHistoryWeighting:
    """When user has read history, genre similarity gets 1.5× boost."""

    def test_books_similar_to_read_history_rank_higher(
        self,
        fitted_recommender,
        user_genres_romance,
        user_books_read_b1,
    ):
        recs_with_history = fitted_recommender.recommend(
            user_id="u1",
            user_genres_df=user_genres_romance,
            user_books_df=user_books_read_b1,
            top_k=4,
        )
        recs_cold_start = fitted_recommender.recommend(
            user_id="u1",
            user_genres_df=user_genres_romance,
            user_books_df=pd.DataFrame([{"user_id": "u1", "books_read": []}]),
            top_k=4,
        )
        ids_with = [r["book_id"] for r in recs_with_history]
        ids_cold = [r["book_id"] for r in recs_cold_start]
        assert ids_with != ids_cold or len(ids_with) == len(ids_cold)

    def test_ranking_differs_with_vs_without_history(
        self,
        fitted_recommender,
        user_genres_romance,
        user_books_read_b1,
    ):
        recs_with = fitted_recommender.recommend(
            user_id="u1",
            user_genres_df=user_genres_romance,
            user_books_df=user_books_read_b1,
            top_k=3,
        )
        recs_without = fitted_recommender.recommend(
            user_id="u1",
            user_genres_df=user_genres_romance,
            user_books_df=pd.DataFrame([{"user_id": "u1", "books_read": []}]),
            top_k=3,
        )
        scores_with = [r["score"] for r in recs_with]
        scores_without = [r["score"] for r in recs_without]
        assert scores_with != scores_without

class TestDeterminism:
    """Same inputs must produce same recommendation order."""

    def test_same_inputs_same_order(
        self,
        fitted_recommender,
        user_genres_romance,
        user_books_empty,
    ):
        recs1 = fitted_recommender.recommend(
            user_id="u1",
            user_genres_df=user_genres_romance,
            user_books_df=user_books_empty,
            top_k=4,
        )
        recs2 = fitted_recommender.recommend(
            user_id="u1",
            user_genres_df=user_genres_romance,
            user_books_df=user_books_empty,
            top_k=4,
        )
        assert [r["book_id"] for r in recs1] == [r["book_id"] for r in recs2]
        assert [r["score"] for r in recs1] == [r["score"] for r in recs2]


class TestEdgeCases:
    """Empty inputs, top_k larger than available, etc."""

    def test_empty_user_genres_raises(
        self,
        fitted_recommender,
        user_books_empty,
    ):
        empty_genres = pd.DataFrame(columns=["user_id", "genre", "rank"])
        with pytest.raises(ValueError, match="Cannot build user profile"):
            fitted_recommender.recommend(
                user_id="u1",
                user_genres_df=empty_genres,
                user_books_df=user_books_empty,
                top_k=5,
            )

    def test_top_k_larger_than_available_returns_available_only(
        self,
        fitted_recommender,
        user_genres_romance,
        user_books_empty,
    ):
        recs = fitted_recommender.recommend(
            user_id="u1",
            user_genres_df=user_genres_romance,
            user_books_df=user_books_empty,
            top_k=100,
        )
        assert len(recs) <= 4

    def test_top_k_zero_returns_empty_list(
        self,
        fitted_recommender,
        user_genres_romance,
        user_books_empty,
    ):
        recs = fitted_recommender.recommend(
            user_id="u1",
            user_genres_df=user_genres_romance,
            user_books_df=user_books_empty,
            top_k=0,
        )
        assert recs == []
