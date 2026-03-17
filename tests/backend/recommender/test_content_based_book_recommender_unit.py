from __future__ import annotations

import importlib
from typing import Any

import numpy as np
import pandas as pd
from scipy import sparse


def _mod():
    m = importlib.import_module("backend.recommender.book_recommender")
    return importlib.reload(m)


def test_safe_json_loads_and_prepare_categories_keywords() -> None:
    br = _mod()

    assert br._safe_json_loads(None) is None
    assert br._safe_json_loads([1, 2]) == [1, 2]
    assert br._safe_json_loads({"a": 1}) == {"a": 1}
    assert br._safe_json_loads(b'["a", "b"]') == ["a", "b"]
    # invalid JSON -> returns original string
    assert br._safe_json_loads("{not json") == "{not json"

    # Keyword mapping and de-dupe ordering
    raw = ["Sci-Fi", "science fiction", "Romance", None, "romance"]
    text = br.ContentBasedBookRecommender._prepare_categories(raw)
    # Should map to official genres, unique, and joined by "|"
    assert "Science Fiction" in text
    assert "Romance" in text
    assert "|" in text


def test_is_cold_start_with_and_without_user_id_column() -> None:
    br = _mod()

    # No uid column: any non-empty df counts as signal
    genres_df = pd.DataFrame([{"genre": "Fantasy"}])
    assert br.ContentBasedBookRecommender._is_cold_start("u", genres_df, None) is False

    # With uid column: only matching rows count
    books_df = pd.DataFrame([{"UID": "other", "parent_asin": "A1"}])
    assert br.ContentBasedBookRecommender._is_cold_start("u", None, books_df) is True
    books_df2 = pd.DataFrame([{"UID": "u", "parent_asin": "A1"}])
    assert br.ContentBasedBookRecommender._is_cold_start("u", None, books_df2) is False


def _make_precomputed_rec(br) -> Any:
    rec = br.ContentBasedBookRecommender()
    # Precomputed mode: books_df None, provide sparse matrix + id mapping + norms.
    n = 3
    d = len(br.GENRE_VOCAB)
    # one-hot vectors for first 3 genres
    rows = np.array([0, 1, 2])
    cols = np.array([0, 1, 2])
    data = np.array([1.0, 1.0, 1.0])
    rec.book_tfidf = sparse.csr_matrix((data, (rows, cols)), shape=(n, d))
    rec.book_id_to_idx = {"A1": 0, "A2": 1, "A3": 2}
    rec._rating_norm = np.array([0.2, 0.5, 0.9])
    rec._rating_number_norm = np.array([0.9, 0.2, 0.1])
    # avoid hitting fit() checks inside build_user_profile
    rec.tfidf_vectorizer = object()  # type: ignore[assignment]
    return rec


def test_recommend_cold_start_uses_rating_norms() -> None:
    br = _mod()
    rec = _make_precomputed_rec(br)

    # Cold start: no genres, no books -> score = 0.7*rating_norm + 0.3*rating_number_norm
    out = rec.recommend(user_id="u", user_genres_df=None, user_books_df=None, top_k=2)
    assert len(out) == 2
    # compute expected ordering
    scores = 0.7 * rec._rating_norm + 0.3 * rec._rating_number_norm  # type: ignore[operator]
    expected = [x for x, _ in sorted(zip(["A1", "A2", "A3"], scores), key=lambda t: -t[1])][:2]
    got = [r["parent_asin"] for r in out]
    assert got == expected


def test_recommend_non_cold_start_excludes_owned_and_fetches_metadata() -> None:
    br = _mod()
    rec = _make_precomputed_rec(br)

    # Patch metadata fetcher so we exercise the books_df=None path output shaping.
    rec._fetch_metadata_for_asins = lambda asins: [  # type: ignore[assignment]
        {"parent_asin": a, "title": f"T-{a}", "average_rating": 4.0, "rating_number": 10, "categories": ["Fantasy"]}
        for a in asins
    ]

    # Non-cold start via books_df containing user row (uid column inferred)
    user_books_df = pd.DataFrame([{"user_id": "u", "parent_asin": "A1"}])
    user_genres_df = pd.DataFrame([{"user_id": "u", "genre": "Fantasy", "rank": 1}])
    out = rec.recommend(user_id="u", user_genres_df=user_genres_df, user_books_df=user_books_df, top_k=3)

    # A1 is owned/read; should be excluded
    got = [r["parent_asin"] for r in out]
    assert "A1" not in got
    assert all("score" in r for r in out)
    assert all(r["title"].startswith("T-") for r in out)


def test_fallback_recommender_excludes_owned() -> None:
    br = _mod()

    class _Store:
        def get_top50_review_books(self):  # type: ignore[no-untyped-def]
            return [{"parent_asin": "A1"}, {"parent_asin": "A2"}]

    # Patch get_storage used inside fallback recommender
    import backend.storage as storage

    orig = storage.get_storage
    storage.get_storage = lambda: _Store()  # type: ignore[assignment]
    try:
        fb = br._FallbackBookRecommender()
        out = fb.recommend(["A1"], top_k=50)
        assert [b["parent_asin"] for b in out] == ["A2"]
    finally:
        storage.get_storage = orig  # type: ignore[assignment]

