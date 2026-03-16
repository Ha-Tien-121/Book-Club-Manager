"""Book recommender.

Same pattern as event_recommender: one class BookRecommender with
recommend(user_book_ids, top_k). If the ML model fails to load, returns
reviews_top50_books from storage (get_top50_review_books) instead.
"""

from __future__ import annotations

import json
import os
import sqlite3
from typing import Any, List

from backend.config import PROCESSED_DIR
from backend.recommender.config import RECOMMENDER_DIR

PROCESSED_DIR_STR = str(PROCESSED_DIR)
_MODEL_FILE = os.path.join(RECOMMENDER_DIR, "book_recommender_model.pkl")
_MODEL_SCALER_FILE = os.path.join(RECOMMENDER_DIR, "feature_scaler.pkl")
_BOOK_SIM_FILE = os.path.join(PROCESSED_DIR_STR, "book_similarity.npz")
_BOOK_RATINGS_FILE = os.path.join(PROCESSED_DIR_STR, "book_ratings.npz")
_BOOK_ID_MAP_FILE = os.path.join(PROCESSED_DIR_STR, "book_id_to_idx.json")
_BOOK_DB = os.path.join(PROCESSED_DIR_STR, "books.db")

try:
    import joblib
    import numpy as np
    from scipy.sparse import csr_matrix, load_npz

    class _MLBookRecommender:
        """
        Book recommender: personalized recommendations via ML model + similarity.
        Model artifacts and book data are loaded at initialization.
        """

        def __init__(self) -> None:
            """Load model artifacts and book data. Raises on missing files."""
            clf = joblib.load(_MODEL_FILE)
            beta = clf.coef_[0]
            scaler = joblib.load(_MODEL_SCALER_FILE)
            self.beta_scaled = beta / scaler.scale_
            self.book_similarity: csr_matrix = load_npz(_BOOK_SIM_FILE).tocsr()
            ratings = np.load(_BOOK_RATINGS_FILE)
            self.book_avg_ratings = ratings["ratings_avg"].astype(np.float32)
            self.book_num_ratings = np.log1p(ratings["log_number_ratings"]).astype(np.float32)
            with open(_BOOK_ID_MAP_FILE, "r", encoding="utf-8") as f:
                self.book_id_to_idx = json.load(f)
            self.idx_to_book_id = {v: k for k, v in self.book_id_to_idx.items()}

        def recommend(
            self,
            user_book_ids: list,
            top_k: int = 50,
        ) -> list[dict[str, Any]]:
            """Return top-k book recommendations. user_book_ids = list of parent_asin from library."""
            book_indices = [
                self.book_id_to_idx[b]
                for b in (user_book_ids or [])
                if b in self.book_id_to_idx
            ]
            book_indices = np.array(book_indices, dtype=np.int32)
            lib_size = len(book_indices)

            if lib_size > 0:
                sim = self.book_similarity[book_indices].sum(axis=0).A1
                sim /= lib_size
            else:
                sim = np.zeros(len(self.book_avg_ratings), dtype=np.float32)

            log_lib_size = np.log1p(lib_size)
            beta = self.beta_scaled
            scores = (
                beta[0] * sim
                + beta[1] * self.book_avg_ratings
                + beta[2] * self.book_num_ratings
                + beta[3] * sim * log_lib_size
            )
            if lib_size > 0:
                scores[book_indices] = -np.inf

            top_idx = np.argpartition(scores, -top_k)[-top_k:]
            top_idx = top_idx[np.argsort(scores[top_idx])[::-1]]
            book_ids = [self.idx_to_book_id[i] for i in top_idx]
            return self._fetch_books(book_ids)

        def _fetch_books(self, book_ids: list) -> list[dict[str, Any]]:
            """Query books.db for metadata of recommended books."""
            if not book_ids:
                return []
            placeholders = ",".join(["?"] * len(book_ids))
            query = f"""
            SELECT parent_asin, title, author_name, average_rating, rating_number, images, categories
            FROM books WHERE parent_asin IN ({placeholders})
            """
            columns = [
                "parent_asin", "title", "author_name",
                "average_rating", "rating_number", "images", "categories",
            ]
            with sqlite3.connect(_BOOK_DB) as conn:
                rows = conn.execute(query, book_ids).fetchall()
            return [dict(zip(columns, r)) for r in rows]

    _MLRecommenderClass = _MLBookRecommender
except Exception:
    _MLRecommenderClass = None  # type: ignore[assignment]


class _FallbackBookRecommender:
    """When ML fails to load: return reviews_top50_books from storage."""

    def recommend(
        self,
        user_book_ids: List[str],
        top_k: int = 50,
    ) -> List[dict[str, Any]]:
        books = self._get_review_books()
        return list(books)[: max(0, top_k)]

    @staticmethod
    def _get_review_books() -> List[dict[str, Any]]:
        from backend.storage import get_storage
        store = get_storage()
        return store.get_top50_review_books() or []


def _create_recommender() -> tuple[type | None, bool]:
    """Return (recommender class or None, is_fallback)."""
    from backend import config
    # Temporary: force fallback unless USE_BOOK_ML_RECOMMENDER=1 (e.g. while building lite model).
    if not getattr(config, "USE_BOOK_ML_RECOMMENDER", False):
        return None, True
    if _MLRecommenderClass is None:
        return None, True
    try:
        _MLRecommenderClass()  # probe load
        return _MLRecommenderClass, False
    except Exception:
        return None, True


_RecommenderClass, _is_fallback = _create_recommender()
_cached_ml_instance: Any = None


def BookRecommender():  # noqa: N802
    """Return recommender instance (cached for ML so model loads once)."""
    global _cached_ml_instance
    if _is_fallback:
        return _FallbackBookRecommender()
    if _cached_ml_instance is None:
        _cached_ml_instance = _RecommenderClass()
    return _cached_ml_instance


GENRE_VOCAB = []
