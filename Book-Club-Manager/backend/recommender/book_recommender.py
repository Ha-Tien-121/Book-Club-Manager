"""Content-based book recommender for the Bookish backend.

Uses Maanya's logic: TF-IDF on genres + average_rating + rating_number.

Two modes:
- Precomputed (full catalog): If data/processed has book_tfidf.npz, book_id_to_idx.json,
  book_rating_norms.npz (from data/scripts/build_recommender_artifacts.py), loads those and
  queries books.db only for top-k metadata. Use this for 1M+ books.
- In-memory (small catalog): Otherwise loads from reviews_top25 + spl_top50 JSON and
  fits in memory. No books.db required.
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Set

import numpy as np
import pandas as pd
from scipy import sparse
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity
from sklearn.preprocessing import MinMaxScaler

from backend.config import (
    PROCESSED_DIR,
    BOOK_TFIDF_S3_KEY,
    BOOK_ID_TO_IDX_ARTIFACT_S3_KEY,
    BOOK_RATING_NORMS_S3_KEY,
)

GENRE_VOCAB: List[str] = [
    "Literature & Fiction",
    "Children's Books",
    "Mystery, Thriller & Suspense",
    "Arts & Photography",
    "History",
    "Biographies & Memoirs",
    "Crafts, Hobbies & Home",
    "Business & Money",
    "Politics & Social Sciences",
    "Growing Up & Facts of Life",
    "Romance",
    "Science & Math",
    "Teen & Young Adult",
    "Cookbooks, Food & Wine",
    "Religion & Spirituality",
    "Poetry",
    "Comics & Graphic Novels",
    "Travel",
    "Fantasy",
    "Action & Adventure",
    "Self-Help",
    "Science Fiction",
    "Sports & Outdoors",
    "Classics",
    "LGBTQ+",
]


GENRE_KEYWORDS: Dict[str, List[str]] = {
    "Literature & Fiction": ["literature", "fiction"],
    "Children's Books": ["children", "kids"],
    "Mystery, Thriller & Suspense": ["mystery", "thriller", "suspense"],
    "Arts & Photography": ["art", "arts", "photography"],
    "History": ["history"],
    "Biographies & Memoirs": ["biography", "memoir"],
    "Crafts, Hobbies & Home": ["craft", "hobbies", "home"],
    "Business & Money": ["business", "finance", "money"],
    "Politics & Social Sciences": ["politics", "social science"],
    "Growing Up & Facts of Life": ["growing up", "coming of age"],
    "Romance": ["romance"],
    "Science & Math": ["science", "math"],
    "Teen & Young Adult": ["young adult", "teen"],
    "Cookbooks, Food & Wine": ["cookbook", "food", "wine", "cooking"],
    "Religion & Spirituality": ["religion", "spirituality"],
    "Poetry": ["poetry"],
    "Comics & Graphic Novels": ["comics", "graphic novel"],
    "Travel": ["travel"],
    "Fantasy": ["fantasy"],
    "Action & Adventure": ["adventure", "action"],
    "Self-Help": ["self help", "self-help"],
    "Science Fiction": ["science fiction", "sci fi", "sci-fi"],
    "Sports & Outdoors": ["sports", "outdoor"],
    "Classics": ["classic"],
    "LGBTQ+": ["lgbt", "lgbtq"],
}


@dataclass(frozen=True)
class RecommenderWeights:
    """Weights used to combine different recommendation signals."""

    genre_similarity: float = 0.5
    average_rating: float = 0.3
    rating_number_popularity: float = 0.2


def _safe_json_loads(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, (list, dict)):
        return value
    if isinstance(value, (bytes, bytearray)):
        try:
            value = value.decode("utf-8", errors="ignore")
        except Exception:
            return value
    if isinstance(value, str):
        s = value.strip()
        if not s:
            return None
        try:
            return json.loads(s)
        except Exception:
            return value
    return value


def _normalize_whitespace(s: str) -> str:
    return " ".join(s.split())


def _infer_column(df: pd.DataFrame, candidates: Sequence[str]) -> Optional[str]:
    lower_to_actual = {c.lower(): c for c in df.columns}
    for cand in candidates:
        if cand.lower() in lower_to_actual:
            return lower_to_actual[cand.lower()]
    return None


class ContentBasedBookRecommender:
    """Content-based recommender operating on processed book metadata."""

    def __init__(
        self,
        data_dir: Optional[Path] = None,
        weights: Optional[RecommenderWeights] = None,
    ) -> None:
        self.data_dir = Path(data_dir) if data_dir is not None else PROCESSED_DIR
        self.weights = weights or RecommenderWeights()

        self.books_df: Optional[pd.DataFrame] = None
        self.book_id_to_idx: Optional[Dict[str, int]] = None
        self.tfidf_vectorizer: Optional[TfidfVectorizer] = None
        self.book_tfidf: Optional[sparse.csr_matrix] = None
        self.scalers: Dict[str, MinMaxScaler] = {}
        # Precomputed path (full catalog): no DataFrame, query books.db for metadata
        self._rating_norm: Optional[np.ndarray] = None
        self._rating_number_norm: Optional[np.ndarray] = None

    def fit(self) -> None:
        """Load from precomputed artifacts (local, or S3 when AWS) or from JSON and build in memory."""
        tfidf_path = self.data_dir / "book_tfidf.npz"
        idx_path = self.data_dir / "book_id_to_idx.json"
        norms_path = self.data_dir / "book_rating_norms.npz"
        if tfidf_path.exists() and idx_path.exists() and norms_path.exists():
            self._load_precomputed(tfidf_path, idx_path, norms_path)
            return
        # AWS: try loading content-based artifacts from S3 (e.g. s3://bucket/books/book_recommender/).
        try:
            from backend import config
            if getattr(config, "IS_AWS", False):
                self._load_precomputed_from_s3()
                return
        except Exception:
            pass
        self._fit_from_json()

    def _load_precomputed(
        self,
        tfidf_path: Path,
        idx_path: Path,
        norms_path: Path,
    ) -> None:
        """Load artifacts built by data/scripts/build_recommender_artifacts.py."""
        self.book_tfidf = sparse.load_npz(str(tfidf_path))
        with idx_path.open("r", encoding="utf-8") as f:
            self.book_id_to_idx = json.load(f)
        data = np.load(norms_path)
        self._rating_norm = data["average_rating_norm"]
        self._rating_number_norm = data["rating_number_norm"]
        self.books_df = None
        self.tfidf_vectorizer = None
        self.scalers = {}

    def _load_precomputed_from_s3(self) -> None:
        """Load precomputed artifacts from S3 (e.g. s3://bucket/books/book_recommender/)."""
        from io import BytesIO

        from backend import config
    except Exception:
        return
    if not getattr(config, "IS_AWS", False):
        return
    if not getattr(config, "USE_BOOK_ML_RECOMMENDER", False):
        return
    if not _use_in_memory_ml_artifacts():
        return

    bucket = getattr(config, "ML_ARTIFACTS_BUCKET", None) or getattr(config, "DATA_BUCKET", None)
    if not bucket:
        return
    region = getattr(config, "AWS_REGION", None)
    keys = [
        getattr(config, "BOOK_RECOMMENDER_MODEL_S3_KEY", None),
        getattr(config, "BOOK_RECOMMENDER_SCALER_S3_KEY", None),
        getattr(config, "BOOK_SIMILARITY_S3_KEY", None),
        getattr(config, "BOOK_RATINGS_S3_KEY", None),
        getattr(config, "BOOK_ID_TO_IDX_S3_KEY", None),
    ]
    for key in keys:
        if key:
            _get_ml_artifact_bytes(bucket=bucket, key=key, region=region)


def _should_use_cloud_books_metadata() -> bool:
    """Return True when running in AWS mode (no local datasets)."""
    return (os.getenv("APP_ENV") or "").strip().lower() == "aws"

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
            # In AWS mode, optionally pull ML artifacts from S3 into memory.
            ml_from_s3 = _use_in_memory_ml_artifacts() and _should_use_cloud_books_metadata()
            if ml_from_s3:
                from backend import config as _cfg

                bucket = getattr(_cfg, "ML_ARTIFACTS_BUCKET", None) or getattr(_cfg, "DATA_BUCKET", None)
                region = getattr(_cfg, "AWS_REGION", None)
                if not bucket:
                    raise RuntimeError("ML_ARTIFACTS_BUCKET/DATA_BUCKET not set")
                model_bytes = _get_ml_artifact_bytes(
                    bucket=bucket,
                    key=getattr(_cfg, "BOOK_RECOMMENDER_MODEL_S3_KEY", ""),
                    region=region,
                )
                scaler_bytes = _get_ml_artifact_bytes(
                    bucket=bucket,
                    key=getattr(_cfg, "BOOK_RECOMMENDER_SCALER_S3_KEY", ""),
                    region=region,
                )
                sim_bytes = _get_ml_artifact_bytes(
                    bucket=bucket,
                    key=getattr(_cfg, "BOOK_SIMILARITY_S3_KEY", ""),
                    region=region,
                )
                ratings_bytes = _get_ml_artifact_bytes(
                    bucket=bucket,
                    key=getattr(_cfg, "BOOK_RATINGS_S3_KEY", ""),
                    region=region,
                )
                idmap_bytes = _get_ml_artifact_bytes(
                    bucket=bucket,
                    key=getattr(_cfg, "BOOK_ID_TO_IDX_S3_KEY", ""),
                    region=region,
                )
                if not (model_bytes and scaler_bytes and sim_bytes and ratings_bytes and idmap_bytes):
                    raise RuntimeError("Missing ML artifacts in S3 (one or more downloads failed)")

                clf = joblib.load(BytesIO(model_bytes))
                scaler = joblib.load(BytesIO(scaler_bytes))
                beta = clf.coef_[0]
                self.beta_scaled = beta / scaler.scale_
                try:
                    self.similarity_boost = float(os.getenv("BOOK_SIMILARITY_BOOST", "1.0"))
                except (TypeError, ValueError):
                    self.similarity_boost = 1.0
                self.book_similarity: csr_matrix = load_npz(BytesIO(sim_bytes)).tocsr()
                ratings = np.load(BytesIO(ratings_bytes))
                avg_ratings = ratings["ratings_avg"].astype(np.float32)
                log_num_ratings = ratings["log_number_ratings"].astype(np.float32)
                self.popularity_score = np.log1p(avg_ratings * log_num_ratings)
                self.book_id_to_idx = json.loads(idmap_bytes.decode("utf-8"))
                self.idx_to_book_id = {v: k for k, v in self.book_id_to_idx.items()}
                return

            # Local filesystem mode.
            clf = joblib.load(_MODEL_FILE)
            scaler = joblib.load(_MODEL_SCALER_FILE)
            beta = clf.coef_[0]
            self.beta_scaled = beta / scaler.scale_

            self.book_similarity: csr_matrix = load_npz(_BOOK_SIM_FILE).tocsr()
            ratings = np.load(_BOOK_RATINGS_FILE)
            book_avg_ratings = ratings["ratings_avg"].astype(np.float32)
            log_book_num_ratings = ratings["log_number_ratings"].astype(np.float32)
            self.popularity_score = np.log1p(book_avg_ratings * log_book_num_ratings)
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
                sim = np.zeros(len(self.popularity_score), dtype=np.float32)

            beta = self.beta_scaled
           
            scores = (
                beta[0] * sim
                + beta[1] * self.popularity_score
                + beta[2] * np.log1p(sim * lib_size)
            )

            if lib_size > 0:
                scores[book_indices] = np.inf

            desired = min(top_k, len(self.popularity_score) - lib_size)
            if desired <= 0:
                return []

            top_indices = np.argpartition(-scores, desired)[:desired]
            top_indices = top_indices[np.argsort(-scores[top_indices])]
            top_book_ids = [self.idx_to_book_id[i] for i in top_indices]
            
            return self._fetch_books(top_book_ids)

        def _fetch_books(self, book_ids: list) -> list[dict[str, Any]]:
            """Fetch display metadata for recommended books.

            - AWS: fetch from DynamoDB via storage.get_book_metadata (no local files).
            - Local: fetch from local books.db for speed.
            """
            if not book_ids:
                return []
            if _should_use_cloud_books_metadata():
                try:
                    from backend.storage import get_storage

                    store = get_storage()
                    out: list[dict[str, Any]] = []
    
                    batch = {}
                    try:
                        if hasattr(store, "get_books_metadata_batch"):
                            batch = store.get_books_metadata_batch(book_ids) or {}
                    except Exception:
                        batch = {}
                    for bid in book_ids:
                        meta = batch.get(str(bid).strip()) if batch else None
                        if meta:
                            out.append(dict(meta))
                    return out
                except Exception:
                    # If Dynamo metadata fails, let it fall through to sqlite attempt
                    pass
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
    _MLRecommenderClass = None


class _FallbackBookRecommender:
    """When ML fails to load: return reviews_top50_books from storage."""
    def __init__(self):
        logging.warning("Using fallback book recommender...")
    def recommend(
        self,
        user_book_ids: List[str],
        top_k: int = 50,
    ) -> List[dict[str, Any]]:
        books = self._get_review_books()

        if user_book_ids:
            owned = {str(b) for b in user_book_ids}
            books = [
                b for b in books
                if str(b.get("parent_asin", "")) not in owned
            ]
        return list(books)[: max(0, top_k)]

    def recommend_for_user(
        self,
        user_email: str,
        user_account: Dict[str, Any],
        user_genres: Optional[List[Dict[str, Any]]],
        top_k: int = 40,
    ) -> List[Dict[str, Any]]:
        library = user_account.get("library") or {}
        finished = library.get("finished") or []
        saved = library.get("saved") or []
        in_progress = library.get("in_progress") or []
        user_book_ids = list(finished) + list(saved) + list(in_progress)
        return self.recommend(user_book_ids, top_k=top_k)


_cached_recommender: Optional[ContentBasedBookRecommender] = None
_using_fallback = True


def _create_recommender() -> tuple[type | None, bool]:
    """Return (recommender class or None, is_fallback)."""
    _maybe_download_ml_artifacts_from_s3()
    if _MLRecommenderClass is None:
        return None, True
    try:
        _MLRecommenderClass()
        return _MLRecommenderClass, False
    except Exception as e:
        logging.exception("Failed to initialize ML recommender")
        return None, True


_RecommenderClass, _ = _create_recommender()
_cached_ml_instance: Any = None


def BookRecommender():  # noqa: N802
    """Return recommender instance (cached for ML so model loads once)."""
    global _cached_ml_instance
    if not _RecommenderClass:
        return _FallbackBookRecommender()
    if _cached_ml_instance is None:
        _cached_ml_instance = _RecommenderClass()
    return _cached_ml_instance


GENRE_VOCAB = []
