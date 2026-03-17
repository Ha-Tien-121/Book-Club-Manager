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
import sqlite3
from io import BytesIO
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Set
import importlib

import numpy as np
import pandas as pd
from scipy import sparse
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity
from sklearn.preprocessing import MinMaxScaler

from backend.config import (
    AWS_REGION,
    BOOK_TFIDF_S3_KEY,
    BOOK_ID_TO_IDX_ARTIFACT_S3_KEY,
    BOOK_RATING_NORMS_S3_KEY,
    DATA_BUCKET,
    IS_AWS,
    ML_ARTIFACTS_BUCKET,
    PROCESSED_DIR,
)
import backend.storage as backend_storage

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
    """Best-effort JSON parse that preserves original values on failure."""
    parsed_input = value
    if value is None:
        return parsed_input
    if isinstance(value, (list, dict, str)):
        parsed_input = value
    elif isinstance(value, (bytes, bytearray)):
        try:
            parsed_input = value.decode("utf-8", errors="ignore")
        except (ValueError, TypeError, AttributeError):
            return value
    s = parsed_input.strip() if isinstance(parsed_input, str) else ""
    if s:
        parsed_input = s
        try:
            return json.loads(s)
        except (ValueError, TypeError):
            pass
    return parsed_input


def _normalize_whitespace(s: str) -> str:
    """Collapse internal whitespace and trim edges."""
    return " ".join(s.split())


def _infer_column(df: pd.DataFrame, candidates: Sequence[str]) -> Optional[str]:
    """Return the first matching dataframe column from candidate names."""
    lower_to_actual = {c.lower(): c for c in df.columns}
    for cand in candidates:
        if cand.lower() in lower_to_actual:
            return lower_to_actual[cand.lower()]
    return None


def _map_genre_name(raw_genre: str) -> str:
    """Map free-form genre labels to the canonical recommender vocabulary."""
    if raw_genre in GENRE_VOCAB:
        return raw_genre
    genre_lower = raw_genre.lower()
    for official, keywords in GENRE_KEYWORDS.items():
        if official in GENRE_VOCAB and any(keyword in genre_lower for keyword in keywords):
            return official
    return ""


class ContentBasedBookRecommender:
    """Content-based recommender operating on processed book metadata."""

    def __init__(
        self,
        data_dir: Optional[Path] = None,
        weights: Optional[RecommenderWeights] = None,
    ) -> None:
        """Initialize recommender paths, weights, and in-memory artifact holders."""
        self.data_dir = Path(data_dir) if data_dir is not None else PROCESSED_DIR
        self.weights = weights or RecommenderWeights()

        self.books_df: Optional[pd.DataFrame] = None
        self.book_id_to_idx: Optional[Dict[str, int]] = None
        self.tfidf_vectorizer: Optional[TfidfVectorizer] = None
        self.book_tfidf: Optional[sparse.csr_matrix] = None
        # Precomputed path (full catalog): no DataFrame, query books.db for metadata.
        self._rating_norms: Dict[str, Optional[np.ndarray]] = {
            "average_rating": None,
            "rating_number": None,
        }

    @property
    def _rating_norm(self) -> Optional[np.ndarray]:
        """Backward-compatible alias for average_rating normalized scores."""
        return self._rating_norms.get("average_rating")

    @_rating_norm.setter
    def _rating_norm(self, value: Optional[np.ndarray]) -> None:
        """Backward-compatible setter for average_rating normalized scores."""
        self._rating_norms["average_rating"] = value

    @property
    def _rating_number_norm(self) -> Optional[np.ndarray]:
        """Backward-compatible alias for rating_number normalized scores."""
        return self._rating_norms.get("rating_number")

    @_rating_number_norm.setter
    def _rating_number_norm(self, value: Optional[np.ndarray]) -> None:
        """Backward-compatible setter for rating_number normalized scores."""
        self._rating_norms["rating_number"] = value

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
            if IS_AWS:
                self._load_precomputed_from_s3()
                return
        except (RuntimeError, ValueError, TypeError, OSError):
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
        self._rating_norms["average_rating"] = data["average_rating_norm"]
        self._rating_norms["rating_number"] = data["rating_number_norm"]
        self.books_df = None
        self.tfidf_vectorizer = None

    def _load_precomputed_from_s3(self) -> None:
        """Load precomputed artifacts from S3 (e.g. s3://bucket/books/book_recommender/)."""
        bucket = DATA_BUCKET or ML_ARTIFACTS_BUCKET
        region = AWS_REGION
        if not bucket:
            raise RuntimeError("DATA_BUCKET / ML_ARTIFACTS_BUCKET not set for S3 artifact load")
        boto3 = importlib.import_module("boto3")
        s3 = boto3.client("s3", region_name=region)
        tfidf_resp = s3.get_object(Bucket=bucket, Key=BOOK_TFIDF_S3_KEY)
        self.book_tfidf = sparse.load_npz(BytesIO(tfidf_resp["Body"].read()))
        idx_resp = s3.get_object(Bucket=bucket, Key=BOOK_ID_TO_IDX_ARTIFACT_S3_KEY)
        self.book_id_to_idx = json.loads(idx_resp["Body"].read().decode("utf-8"))
        norms_resp = s3.get_object(Bucket=bucket, Key=BOOK_RATING_NORMS_S3_KEY)
        data = np.load(BytesIO(norms_resp["Body"].read()))
        self._rating_norms["average_rating"] = data["average_rating_norm"]
        self._rating_norms["rating_number"] = data["rating_number_norm"]
        self.books_df = None
        self.tfidf_vectorizer = None

    def _fetch_metadata_for_asins(self, asin_list: List[str]) -> List[Dict[str, Any]]:
        """Fetch metadata for parent_asins from books.db (local) or storage (AWS). Returns list of dicts."""
        if not asin_list:
            return []
        # AWS / no local DB: use storage.get_books_metadata_batch (DynamoDB or S3).
        db_path = self.data_dir / "books.db"
        if not db_path.exists():
            return self._fetch_metadata_from_storage(asin_list)

        placeholders = ",".join(["?"] * len(asin_list))
        query = f"""
        SELECT parent_asin, title, author_name, average_rating, rating_number, images, categories
        FROM books WHERE parent_asin IN ({placeholders})
        """
        try:
            with sqlite3.connect(str(db_path)) as conn:
                rows = conn.execute(query, asin_list).fetchall()
        except sqlite3.Error:
            return []
        columns = [
            "parent_asin", "title", "author_name",
            "average_rating", "rating_number", "images", "categories",
        ]
        out = []
        for r in rows:
            d = dict(zip(columns, r))
            cats = d.get("categories")
            if isinstance(cats, str):
                try:
                    parsed = _safe_json_loads(cats)
                    d["categories_list"] = (
                        [str(x) for x in parsed] if isinstance(parsed, list) else []
                    )
                except (ValueError, TypeError):
                    d["categories_list"] = []
            else:
                d["categories_list"] = d.get("categories_list") or []
            out.append(d)
        return out

    def _fetch_metadata_from_storage(self, asin_list: List[str]) -> List[Dict[str, Any]]:
        """Fetch metadata via storage backend when local sqlite is unavailable."""
        try:
            store = backend_storage.get_storage()
            if not hasattr(store, "get_books_metadata_batch"):
                return []
            batch = store.get_books_metadata_batch(asin_list) or {}
        except (RuntimeError, ValueError, TypeError, KeyError):
            return []
        out: list[dict[str, Any]] = []
        for asin in asin_list:
            metadata = batch.get(str(asin))
            if not metadata:
                continue
            categories = metadata.get("categories") or metadata.get("categories_list") or []
            if isinstance(categories, str):
                parsed_categories = _safe_json_loads(categories) or []
                categories = parsed_categories if isinstance(parsed_categories, list) else []
            if not isinstance(categories, list):
                categories = []
            out.append(
                {
                    "parent_asin": str(asin),
                    "title": metadata.get("title") or "",
                    "author_name": metadata.get("author_name") or metadata.get("author"),
                    "average_rating": float(metadata.get("average_rating") or 0),
                    "rating_number": int(
                        metadata.get("rating_number")
                        or metadata.get("rating_count")
                        or 0
                    ),
                    "images": metadata.get("images"),
                    "categories": metadata.get("categories"),
                    "categories_list": [str(x) for x in categories],
                }
            )
        return out

    def _build_genres_vector(
        self,
        user_id: str,
        user_genres_df: Optional[pd.DataFrame],
    ) -> tuple[np.ndarray, bool]:
        """Build weighted genre preference vector from user preference rows."""
        genres_vec = np.zeros(len(GENRE_VOCAB), dtype=float)
        if user_genres_df is None or user_genres_df.empty:
            return genres_vec, False
        genres_df = user_genres_df
        uid_col = _infer_column(genres_df, ["user_id", "user", "uid"])
        if uid_col is not None:
            genres_df = genres_df[genres_df[uid_col].astype(str) == str(user_id)]
        genre_col = _infer_column(
            genres_df, ["genre", "category", "categories", "preference", "name"]
        )
        rank_col = _infer_column(
            genres_df, ["rank", "preference_rank", "order", "priority"]
        )
        if genre_col is None or genres_df.empty:
            return genres_vec, False
        has_genres = False
        for _, row in genres_df.iterrows():
            raw_genre = row.get(genre_col)
            if raw_genre is None:
                continue
            genre_name = str(raw_genre).strip()
            if not genre_name:
                continue
            genre_name = _map_genre_name(genre_name)
            if not genre_name:
                continue
            rank_val = row.get(rank_col) if rank_col is not None else None
            weight = 1.0
            try:
                rank = int(rank_val)
                if rank == 1:
                    weight = 3.0
                elif rank == 2:
                    weight = 2.0
            except (TypeError, ValueError):
                weight = 1.0
            genres_vec[GENRE_VOCAB.index(genre_name)] += weight
            has_genres = True
        return genres_vec, has_genres

    def _fit_from_json(self) -> None:
        """Load book catalog from existing JSON files and build TF-IDF + scalers."""
        reviews_path = self.data_dir / "reviews_top25_books.json"
        spl_path = self.data_dir / "spl_top50_checkouts_in_books.json"
        self._rating_norms["average_rating"] = None
        self._rating_norms["rating_number"] = None

        def _load_json(path: Path) -> list:
            """Load a JSON list file; return empty list when missing/unusable."""
            if not path.exists():
                return []
            with path.open("r", encoding="utf-8") as f:
                data = json.load(f)
            return data if isinstance(data, list) else []

        def _normalize_cats(cats: Any) -> list:
            """Normalize category payloads to a list of non-empty strings."""
            if cats is None:
                return []
            parsed = _safe_json_loads(cats)
            if isinstance(parsed, list):
                return [str(x) for x in parsed if x is not None and str(x).strip()]
            if isinstance(parsed, str) and parsed.strip():
                return [parsed.strip()]
            return []

        rows: list[dict] = []
        seen_asins: set[str] = set()
        for path in (reviews_path, spl_path):
            for r in _load_json(path):
                if not isinstance(r, dict):
                    continue
                asin = (r.get("parent_asin") or r.get("source_id") or "").strip()
                if not asin or asin in seen_asins:
                    continue
                seen_asins.add(asin)
                rows.append({
                    "parent_asin": asin,
                    "title": (r.get("title") or "").strip() or "Unknown",
                    "author_name": (r.get("author_name") or r.get("author") or "").strip() or "Unknown",
                    "average_rating": float(r.get("average_rating") or 0),
                    "rating_number": int(r.get("rating_number") or r.get("rating_count") or 0),
                    "images": (r.get("images") or "").strip(),
                    "categories": r.get("categories"),
                })

        if not rows:
            raise FileNotFoundError(
                f"No books found in {reviews_path} or {spl_path}"
            )

        books_df = pd.DataFrame(rows)
        books_df["categories_list"] = books_df["categories"].apply(_normalize_cats)
        books_df["genre_text"] = books_df["categories"].apply(self._prepare_categories)
        self.book_id_to_idx = {str(row["parent_asin"]): i for i, row in books_df.iterrows()}

        self.tfidf_vectorizer = TfidfVectorizer(
            vocabulary=GENRE_VOCAB,
            tokenizer=lambda s: s.split("|") if s else [],
            token_pattern=None,
            lowercase=False,
            norm="l2",
        )
        self.book_tfidf = self.tfidf_vectorizer.fit_transform(
            books_df["genre_text"].fillna("").astype(str)
        )

        books_df["average_rating"] = pd.to_numeric(books_df["average_rating"], errors="coerce")
        books_df["rating_number"] = pd.to_numeric(books_df["rating_number"], errors="coerce")

        rating_vals = books_df[["average_rating"]].fillna(0.0).to_numpy(dtype=float)
        rating_num_vals = books_df[["rating_number"]].fillna(0.0).to_numpy(dtype=float)

        rating_scaler = MinMaxScaler()
        rating_num_scaler = MinMaxScaler()
        books_df["average_rating_norm"] = rating_scaler.fit_transform(rating_vals).reshape(-1)
        books_df["rating_number_norm"] = rating_num_scaler.fit_transform(
            rating_num_vals
        ).reshape(-1)
        _ = rating_scaler, rating_num_scaler

        self.books_df = books_df

    @staticmethod
    def _prepare_categories(raw: Any) -> str:
        """Map raw categories text/list to a pipe-delimited controlled genre set."""
        parsed = _safe_json_loads(raw)
        values: List[str] = []
        if isinstance(parsed, list):
            values = [str(x) for x in parsed if x is not None]
        elif isinstance(parsed, str):
            values = [parsed]
        else:
            values = []

        text = _normalize_whitespace(" ".join(values)).lower()
        if not text:
            return ""

        matched: List[str] = []
        for genre in GENRE_VOCAB:
            keywords = GENRE_KEYWORDS.get(genre, [])
            for keyword in keywords:
                if keyword in text:
                    matched.append(genre)
                    break

        seen: Set[str] = set()
        ordered_unique = [g for g in matched if not (g in seen or seen.add(g))]
        return "|".join(ordered_unique)

    @staticmethod
    def _is_cold_start(
        user_id: Any,
        user_genres_df: Optional[pd.DataFrame],
        user_books_df: Optional[pd.DataFrame],
    ) -> bool:
        """Return True when user has no usable genre prefs and no reading history."""
        has_genres = False
        if user_genres_df is not None and not user_genres_df.empty:
            uid_col = _infer_column(user_genres_df, ["user_id", "user", "uid"])
            if uid_col is None:
                has_genres = user_genres_df.shape[0] > 0
            else:
                has_genres = (
                    user_genres_df[user_genres_df[uid_col].astype(str) == str(user_id)].shape[0]
                    > 0
                )

        has_books = False
        if user_books_df is not None and not user_books_df.empty:
            uid_col = _infer_column(user_books_df, ["user_id", "user", "uid"])
            if uid_col is None:
                has_books = user_books_df.shape[0] > 0
            else:
                has_books = (
                    user_books_df[user_books_df[uid_col].astype(str) == str(user_id)].shape[0] > 0
                )

        return (not has_genres) and (not has_books)

    def _get_read_parent_asins(
        self, user_id: Any, user_books_df: Optional[pd.DataFrame]
    ) -> Set[str]:
        """Extract read/saved parent ASINs for a specific user from an input frame."""
        if user_books_df is None or user_books_df.empty:
            return set()

        books_df = user_books_df
        uid_col = _infer_column(books_df, ["user_id", "user", "uid"])
        if uid_col is not None:
            books_df = books_df[books_df[uid_col].astype(str) == str(user_id)]

        asin_col = _infer_column(
            books_df, ["parent_asin", "asin", "book_id", "book_asin"]
        )
        if asin_col is None:
            return set()
        return set(books_df[asin_col].dropna().astype(str).tolist())

    def _get_book_indices_for_asins(self, parent_asins: Iterable[str]) -> List[int]:
        """Translate parent ASINs into fitted TF-IDF row indices."""
        if self.book_id_to_idx is None:
            raise RuntimeError("Call fit() before using the recommender.")
        indices: List[int] = []
        for asin in parent_asins:
            idx = self.book_id_to_idx.get(str(asin))
            if idx is not None:
                indices.append(idx)
        return indices

    def build_user_profile(
        self,
        user_id: Any,
        user_genres_df: pd.DataFrame,
        user_books_df: pd.DataFrame,
    ) -> np.ndarray:
        """Build a normalized user preference vector for recommendation scoring.

        Args:
            user_id: User identifier used to filter input frames.
            user_genres_df: DataFrame of genre preferences/ranks.
            user_books_df: DataFrame of user read/saved book ASINs.

        Returns:
            np.ndarray: Dense profile vector aligned with the TF-IDF feature space.

        Exceptions:
            RuntimeError: If recommender artifacts have not been fitted/loaded.
        """
        if self.tfidf_vectorizer is None or self.book_tfidf is None:
            raise RuntimeError("Call fit() before building user profiles.")

        genres_vec, has_genres = self._build_genres_vector(user_id, user_genres_df)

        if has_genres and np.linalg.norm(genres_vec) > 0:
            genres_vec = genres_vec / (np.linalg.norm(genres_vec) + 1e-12)

        read_asins = self._get_read_parent_asins(user_id, user_books_df)
        read_indices = self._get_book_indices_for_asins(read_asins)
        has_history = len(read_indices) > 0

        history_vec = np.zeros(len(GENRE_VOCAB), dtype=float)
        if has_history:
            history_mat = self.book_tfidf[read_indices]
            history_mean = np.asarray(history_mat.mean(axis=0)).reshape(-1)
            if np.linalg.norm(history_mean) > 0:
                history_vec = history_mean / (np.linalg.norm(history_mean) + 1e-12)

        if has_genres and has_history:
            return 0.7 * genres_vec + 0.3 * history_vec
        if has_history and not has_genres:
            return history_vec
        return genres_vec

    def recommend(
        self,
        user_id: str,
        user_genres_df: Optional[pd.DataFrame] = None,
        user_books_df: Optional[pd.DataFrame] = None,
        top_k: int = 40,
    ) -> List[Dict[str, Any]]:
        """Generate top-K recommendations for a user.

        Args:
            user_id: User identifier.
            user_genres_df: Optional genre-preference rows for the user.
            user_books_df: Optional user reading-history rows.
            top_k: Number of recommendations to return.

        Returns:
            list[dict[str, Any]]: Ranked recommendation payloads.

        Exceptions:
            RuntimeError: If recommender artifacts are not loaded.
        """
        if self.book_tfidf is None:
            raise RuntimeError("Call fit() before calling recommend().")

        read_asins = self._get_read_parent_asins(user_id, user_books_df)
        n_books = self.book_tfidf.shape[0]

        if self.books_df is not None:
            books_df = self.books_df
            exclude_mask = books_df["parent_asin"].astype(str).isin(
                {str(a) for a in read_asins}
            )
            rating_norm = books_df["average_rating_norm"].to_numpy(dtype=float)
            rating_number_norm = books_df["rating_number_norm"].to_numpy(dtype=float)
        else:
            exclude_mask = np.zeros(n_books, dtype=bool)
            for asin in read_asins:
                idx = self.book_id_to_idx.get(str(asin))
                if idx is not None:
                    exclude_mask[idx] = True
            rating_norm = self._rating_norms["average_rating"]
            rating_number_norm = self._rating_norms["rating_number"]
        if rating_norm is None or rating_number_norm is None:
            raise RuntimeError("Rating norms not loaded.")

        cold_start = self._is_cold_start(user_id, user_genres_df, user_books_df)

        if cold_start:
            scores = 0.7 * rating_norm + 0.3 * rating_number_norm
        else:
            profile = self.build_user_profile(
                user_id=user_id,
                user_genres_df=user_genres_df if user_genres_df is not None else pd.DataFrame(),
                user_books_df=user_books_df if user_books_df is not None else pd.DataFrame(),
            )
            sim = cosine_similarity(
                profile.reshape(1, -1), self.book_tfidf
            ).reshape(-1)
            if len(read_asins) > 0:
                sim = sim * 1.5
            scores = (
                self.weights.genre_similarity * sim
                + self.weights.average_rating * rating_norm
                + self.weights.rating_number_popularity * rating_number_norm
            )

        scores = np.where(exclude_mask, -np.inf, scores)

        k = int(top_k) if top_k is not None else 40
        k = max(1, k)
        candidate_idx = np.where(np.isfinite(scores))[0]
        if candidate_idx.size == 0:
            return []
        k = min(k, candidate_idx.size)
        candidate_scores = scores[candidate_idx]
        top_local = np.argpartition(-candidate_scores, kth=k - 1)[:k]
        top_local = top_local[np.argsort(-candidate_scores[top_local])]
        top_idx = candidate_idx[top_local]

        if self.books_df is not None:
            out = []
            for i in top_idx.tolist():
                row = self.books_df.iloc[int(i)]
                categories_list = row.get("categories_list", [])
                if not isinstance(categories_list, list):
                    categories_list = []
                asin_str = (
                    ""
                    if pd.isna(row.get("parent_asin"))
                    else str(row.get("parent_asin"))
                )
                out.append({
                    "book_id": asin_str,
                    "parent_asin": asin_str,
                    "title": "" if pd.isna(row.get("title")) else str(row.get("title")),
                    "author_name": (
                        None if pd.isna(row.get("author_name")) else str(row.get("author_name"))
                    ),
                    "average_rating": float(row.get("average_rating") or 0.0),
                    "rating_number": int(row.get("rating_number") or 0),
                    "images": None if pd.isna(row.get("images")) else row.get("images"),
                    "categories": [str(x) for x in categories_list],
                    "score": float(scores[int(i)]),
                })
            return out

        idx_to_asin = [None] * n_books
        for asin, idx in (self.book_id_to_idx or {}).items():
            if 0 <= idx < n_books:
                idx_to_asin[idx] = asin
        asins = [idx_to_asin[i] for i in top_idx.tolist() if idx_to_asin[i]]
        metadata_list = self._fetch_metadata_for_asins(asins)
        meta_by_asin = {str(m.get("parent_asin", "")): m for m in metadata_list}
        out = []
        for i in top_idx.tolist():
            asin_str = idx_to_asin[i] if i < len(idx_to_asin) else ""
            m = meta_by_asin.get(asin_str) or {}
            cats = m.get("categories_list") or m.get("categories") or []
            if isinstance(cats, str):
                cats = [cats]
            out.append({
                "book_id": asin_str,
                "parent_asin": asin_str,
                "title": m.get("title") or "",
                "author_name": m.get("author_name"),
                "average_rating": float(m.get("average_rating") or 0.0),
                "rating_number": int(m.get("rating_number") or 0),
                "images": m.get("images"),
                "categories": [str(x) for x in cats],
                "score": float(scores[int(i)]),
            })
        return out

    def recommend_for_user(
        self,
        user_email: str,
        user_account: Dict[str, Any],
        user_genres: Optional[List[Dict[str, Any]]],
        top_k: int = 40,
    ) -> List[Dict[str, Any]]:
        """Convenience wrapper to recommend directly from user account payloads.

        Args:
            user_email: User identifier/email.
            user_account: User account dict containing library shelves.
            user_genres: Optional ordered genre preferences.
            top_k: Number of recommendations to return.

        Returns:
            list[dict[str, Any]]: Ranked recommendation payloads.

        Exceptions:
            RuntimeError: Propagated if underlying recommend() is not initialized.
        """
        library = user_account.get("library") or {}
        finished = library.get("finished") or []
        saved = library.get("saved") or []
        in_progress = library.get("in_progress") or []
        finished_ids = list(dict.fromkeys(finished + saved + in_progress))

        if finished_ids:
            user_books_df = pd.DataFrame(
                {
                    "user_id": [user_email] * len(finished_ids),
                    "parent_asin": list(finished_ids),
                }
            )
        else:
            user_books_df = None

        if user_genres:
            genres_df = pd.DataFrame(
                {
                    "user_id": [user_email] * len(user_genres),
                    "genre": [g.get("genre") for g in user_genres],
                    "rank": [g.get("rank") for g in user_genres],
                }
            )
        else:
            genres_df = None

        return self.recommend(
            user_id=user_email,
            user_genres_df=genres_df,
            user_books_df=user_books_df,
            top_k=top_k,
        )


class _FallbackBookRecommender:
    """When books.db is missing: return top 50 from reviews JSON (exclude owned)."""

    def recommend(
        self,
        user_book_ids: List[str],
        top_k: int = 50,
    ) -> List[Dict[str, Any]]:
        """Return popular fallback recommendations excluding already-owned books.

        Args:
            user_book_ids: Parent ASINs already in the user's library.
            top_k: Number of books to return.

        Returns:
            list[dict[str, Any]]: Popular fallback books.

        Exceptions:
            None. Storage errors are handled by fallback list retrieval.
        """
        store = backend_storage.get_storage()
        books = store.get_top50_review_books() or []
        owned = {str(b) for b in (user_book_ids or [])}
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
        """Fallback wrapper using user library shelves as exclusion history.

        Args:
            user_email: User identifier/email (unused but kept for parity).
            user_account: User account dict containing library shelves.
            user_genres: Genre preferences (unused in fallback mode).
            top_k: Number of books to return.

        Returns:
            list[dict[str, Any]]: Popular fallback books.

        Exceptions:
            None.
        """
        _ = user_email, user_genres
        library = user_account.get("library") or {}
        finished = library.get("finished") or []
        saved = library.get("saved") or []
        in_progress = library.get("in_progress") or []
        user_book_ids = list(finished) + list(saved) + list(in_progress)
        return self.recommend(user_book_ids, top_k=top_k)


_RECOMMENDER_CACHE: dict[str, Any] = {"instance": None, "using_fallback": True}


def _get_recommender() -> tuple[Any, bool]:
    """Return (recommender instance, is_fallback)."""
    if _RECOMMENDER_CACHE["instance"] is not None:
        return _RECOMMENDER_CACHE["instance"], bool(_RECOMMENDER_CACHE["using_fallback"])
    try:
        rec = ContentBasedBookRecommender()
        rec.fit()
        _RECOMMENDER_CACHE["instance"] = rec
        _RECOMMENDER_CACHE["using_fallback"] = False
        return rec, False
    except (RuntimeError, ValueError, TypeError, OSError) as e:
        logging.warning(
            "Content-based recommender failed to load (missing books.db?); using fallback. %s",
            e,
        )
        return _FallbackBookRecommender(), True


class BookRecommender(ContentBasedBookRecommender):
    """Compatibility recommender supporting both legacy and content-based call styles."""

    def __init__(self) -> None:
        """Initialize a content-based instance plus a lazy legacy delegate."""
        super().__init__()
        self._legacy_delegate: Any | None = None

    def _delegate(self) -> Any:
        """Return lazily loaded delegate used by legacy recommend(list[str]) calls."""
        if self._legacy_delegate is None:
            self._legacy_delegate, _ = _get_recommender()
        return self._legacy_delegate

    def recommend(self, *args: Any, **kwargs: Any) -> List[Dict[str, Any]]:
        """Recommend books using either legacy or content-based signature.

        Supported signatures:
        - recommend(user_book_ids: list[str], top_k: int=50)  [legacy]
        - recommend(user_id: str, user_genres_df=None, user_books_df=None, top_k=40)
        """
        first_arg = args[0] if args else None
        is_legacy = isinstance(first_arg, list) and "user_id" not in kwargs
        if is_legacy:
            top_k = int(kwargs.get("top_k", args[1] if len(args) > 1 else 50))
            return self._delegate().recommend(first_arg, top_k=top_k)
        return super().recommend(*args, **kwargs)

    def recommend_for_user(
        self,
        user_email: str,
        user_account: Dict[str, Any],
        user_genres: Optional[List[Dict[str, Any]]],
        top_k: int = 40,
    ) -> List[Dict[str, Any]]:
        """Delegate account-based recommendations to the active recommender."""
        return self._delegate().recommend_for_user(
            user_email=user_email,
            user_account=user_account,
            user_genres=user_genres,
            top_k=top_k,
        )

    @classmethod
    def using_fallback(cls) -> bool:
        """Return whether the cached recommender currently uses fallback mode."""
        return bool(_RECOMMENDER_CACHE.get("using_fallback"))
