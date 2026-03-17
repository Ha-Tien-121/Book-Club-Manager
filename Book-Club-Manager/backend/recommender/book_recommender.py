"""Book recommender for the Bookish backend.

Scoring uses the trained logistic regression model from
``book_recommender_fitting.py``. When model artifacts are unavailable the
recommender degrades gracefully to a popularity-based fallback so the
application always returns results.

Scoring modes (in priority order):

1. **LR model** (non-cold-start, artifacts present): ranks candidates by the
   predicted probability that a user will interact with a book, using three
   features — library-normalised similarity, log-popularity, and a log
   interaction term between similarity and library size.

2. **Popularity fallback** (cold-start *or* model artifacts missing): ranks
   candidates by ``0.7 * average_rating_norm + 0.3 * rating_count_norm``.

Required artifacts (produced by ``book_recommender_fitting.py`` and the
data processing pipeline):

* ``<RECOMMENDER_DIR>/book_recommender_model.pkl`` — trained ``LogisticRegression``
* ``<RECOMMENDER_DIR>/feature_scaler.pkl``          — fitted ``StandardScaler``
* ``<data_dir>/book_similarity.npz``                — sparse book-book cosine similarity matrix
* ``<data_dir>/book_ratings.npz``                   — ``ratings_avg`` and ``log_number_ratings`` arrays
* ``<data_dir>/book_id_to_idx.json``                — mapping from parent_asin to matrix column index
* ``<data_dir>/book_rating_norms.npz``              — ``average_rating_norm`` and ``rating_number_norm``
  arrays used by the popularity fallback
"""

from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Set

import joblib
import numpy as np
import pandas as pd
from scipy import sparse

from backend.config import PROCESSED_DIR
from backend.recommender.config import RECOMMENDER_DIR


def _safe_json_loads(value: Any) -> Any:
    """Safely parse *value* as JSON, returning the original on failure.

    Handles ``None``, already-parsed lists/dicts, byte strings, and plain
    strings. Never raises.
    """
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


def _infer_column(df: pd.DataFrame, candidates: List[str]) -> Optional[str]:
    """Return the first column name in *df* that case-insensitively matches
    one of *candidates*, or ``None`` if none match.

    Args:
        df: DataFrame whose columns are searched.
        candidates: Ordered list of preferred column name candidates.

    Returns:
        The actual column name as it appears in *df*, or ``None``.
    """
    lower_to_actual = {c.lower(): c for c in df.columns}
    for cand in candidates:
        if cand.lower() in lower_to_actual:
            return lower_to_actual[cand.lower()]
    return None


class BookRecommender:
    """Recommender that scores books with a trained logistic regression model.

    The model was trained in ``book_recommender_fitting.py`` on implicit
    user-book interaction data. At recommendation time the same three features
    are recomputed per (user, candidate-book) pair and fed to the model's
    ``predict_proba`` method.

    When model artifacts are absent the recommender falls back to a simple
    popularity score so the application remains functional during local
    development or before the first training run.

    Attributes:
        data_dir: Directory containing processed data artifacts.
    """

    def __init__(self, data_dir: Optional[Path] = None) -> None:
        """Initialise the recommender without loading any artifacts.

        Call :meth:`fit` before calling :meth:`recommend` or
        :meth:`recommend_for_user`.

        Args:
            data_dir: Path to the processed data directory. Defaults to
                ``PROCESSED_DIR`` from ``backend.config``.
        """
        self.data_dir = Path(data_dir) if data_dir is not None else PROCESSED_DIR

        self._book_id_to_idx: Optional[Dict[str, int]] = None

        self._rating_norm: Optional[np.ndarray] = None
        self._rating_number_norm: Optional[np.ndarray] = None


        self._lr_model: Optional[Any] = None
        self._lr_scaler: Optional[Any] = None
        self._book_similarity_matrix: Optional[sparse.csc_matrix] = None
        self._book_avg_ratings: Optional[np.ndarray] = None
        self._book_num_ratings: Optional[np.ndarray] = None

    def fit(self) -> None:
        """Load all artifacts from disk.

        Loads the book index and popularity norms (always required), then
        attempts to load the LR model artifacts. A warning is emitted if any
        model artifact is missing so the fallback path is always visible in
        logs.

        Raises:
            FileNotFoundError: If the book index or rating norms are absent,
                since those are required for both scoring paths.
        """
        self._load_catalog_artifacts()
        self._load_lr_model_artifacts()

    def recommend(
        self,
        user_id: str,
        user_books_df: Optional[pd.DataFrame] = None,
        top_k: int = 40,
    ) -> List[Dict[str, Any]]:
        """Return the top-*k* recommended books for a user.

        Scores every unread book in the catalog using either the trained LR
        model (when artifacts are loaded and the user has library history) or
        a popularity fallback, then returns the top-*k* results with metadata.

        Args:
            user_id: Identifier used to filter rows from *user_books_df*.
            user_books_df: DataFrame with at least a ``parent_asin`` (or
                ``asin`` / ``book_id``) column listing books the user has
                already read. Rows are expected to match *user_id* via a
                ``user_id`` column; if no such column exists every row is used.
                Pass ``None`` for cold-start users.
            top_k: Maximum number of recommendations to return.

        Returns:
            List of dicts, each containing ``book_id``, ``parent_asin``,
            ``title``, ``author_name``, ``average_rating``, ``rating_number``,
            ``images``, ``categories``, and ``score``. Sorted descending by
            ``score``. May be shorter than *top_k* if the catalog is small or
            many books are already read.

        Raises:
            RuntimeError: If :meth:`fit` has not been called.
        """
        if self._book_id_to_idx is None:
            raise RuntimeError("Call fit() before calling recommend().")

        n_books = len(self._book_id_to_idx)
        read_asins = self._get_read_parent_asins(user_id, user_books_df)

        exclude_mask = np.zeros(n_books, dtype=bool)
        for asin in read_asins:
            idx = self._book_id_to_idx.get(str(asin))
            if idx is not None:
                exclude_mask[idx] = True

        cold_start = len(read_asins) == 0
        use_lr_model = (
            not cold_start
            and self._lr_model is not None
            and self._lr_scaler is not None
            and self._book_similarity_matrix is not None
            and self._book_avg_ratings is not None
            and self._book_num_ratings is not None
        )

        if use_lr_model:
            scores = self._score_with_lr_model(
                user_library_indices=self._get_book_indices_for_asins(read_asins),
                candidate_indices=np.where(~exclude_mask)[0],
                n_books=n_books,
            )
        else:
            scores = self._score_with_popularity(n_books)

        scores = np.where(exclude_mask, -np.inf, scores)

        return self._top_k_results(scores, top_k)

    def recommend_for_user(
        self,
        user_email: str,
        user_account: Dict[str, Any],
        user_genres: Optional[List[Dict[str, Any]]],
        top_k: int = 40,
    ) -> List[Dict[str, Any]]:
        """Convenience wrapper that extracts library data from an account dict.

        Combines ``finished``, ``saved``, and ``in_progress`` lists from
        *user_account* into a single ``user_books_df`` and delegates to
        :meth:`recommend`. The *user_genres* argument is accepted for API
        compatibility but is not used by the LR model.

        Args:
            user_email: User identifier (used as ``user_id``).
            user_account: Account dict containing a ``library`` key with
                ``finished``, ``saved``, and ``in_progress`` sublists.
            user_genres: Unused; kept for interface compatibility.
            top_k: Maximum number of recommendations to return.

        Returns:
            See :meth:`recommend`.
        """
        library = user_account.get("library") or {}
        finished = library.get("finished") or []
        saved = library.get("saved") or []
        in_progress = library.get("in_progress") or []
        all_ids = list(dict.fromkeys(finished + saved + in_progress))

        user_books_df = (
            pd.DataFrame({"user_id": [user_email] * len(all_ids), "parent_asin": all_ids})
            if all_ids
            else None
        )

        return self.recommend(
            user_id=user_email,
            user_books_df=user_books_df,
            top_k=top_k,
        )

    def _load_catalog_artifacts(self) -> None:
        """Load the book index and rating norm arrays from disk.

        These are required by both the LR model path and the popularity
        fallback, so a missing file raises immediately.

        Raises:
            FileNotFoundError: If ``book_id_to_idx.json`` or
                ``book_rating_norms.npz`` do not exist in ``data_dir``.
        """
        idx_path = self.data_dir / "book_id_to_idx.json"
        norms_path = self.data_dir / "book_rating_norms.npz"

        if not idx_path.exists():
            raise FileNotFoundError(f"Book index not found: {idx_path}")
        if not norms_path.exists():
            raise FileNotFoundError(f"Rating norms not found: {norms_path}")

        with idx_path.open("r", encoding="utf-8") as f:
            self._book_id_to_idx = json.load(f)

        data = np.load(str(norms_path))
        self._rating_norm = data["average_rating_norm"]
        self._rating_number_norm = data["rating_number_norm"]

        logging.info("Loaded book catalog: %d books.", len(self._book_id_to_idx))

    def _load_lr_model_artifacts(self) -> None:
        """Attempt to load the trained LR model and its supporting files.

        All four files must be present for the LR model path to activate:

        * ``<RECOMMENDER_DIR>/book_recommender_model.pkl``
        * ``<RECOMMENDER_DIR>/feature_scaler.pkl``
        * ``<data_dir>/book_similarity.npz``
        * ``<data_dir>/book_ratings.npz``

        Emits a ``WARNING`` naming every missing file when any are absent, so
        the fallback is always visible in logs. Emits ``INFO`` on success.
        """
        self._lr_model = None
        self._lr_scaler = None
        self._book_similarity_matrix = None
        self._book_avg_ratings = None
        self._book_num_ratings = None

        model_path = Path(RECOMMENDER_DIR) / "book_recommender_model.pkl"
        scaler_path = Path(RECOMMENDER_DIR) / "feature_scaler.pkl"
        sim_path = self.data_dir / "book_similarity.npz"
        ratings_path = self.data_dir / "book_ratings.npz"

        missing = [
            str(p) for p in (model_path, scaler_path, sim_path, ratings_path)
            if not p.exists()
        ]
        if missing:
            logging.warning(
                "LR recommender model not loaded; using popularity fallback. "
                "Missing files: %s",
                ", ".join(missing),
            )
            return

        try:
            self._lr_model = joblib.load(str(model_path))
            self._lr_scaler = joblib.load(str(scaler_path))
            self._book_similarity_matrix = sparse.load_npz(str(sim_path)).tocsc()
            npzfile = np.load(str(ratings_path))
            self._book_avg_ratings = npzfile["ratings_avg"]
            self._book_num_ratings = npzfile["log_number_ratings"]
            logging.info("Loaded trained LR recommender model.")
        except Exception as exc:
            logging.warning(
                "Failed to load LR model artifacts; using popularity fallback. %s", exc
            )
            self._lr_model = None

    # ------------------------------------------------------------------
    # Scoring
    # ------------------------------------------------------------------

    def _score_with_lr_model(
        self,
        user_library_indices: List[int],
        candidate_indices: np.ndarray,
        n_books: int,
    ) -> np.ndarray:
        """Score candidate books with the trained logistic regression model.

        Replicates the three features used during training
        (see ``book_recommender_fitting.py``):

        1. **similarity** — mean cosine similarity between the candidate and
           every book in the user's library, computed via
           ``_book_similarity_matrix`` and normalised by library size.
        2. **popularity** — ``log1p(avg_rating * num_ratings)`` of the candidate.
        3. **interaction** — ``log1p(similarity * log1p(library_size))``.

        Non-candidate positions (already-read books) are left as ``-inf``.

        Args:
            user_library_indices: Row indices into the similarity matrix for
                books the user has already read.
            candidate_indices: 1-D array of book indices to score.
            n_books: Total number of books in the catalog (size of output array).

        Returns:
            Float64 array of length *n_books* with predicted positive-class
            probabilities at candidate positions and ``-inf`` elsewhere.
        """
        library_size = max(len(user_library_indices), 1)
        log_library_size = np.log1p(library_size)
        n_candidates = len(candidate_indices)

        sim_scores = np.zeros(n_candidates, dtype=np.float32)
        if user_library_indices:
            lib_rows = self._book_similarity_matrix[user_library_indices, :]
            sim_scores = (
                np.asarray(lib_rows[:, candidate_indices].sum(axis=0)).reshape(-1)
                / library_size
            )

        popularity = np.log1p(
            self._book_avg_ratings[candidate_indices]
            * self._book_num_ratings[candidate_indices]
        )
        interaction = np.log1p(sim_scores * log_library_size)

        features = np.column_stack([sim_scores, popularity, interaction]).astype(np.float32)
        features_scaled = self._lr_scaler.transform(features)
        proba = self._lr_model.predict_proba(features_scaled)[:, 1]

        scores = np.full(n_books, -np.inf, dtype=np.float64)
        scores[candidate_indices] = proba
        return scores

    def _score_with_popularity(self, n_books: int) -> np.ndarray:
        """Score all books by popularity when the LR model is unavailable.

        Uses a weighted combination of normalised average rating and normalised
        rating count: ``0.7 * rating_norm + 0.3 * rating_count_norm``.

        Used for cold-start users (no reading history) and whenever model
        artifacts have not been loaded.

        Args:
            n_books: Expected length of the output array; used only for the
                ``RuntimeError`` guard below.

        Returns:
            Float64 array of length *n_books* with a popularity score for
            every book.

        Raises:
            RuntimeError: If rating norm arrays have not been loaded.
        """
        if self._rating_norm is None or self._rating_number_norm is None:
            raise RuntimeError("Rating norms not loaded; call fit() first.")
        return (0.7 * self._rating_norm + 0.3 * self._rating_number_norm).astype(np.float64)

    def _get_read_parent_asins(
        self,
        user_id: Any,
        user_books_df: Optional[pd.DataFrame],
    ) -> Set[str]:
        """Extract the set of parent ASINs that *user_id* has already read.

        Args:
            user_id: Identifier to filter rows from *user_books_df*.
            user_books_df: DataFrame containing book interaction records. A
                ``user_id`` column (or synonym) is used for filtering when
                present; otherwise all rows are treated as belonging to the
                user.

        Returns:
            Set of parent ASIN strings. Empty set when *user_books_df* is
            ``None`` or empty.
        """
        if user_books_df is None or user_books_df.empty:
            return set()

        df = user_books_df
        uid_col = _infer_column(df, ["user_id", "user", "uid"])
        if uid_col is not None:
            df = df[df[uid_col].astype(str) == str(user_id)]

        asin_col = _infer_column(df, ["parent_asin", "asin", "book_id", "book_asin"])
        if asin_col is None:
            return set()
        return set(df[asin_col].dropna().astype(str).tolist())

    def _get_book_indices_for_asins(self, parent_asins: Iterable[str]) -> List[int]:
        """Map an iterable of parent ASINs to their integer catalog indices.

        ASINs not present in ``_book_id_to_idx`` are silently skipped.

        Args:
            parent_asins: Parent ASIN strings to look up.

        Returns:
            List of integer indices, one per recognised ASIN.

        Raises:
            RuntimeError: If :meth:`fit` has not been called.
        """
        if self._book_id_to_idx is None:
            raise RuntimeError("Call fit() before using the recommender.")
        return [
            idx
            for asin in parent_asins
            if (idx := self._book_id_to_idx.get(str(asin))) is not None
        ]

    def _top_k_results(
        self,
        scores: np.ndarray,
        top_k: int,
    ) -> List[Dict[str, Any]]:
        """Select the top-*k* books by score and fetch their metadata.

        Args:
            scores: Float array of length ``n_books``. Books with ``-inf``
                score are excluded (already-read or unscored).
            top_k: Maximum number of results to return.

        Returns:
            List of book metadata dicts sorted descending by score.
        """
        k = max(1, int(top_k))
        candidate_idx = np.where(np.isfinite(scores))[0]
        if candidate_idx.size == 0:
            return []

        k = min(k, candidate_idx.size)
        candidate_scores = scores[candidate_idx]
        top_local = np.argpartition(-candidate_scores, kth=k - 1)[:k]
        top_local = top_local[np.argsort(-candidate_scores[top_local])]
        top_idx = candidate_idx[top_local]

        n_books = len(scores)
        idx_to_asin: List[Optional[str]] = [None] * n_books
        for asin, idx in (self._book_id_to_idx or {}).items():
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

    def _fetch_metadata_for_asins(self, asin_list: List[str]) -> List[Dict[str, Any]]:
        """Fetch book metadata for a list of parent ASINs.

        Tries the local ``books.db`` SQLite database first, then falls back to
        the configured storage backend (DynamoDB / S3 on AWS).

        Args:
            asin_list: Parent ASIN strings to look up.

        Returns:
            List of dicts with keys ``parent_asin``, ``title``,
            ``author_name``, ``average_rating``, ``rating_number``,
            ``images``, ``categories``, ``categories_list``. Missing ASINs
            are silently omitted. Returns an empty list on any error.
        """
        if not asin_list:
            return []

        db_path = self.data_dir / "books.db"
        if not db_path.exists():
            return self._fetch_metadata_from_storage(asin_list)

        import sqlite3

        placeholders = ",".join(["?"] * len(asin_list))
        query = f"""
            SELECT parent_asin, title, author_name, average_rating,
                   rating_number, images, categories
            FROM books WHERE parent_asin IN ({placeholders})
        """
        try:
            with sqlite3.connect(str(db_path)) as conn:
                rows = conn.execute(query, asin_list).fetchall()
        except Exception as exc:
            logging.warning("books.db query failed: %s", exc)
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
                parsed = _safe_json_loads(cats)
                d["categories_list"] = (
                    [str(x) for x in parsed] if isinstance(parsed, list) else []
                )
            else:
                d["categories_list"] = d.get("categories_list") or []
            out.append(d)
        return out

    def _fetch_metadata_from_storage(self, asin_list: List[str]) -> List[Dict[str, Any]]:
        """Fetch book metadata from the configured storage backend.

        Used when ``books.db`` is absent, e.g. on AWS where metadata lives in
        DynamoDB or S3.

        Args:
            asin_list: Parent ASIN strings to look up.

        Returns:
            List of metadata dicts (same schema as :meth:`_fetch_metadata_for_asins`).
            Returns an empty list if the storage backend is unavailable or
            does not support ``get_books_metadata_batch``.
        """
        try:
            from backend.storage import get_storage

            store = get_storage()
            if not hasattr(store, "get_books_metadata_batch"):
                return []
            batch = store.get_books_metadata_batch(asin_list) or {}
            out = []
            for asin in asin_list:
                m = batch.get(str(asin))
                if not m:
                    continue
                cats = m.get("categories") or m.get("categories_list") or []
                if isinstance(cats, str):
                    cats = _safe_json_loads(cats) or []
                if not isinstance(cats, list):
                    cats = []
                out.append({
                    "parent_asin": str(asin),
                    "title": m.get("title") or "",
                    "author_name": m.get("author_name") or m.get("author"),
                    "average_rating": float(m.get("average_rating") or 0),
                    "rating_number": int(
                        m.get("rating_number") or m.get("rating_count") or 0
                    ),
                    "images": m.get("images"),
                    "categories": m.get("categories"),
                    "categories_list": [str(x) for x in cats],
                })
            return out
        except Exception as exc:
            logging.warning("Storage metadata fetch failed: %s", exc)
            return []


class _FallbackBookRecommender:
    """Last-resort recommender used when the main recommender fails to initialise.

    Returns the top-*k* books from the pre-built review JSON, excluding any
    books the user already owns. No model or database required.
    """

    def recommend(
        self,
        user_book_ids: List[str],
        top_k: int = 50,
    ) -> List[Dict[str, Any]]:
        """Return top-*k* popular books excluding those in *user_book_ids*.

        Args:
            user_book_ids: Parent ASINs the user already owns.
            top_k: Maximum number of results to return.

        Returns:
            List of book dicts from the review JSON, sorted by pre-existing
            order (assumed to be popularity-ranked).
        """
        from backend.storage import get_storage

        store = get_storage()
        books = store.get_top50_review_books() or []
        owned = {str(b) for b in (user_book_ids or [])}
        books = [b for b in books if str(b.get("parent_asin", "")) not in owned]
        return list(books)[: max(0, top_k)]

    def recommend_for_user(
        self,
        user_email: str,
        user_account: Dict[str, Any],
        user_genres: Optional[List[Dict[str, Any]]],
        top_k: int = 40,
    ) -> List[Dict[str, Any]]:
        """Convenience wrapper matching the :class:`BookRecommender` interface.

        Args:
            user_email: User identifier (unused beyond interface compatibility).
            user_account: Account dict containing a ``library`` key.
            user_genres: Unused; kept for interface compatibility.
            top_k: Maximum number of results to return.

        Returns:
            See :meth:`recommend`.
        """
        library = user_account.get("library") or {}
        finished = library.get("finished") or []
        saved = library.get("saved") or []
        in_progress = library.get("in_progress") or []
        user_book_ids = list(finished) + list(saved) + list(in_progress)
        return self.recommend(user_book_ids, top_k=top_k)


_cached_recommender: Optional[BookRecommender] = None


def _get_recommender() -> Any:
    """Return the module-level recommender singleton, initialising it on first call.

    Attempts to create and fit a :class:`BookRecommender`. Falls back to
    :class:`_FallbackBookRecommender` if initialisation fails (e.g. missing
    catalog artifacts).

    Returns:
        A fitted :class:`BookRecommender`, or a :class:`_FallbackBookRecommender`
        if the main recommender could not be loaded.
    """
    global _cached_recommender
    if _cached_recommender is not None:
        return _cached_recommender
    try:
        rec = BookRecommender()
        rec.fit()
        _cached_recommender = rec
        return rec
    except Exception as exc:
        logging.warning(
            "BookRecommender failed to initialise; using fallback. %s", exc
        )
        return _FallbackBookRecommender()


def get_recommender() -> Any:
    """Return the active recommender instance.

    Returns a fitted :class:`BookRecommender` when all catalog artifacts are
    present, otherwise a :class:`_FallbackBookRecommender`.

    Returns:
        Recommender instance with ``recommend`` and ``recommend_for_user``
        methods.
    """
    return _get_recommender()
