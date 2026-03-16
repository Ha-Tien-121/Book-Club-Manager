"""Book recommender.

Same pattern as event_recommender: one class BookRecommender with
recommend(user_book_ids, top_k). If the ML model fails to load, returns
reviews_top50_books from storage (get_top50_review_books) instead.
"""

from __future__ import annotations

import json
import logging
import os
import sqlite3
from io import BytesIO
from pathlib import Path
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


# In-memory ML artifact cache (populated from S3 when APP_ENV=aws).
_ML_ARTIFACT_BYTES: dict[str, bytes] = {}


def _use_in_memory_ml_artifacts() -> bool:
    """Return True when artifacts should be pulled into memory from S3.

    This is intended for AWS deployments where the runtime filesystem may not
    include the ML artifacts (containers/ephemeral hosts).
    """
    val = (os.getenv("ML_ARTIFACTS_IN_MEMORY") or "").strip().lower()
    if val in ("0", "false", "no"):
        return False
    if val in ("1", "true", "yes"):
        return True
    # Default: use in-memory mode in AWS.
    return (os.getenv("APP_ENV") or "").strip().lower() == "aws"


def _get_ml_artifact_bytes(*, bucket: str, key: str, region: str | None) -> bytes | None:
    """Fetch an artifact from S3 and memoize it in-process."""
    if not key:
        return None
    if key in _ML_ARTIFACT_BYTES:
        return _ML_ARTIFACT_BYTES[key]
    try:
        import boto3

        s3 = boto3.client("s3", region_name=region)
        obj = s3.get_object(Bucket=bucket, Key=key)
        data = obj["Body"].read()
        if data:
            _ML_ARTIFACT_BYTES[key] = data
        return data
    except Exception as e:
        logging.warning("Failed to fetch ML artifact s3://%s/%s: %s", bucket, key, e)
        return None


def _maybe_download_ml_artifacts_from_s3() -> None:
    """Preload ML artifacts from S3 into memory when running in AWS mode.

    This function intentionally does not write to disk. The recommender will
    load from these bytes via BytesIO.
    """
    try:
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
                beta = clf.coef_[0]
                scaler = joblib.load(BytesIO(scaler_bytes))
                self.beta_scaled = beta / scaler.scale_
                try:
                    self.similarity_boost = float(os.getenv("BOOK_SIMILARITY_BOOST", "1.0"))
                except (TypeError, ValueError):
                    self.similarity_boost = 1.0
                self.book_similarity: csr_matrix = load_npz(BytesIO(sim_bytes)).tocsr()
                ratings = np.load(BytesIO(ratings_bytes))
                self.book_avg_ratings = ratings["ratings_avg"].astype(np.float32)
                self.book_num_ratings = np.log1p(ratings["log_number_ratings"]).astype(np.float32)
                self.book_id_to_idx = json.loads(idmap_bytes.decode("utf-8"))
                self.idx_to_book_id = {v: k for k, v in self.book_id_to_idx.items()}
                return

            # Local filesystem mode.
            clf = joblib.load(_MODEL_FILE)
            beta = clf.coef_[0]
            scaler = joblib.load(_MODEL_SCALER_FILE)
            self.beta_scaled = beta / scaler.scale_
            # Heuristic boost to make similarity matter more at inference time.
            # Some trained coefficients can end up heavily favoring global popularity
            # features, which makes recommendations look "stuck" even as the user's
            # library grows. This boost is intentionally small by default and can
            # be tuned via env var without retraining the model.
            try:
                self.similarity_boost = float(os.getenv("BOOK_SIMILARITY_BOOST", "1.0"))
            except (TypeError, ValueError):
                self.similarity_boost = 1.0
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
            owned = {str(b).strip() for b in (user_book_ids or []) if str(b).strip()}
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
            if lib_size > 0 and self.similarity_boost:
                scores = scores + (self.similarity_boost * sim)
            if lib_size > 0:
                scores[book_indices] = -np.inf
            # Pull a larger pool, then filter out owned IDs by string. This makes
            # recommendations respond even when user_book_ids contain values that
            # don't line up perfectly with the model's index mapping.
            desired = max(0, int(top_k))
            if desired <= 0:
                return []
            pool = min(len(scores), max(desired + len(owned) + 50, desired * 3))
            top_idx = np.argpartition(scores, -pool)[-pool:]
            top_idx = top_idx[np.argsort(scores[top_idx])[::-1]]
            book_ids = [self.idx_to_book_id[i] for i in top_idx]
            if owned:
                book_ids = [bid for bid in book_ids if str(bid).strip() not in owned]
            return self._fetch_books(book_ids[:desired])

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
                    # Prefer batch metadata fetch when available (much faster than N get_item calls).
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
    _MLRecommenderClass = None  # type: ignore[assignment]


class _FallbackBookRecommender:
    """When ML fails to load: return reviews_top50_books from storage."""

    def recommend(
        self,
        user_book_ids: List[str],
        top_k: int = 50,
    ) -> List[dict[str, Any]]:
        books = self._get_review_books()
        # Exclude books already in the user's library so recommendations change
        # as the user adds books, even when using the fallback (no ML model).
        if user_book_ids:
            owned = {str(b) for b in user_book_ids}
            books = [
                b for b in books
                if str(b.get("parent_asin", "")) not in owned
            ]
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
    _maybe_download_ml_artifacts_from_s3()
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
