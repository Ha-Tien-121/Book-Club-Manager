from __future__ import annotations
from dataclasses import dataclass
import json
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence
import numpy as np
import pandas as pd

try:
    from scipy.sparse import hstack
    from sklearn.feature_extraction.text import TfidfVectorizer
    from sklearn.metrics.pairwise import cosine_similarity
    from sklearn.preprocessing import MinMaxScaler
    HAS_ML_DEPS = True
except ImportError:
    hstack = None
    TfidfVectorizer = None
    cosine_similarity = None
    MinMaxScaler = None
    HAS_ML_DEPS = False


@dataclass
class RecommenderWeights:
    """Weights for the different components of the final score."""

    genre_similarity: float = 0.5
    rating_popularity: float = 0.3
    checkout_popularity: float = 0.2

class BookRecommender:
    """
    Content-based book recommender.

    The recommender:
    - Learns a TF-IDF representation of book categories (genres)
    - Normalizes rating and popularity signals
    - Computes a weighted score that combines genre similarity,
      rating/review popularity, and SPL checkout popularity
    """

    def __init__(
        self,
        data_dir: Optional[Path] = None,
        weights: Optional[RecommenderWeights] = None,
    ) -> None:
        if data_dir is None:
            project_root = Path(__file__).resolve().parents[2]
            data_dir = project_root / "data" / "processed"

        self.data_dir: Path = data_dir
        self.weights: RecommenderWeights = weights or RecommenderWeights()

        self.vectorizer: Optional[Any] = None
        self.genre_tfidf_matrix: Optional[Any] = None
        self.feature_matrix: Optional[Any] = None

        self.books_df: Optional[pd.DataFrame] = None
        self.book_id_to_index: Dict[str, int] = {}

        self._fitted: bool = False

    @staticmethod
    def _clean_title(title: Any) -> str:
        if pd.isna(title):
            return ""
        return str(title).strip().lower()

    @staticmethod
    def _prepare_categories(raw: Any) -> str:
        if raw is None:
            return ""
        if isinstance(raw, list):
            text = " ".join(str(item) for item in raw)
        else:
            try:
                if pd.isna(raw):
                    return ""
            except (TypeError, ValueError):
                pass
            text = str(raw)

        for ch in "[]'\"":
            text = text.replace(ch, " ")
        return " ".join(text.lower().split())

    def fit(self) -> None:
        """
        Load data, build feature matrices, and prepare lookup structures.
        """
        amazon_path = self.data_dir / "AMAZON_books_meta_data_first_100_rows.csv"
        spl_checkouts_path = self.data_dir / "SPL_checkouts_first_100_rows.csv"
        spl_catalog_path = self.data_dir / "SPL_catalog_first_100_rows.csv"
        jsonl_books_path = self.data_dir / "first_100_books_by_parent_asin.jsonl"
        json_books_path = self.data_dir / "books_sample_100.json"

        if amazon_path.exists():
            amazon_df = pd.read_csv(amazon_path)
        elif jsonl_books_path.exists():
            rows: List[Dict[str, Any]] = []
            with jsonl_books_path.open("r", encoding="utf-8") as file_obj:
                for line in file_obj:
                    line = line.strip()
                    if not line:
                        continue
                    parsed = json.loads(line)
                    source_id, payload = next(iter(parsed.items()))
                    rows.append(
                        {
                            "parent_asin": str(payload.get("parent_asin") or source_id),
                            "title": payload.get("title"),
                            "author_name": payload.get("author_name"),
                            "average_rating": payload.get("average_rating"),
                            "rating_number": payload.get("rating_number"),
                            "categories": payload.get("categories"),
                        }
                    )
            amazon_df = pd.DataFrame(rows)
        elif json_books_path.exists():
            amazon_df = pd.read_json(json_books_path)
        else:
            raise FileNotFoundError(
                "No supported books metadata file found in data/processed. "
                "Expected one of AMAZON_books_meta_data_first_100_rows.csv, "
                "first_100_books_by_parent_asin.jsonl, books_sample_100.json."
            )

        if spl_checkouts_path.exists():
            checkouts_df = pd.read_csv(spl_checkouts_path)
        else:
            checkouts_df = pd.DataFrame(columns=["Title", "Checkouts"])

        if spl_catalog_path.exists():
            catalog_df = pd.read_csv(spl_catalog_path)
        else:
            catalog_df = pd.DataFrame(columns=["Title", "Author", "ISBN"])

        # Standardize titles for joins.
        amazon_df["title_clean"] = amazon_df["title"].apply(self._clean_title)
        if "Title" not in checkouts_df.columns:
            checkouts_df["Title"] = ""
        if "Checkouts" not in checkouts_df.columns:
            checkouts_df["Checkouts"] = 0
        if "Title" not in catalog_df.columns:
            catalog_df["Title"] = ""
        if "Author" not in catalog_df.columns:
            catalog_df["Author"] = ""
        if "ISBN" not in catalog_df.columns:
            catalog_df["ISBN"] = ""

        checkouts_df["title_clean"] = checkouts_df["Title"].apply(self._clean_title)
        catalog_df["title_clean"] = catalog_df["Title"].apply(self._clean_title)

        # Aggregate checkout counts per standardized title.
        checkouts_agg = (
            checkouts_df.groupby("title_clean", as_index=False)["Checkouts"]
            .sum()
            .rename(columns={"Checkouts": "Checkouts"})
        )

        # Merge Amazon metadata with SPL checkouts (left join, keep all Amazon books).
        merged = pd.merge(
            amazon_df,
            checkouts_agg,
            on="title_clean",
            how="left",
        )

        # Optionally enrich with author/ISBN metadata from catalog (best-effort).
        catalog_meta = catalog_df[["title_clean", "Author", "ISBN"]].drop_duplicates(
            "title_clean"
        )
        merged = pd.merge(
            merged,
            catalog_meta,
            on="title_clean",
            how="left",
            suffixes=("", "_catalog"),
        )

        # Handle missing numeric values before scaling.
        for col in ["average_rating", "rating_number", "Checkouts"]:
            if col not in merged.columns:
                merged[col] = 0.0
            merged[col] = pd.to_numeric(merged[col], errors="coerce")
            merged[col] = merged[col].fillna(0.0)

        # Prepare genres/categories text for TF-IDF.
        if "categories" not in merged.columns:
            merged["categories"] = ""
        merged["categories_text"] = merged["categories"].apply(
            self._prepare_categories
        )

        # Normalize rating and popularity features.
        numeric_features = merged[["average_rating", "rating_number", "Checkouts"]]
        if HAS_ML_DEPS and MinMaxScaler is not None:
            scaler = MinMaxScaler()
            scaled_numeric = scaler.fit_transform(numeric_features.values)
        else:
            scaled_numeric = np.zeros_like(numeric_features.values, dtype=float)
            for col_idx, col_name in enumerate(
                ["average_rating", "rating_number", "Checkouts"]
            ):
                series = merged[col_name].astype(float)
                min_v = float(series.min())
                max_v = float(series.max())
                if max_v <= min_v:
                    scaled_numeric[:, col_idx] = 0.0
                else:
                    scaled_numeric[:, col_idx] = (series - min_v) / (max_v - min_v)

        merged["average_rating_norm"] = scaled_numeric[:, 0]
        merged["rating_number_norm"] = scaled_numeric[:, 1]
        merged["checkouts_norm"] = scaled_numeric[:, 2]

        if HAS_ML_DEPS and TfidfVectorizer is not None and hstack is not None:
            self.vectorizer = TfidfVectorizer()
            self.genre_tfidf_matrix = self.vectorizer.fit_transform(
                merged["categories_text"].fillna("")
            )
            from scipy.sparse import csr_matrix  # lazy import for optional dependency

            numeric_sparse = csr_matrix(scaled_numeric)
            self.feature_matrix = hstack([self.genre_tfidf_matrix, numeric_sparse]).tocsr()
        else:
            self.vectorizer = None
            self.genre_tfidf_matrix = None
            self.feature_matrix = None

        # Persist dataframe and lookup structures.
        self.books_df = merged
        self.book_id_to_index = {}
        for idx, book_id in enumerate(merged["parent_asin"]):
            if pd.isna(book_id):
                continue
            self.book_id_to_index[str(book_id)] = idx

        self._fitted = True

    def _ensure_fitted(self) -> None:
        if not self._fitted or self.books_df is None:
            raise RuntimeError("BookRecommender.fit() must be called before recommend().")
        if HAS_ML_DEPS and self.genre_tfidf_matrix is None:
            raise RuntimeError("Genre matrix is unavailable after fit().")

    def build_user_profile(
        self,
        user_id: str,
        user_genres_df: Optional["pd.DataFrame"] = None,
        user_books_df: Optional["pd.DataFrame"] = None,
    ) -> np.ndarray:
        """
        Build a user profile vector in the TF-IDF genre space.

        Combines:
        - Explicit user genre preferences (with rank weighting)
        - Books the user has already read
        """
        if not HAS_ML_DEPS:
            raise ValueError("User profile vectors require scipy/sklearn dependencies.")
        self._ensure_fitted()
        assert self.vectorizer is not None
        assert self.genre_tfidf_matrix is not None

        genre_vector: Optional[np.ndarray] = None
        books_vector: Optional[np.ndarray] = None

        #Genre preference vector
        if user_genres_df is not None and not user_genres_df.empty:
            user_rows = user_genres_df[user_genres_df["user_id"] == user_id]
            if not user_rows.empty:
                weighted_tokens: List[str] = []
                for _, row in user_rows.iterrows():
                    genre = str(row["genre"]).strip()
                    if not genre:
                        continue
                    # Lower rank => higher importance. Map to repetition count.
                    try:
                        rank = int(row["rank"])
                    except (TypeError, ValueError):
                        rank = 3
                    if rank == 1:
                        repeat = 3
                    elif rank == 2:
                        repeat = 2
                    else:
                        repeat = 1
                    weighted_tokens.extend([genre] * repeat)

                if weighted_tokens:
                    weighted_string = " ".join(weighted_tokens)
                    genre_vector = self.vectorizer.transform([weighted_string]).toarray()

        # Books-read profile vector
        if user_books_df is not None and not user_books_df.empty:
            books_row = user_books_df[user_books_df["user_id"] == user_id]
            if not books_row.empty:
                books_read: Sequence[str] = books_row.iloc[0]["books_read"] or []
                indices: List[int] = []
                for book_id in books_read:
                    idx = self.book_id_to_index.get(str(book_id))
                    if idx is not None:
                        indices.append(idx)

                if indices:
                    read_matrix = self.genre_tfidf_matrix[indices]
                    read_profile = read_matrix.mean(axis=0)
                    books_vector = np.asarray(read_profile)

        # Combine both signals
        if genre_vector is not None and books_vector is not None:
            final_vector = 0.7 * genre_vector + 0.3 * books_vector
        elif genre_vector is not None:
            final_vector = genre_vector
        elif books_vector is not None:
            final_vector = books_vector
        else:
            raise ValueError(
                "Cannot build user profile: no genres or books found for user_id "
                f"{user_id!r}. Provide user_genres_df and/or user_books_df."
            )

        return final_vector

    def recommend(
        self,
        user_id: str,
        user_genres_df: Optional["pd.DataFrame"] = None,
        user_books_df: Optional["pd.DataFrame"] = None,
        top_k: int = 10,
    ) -> List[Dict[str, Any]]:
        """
        Recommend books for a user based on genres and previously read books.

        Returns
        -------
        List of dicts with keys:
            - "book_id"
            - "title"
            - "score"
        """
        self._ensure_fitted()
        assert self.books_df is not None

        if top_k <= 0:
            return []

        if HAS_ML_DEPS and self.genre_tfidf_matrix is not None and cosine_similarity is not None:
            user_profile = self.build_user_profile(
                user_id=user_id,
                user_genres_df=user_genres_df,
                user_books_df=user_books_df,
            )
            genre_sim = cosine_similarity(user_profile, self.genre_tfidf_matrix).ravel()
        else:
            preferred_genres: set[str] = set()
            if user_genres_df is not None and not user_genres_df.empty:
                user_rows = user_genres_df[user_genres_df["user_id"] == user_id]
                preferred_genres.update(
                    {
                        str(v).strip().lower()
                        for v in user_rows.get("genre", pd.Series(dtype=str)).tolist()
                        if str(v).strip()
                    }
                )
            if user_books_df is not None and not user_books_df.empty:
                books_row = user_books_df[user_books_df["user_id"] == user_id]
                if not books_row.empty:
                    for book_id in books_row.iloc[0]["books_read"] or []:
                        idx = self.book_id_to_index.get(str(book_id))
                        if idx is None:
                            continue
                        categories_text = str(self.books_df.iloc[idx]["categories_text"])
                        preferred_genres.update(categories_text.split())
            if not preferred_genres:
                raise ValueError(
                    "Cannot build recommendations without user preference signals."
                )

            genre_sim = np.zeros(len(self.books_df), dtype=float)
            for idx, row in self.books_df.iterrows():
                tokens = set(str(row.get("categories_text", "")).split())
                if not tokens:
                    continue
                overlap = len(tokens.intersection(preferred_genres))
                genre_sim[idx] = overlap / max(1, len(preferred_genres))

        # If the user has read books, strengthen the influence of genre similarity.
        has_read_books = False
        if user_books_df is not None and not user_books_df.empty:
            books_row = user_books_df[user_books_df["user_id"] == user_id]
            if not books_row.empty:
                books_read = books_row.iloc[0]["books_read"] or []
                has_read_books = bool(books_read)
        if has_read_books:
            genre_sim = genre_sim * 1.5

        # Popularity signals (already normalized to [0, 1]).
        rating_pop = (
            self.books_df["average_rating_norm"] + self.books_df["rating_number_norm"]
        ) / 2.0
        checkout_pop = self.books_df["checkouts_norm"]

        # Final weighted score.
        w = self.weights
        final_score = (
            w.genre_similarity * genre_sim
            + w.rating_popularity * rating_pop.values
            + w.checkout_popularity * checkout_pop.values
        )

        # Exclude already read books if provided.
        already_read_ids: set[str] = set()
        if user_books_df is not None and not user_books_df.empty:
            books_row = user_books_df[user_books_df["user_id"] == user_id]
            if not books_row.empty:
                books_read = books_row.iloc[0]["books_read"] or []
                already_read_ids = {str(bid) for bid in books_read}
        results: List[Dict[str, Any]] = []

        # Sort indices by score descending.
        ranked_indices = np.argsort(-final_score)
        for idx in ranked_indices:
            row = self.books_df.iloc[idx]
            book_id = str(row["parent_asin"]) if not pd.isna(row["parent_asin"]) else None

            if book_id is not None and book_id in already_read_ids:
                continue

            results.append(
                {
                    "book_id": book_id,
                    "title": row["title"],
                    "score": float(final_score[idx]),
                }
            )

            if len(results) >= top_k:
                break

        return results


if __name__ == "__main__":
    recommender = BookRecommender()
    recommender.fit()

    # Dummy user for quick manual testing.
    demo_user_id = "demo_user"

    # Explicit genre preferences (Ex. Romance rank 1, Sci-Fi/Fantasy rank 2).
    user_genres_df = pd.DataFrame(
        [
            {"user_id": demo_user_id, "genre": "Romance", "rank": 1},
            {"user_id": demo_user_id, "genre": "Science Fiction & Fantasy", "rank": 2},
        ]
    )

    # Use one existing book_id as an example of already-read history.
    sample_book_id = None
    if recommender.books_df is not None:
        non_null = recommender.books_df["parent_asin"].dropna()
        if not non_null.empty:
            sample_book_id = str(non_null.iloc[0])

    user_books_df = pd.DataFrame(
        [
            {
                "user_id": demo_user_id,
                "books_read": [sample_book_id] if sample_book_id is not None else [],
            }
        ]
    )

    recommendations = recommender.recommend(
        user_id=demo_user_id,
        user_genres_df=user_genres_df,
        user_books_df=user_books_df,
        top_k=5,
    )

    print(recommendations)
