"""
Book recommender module.

Provides the BookRecommender class to generate personalized
book recommendations using a pre-trained logistic regression
model, book similarity matrix, and rating statistics.
All required data is loaded at initialization for fast queries.
"""


import os
import json
import sqlite3
import numpy as np
import joblib
from scipy.sparse import load_npz

from data.scripts.config import PROCESSED_DIR
from backend.recommender.config import RECOMMENDER_DIR
from backend.storage import LocalStorage

MODEL_FILE = os.path.join(RECOMMENDER_DIR, "book_recommender_model.pkl")
MODEL_SCALER_FILE = os.path.join(RECOMMENDER_DIR, "feature_scaler.pkl")

BOOK_SIM_FILE = os.path.join(PROCESSED_DIR, "book_similarity.npz")
BOOK_RATINGS_FILE = os.path.join(PROCESSED_DIR, "book_ratings.npz")
BOOK_ID_MAP_FILE = os.path.join(PROCESSED_DIR, "book_id_to_idx.json")
BOOK_DB = os.path.join(PROCESSED_DIR, "books.db")

def load_recommender_artifacts(model_file, scaler_file, sim_file, ratings_file, id_map_file):
    """Load model artifacts and book data required for recommendation."""
    clf = joblib.load(model_file)
    beta = clf.coef_[0]
    scaler = joblib.load(scaler_file)
    beta_scaled = beta / scaler.scale_
    book_similarity = load_npz(sim_file).tocsr()
    ratings = np.load(ratings_file)
    avg_ratings = ratings["ratings_avg"].astype(np.float32)
    num_ratings = ratings["log_number_ratings"].astype(np.float32)
    popularity_score = np.log1p(avg_ratings * num_ratings)
    with open(id_map_file, "r", encoding="utf-8") as f:
        book_id_to_idx = json.load(f)
    idx_to_book_id = {v: k for k, v in book_id_to_idx.items()}
    return beta_scaled, book_similarity, popularity_score, book_id_to_idx, idx_to_book_id

class BookRecommender:
    """
    Book recommender used by the backend API.

    Model artifacts and book data are loaded once at initialization
    to allow fast recommendation queries.
    """

    def __init__(self):
        """
        Load model artifacts and book data required for recommendation.

        Loads the trained logistic regression model, feature scaler,
        book similarity matrix, rating statistics, and book ID mappings.
        Also initializes access to user storage.
        """
        (self.beta_scaled,
         self.book_similarity,
         self.popularity_score,
         self.book_id_to_idx,
         self.idx_to_book_id,
         ) = load_recommender_artifacts(
             MODEL_FILE, MODEL_SCALER_FILE, BOOK_SIM_FILE, BOOK_RATINGS_FILE, BOOK_ID_MAP_FILE
             )
        self.storage = LocalStorage()

    def recommend(self, user_id: str, top_k: int = 50):
        """
        Generate top-k book recommendations for a given user.

        Parameters
        user_id : str
            Unique identifier for the user to recommend books to.
        top_k : int, default=50
            Number of top recommendations to return.
        
        Returns
        list of dict
            List of recommended books with metadata (title, author, rating, etc.).
    """
        if top_k >= len(self.book_id_to_idx):
            raise ValueError(f"top_k ({top_k}) must be less than n_books ({len(self.book_id_to_idx)})")

        user_books = self.storage.get_user_books(user_id)

        book_indices = [
            self.book_id_to_idx[b]
            for b in user_books
            if b in self.book_id_to_idx
        ]

        book_indices = np.array(book_indices, dtype=np.int32)
        lib_size = len(book_indices)

        if lib_size > 0:
            sim = self.book_similarity[book_indices].sum(axis=0).A1
            sim /= lib_size
        else:
            sim = np.zeros(len(self.popularity_score), dtype=np.float32)

        log_lib_size = np.log1p(lib_size)

        beta = self.beta_scaled

        scores = (
            beta[0] * sim +
            beta[1] * self.popularity_score +
            beta[2] * np.log1p(sim * log_lib_size)
        )

        if lib_size > 0:
            scores[book_indices] = -np.inf

        top_idx = np.argpartition(scores, -top_k)[-top_k:]
        top_idx = top_idx[np.argsort(scores[top_idx])[::-1]]

        book_ids = [self.idx_to_book_id[i] for i in top_idx]

        return self.fetch_books(book_ids)

    def fetch_books(self, book_ids):
        """
        Query books.db to retrieve metadata for recommended books.
        """

        if not book_ids:
            return []

        placeholders = ",".join(["?"] * len(book_ids))

        query = f"""
        SELECT
            parent_asin,
            title,
            author_name,
            average_rating,
            rating_number,
            images,
            categories
        FROM books
        WHERE parent_asin IN ({placeholders})
        """

        with sqlite3.connect(BOOK_DB) as conn:
            rows = conn.execute(query, book_ids).fetchall()

        columns = [
            "parent_asin",
            "title",
            "author_name",
            "average_rating",
            "rating_number",
            "images",
            "categories",
        ]

        return [dict(zip(columns, r)) for r in rows]
    