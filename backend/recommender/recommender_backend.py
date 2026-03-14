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
from scipy.sparse import load_npz, csr_matrix

from data.scripts.config import PROCESSED_DIR
from backend.recommender.config import RECOMMENDER_DIR
from backend.storage import LocalStorage

MODEL_FILE = os.path.join(RECOMMENDER_DIR, "book_recommender_model.pkl")
MODEL_SCALER_FILE = os.path.join(RECOMMENDER_DIR, "feature_scaler.pkl")

BOOK_SIM_FILE = os.path.join(PROCESSED_DIR, "book_similarity.npz")
BOOK_RATINGS_FILE = os.path.join(PROCESSED_DIR, "book_ratings.npz")
BOOK_ID_MAP_FILE = os.path.join(PROCESSED_DIR, "book_id_to_idx.json")
BOOK_DB = os.path.join(PROCESSED_DIR, "books.db")


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
        clf = joblib.load(MODEL_FILE)
        beta = clf.coef_[0]
        scaler = joblib.load(MODEL_SCALER_FILE)
        self.beta_scaled = beta / scaler.scale_
        self.book_similarity: csr_matrix = load_npz(BOOK_SIM_FILE).tocsr()
        ratings = np.load(BOOK_RATINGS_FILE)
        self.book_avg_ratings = ratings["ratings_avg"].astype(np.float32)
        self.book_num_ratings = np.log1p(ratings["log_number_ratings"]).astype(np.float32)

        with open(BOOK_ID_MAP_FILE, "r", encoding="utf-8") as f:
            self.book_id_to_idx = json.load(f)

        self.idx_to_book_id = {
            v: k for k, v in self.book_id_to_idx.items()
        }
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
            sim = np.zeros(len(self.book_avg_ratings), dtype=np.float32)

        log_lib_size = np.log1p(lib_size)

        beta = self.beta_scaled

        scores = (
            beta[0] * sim +
            beta[1] * self.book_avg_ratings +
            beta[2] * self.book_num_ratings +
            beta[3] * sim * log_lib_size
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
    