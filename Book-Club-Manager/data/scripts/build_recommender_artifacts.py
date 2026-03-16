"""Build recommender artifacts from the full books.db (e.g. 1M books) in chunks.

Run once (or when books.db is updated). Writes to data/processed/:
  - book_tfidf.npz       (sparse TF-IDF matrix)
  - book_id_to_idx.json  (parent_asin -> row index)
  - book_rating_norms.npz (average_rating_norm, rating_number_norm arrays)

At runtime the recommender loads these and queries books.db only for top-k metadata.

Usage (from repo root):
  cd Book-Club-Manager && python data/scripts/build_recommender_artifacts.py
"""

from __future__ import annotations

import json
import sqlite3
import sys
from pathlib import Path

import numpy as np
from scipy import sparse
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.preprocessing import MinMaxScaler

# Repo root so we can import backend
REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from backend.config import PROCESSED_DIR
from backend.recommender.book_recommender import (
    GENRE_VOCAB,
    ContentBasedBookRecommender,
)

CHUNK_SIZE = 50_000


def main() -> None:
    books_db = PROCESSED_DIR / "books.db"
    if not books_db.exists():
        print(f"Missing {books_db}. Create it or use the JSON fallback.")
        sys.exit(1)

    conn = sqlite3.connect(str(books_db))
    conn.row_factory = sqlite3.Row
    cur = conn.execute("SELECT COUNT(*) FROM books")
    total = cur.fetchone()[0]
    conn.close()
    print(f"books.db has {total} rows. Building artifacts in chunks of {CHUNK_SIZE}...")

    # First pass: stream chunks, collect genre_text + ratings, build TF-IDF incrementally
    vectorizer = TfidfVectorizer(
        vocabulary=GENRE_VOCAB,
        tokenizer=lambda s: s.split("|") if s else [],
        token_pattern=None,
        lowercase=False,
        norm="l2",
    )
    all_genre_texts: list[str] = []
    all_ratings: list[tuple[float, float]] = []
    book_id_to_idx: dict[str, int] = {}

    conn = sqlite3.connect(str(books_db))
    conn.row_factory = sqlite3.Row
    offset = 0
    while True:
        cur = conn.execute(
            """
            SELECT parent_asin, title, author_name, average_rating, rating_number, categories
            FROM books
            ORDER BY parent_asin
            LIMIT ? OFFSET ?
            """,
            (CHUNK_SIZE, offset),
        )
        rows = cur.fetchall()
        if not rows:
            break
        for row in rows:
            asin = (row["parent_asin"] or "").strip()
            if not asin:
                continue
            idx = len(all_genre_texts)
            book_id_to_idx[asin] = idx
            cats = row["categories"]
            genre_text = ContentBasedBookRecommender._prepare_categories(cats)
            all_genre_texts.append(genre_text or "")
            try:
                r = float(row["average_rating"] or 0)
            except (TypeError, ValueError):
                r = 0.0
            try:
                rn = int(row["rating_number"] or 0)
            except (TypeError, ValueError):
                rn = 0
            all_ratings.append((r, rn))
        offset += len(rows)
        print(f"  Read {min(offset, total)} / {total} rows...")
        if len(rows) < CHUNK_SIZE:
            break
    conn.close()

    n = len(all_genre_texts)
    if n == 0:
        print("No books found.")
        sys.exit(1)
    print(f"Building TF-IDF for {n} books...")

    # Fit TF-IDF on full list (vectorizer is efficient with fixed vocab)
    book_tfidf = vectorizer.fit_transform(all_genre_texts)
    del all_genre_texts

    # Normalize ratings
    rating_arr = np.array([x[0] for x in all_ratings], dtype=float)
    rating_num_arr = np.array([x[1] for x in all_ratings], dtype=float)
    del all_ratings
    rating_arr = np.nan_to_num(rating_arr, nan=0.0)
    rating_num_arr = np.nan_to_num(rating_num_arr, nan=0.0)
    scaler_r = MinMaxScaler()
    scaler_n = MinMaxScaler()
    average_rating_norm = scaler_r.fit_transform(rating_arr.reshape(-1, 1)).reshape(-1)
    rating_number_norm = scaler_n.fit_transform(rating_num_arr.reshape(-1, 1)).reshape(-1)

    # Save
    sparse.save_npz(PROCESSED_DIR / "book_tfidf.npz", book_tfidf)
    with open(PROCESSED_DIR / "book_id_to_idx.json", "w", encoding="utf-8") as f:
        json.dump(book_id_to_idx, f, separators=(",", ":"))
    np.savez(
        PROCESSED_DIR / "book_rating_norms.npz",
        average_rating_norm=average_rating_norm,
        rating_number_norm=rating_number_norm,
    )
    print(f"Wrote {PROCESSED_DIR / 'book_tfidf.npz'}, book_id_to_idx.json, book_rating_norms.npz")


if __name__ == "__main__":
    main()
