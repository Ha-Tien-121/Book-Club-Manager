"""
Extracts book rating statistics from the books.db and stores them in a compact
NumPy format for use in books reccomender. Each book's average rating and log of number of ratings
are retrieved and saved with reduced precision for efficient storage.

Args:
    books.db : SQLite database containing the books table with average_rating and rating_number.

Returns:
    book_ratings.npz : NPZ file containing two arrays:
        ratings_avg : numpy.ndarray of float16 containing the average rating for each book.
        rating_counts : numpy.ndarray of integer type (uint16 or uint32) containing the number
                        of ratings for each book.

Usage:
    Run script from the project root using:
    python -m data.scripts.amazon_books_data.book_ratings_vectors
"""


import os

import sqlite3
import numpy as np

from data.scripts.config import PROCESSED_DIR

BOOK_DB = os.path.join(PROCESSED_DIR, "books.db")
OUTPUT_FILE = os.path.join(PROCESSED_DIR, "book_ratings.npz")

conn = sqlite3.connect(BOOK_DB)

query = "SELECT average_rating, rating_number FROM books ORDER BY parent_asin"
data = np.array(conn.execute(query).fetchall())

conn.close()

ratings_avg = np.round(data[:, 0].astype(np.float32), 2).astype(np.float16)
number_ratings = data[:, 1].astype(np.int64)
log_number_ratings = np.log1p(number_ratings.astype(np.uint32))
ratings_data = np.column_stack((ratings_avg, number_ratings))

np.savez(OUTPUT_FILE,
         ratings_avg=ratings_avg,
         log_number_ratings=log_number_ratings)
