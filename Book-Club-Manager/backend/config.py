"""
Central configuration for the backend.

This module is the single source of truth for:
  - Which environment the app is running in (local vs AWS).
  - Where local JSON "databases" live.
  - Which DynamoDB tables and AWS region to use in cloud mode.

The rest of the backend should import from here instead of hard‑coding
paths, table names, or environment flags.
"""

from __future__ import annotations

import os
from pathlib import Path


# Environment selection (local or aws).
# Set APP_ENV=aws to use CloudStorage (DynamoDB + S3) so you can develop against AWS
APP_ENV = os.getenv("APP_ENV", "local")
IS_LOCAL = APP_ENV == "local"
IS_AWS = APP_ENV == "aws"


BASE_DIR = Path(__file__).resolve().parents[1]
PROCESSED_DIR = BASE_DIR / "data" / "processed"
USERS_DIR = BASE_DIR / "data" / "users"
REVIEWS_TOP50_BOOKS_LOCAL_PATH = PROCESSED_DIR / "reviews_top50_books.json"

USER_ACCOUNTS_PATH = PROCESSED_DIR / "user_accounts.json"
USER_BOOKS_PATH = PROCESSED_DIR / "user_books.json"
USER_CLUBS_PATH = PROCESSED_DIR / "user_clubs.json"
USER_FORUM_PATH = PROCESSED_DIR / "user_forum.json"
FORUM_DB_PATH = PROCESSED_DIR / "forum_posts.json"
USER_RECOMMENDATIONS_PATH = PROCESSED_DIR / "user_recommendations.json"
USER_EVENTS_PATH = USERS_DIR / "user_events.json"

AWS_REGION = os.getenv("AWS_REGION", "us-west-2")

USER_BOOKS_TABLE = os.getenv("USER_BOOKS_TABLE", "user_books")
USER_EVENTS_TABLE = os.getenv("USER_EVENTS_TABLE", "user_events")
USER_ACCOUNTS_TABLE = os.getenv("USER_ACCOUNTS_TABLE", "user_accounts")
USER_ACCOUNTS_PK = os.getenv("USER_ACCOUNTS_PK", "user_email").strip() or "user_email"
USER_EVENTS_PK = os.getenv("USER_EVENTS_PK", "user_email").strip() or "user_email"
USER_BOOKS_PK = os.getenv("USER_BOOKS_PK", "user_email").strip() or "user_email"
FORUM_POSTS_TABLE = os.getenv("FORUM_POSTS_TABLE", "forum_posts")
FORUM_POSTS_PK = os.getenv("FORUM_POSTS_PK", "pk").strip() or "pk"
FORUM_POSTS_SK = os.getenv("FORUM_POSTS_SK", "sk").strip() or "sk"
FORUM_POSTS_PK_VALUE = os.getenv("FORUM_POSTS_PK_VALUE", "POST").strip() or "POST"
FORUM_POSTS_META_PK = os.getenv("FORUM_POSTS_META_PK", "META").strip() or "META"
FORUM_POSTS_NEXT_ID_SK = os.getenv("FORUM_POSTS_NEXT_ID_SK",
                                   "next_post_id").strip() or"next_post_id"
USER_FORUMS_TABLE = os.getenv("USER_FORUMS_TABLE", "user_forums")
USER_RECOMMENDATIONS_TABLE = os.getenv("USER_RECOMMENDATIONS_TABLE", "user_recommendations")

FORUM_POSTS_GSI = os.getenv("FORUM_POSTS_GSI", "parent_asin-index").strip() or None
FORUM_POSTS_CREATED_AT_GSI = os.getenv("FORUM_POSTS_CREATED_AT_GSI", "created_at-index").strip() or None
BOOKS_TABLE = os.getenv("BOOKS_TABLE", "books")
EVENTS_TABLE = os.getenv("EVENTS_TABLE", "events")

EVENTS_GSI = os.getenv("EVENTS_GSI", "ttl-index").strip() or None
EVENTS_CITY_STATE_GSI = os.getenv("EVENTS_CITY_STATE_GSI", "city_state_ttl-index").strip() or None
EVENTS_PARENT_ASIN_GSI = os.getenv("EVENTS_PARENT_ASIN_GSI", "parent_asin_ttl-index").strip() or None

DATA_BUCKET = os.getenv("DATA_BUCKET", "bookish-data-elsie")
BOOK_SHARDS_S3_PREFIX = os.getenv("BOOK_SHARDS_S3_PREFIX", "books/shard/parent_asin")
TOP50_BOOKS_S3_KEY = os.getenv("TOP50_BOOKS_S3_KEY", "books/spl_top50_checkouts_in_books.json")
REVIEWS_TOP50_BOOKS_S3_KEY = os.getenv("REVIEWS_TOP50_BOOKS_S3_KEY",
                                       "books/reviews_top50_books.json")
CDN_BASE_URL = os.getenv("CDN_BASE_URL", f"https://{DATA_BUCKET}.s3.{AWS_REGION}.amazonaws.com")
DEFAULT_BOOK_IMAGE_KEY = os.getenv("DEFAULT_BOOK_IMAGE_KEY", "images/book_icon.png")
RECOMMENDED_BOOKS_SIZE = 50
RECOMMENDED_EVENTS_SIZE = 10
ADDS_BEFORE_BOOK_RERUN = 3

EVENT_RECOMMENDATION_POOL_SIZE = 200

BCRYPT_ROUNDS = int(os.getenv("BCRYPT_ROUNDS", "12"))

USE_BOOK_ML_RECOMMENDER = os.getenv("USE_BOOK_ML_RECOMMENDER",
                                    "1").strip().lower() in ("1","true", "yes")

# ML artifact location (optional).
ML_ARTIFACTS_BUCKET = os.getenv("ML_ARTIFACTS_BUCKET", DATA_BUCKET)
ML_ARTIFACTS_PREFIX = os.getenv("ML_ARTIFACTS_PREFIX", "books").strip().strip("/")
BOOK_RECOMMENDER_MODEL_S3_KEY = os.getenv(
    "BOOK_RECOMMENDER_MODEL_S3_KEY",
    f"{ML_ARTIFACTS_PREFIX}/book_recommender_model.pkl",
)
BOOK_RECOMMENDER_SCALER_S3_KEY = os.getenv(
    "BOOK_RECOMMENDER_SCALER_S3_KEY",
    f"{ML_ARTIFACTS_PREFIX}/feature_scaler.pkl",
)
BOOK_SIMILARITY_S3_KEY = os.getenv(
    "BOOK_SIMILARITY_S3_KEY",
    f"{ML_ARTIFACTS_PREFIX}/book_similarity.npz",
)
BOOK_RATINGS_S3_KEY = os.getenv(
    "BOOK_RATINGS_S3_KEY",
    f"{ML_ARTIFACTS_PREFIX}/book_ratings.npz",
)
BOOK_ID_TO_IDX_S3_KEY = os.getenv(
    "BOOK_ID_TO_IDX_S3_KEY",
    f"{ML_ARTIFACTS_PREFIX}/book_id_to_idx.json",
)

BOOK_RECOMMENDER_ARTIFACTS_S3_PREFIX = (
    os.getenv("BOOK_RECOMMENDER_ARTIFACTS_S3_PREFIX", "books/book_recommender").strip().rstrip("/")
)
BOOK_TFIDF_S3_KEY = os.getenv("BOOK_TFIDF_S3_KEY",
                              f"{BOOK_RECOMMENDER_ARTIFACTS_S3_PREFIX}/book_tfidf.npz")
BOOK_ID_TO_IDX_ARTIFACT_S3_KEY = os.getenv(
    "BOOK_ID_TO_IDX_ARTIFACT_S3_KEY",
    f"{BOOK_RECOMMENDER_ARTIFACTS_S3_PREFIX}/book_id_to_idx.json",
)
BOOK_RATING_NORMS_S3_KEY = os.getenv(
    "BOOK_RATING_NORMS_S3_KEY",
    f"{BOOK_RECOMMENDER_ARTIFACTS_S3_PREFIX}/book_rating_norms.npz",
)

ML_ARTIFACTS_LOCAL_CACHE_DIR = os.getenv("ML_ARTIFACTS_LOCAL_CACHE_DIR", "/tmp/bookish-ml")

FORUM_PREVIEW_MAX_CHARS = int(os.getenv("FORUM_PREVIEW_MAX_CHARS", "280").strip() or "280")
BOOK_DESCRIPTION_PREVIEW_CHARS = int(os.getenv("BOOK_DESCRIPTION_PREVIEW_CHARS", "600").strip() or "600")

GENRE_DROPDOWN_OPTIONS = sorted([
    "Action & Adventure", "Arts & Photography", "Biographies & Memoirs", "Business & Money",
    "Children's Books", "Classics", "Comics & Graphic Novels", "Cookbooks, Food & Wine",
    "Crafts, Hobbies & Home", "Fantasy", "History", "Growing Up & Facts of Life",
    "LGBTQ+ Books", "Literature & Fiction", "Mystery, Thriller & Suspense",
    "Poetry", "Politics & Social Sciences", "Religion & Spirituality", "Romance",
    "Science & Math", "Science Fiction", "Self-Help", "Sports & Outdoors",
    "Teen & Young Adult", "Travel",
])
