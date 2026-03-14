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
# even when local file storage is not complete.
APP_ENV = os.getenv("APP_ENV", "local")
IS_LOCAL = APP_ENV == "local"
IS_AWS = APP_ENV == "aws"


# Base paths for local file-backed storage
BASE_DIR = Path(__file__).resolve().parents[1]
PROCESSED_DIR = BASE_DIR / "data" / "processed"
USERS_DIR = BASE_DIR / "data" / "users"
# Local path for default book recs when not using S3 (reviews top 25).
REVIEWS_TOP50_BOOKS_LOCAL_PATH = PROCESSED_DIR / "reviews_top25_books.json"

# Local JSON "databases" for the mock / local mode.
USER_ACCOUNTS_PATH = PROCESSED_DIR / "user_accounts.json"
USER_BOOKS_PATH = PROCESSED_DIR / "user_books.json"
USER_CLUBS_PATH = PROCESSED_DIR / "user_clubs.json"
USER_FORUM_PATH = PROCESSED_DIR / "user_forum.json"
FORUM_DB_PATH = PROCESSED_DIR / "forum_posts.json"
USER_RECOMMENDATIONS_PATH = PROCESSED_DIR / "user_recommendations.json"

# Separate users directory for hand-authored user data (events, etc.).
USER_EVENTS_PATH = USERS_DIR / "user_events.json"


# AWS / DynamoDB / S3 configuration (used when IS_AWS is True)
AWS_REGION = os.getenv("AWS_REGION", "us-west-2")

# DynamoDB table names. These can be overridden per‑environment via env vars.
USER_BOOKS_TABLE = os.getenv("USER_BOOKS_TABLE", "user_books")
USER_EVENTS_TABLE = os.getenv("USER_EVENTS_TABLE", "user_events")
USER_ACCOUNTS_TABLE = os.getenv("USER_ACCOUNTS_TABLE", "user_accounts")
FORUM_POSTS_TABLE = os.getenv("FORUM_POSTS_TABLE", "forum_posts")
USER_FORUMS_TABLE = os.getenv("USER_FORUMS_TABLE", "user_forums")
USER_RECOMMENDATIONS_TABLE = os.getenv("USER_RECOMMENDATIONS_TABLE", "user_recommendations")
# user_recommendations table: partition key user_email (string). Attributes: recommended_books (50), recommended_events (10),
# book_updated_at, events_soonest_expiry, adds_since_last_book_run.
# user_forums table: partition key user_email (string), no sort key. Attributes: saved_forum_post_ids, liked_post_ids, liked_comment_ids.
# GSI on forum_posts for querying thread by parent_asin (partition key = parent_asin, sort key = sk).
# Set to GSI name (e.g. "parent_asin-index") or leave empty to use full load + filter.
FORUM_POSTS_GSI = os.getenv("FORUM_POSTS_GSI", "parent_asin-index").strip() or None
# Optional GSI on forum_posts for \"all posts by created_at\": partition key pk, sort key created_at.
FORUM_POSTS_CREATED_AT_GSI = os.getenv("FORUM_POSTS_CREATED_AT_GSI", "created_at-index").strip() or None
BOOKS_TABLE = os.getenv("BOOKS_TABLE", "books")
EVENTS_TABLE = os.getenv("EVENTS_TABLE", "events")
# GSI on events for soonest-upcoming: partition key type (e.g. "event"), sort key ttl. Set to GSI name (e.g. "type-ttl-index") or leave empty to use scan.
EVENTS_GSI = os.getenv("EVENTS_GSI", "ttl-index").strip() or None
EVENTS_CITY_STATE_GSI = os.getenv("EVENTS_CITY_STATE_GSI", "city_state_ttl-index").strip() or None
# GSI on events for "events related to a book": partition key parent_asin, sort key ttl.
EVENTS_PARENT_ASIN_GSI = os.getenv("EVENTS_PARENT_ASIN_GSI", "parent_asin_ttl-index").strip() or None

# S3 bucket for book data and images.
DATA_BUCKET = os.getenv("DATA_BUCKET", "bookish-data-elsie")
# S3 key for SPL top-50 checkouts JSON (list of book dicts).
TOP50_BOOKS_S3_KEY = os.getenv("TOP50_BOOKS_S3_KEY", "books/spl_top50_checkouts_in_books.json")
# S3 key for default/cold-start book recs (no genre prefs): top 25 most popular from reviews.
REVIEWS_TOP25_BOOKS_S3_KEY = os.getenv("REVIEWS_TOP25_BOOKS_S3_KEY", "books/reviews_top25_books.json")
# Optional base URL for serving public images (e.g. CloudFront or direct S3 URL).
CDN_BASE_URL = os.getenv("CDN_BASE_URL", f"https://{DATA_BUCKET}.s3.{AWS_REGION}.amazonaws.com")
# Default image key for books that don't have a thumbnail.
DEFAULT_BOOK_IMAGE_KEY = os.getenv("DEFAULT_BOOK_IMAGE_KEY", "images/book_icon.png")

# Recommendation list sizes (stored in user_recommendations table).
RECOMMENDED_BOOKS_SIZE = 50
RECOMMENDED_EVENTS_SIZE = 10
# Run book recommender after this many adds-to-shelf since last run.
ADDS_BEFORE_BOOK_RERUN = 3
# Max number of upcoming events to score for personalized event recommendations (~90–100 typical; 200 covers full pool).
EVENT_RECOMMENDATION_POOL_SIZE = 200

# Auth / security tuning (bcrypt work factor)
BCRYPT_ROUNDS = int(os.getenv("BCRYPT_ROUNDS", "12"))

# Book recommender: set USE_BOOK_ML_RECOMMENDER=1 to try loading ML model; otherwise use
# fallback (reviews_top25 from storage). Use fallback when ML artifacts are not ready.
USE_BOOK_ML_RECOMMENDER = os.getenv("USE_BOOK_ML_RECOMMENDER", "").strip().lower() in ("1", "true", "yes")
