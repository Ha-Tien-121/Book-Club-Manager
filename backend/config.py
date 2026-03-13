"""
Central configuration for the backend.

This module is the single source of truth for:
  - Which environment the app is running in (local vs AWS).
  - Where local JSON “databases” live.
  - Which DynamoDB tables and AWS region to use in cloud mode.

The rest of the backend should import from here instead of hard‑coding
paths, table names, or environment flags.
"""

from __future__ import annotations

import os
from pathlib import Path


# Environment selection (local or aws)
APP_ENV = os.getenv("APP_ENV", "local")
IS_LOCAL = APP_ENV == "local"
IS_AWS = APP_ENV == "aws"


# Base paths for local file-backed storage
BASE_DIR = Path(__file__).resolve().parents[1]
PROCESSED_DIR = BASE_DIR / "data" / "processed"
USERS_DIR = BASE_DIR / "data" / "users"

# Local JSON “databases” for the mock / local mode.
USER_ACCOUNTS_PATH = PROCESSED_DIR / "user_accounts.json"
USER_BOOKS_PATH = PROCESSED_DIR / "user_books.json"
USER_CLUBS_PATH = PROCESSED_DIR / "user_clubs.json"
USER_FORUM_PATH = PROCESSED_DIR / "user_forum.json"
FORUM_DB_PATH = PROCESSED_DIR / "forum_posts.json"

# Separate users directory for hand-authored user data (events, etc.).
USER_EVENTS_PATH = USERS_DIR / "user_events.json"


# AWS / DynamoDB configuration (used when IS_AWS is True)
AWS_REGION = os.getenv("AWS_REGION", "us-west-2")

# DynamoDB table names. These can be overridden per‑environment via env vars.
USER_BOOKS_TABLE = os.getenv("USER_BOOKS_TABLE", "user_books")
USER_EVENTS_TABLE = os.getenv("USER_EVENTS_TABLE", "user_events")
USER_ACCOUNTS_TABLE = os.getenv("USER_ACCOUNTS_TABLE", "user_accounts")
FORUM_POSTS_TABLE = os.getenv("FORUM_POSTS_TABLE", "forum_posts")

# Auth / security tuning (bcrypt work factor)
BCRYPT_ROUNDS = int(os.getenv("BCRYPT_ROUNDS", "12"))
