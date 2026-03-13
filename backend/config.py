"""Shared configuration paths for backend services."""

from __future__ import annotations

from pathlib import Path

BASE_DIR = Path(__file__).resolve().parents[1]
PROCESSED_DIR = BASE_DIR / "data" / "processed"

USER_ACCOUNTS_PATH = PROCESSED_DIR / "user_accounts.json"
USER_BOOKS_PATH = PROCESSED_DIR / "user_books.json"
USER_CLUBS_PATH = PROCESSED_DIR / "user_clubs.json"
USER_FORUM_PATH = PROCESSED_DIR / "user_forum.json"

FORUM_DB_PATH = PROCESSED_DIR / "forum_posts.json"

