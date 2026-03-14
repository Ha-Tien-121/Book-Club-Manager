"""Authentication and account bootstrap services.

This module is responsible for:

- Creating user accounts with securely hashed passwords (bcrypt).
- Validating credentials on login.
- Reading user account records via the storage abstraction (local JSON or AWS).
- Bootstrapping a new user's related data:
  - Initial empty library + genre_preferences in user_books.
  - Default book and event recommendations in user_recommendations.

The high-level UI flow this supports is:

1. Create account (email + password) → `create_user`.
2. Optional genre-prompting screen (handled in a separate service using user_books).
3. Homepage that reads recommendations from `recommender_service`.
"""

from __future__ import annotations

from typing import Any

import bcrypt

from backend.config import BCRYPT_ROUNDS
from backend.storage import get_storage
from backend.recommender_service import ensure_default_recommendations


def _hash_password(password: str) -> str:
    """Hash a plaintext password using bcrypt.

    Args:
        password: Plaintext password from the user.

    Returns:
        Bcrypt hash string suitable for storage in the user account record.
    """
    salt = bcrypt.gensalt(rounds=BCRYPT_ROUNDS)
    return bcrypt.hashpw(password.encode("utf-8"), salt).decode("utf-8")


def _check_password(password: str, password_hash: str | None) -> bool:
    """Compare a plaintext password against a stored bcrypt hash.

    Args:
        password: Plaintext password supplied at login.
        password_hash: Stored bcrypt hash from the user account (may be None).

    Returns:
        True if the password matches the hash; False otherwise.
    """
    if not password_hash:
        return False
    try:
        return bcrypt.checkpw(password.encode("utf-8"), password_hash.encode("utf-8"))
    except ValueError:
        # Malformed hash or encoding issue → treat as non-match.
        return False


def create_user(email: str, password: str) -> dict[str, Any]:
    """Create a new user account and bootstrap related data.

    This:
      - Normalizes and validates the email/password.
      - Ensures the email is not already taken.
      - Persists the account via the configured storage backend (local JSON or AWS).
      - Initializes an empty user_books record (library + genre_preferences).
      - Seeds default recommendations in user_recommendations.

    Args:
        email: User's email address (used as primary key; case-insensitive).
        password: Plaintext password to hash and store.

    Returns:
        A sanitized user dict without the password_hash field.

    Raises:
        ValueError: If email/password are missing or the email is already taken.
    """
    email = str(email).strip().lower()
    password = str(password).strip()
    if not email or not password:
        raise ValueError("Email and Password required")

    store = get_storage()

    # Ensure email is not already taken.
    existing = store.get_user_account(email)
    if existing:
        raise ValueError("Email has been taken.")

    user_record: dict[str, Any] = {
        "user_id": email,
        "email": email,
        "name": email.split("@", maxsplit=1)[0],
        "password_hash": _hash_password(password),
    }

    # Persist account via storage backend.
    store.save_user_account(user_record)

    # Initialize an empty user_books record so library & genre preferences exist.
    current_books = store.get_user_books(email) or {}
    if not isinstance(current_books, dict):
        current_books = {}
    if not current_books.get("library") and not current_books.get("genre_preferences"):
        current_books.setdefault(
            "library",
            {"in_progress": [], "saved": [], "finished": []},
        )
        current_books.setdefault("genre_preferences", [])
        store.save_user_books(email, current_books)

    # Seed default recommendations (top books + soonest events).
    ensure_default_recommendations(email)
    clean = dict(user_record)
    clean.pop("password_hash", None)
    return clean


def login_user(email: str, password: str) -> dict[str, Any]:
    """Validate credentials and return a sanitized user record.

    Args:
        email: User email provided at login.
        password: Plaintext password provided at login.

    Returns:
        User dict without the password_hash field.

    Raises:
        ValueError: If the email does not exist or the password is invalid.
    """
    email = str(email).strip().lower()
    password = str(password).strip()
    user = get_user(email)
    if not user or not _check_password(password, user.get("password_hash")):
        raise ValueError("Invalid email or password.")
    clean = dict(user)
    clean.pop("password_hash", None)
    return clean


def get_user(user_id: str) -> dict[str, Any]:
    """Retrieve a user account by id/email via the storage backend.

    Args:
        user_id: User identifier, typically the email address.

    Returns:
        Account dict from storage, or an empty dict if not found.
    """
    store = get_storage()
    return store.get_user_account(str(user_id).strip().lower())
