"""Authentication and user preference services."""

from __future__ import annotations

from typing import Any

import bcrypt

from backend.config import BCRYPT_ROUNDS, USER_ACCOUNTS_PATH, USER_BOOKS_PATH
from backend import storage


def _hash_password(password: str) -> str:
    """Return a bcrypt hash for the given plaintext password."""
    salt = bcrypt.gensalt(rounds=BCRYPT_ROUNDS)
    return bcrypt.hashpw(password.encode("utf-8"), salt).decode("utf-8")


def _check_password(password: str, password_hash: str | None) -> bool:
    """Safely compare plaintext password to stored bcrypt hash."""
    if not password_hash:
        return False
    try:
        return bcrypt.checkpw(password.encode("utf-8"), password_hash.encode("utf-8"))
    except ValueError:
        # Malformed hash or encoding issue → treat as non-match.
        return False


def create_user(email: str, password: str) -> dict[str, Any]:
    """Create a user; raises ValueError if email is taken."""
    email = str(email).strip().lower()
    password = str(password).strip()
    if not email or not password:
        raise ValueError("email and password required")
    accounts = storage._read_json(  # pylint: disable=protected-access
        USER_ACCOUNTS_PATH,
        {"users": {}},
    )
    users = accounts.setdefault("users", {})
    if email in users:
        raise ValueError("Email has been taken.")
    user_record: dict[str, Any] = {
        "user_id": email,
        "email": email,
        "name": email.split("@", maxsplit=1)[0],
        "password_hash": _hash_password(password),
    }
    users[email] = user_record
    storage._save_user_accounts_all(accounts)  # pylint: disable=protected-access
    books = storage._read_json(USER_BOOKS_PATH, {})  # pylint: disable=protected-access
    books.setdefault(
        email,
        {"library": {"in_progress": [], "saved": [], "finished": []}, "genre_preferences": []},
    )
    storage._save_user_books_all(books)  # pylint: disable=protected-access
    clean = dict(user_record)
    clean.pop("password_hash", None)
    return clean


def login_user(email: str, password: str) -> dict[str, Any]:
    """Validate credentials and return user record."""
    email = str(email).strip().lower()
    password = str(password).strip()
    user = get_user(email)
    if not user or not _check_password(password, user.get("password_hash")):
        raise ValueError("Invalid email or password.")
    clean = dict(user)
    clean.pop("password_hash", None)
    return clean


def get_user(user_id: str) -> dict[str, Any]:
    """Get user account by id/email."""
    return storage.get_user_accounts(str(user_id).strip().lower())


#move this to the library 
def update_user_preferences(user_id: str, genres: list[str]) -> dict[str, Any]:
    """Update user's genre preferences."""
    user_id = str(user_id).strip().lower()
    books = storage._read_json(USER_BOOKS_PATH, {})  # pylint: disable=protected-access
    rec = books.setdefault(
        user_id,
        {"library": {"in_progress": [], "saved": [], "finished": []}, "genre_preferences": []},
    )
    rec["genre_preferences"] = [str(g) for g in (genres or []) if str(g).strip()]
    storage._save_user_books_all(books)  # pylint: disable=protected-access
    return dict(rec)
