"""Authentication and user preference services."""

from __future__ import annotations

from backend.config import USER_ACCOUNTS_PATH, USER_BOOKS_PATH
from backend import storage


def create_user(email: str, password: str) -> dict:
    """Create a user; raises ValueError if email is taken."""
    email = str(email).strip().lower()
    password = str(password).strip()
    if not email or not password:
        raise ValueError("email and password required")
    accounts = storage._read_json(USER_ACCOUNTS_PATH, {"users": {}})  # pylint: disable=protected-access
    users = accounts.setdefault("users", {})
    if email in users:
        raise ValueError("Email has been taken.")
    users[email] = {
        "user_id": email,
        "email": email,
        "name": email.split("@")[0],
        "password": password,
    }
    storage._save_user_accounts_all(accounts)  # pylint: disable=protected-access
    books = storage._read_json(USER_BOOKS_PATH, {})  # pylint: disable=protected-access
    books.setdefault(
        email,
        {"library": {"in_progress": [], "saved": [], "finished": []}, "genre_preferences": []},
    )
    storage._save_user_books_all(books)  # pylint: disable=protected-access
    return dict(users[email])


def login_user(email: str, password: str) -> dict:
    """Validate credentials and return user record."""
    email = str(email).strip().lower()
    password = str(password).strip()
    user = get_user(email)
    if not user or user.get("password") != password:
        raise ValueError("Invalid email or password.")
    return user


def get_user(user_id: str) -> dict:
    """Get user account by id/email."""
    return storage.get_user_accounts(str(user_id).strip().lower())


def update_user_preferences(user_id: str, genres: list[str]) -> dict:
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

