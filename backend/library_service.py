"""Library service layer."""

from __future__ import annotations

from backend.config import IS_AWS, USER_BOOKS_PATH
from backend import storage


def add_book_to_library(user_id: str, book_id: int, shelf: str) -> dict:
    """Add a book to shelf: saved|in_progress|finished."""
    user_id = str(user_id).strip().lower()
    shelf = str(shelf).strip().lower()
    if shelf not in {"saved", "in_progress", "finished"}:
        raise ValueError("invalid shelf")
    books = storage._read_json(USER_BOOKS_PATH, {})  # pylint: disable=protected-access
    rec = books.setdefault(
        user_id,
        {"library": {"in_progress": [], "saved": [], "finished": []}, "genre_preferences": []},
    )
    lib = rec.setdefault("library", {"in_progress": [], "saved": [], "finished": []})
    for key in ("saved", "in_progress", "finished"):
        lib[key] = [bid for bid in lib.get(key, []) if int(bid) != int(book_id)]
    lib[shelf].append(int(book_id))
    storage._save_user_books_all(books)  # pylint: disable=protected-access
    return dict(rec)


def remove_book_from_library(user_id: str, book_id: int) -> dict:
    """Remove a book from all shelves."""
    user_id = str(user_id).strip().lower()
    books = storage._read_json(USER_BOOKS_PATH, {})  # pylint: disable=protected-access
    rec = books.setdefault(
        user_id,
        {"library": {"in_progress": [], "saved": [], "finished": []}, "genre_preferences": []},
    )
    lib = rec.setdefault("library", {"in_progress": [], "saved": [], "finished": []})
    for key in ("saved", "in_progress", "finished"):
        lib[key] = [bid for bid in lib.get(key, []) if int(bid) != int(book_id)]
    storage._save_user_books_all(books)  # pylint: disable=protected-access
    return dict(rec)


def get_user_library(user_id: str) -> dict: #check the functions
    """Return user's library dictionary."""
    rec = storage.get_user_books(str(user_id).strip().lower())
    return dict(rec.get("library") or {"in_progress": [], "saved": [], "finished": []})


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


def update_book_status(user_id: str, book_id: int, shelf: str) -> dict:
    """Move a book to a specific shelf; removes from all other shelves first."""
    user_id = str(user_id).strip().lower()
    shelf = str(shelf).strip().lower()
    if shelf not in {"saved", "in_progress", "finished"}:
        raise ValueError("invalid shelf")
    books = storage._read_json(USER_BOOKS_PATH, {})  # pylint: disable=protected-access
    rec = books.setdefault(
        user_id,
        {"library": {"in_progress": [], "saved": [], "finished": []}, "genre_preferences": []},
    )
    lib = rec.setdefault("library", {"in_progress": [], "saved": [], "finished": []})
    for key in ("saved", "in_progress", "finished"):
        lib[key] = [bid for bid in lib.get(key, []) if int(bid) != int(book_id)]
    lib[shelf].append(int(book_id))
    storage._save_user_books_all(books)  # pylint: disable=protected-access
    return dict(rec)


def remove_book_from_shelf(user_id: str, shelf: str, parent_asin: str) -> dict:
    """Remove a book from a single shelf. Uses CloudStorage when IS_AWS else local JSON."""
    user_id = str(user_id).strip().lower()
    shelf = str(shelf).strip().lower()
    parent_asin = str(parent_asin).strip()
    if shelf not in {"saved", "in_progress", "finished"}:
        raise ValueError("invalid shelf")
    if IS_AWS:
        from backend.storage import CloudStorage  # pylint: disable=import-outside-toplevel
        return CloudStorage().remove_book_from_shelf(user_id, shelf, parent_asin)
    books = storage._read_json(USER_BOOKS_PATH, {})  # pylint: disable=protected-access
    rec = books.setdefault(
        user_id,
        {"library": {"in_progress": [], "saved": [], "finished": []}, "genre_preferences": []},
    )
    lib = rec.setdefault("library", {"in_progress": [], "saved": [], "finished": []})
    lib[shelf] = [bid for bid in lib.get(shelf, []) if str(bid) != parent_asin]
    storage._save_user_books_all(books)  # pylint: disable=protected-access
    return dict(lib)
