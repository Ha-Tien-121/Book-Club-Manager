from __future__ import annotations

import ast
import re
from typing import Any, Dict, List

import pandas as pd

from backend.recommender.service import (
    get_book_details,
    get_recommendations,
    get_recommender,
    mark_book_as_read,
)


# user_id -> list of {genre, rank}
user_genres_store: Dict[str, List[Dict[str, Any]]] = {}

# user_id -> list of book_id
user_books_read_store: Dict[str, List[str]] = {}

# book_id -> {
#     "title": str,
#     "author": str,
#     "genre": List[str],
#     "parent_asin": Optional[str],
#     "available_libraries": Optional[List[str]],
# }
books_store: Dict[str, Dict[str, Any]] = {}


def _parse_categories_to_list(raw: Any) -> List[str]:
    """Turn categories field (string or list-like) into a list of genre strings."""
    if pd.isna(raw):
        return []
    s = str(raw).strip()
    if not s or s == "[]":
        return []
    # Try literal_eval for Python list syntax
    try:
        out = ast.literal_eval(s)
        if isinstance(out, list):
            return [str(x).strip() for x in out if str(x).strip()]
    except (ValueError, SyntaxError):
        pass
    # Fallback: remove brackets, split by comma, strip quotes
    s = re.sub(r"^\[|\]$", "", s)
    parts = [p.strip().strip("'\"").strip() for p in s.split(",") if p.strip()]
    return parts


def build_books_store(books_df: pd.DataFrame) -> None:
    """Populate books_store from the recommender's books_df."""
    global books_store
    books_store.clear()
    for _, row in books_df.iterrows():
        book_id = row.get("parent_asin")
        if pd.isna(book_id):
            continue
        book_id = str(book_id)
        author = ""
        if "Author" in row and pd.notna(row["Author"]):
            author = str(row["Author"]).strip()
        elif "author_name" in row and pd.notna(row["author_name"]):
            author = str(row["author_name"]).strip()
        books_store[book_id] = {
            "title": str(row["title"]).strip() if pd.notna(row["title"]) else "",
            "author": author,
            "genre": _parse_categories_to_list(row.get("categories")),
            "parent_asin": book_id,
            # Placeholder for future enrichment from SPL datasets.
            "available_libraries": None,
        }


def get_top_genres_list(books_df: pd.DataFrame, max_genres: int = 15) -> List[str]:
    """Derive a unique, ordered list of genres from the dataset for user selection."""
    seen: set[str] = set()
    result: List[str] = []
    for _, row in books_df.iterrows():
        for g in _parse_categories_to_list(row.get("categories")):
            if g and g not in seen:
                seen.add(g)
                result.append(g)
                if len(result) >= max_genres:
                    return result
    return result


def print_recommendations(
    recommendations: List[Dict[str, Any]],
    books: Dict[str, Dict[str, Any]],
) -> None:
    """Print top recommendations with full book details."""
    print()
    print("----------------------------------")
    print("Top Recommendations:")
    for i, rec in enumerate(recommendations, start=1):
        book_id = rec.get("book_id")
        if not book_id:
            continue

        details = get_book_details(book_id, books)
        title = details.get("title", "")
        author = details.get("author", "")
        genres = details.get("genres", []) or []
        parent_asin = details.get("parent_asin")
        available_libraries = details.get("available_libraries")

        genre_str = ", ".join(genres) if genres else "—"
        print(f"{i}. {title} - {author}")
        print(f"   Genres: {genre_str}")
        if parent_asin is not None:
            print(f"   Parent ASIN: {parent_asin}")
        if available_libraries:
            libs_str = ", ".join(available_libraries)
            print(f"   Available at: {libs_str}")
    print("----------------------------------")
    print()


def run_cli() -> None:
    """Run the interactive CLI demo."""
    recommender = get_recommender()
    build_books_store(recommender.books_df)
    top_genres = get_top_genres_list(recommender.books_df)

    print("\n=== Book Recommender Demo ===\n")
    user_id = input("Enter your user_id: ").strip()
    if not user_id:
        print("No user_id entered. Exiting.")
        return

    # New user: prompt for genre selection (order = rank)
    if user_id not in user_genres_store:
        print("\nSelect your top genres (order = preference). Enter numbers separated by commas (e.g. 1,3,5):")
        for i, g in enumerate(top_genres, start=1):
            print(f"  {i}. {g}")
        choice = input("Your selection: ").strip()
        indices: List[int] = []
        for part in choice.split(","):
            part = part.strip()
            if not part:
                continue
            try:
                idx = int(part)
                if 1 <= idx <= len(top_genres):
                    indices.append(idx)
            except ValueError:
                continue
        # Order of selection = rank (first selected = rank 1)
        user_genres_store[user_id] = [
            {"genre": top_genres[i - 1], "rank": r}
            for r, i in enumerate(indices, start=1)
        ]
        user_books_read_store[user_id] = []

    # Initial recommendations
    recommendations = get_recommendations(
        user_id,
        user_genres_store,
        user_books_read_store,
        top_k=5,
    )
    print_recommendations(recommendations, books_store)

    # Interactive loop: mark as read or quit
    while True:
        prompt = "Enter the number of a book you've read (or press Enter to quit): "
        raw = input(prompt).strip()
        if not raw:
            print("Goodbye.")
            break
        try:
            num = int(raw)
        except ValueError:
            print("Please enter a number or press Enter to quit.")
            continue
        if num < 1 or num > len(recommendations):
            print(f"Please enter a number between 1 and {len(recommendations)}.")
            continue

        book_id = recommendations[num - 1].get("book_id")
        if not book_id:
            print("That book has no ID; skipping.")
            continue

        recommendations = mark_book_as_read(
            user_id,
            book_id,
            user_genres_store,
            user_books_read_store,
            top_k=5,
        )
        print_recommendations(recommendations, books_store)


if __name__ == "__main__":
    run_cli()
