"""
Tests for Book-Club-Manager.backend.data_loader.

Focus on the pure helpers and bootstrap composition:
- _books_from_services_to_ui_shape / books_to_ui_shape
- _events_to_clubs_ui_shape
- _forum_posts_to_ui_shape
- _read_jsonl_dict_lines
- _read_isbn_index_file
- _parse_tags
- build_ui_bootstrap (fallback behavior for clubs and forum_posts)
"""

import csv
import json
import sys
from pathlib import Path
from typing import Any, Dict, List

import pytest


def _ensure_inner_project_on_path() -> None:
    """Ensure inner Book-Club-Manager root is on sys.path when running from outer root."""
    tests_dir = Path(__file__).resolve().parents[1]
    repo_root = tests_dir.parent
    inner_root = repo_root / "Book-Club-Manager"
    if inner_root.is_dir() and str(inner_root) not in sys.path:
        sys.path.insert(0, str(inner_root))


_ensure_inner_project_on_path()


import backend.data_loader as dl  # noqa: E402


def test_books_from_services_to_ui_shape_normalizes_fields() -> None:
    "Test books from services to ui shape normalizes fields."
    raw = [
        {
            "parent_asin": "P1",
            "title": "Title",
            "author_name": "Author",
            "images": "cover-url",
            "average_rating": "4.25",
            "rating_number": "10",
            "categories": ["Fantasy", "Sci-Fi"],
            "description": "Desc",
        }
    ]

    out = dl.books_to_ui_shape(raw, max_count=10)

    assert len(out) == 1
    b = out[0]
    assert b["id"] == 1
    assert b["source_id"] == "P1"
    assert b["title"] == "Title"
    assert b["author"] == "Author"
    assert b["cover"] == "cover-url"
    assert b["rating"] == 4.2 or b["rating"] == 4.3  # rounding
    assert b["rating_count"] == 10
    assert b["genres"] == ["Fantasy", "Sci-Fi"]
    assert b["description"].startswith("Desc")
    assert b["spl_available"] is False


def test_books_from_services_to_ui_shape_parses_categories_string_and_handles_bad_literal() -> None:
    """Covers branches where categories is a string, including bad ast.literal_eval."""
    # First book: categories as a list-like string literal.
    # Second book: categories as a non-list string.
    # Third book: malformed list-like string that triggers the ast.literal_eval exception branch.
    raw = [
        {"parent_asin": "P1", "title": "T1", "author_name": "A1", "categories": "['X', ' Y ']"},
        {"parent_asin": "P2", "title": "T2", "author_name": "A2", "categories": "Not a list"},
        {"parent_asin": "P3", "title": "T3", "author_name": "A3", "categories": "[not valid"},
    ]

    out = dl.books_to_ui_shape(raw, max_count=10)

    assert len(out) == 3
    b1, b2, b3 = out
    # First book: parsed list literal, trimmed genres.
    assert b1["genres"] == ["X", "Y"]
    # Second book: falls back to treating the string as a single category.
    assert b2["genres"] == ["Not a list"]
    # Third book: malformed list-like string still yields a single category after exception.
    assert b3["genres"] == ["[not valid"]


def test_events_to_clubs_ui_shape_parses_tags_and_links_book() -> None:
    "Test events to clubs ui shape parses tags and links book."
    books = [{"id": 1, "source_id": "P1", "title": "Book One"}]
    books_by_source_id: Dict[str, Dict[str, Any]] = {str(b["source_id"]): b for b in books}
    events = [
        {
            "event_id": "E1",
            "title": "Event Title",
            "description": "Event desc",
            "genre": "Fantasy",
            "city_state": "Seattle, WA",
            "meeting_day": "Mon",
            "start_time": "7pm",
            "start_iso": "2025-01-01T00:00:00Z",
            "tags": ["Tag1", " Tag2 "],
            "parent_asin": "P1",
            "thumbnail": "thumb.png",
            "link": "https://example.com",
        }
    ]

    clubs = dl._events_to_clubs_ui_shape(events, books_by_source_id)

    assert len(clubs) == 1
    c = clubs[0]
    assert c["event_id"] == "E1"
    assert c["name"] == "Event Title"
    assert c["genre"] == "Fantasy"
    assert c["location"] == "Seattle, WA"
    assert c["meeting_day"] == "Mon"
    assert c["meeting_time"] == "7pm"
    assert c["start_iso"] == "2025-01-01T00:00:00Z"
    assert c["tags"] == ["Tag1", "Tag2"]
    assert c["current_book_id"] == 1
    assert c["current_book_title"] == "Book One"
    assert c["thumbnail"] == "thumb.png"
    assert c["is_external"] is True
    assert c["external_link"] == "https://example.com"


def test_events_to_clubs_ui_shape_tag_sources_and_genre_fallbacks() -> None:
    """Covers tags as set/string and genre/General fallbacks."""
    books_by_source_id: Dict[str, Dict[str, Any]] = {}
    events = [
        # tags as set
        {"id": 1, "tags": {"A", "B"}, "genre": "Fantasy"},
        # tags as comma/semicolon-separated string
        {"id": 2, "tags": "X, Y; Z", "genre": "Mystery"},
        # no tags but explicit genre
        {"id": 3, "genre": "Sci-Fi"},
        # no tags and no genre -> "General"
        {"id": 4},
        # tags list where all elements are whitespace -> initial tags == [] then fallback
        {"id": 5, "tags": ["  ", ""], "genre": "Romance"},
        # non-string start_iso -> triggers str() conversion branch
        {"id": 6, "tags": ["T"], "genre": "Drama", "start_iso": 123456},
    ]

    clubs = dl._events_to_clubs_ui_shape(events, books_by_source_id)

    assert len(clubs) == 6
    # First: tags from set, order-insensitive but not empty.
    assert set(clubs[0]["tags"]) == {"A", "B"}
    assert clubs[0]["genre"] == "Fantasy"
    # Second: string split into tags.
    assert clubs[1]["tags"] == ["X", "Y", "Z"]
    assert clubs[1]["genre"] == "Mystery"
    # Third: falls back to ['Sci-Fi'] tag.
    assert clubs[2]["tags"] == ["Sci-Fi"]
    # Fourth: falls back to ['General'] tag and "General" genre.
    assert clubs[3]["tags"] == ["General"]
    assert clubs[3]["genre"] == "General"
    # Fifth: tags list with only whitespace but genre present -> ['Romance']
    assert clubs[4]["tags"] == ["Romance"]
    # Sixth: numeric start_iso coerced to string
    assert clubs[5]["start_iso"] == "123456"


def test_forum_posts_to_ui_shape_uses_tags_and_truncates_preview() -> None:
    "Test forum posts to ui shape uses tags and truncates preview."
    long_text = "x" * 150
    posts = [
        {
            "id": "5",
            "title": "Post Title",
            "author": "Author",
            "tags": ["Mystery"],
            "text": long_text,
            "replies": "3",
            "likes": "4",
        }
    ]

    ui = dl._forum_posts_to_ui_shape(posts)

    assert len(ui) == 1
    p = ui[0]
    assert p["id"] == 5
    assert p["title"] == "Post Title"
    assert p["author"] == "Author"
    assert p["genre"] == "Mystery"
    assert p["replies"] == 3
    assert p["likes"] == 4
    assert len(p["preview"]) <= 121  # 120 chars plus ellipsis


def test_forum_posts_to_ui_shape_uses_genre_when_no_tags_and_handles_empty_text() -> None:
    "Test forum posts to ui shape uses genre when no tags and handles empty text."
    posts = [
        {
            "id": None,
            "title": None,
            "author": None,
            "genre": "Sci-Fi",
            "text": "",
            "replies": None,
            "likes": None,
        }
    ]

    ui = dl._forum_posts_to_ui_shape(posts)

    assert len(ui) == 1
    p = ui[0]
    # Defaults for id/title/author/genre and preview fallback.
    assert p["id"] == 1
    assert p["title"] == "Post"
    assert p["author"] == "Anonymous"
    assert p["genre"] == "Sci-Fi"
    assert p["preview"] == "No preview."


def test_read_jsonl_dict_lines_reads_non_empty_lines(tmp_path: Path) -> None:
    "Test read jsonl dict lines reads non empty lines."
    path = tmp_path / "data.jsonl"
    path.write_text('{"a": 1}\n\n{"b": 2}\n', encoding="utf-8")

    rows = dl._read_jsonl_dict_lines(path)

    assert rows == [{"a": 1}, {"b": 2}]


def test_read_jsonl_dict_lines_missing_file_returns_empty(tmp_path: Path) -> None:
    "Test read jsonl dict lines missing file returns empty."
    path = tmp_path / "missing.jsonl"

    rows = dl._read_jsonl_dict_lines(path)

    assert rows == []


def test_read_isbn_index_file_builds_uppercase_set(tmp_path: Path) -> None:
    "Test read isbn index file builds uppercase set."
    path = tmp_path / "isbn.json"
    data = [{"0": "abc"}, {"0": " Def "}, {"0": ""}]
    path.write_text(json.dumps(data), encoding="utf-8")

    result = dl._read_isbn_index_file(path)

    assert result == {"ABC", "DEF"}


def test_read_isbn_index_file_missing_file_returns_empty_set(tmp_path: Path) -> None:
    "Test read isbn index file missing file returns empty set."
    path = tmp_path / "missing.json"

    result = dl._read_isbn_index_file(path)

    assert result == set()


def test_parse_tags_handles_valid_and_invalid_strings() -> None:
    "Test parse tags handles valid and invalid strings."
    assert dl._parse_tags("") == []
    assert dl._parse_tags("['A', ' B ']") == ["a", "b"]
    # Invalid literal should return empty list
    assert dl._parse_tags("not-a-list") == []
    # Literal that parses but is not a list should return empty list as well.
    assert dl._parse_tags("{'a': 1}") == []


def test_build_ui_bootstrap_creates_fallbacks_for_clubs_and_forum() -> None:
    "Test build ui bootstrap creates fallbacks for clubs and forum."
    raw_books = [
        {
            "parent_asin": "P1",
            "title": "Book One",
            "author_name": "Author One",
            "categories": ["Fantasy"],
        },
        {
            "parent_asin": "P2",
            "title": "Book Two",
            "author_name": "Author Two",
            "categories": ["Sci-Fi"],
        },
    ]
    events: List[Dict[str, Any]] = []  # no events to force fallback club
    forum_posts: List[Dict[str, Any]] = []  # no posts to force fallback forum posts when books exist

    ui = dl.build_ui_bootstrap(raw_books, events, forum_posts)

    # Fallback club should exist.
    assert ui["clubs"]
    club = ui["clubs"][0]
    assert club["name"] == "Seattle Readers"
    # Fallback forum posts should have at least two entries.
    assert len(ui["forum_posts"]) >= 2
    # Genres list should be derived from book genres.
    assert sorted(ui["genres"]) == ["Fantasy", "Sci-Fi"]
    # Library shelves should be lists of book ids.
    assert set(ui["library"].keys()) == {"in_progress", "saved", "finished"}


def test_load_data_uses_jsonl_and_isbn_and_clubs_csv(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """load_data should prefer JSONL, join with ISBN index files, and build clubs from CSV."""
    # Point data_loader.PROCESSED_DIR at our temporary directory.
    monkeypatch.setattr(dl, "PROCESSED_DIR", tmp_path, raising=False)

    # Primary JSONL bootstrap: two books with simple metadata.
    jsonl_path = tmp_path / "first_100_books_by_parent_asin.jsonl"
    rows = [
        {
            "ISBN1": {
                "title": "Book One",
                "author_name": "Author One",
                "categories": ["Fiction"],
                "description": ["A", "B"],
                "images": "img1",
                "rating_number": 5,
                "average_rating": 4.0,
            }
        },
        {
            "ISBN2": {
                "title": "Book Two",
                "author_name": "Author Two",
                # Well-formed list-like string.
                "categories": "['X', 'Y']",
                "description": "Desc2",
                "images": "img2",
                "rating_number": 3,
                "average_rating": 3.5,
            }
        },
        {
            "ISBN3": {
                "title": "Malformed Cats",
                "author_name": "Author Three",
                # Malformed list-like string exercises the ast.literal_eval exception path.
                "categories": "[not valid",
                "description": "Desc3",
                "images": "img3",
                "rating_number": 1,
                "average_rating": 2.0,
            }
        },
    ]
    jsonl_path.write_text(
        "\n".join(json.dumps(r) for r in rows) + "\n",
        encoding="utf-8",
    )

    # ISBN index files: mark ISBN1 as available in catalog, ISBN2 in checkouts.
    (tmp_path / "first_100_spl_catalog_by_isbn.json").write_text(
        json.dumps([{"0": "isbn1"}]), encoding="utf-8"
    )
    (tmp_path / "first_100_spl_checkouts_by_isbn.json").write_text(
        json.dumps([{"0": "isbn2"}]), encoding="utf-8"
    )

    # Clubs CSV with a single row that references Book One.
    csv_path = tmp_path / "bookclubs_seattle_clean.csv"
    with csv_path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=[
                "event_id",
                "title",
                "description",
                "city_state",
                "day_of_week_start",
                "start_time",
                "tags",
                "book_title",
                "book_author",
                "thumbnail",
                "link",
            ],
        )
        writer.writeheader()
        writer.writerow(
            {
                "event_id": "E1",
                "title": "Club One",
                "description": "Desc",
                "city_state": "Seattle, WA",
                "day_of_week_start": "Mon",
                "start_time": "7pm",
                "tags": "['club', ' fiction ']",
                "book_title": "Book One",
                "book_author": "Author One",
                "thumbnail": "thumb.png",
                "link": "https://example.com/club",
            }
        )

    ui = dl.load_data()

    # Books should come from JSONL.
    assert len(ui["books"]) == 3
    # Both books should be marked as available in SPL via the ISBN index files.
    spl_flags = {b["source_id"]: b["spl_available"] for b in ui["books"]}
    assert spl_flags == {"ISBN1": True, "ISBN2": True, "ISBN3": False}

    # Clubs should be created from the CSV, not fallback.
    assert ui["clubs"]
    club = ui["clubs"][0]
    assert club["name"] == "Club One"
    assert club["current_book_title"] == "Book One"
    assert club["is_external"] is True
    assert club["external_link"] == "https://example.com/club"


def test_load_data_falls_back_to_reviews_json_when_jsonl_missing(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """When JSONL is missing, load_data should fall back to reviews JSON."""
    monkeypatch.setattr(dl, "PROCESSED_DIR", tmp_path, raising=False)

    # No JSONL file created -> books_parent will be empty and trigger fallback.
    reviews_path = tmp_path / "reviews_top25_books.json"
    payload = {
        "books": [
            # Non-dict entry exercises the `if not isinstance(b, dict)` continue branch.
            123,
            {
                "parent_asin": "R1",
                "title": "From Reviews",
                "author_name": "Reviewer",
                "categories": ["Cats"],
            },
        ]
    }
    reviews_path.write_text(json.dumps(payload), encoding="utf-8")

    # Minimal ISBN and CSV files so later stages still work.
    (tmp_path / "first_100_spl_catalog_by_isbn.json").write_text(
        json.dumps([]), encoding="utf-8"
    )
    (tmp_path / "first_100_spl_checkouts_by_isbn.json").write_text(
        json.dumps([]), encoding="utf-8"
    )

    ui = dl.load_data()

    # Book should have come from the reviews JSON.
    assert any(b["source_id"] == "R1" for b in ui["books"])


def test_load_data_handles_reviews_json_exception_and_uses_fallbacks(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """If reviews JSON cannot be parsed, load_data should still succeed with fallbacks."""
    monkeypatch.setattr(dl, "PROCESSED_DIR", tmp_path, raising=False)

    # No JSONL file; create an invalid reviews JSON to trigger the exception path.
    reviews_path = tmp_path / "reviews_top25_books.json"
    reviews_path.write_text("{not-valid-json}", encoding="utf-8")

    # Missing ISBN and clubs files cause empty sets/lists and fallback club/forum generation.
    ui = dl.load_data()

    # With no valid books or clubs data, load_data should still return a dict
    # containing fallback clubs and forum posts.
    assert ui["clubs"]
    assert ui["forum_posts"]
