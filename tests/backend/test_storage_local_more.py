from __future__ import annotations

import importlib
import json
from pathlib import Path


def _import_storage():
    "Helper for  import storage."
    mod = importlib.import_module("backend.storage")
    return importlib.reload(mod)


def test_local_storage_user_recommendations_round_trip(tmp_path, monkeypatch) -> None:  # type: ignore[no-untyped-def]
    "Test local storage user recommendations round trip."
    storage = _import_storage()
    ls = storage.LocalStorage()

    from backend import config as cfg

    rec_path = tmp_path / "user_recommendations.json"
    monkeypatch.setattr(cfg, "USER_RECOMMENDATIONS_PATH", rec_path, raising=False)

    assert ls.get_user_recommendations("u@example.com") is None

    ls.save_user_recommendations("U@Example.com", {"book_updated_at": 1, "recommended_books": []})
    out = ls.get_user_recommendations("u@example.com")
    assert out is not None
    assert out["book_updated_at"] == 1

    # Overwrite with empty dict when rec is None
    ls.save_user_recommendations("u@example.com", None)
    out2 = ls.get_user_recommendations("u@example.com")
    assert out2 == {}


def test_local_storage_top50_review_books_fallback_and_book_metadata(tmp_path, monkeypatch) -> None:  # type: ignore[no-untyped-def]
    "Test local storage top50 review books fallback and book metadata."
    storage = _import_storage()
    ls = storage.LocalStorage()

    from backend import config as cfg

    processed = tmp_path / "processed"
    processed.mkdir(parents=True, exist_ok=True)
    monkeypatch.setattr(cfg, "PROCESSED_DIR", processed, raising=False)

    # Create only the fallback top25 file (top50 missing)
    top25 = processed / "reviews_top25_books.json"
    top25.write_text(json.dumps([{"parent_asin": "P1", "title": "T1"}]), encoding="utf-8")

    books = ls.get_top50_review_books()
    assert books and books[0]["parent_asin"] == "P1"

    # book metadata finds from review list
    meta = ls.get_book_metadata("P1")
    assert meta is not None and meta["title"] == "T1"


def test_local_storage_soonest_events_sorting_and_filters(tmp_path, monkeypatch) -> None:  # type: ignore[no-untyped-def]
    "Test local storage soonest events sorting and filters."
    storage = _import_storage()
    ls = storage.LocalStorage()

    from backend import config as cfg

    processed = tmp_path / "processed"
    processed.mkdir(parents=True, exist_ok=True)
    monkeypatch.setattr(cfg, "PROCESSED_DIR", processed, raising=False)

    events_path = processed / "book_events_clean.json"
    events_path.write_text(
        json.dumps(
            [
                {"event_id": "e2", "ttl": 20, "city_state": "Seattle, WA", "parent_asin": "P1"},
                {"event_id": "e1", "expiry": 10, "city_state": "Seattle, WA", "parent_asin": "P1"},
                {"event_id": "e3", "ttl": 30, "city_state": "Portland, OR", "parent_asin": "P2"},
            ]
        ),
        encoding="utf-8",
    )

    soon = ls.get_soonest_events(limit=2)
    assert [e["event_id"] for e in soon] == ["e1", "e2"]

    assert ls.get_event_details("e2")["event_id"] == "e2"  # type: ignore[index]
    assert ls.get_events_by_city("Seattle, WA")
    assert ls.get_events_by_city("") == []
    # Uses soonest-events ordering (ttl/expiry ascending), so e1 comes first.
    assert ls.get_events_for_book("P1", limit=1)[0]["event_id"] == "e1"
    assert ls.get_events_for_book("", limit=1) == []


def test_local_storage_spl_top50_checkout_books_list_and_dict(tmp_path, monkeypatch) -> None:  # type: ignore[no-untyped-def]
    "Test local storage spl top50 checkout books list and dict."
    storage = _import_storage()
    ls = storage.LocalStorage()

    from backend import config as cfg

    processed = tmp_path / "processed"
    processed.mkdir(parents=True, exist_ok=True)
    monkeypatch.setattr(cfg, "PROCESSED_DIR", processed, raising=False)

    spl_path = processed / "spl_top50_checkouts_in_books.json"
    spl_path.write_text(json.dumps([{"parent_asin": "S1"}]), encoding="utf-8")
    assert ls.get_spl_top50_checkout_books() == [{"parent_asin": "S1"}]

    spl_path.write_text(json.dumps({"items": [{"parent_asin": "S2"}]}), encoding="utf-8")
    # LocalStorage caches JSON reads by cache_key; reset cache to force re-read.
    ls._cache = {}  # type: ignore[attr-defined]
    assert ls.get_spl_top50_checkout_books() == [{"parent_asin": "S2"}]

