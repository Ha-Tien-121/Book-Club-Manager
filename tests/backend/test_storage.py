"""
Tests for Book-Club-Manager.backend.storage.

Focus on lightweight, deterministic unit tests:
- Pure conversion helpers: _from_dynamo, _to_dynamo
- Sharding / forum helpers: _get_shard_key, _forum_post_to_item
- Selected storage entrypoints that are safe to unit-test with stubs:
  - get_book_details (local_dir path; parquet read mocked)
  - get_book_metadata (DynamoDB read mocked)
  - increment_library_actions_since_recs / reset_library_actions_since_recs
  - get_storage (chooses LocalStorage vs CloudStorage)

This test file stubs `boto3` and `pandas` at import-time to avoid optional or
environment-specific dependencies during test collection.
"""

from __future__ import annotations

import importlib
import sys
import types
from decimal import Decimal
from pathlib import Path
from typing import Any, Dict, Optional

import pytest
from unittest.mock import MagicMock


def _ensure_inner_project_on_path() -> None:
    """Ensure inner Book-Club-Manager root is on sys.path when running from outer root."""
    tests_dir = Path(__file__).resolve().parents[1]
    repo_root = tests_dir.parent
    inner_root = repo_root / "Book-Club-Manager"
    if inner_root.is_dir() and str(inner_root) not in sys.path:
        sys.path.insert(0, str(inner_root))


def _stub_boto3_and_pandas() -> None:
    """Install lightweight stubs so backend.storage can import cleanly."""
    # --- boto3 stub ---
    existing = sys.modules.get("boto3")
    # Replace missing or MagicMock boto3 with a real stub so tests that
    # install a simplistic MagicMock don't interfere with these expectations.
    if existing is None or isinstance(existing, MagicMock) or not hasattr(existing, "resource"):
        boto3_mod = types.ModuleType("boto3")

        class _Key:
            def __init__(self, name: str):
                "Support __init__ for test doubles."
                self.name = name

            def eq(self, value: Any) -> tuple:
                "Helper for eq."
                return ("eq", self.name, value)

        class _FakeTable:
            def __init__(self):
                "Support __init__ for test doubles."
                self.get_item_calls: list[dict[str, Any]] = []
                self.update_item_calls: list[dict[str, Any]] = []
                self._next_get_item: dict[str, Any] = {}
                self._next_update_item: dict[str, Any] = {}
                self.raise_on_get: Optional[Exception] = None
                self.raise_on_update: Optional[Exception] = None
                self._name: str = "fake_table"

            def get_item(self, **kwargs: Any) -> dict[str, Any]:
                "Helper for get item."
                self.get_item_calls.append(kwargs)
                if self.raise_on_get:
                    raise self.raise_on_get
                return dict(self._next_get_item)

            def update_item(self, **kwargs: Any) -> dict[str, Any]:
                "Helper for update item."
                self.update_item_calls.append(kwargs)
                if self.raise_on_update:
                    raise self.raise_on_update
                return dict(self._next_update_item)

            def put_item(self, **kwargs: Any) -> dict[str, Any]:
                "Helper for put item."
                return {}

            def scan(self, **kwargs: Any) -> dict[str, Any]:
                "Helper for scan."
                return {"Items": []}

            def query(self, **kwargs: Any) -> dict[str, Any]:
                "Helper for query."
                return {"Items": []}

            @property
            def name(self) -> str:
                "Helper for name."
                return self._name

        class _FakeDynamo:
            def __init__(self):
                "Support __init__ for test doubles."
                self.tables: dict[str, _FakeTable] = {}

            def Table(self, name: str) -> _FakeTable:
                "Helper for Table."
                if name not in self.tables:
                    t = _FakeTable()
                    t._name = name
                    self.tables[name] = t
                return self.tables[name]

        _dynamo_singleton = _FakeDynamo()

        def resource(service_name: str, **kwargs: Any) -> Any:
            "Helper for resource."
            assert service_name == "dynamodb"
            return _dynamo_singleton

        def client(service_name: str, **kwargs: Any) -> Any:
            "Helper for client."
            return types.SimpleNamespace(batch_get_item=lambda **_kw: {})

        boto3_mod.resource = resource  # type: ignore[attr-defined]
        boto3_mod.client = client  # type: ignore[attr-defined]

        dyn_mod = types.ModuleType("boto3.dynamodb")
        cond_mod = types.ModuleType("boto3.dynamodb.conditions")
        setattr(cond_mod, "Key", _Key)
        sys.modules["boto3"] = boto3_mod
        sys.modules["boto3.dynamodb"] = dyn_mod
        sys.modules["boto3.dynamodb.conditions"] = cond_mod

    # --- pandas stub ---
    if "pandas" not in sys.modules:
        pd_mod = types.ModuleType("pandas")

        def _missing(*_a: Any, **_kw: Any) -> Any:
            "Helper for  missing."
            raise AssertionError("pd.read_parquet should be patched in tests")

        pd_mod.read_parquet = _missing  # type: ignore[attr-defined]
        sys.modules["pandas"] = pd_mod


_ensure_inner_project_on_path()
_stub_boto3_and_pandas()


def _import_storage():
    "Helper for  import storage."
    mod = importlib.import_module("backend.storage")
    return importlib.reload(mod)


# ---------------------------------------------------------------------------
# Pure helpers
# ---------------------------------------------------------------------------

def test_from_dynamo_converts_decimals_recursively() -> None:
    "Test from dynamo converts decimals recursively."
    storage = _import_storage()
    raw = {
        "a": Decimal("2"),
        "b": Decimal("2.5"),
        "c": {"d": [Decimal("1"), {"e": Decimal("3.0")}]},
    }
    out = storage._from_dynamo(raw)
    assert out == {"a": 2, "b": 2.5, "c": {"d": [1, {"e": 3}]}}


def test_to_dynamo_converts_floats_recursively() -> None:
    "Test to dynamo converts floats recursively."
    storage = _import_storage()
    raw = {"a": 1.25, "b": {"c": [2.5, 3]}}
    out = storage._to_dynamo(raw)
    assert isinstance(out["a"], Decimal)
    assert out["a"] == Decimal("1.25")
    assert isinstance(out["b"]["c"][0], Decimal)
    assert out["b"]["c"][0] == Decimal("2.5")
    assert out["b"]["c"][1] == 3


def test_forum_post_to_item_sets_keys_and_normalizes_id() -> None:
    "Test forum post to item sets keys and normalizes id."
    storage = _import_storage()
    post = {"id": "7", "title": "T"}
    item = storage._forum_post_to_item(post, pk="pk", sk="sk", pk_value="POST")
    assert item["id"] == 7
    assert item["post_id"] == 7
    assert item["pk"] == "POST"
    assert item["sk"] == "7"

    # Bad id becomes 0.
    item2 = storage._forum_post_to_item({"id": "not-int"}, pk="pk", sk="sk", pk_value="POST")
    assert item2["id"] == 0
    assert item2["sk"] == "0"


def test_get_shard_key_uses_heavy_prefixes() -> None:
    "Test get shard key uses heavy prefixes."
    storage = _import_storage()
    # Heavy prefixes use 5 chars
    assert storage._get_shard_key("0312ABCDE") == "0312a"
    assert storage._get_shard_key("B000XYZ") == "b000x"
    # Non-heavy uses 4 chars
    assert storage._get_shard_key("abcd1234") == "abcd"


# ---------------------------------------------------------------------------
# Module-level functions
# ---------------------------------------------------------------------------

def test_get_book_details_local_dir_reads_parquet_and_parses_rating(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    "Test get book details local dir reads parquet and parses rating."
    storage = _import_storage()

    class _ILoc:
        def __init__(self, item: dict):
            "Support __init__ for test doubles."
            self._item = item

        def __getitem__(self, idx: int):
            "Support __getitem__ for test doubles."
            assert idx == 0
            return types.SimpleNamespace(to_dict=lambda: dict(self._item))

    class _FakeDF:
        def __init__(self, rows: list[dict]):
            "Support __init__ for test doubles."
            self._rows = rows
            self.columns = list(rows[0].keys()) if rows else []

        def __getitem__(self, _mask: Any):
            "Support __getitem__ for test doubles."
            return self

        @property
        def empty(self) -> bool:
            "Helper for empty."
            return not self._rows

        @property
        def iloc(self) -> _ILoc:
            "Helper for iloc."
            return _ILoc(self._rows[0])

    captured: dict[str, Any] = {}

    def fake_read_parquet(path: str, engine: str = "pyarrow") -> _FakeDF:
        "Helper for fake read parquet."
        captured["path"] = path
        captured["engine"] = engine
        return _FakeDF([{"parent_asin": "P1", "average_rating": "4.5"}])

    monkeypatch.setattr(storage.pd, "read_parquet", fake_read_parquet, raising=False)

    out = storage.get_book_details("P1", local_dir=str(tmp_path), engine="fastparquet")
    assert out is not None
    assert out["parent_asin"] == "P1"
    assert out["average_rating"] == 4.5
    assert str(tmp_path) in captured["path"]
    assert captured["engine"] == "fastparquet"


def test_get_book_metadata_reads_dynamo_and_casts_average_rating() -> None:
    "Test get book metadata reads dynamo and casts average rating."
    storage = _import_storage()
    boto3_mod = sys.modules["boto3"]
    dynamo = boto3_mod.resource("dynamodb")
    table = dynamo.Table(storage.BOOKS_TABLE)
    table._next_get_item = {"Item": {"parent_asin": "P1", "average_rating": Decimal("4.0")}}

    out = storage.get_book_metadata("P1")
    assert out is not None
    assert out["parent_asin"] == "P1"
    assert out["average_rating"] == 4.0


def test_get_book_metadata_returns_none_on_exception() -> None:
    "Test get book metadata returns none on exception."
    storage = _import_storage()
    boto3_mod = sys.modules["boto3"]
    dynamo = boto3_mod.resource("dynamodb")
    table = dynamo.Table(storage.BOOKS_TABLE)
    table.raise_on_get = RuntimeError("boom")
    assert storage.get_book_metadata("P1") is None
    # Reset so other tests are not affected
    table.raise_on_get = None


def test_increment_and_reset_library_actions_since_recs() -> None:
    "Test increment and reset library actions since recs."
    storage = _import_storage()
    boto3_mod = sys.modules["boto3"]
    dynamo = boto3_mod.resource("dynamodb")
    table = dynamo.Table(storage.USER_LIBRARY_TABLE)

    # increment: raw_count is str -> int conversion
    table._next_update_item = {"Attributes": {"actions_since_recs": "3"}}
    out = storage.increment_library_actions_since_recs("u@example.com", threshold=3)
    assert out == {
        "user_id": "u@example.com",
        "actions_since_recs": 3,
        "should_run_recommender": True,
    }

    # reset succeeds
    table._next_update_item = {}
    table.raise_on_update = None
    assert storage.reset_library_actions_since_recs("u@example.com") is True

    # reset fails on exception
    table.raise_on_update = RuntimeError("nope")
    assert storage.reset_library_actions_since_recs("u@example.com") is False
    table.raise_on_update = None


def test_get_storage_chooses_local_or_cloud(monkeypatch: pytest.MonkeyPatch) -> None:
    "Test get storage chooses local or cloud."
    storage = _import_storage()
    import backend.config as cfg

    monkeypatch.setattr(cfg, "IS_AWS", False, raising=False)
    assert isinstance(storage.get_storage(), storage.LocalStorage)

    monkeypatch.setattr(cfg, "IS_AWS", True, raising=False)
    assert isinstance(storage.get_storage(), storage.CloudStorage)


def test_trivial_top_level_placeholders_return_defaults() -> None:
    """Module-level stub functions return their documented defaults."""
    storage = _import_storage()
    assert storage.get_cached_event_recs("u@example.com") is None
    assert storage.put_cached_event_recs("u@example.com", {"events": []}) is False
    assert storage.get_catalog("P1") is None
    assert storage.get_user_accounts("u@example.com") is None
    # These module-level stubs return None (not the class methods)
    assert storage.get_user_books("u@example.com") is None
    assert storage.get_user_clubs("u@example.com") is None
    assert storage.get_user_forums("u@example.com") is None
    assert storage.get_form_thread("P1") is None


# ---------------------------------------------------------------------------
# LocalStorage
# ---------------------------------------------------------------------------

def test_local_storage_top50_reviews_and_spl_fallback(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    "Test local storage top50 reviews and spl fallback."
    storage = _import_storage()
    from backend import config as cfg

    monkeypatch.setattr(cfg, "PROCESSED_DIR", tmp_path, raising=False)

    # Case 1: explicit REVIEWS_TOP50_BOOKS_LOCAL_PATH exists and is a list.
    top50_path = tmp_path / "top50.json"
    top50_path.write_text('[{"id": 1}, {"id": 2}]', encoding="utf-8")

    storage_module = sys.modules["backend.storage"]
    # Use monkeypatch so teardown is automatic and safe even if attr didn't exist
    monkeypatch.setattr(storage_module, "REVIEWS_TOP50_BOOKS_LOCAL_PATH", top50_path, raising=False)

    ls = storage.LocalStorage()
    # Clear the instance cache so the fresh path is read
    ls._cache.clear()
    books = ls.get_top50_review_books()
    assert books == [{"id": 1}, {"id": 2}]

    # Case 2: top50 path missing -> fall back to reviews_top25_books.json with {"books": [...]} shape.
    top50_path.unlink()
    # Remove the module-level attr so the method falls through to the config fallback
    monkeypatch.delattr(storage_module, "REVIEWS_TOP50_BOOKS_LOCAL_PATH", raising=False)

    reviews25_path = tmp_path / "reviews_top25_books.json"
    reviews25_path.write_text('{"books": [{"id": 3}]}', encoding="utf-8")

    ls._cache.clear()
    books2 = ls.get_top50_review_books()
    assert books2 == [{"id": 3}]


def test_local_storage_get_soonest_events_and_helpers(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    "Test local storage get soonest events and helpers."
    storage = _import_storage()
    from backend import config as cfg

    monkeypatch.setattr(cfg, "PROCESSED_DIR", tmp_path, raising=False)
    events_path = tmp_path / "book_events_clean.json"

    import json
    events = [
        {"event_id": "E2", "ttl": 20, "city_state": "Seattle, WA", "parent_asin": "P1"},
        {"event_id": "E1", "ttl": 10, "city_state": "Seattle, WA", "parent_asin": "P1"},
        "not-a-dict",
    ]
    events_path.write_text(json.dumps(events), encoding="utf-8")

    ls = storage.LocalStorage()
    ls._cache.clear()

    soonest = ls.get_soonest_events(limit=5)
    # Sorted by ttl ascending; non-dict entries filtered out
    assert [e["event_id"] for e in soonest] == ["E1", "E2"]

    # get_event_details finds by event_id
    detail = ls.get_event_details("E2")
    assert detail["event_id"] == "E2"

    # get_events_by_city filters by city_state
    seattle_events = ls.get_events_by_city("Seattle, WA")
    assert len(seattle_events) == 2

    # get_events_for_book filters by parent_asin and respects limit
    events_for_book = ls.get_events_for_book("P1", limit=1)
    assert len(events_for_book) == 1


def test_local_storage_user_recommendations_round_trip(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    "Test local storage user recommendations round trip."
    storage = _import_storage()
    from backend import config as cfg

    rec_path = tmp_path / "user_recs.json"
    monkeypatch.setattr(cfg, "USER_RECOMMENDATIONS_PATH", rec_path, raising=False)

    ls = storage.LocalStorage()

    # No file yet -> None
    assert ls.get_user_recommendations("User@example.com") is None

    # Save; should create file and normalize key to lowercase
    ls.save_user_recommendations("User@example.com", {"events": [1, 2, 3]})
    recs = ls.get_user_recommendations("user@example.com")
    assert recs == {"events": [1, 2, 3]}


# ---------------------------------------------------------------------------
# CloudStorage
# ---------------------------------------------------------------------------

def test_cloud_storage_get_top50_review_books_and_spl_top50(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    "Test cloud storage get top50 review books and spl top50."
    storage = _import_storage()
    cs = storage.CloudStorage()
    from backend import config as cfg

    monkeypatch.setattr(cfg, "DATA_BUCKET", "bucket", raising=False)
    monkeypatch.setattr(cfg, "REVIEWS_TOP50_BOOKS_S3_KEY", "reviews.json", raising=False)
    monkeypatch.setattr(cfg, "TOP50_BOOKS_S3_KEY", "spl_top50.json", raising=False)

    import json as _json
    boto3_mod = sys.modules["boto3"]
    orig_client = getattr(boto3_mod, "client")

    def client_reviews(service_name: str, **kwargs: Any) -> Any:
        "Helper for client reviews."
        body_bytes = _json.dumps({"books": [{"id": "R1"}]}).encode("utf-8")
        return types.SimpleNamespace(
            get_object=lambda **kw: {"Body": types.SimpleNamespace(read=lambda: body_bytes)}
        )

    boto3_mod.client = client_reviews  # type: ignore[assignment]
    reviews = cs.get_top50_review_books()
    assert reviews == [{"id": "R1"}]

    def client_spl(service_name: str, **kwargs: Any) -> Any:
        "Helper for client spl."
        body_bytes = _json.dumps({"items": [{"id": "S1"}]}).encode("utf-8")
        return types.SimpleNamespace(
            get_object=lambda **kw: {"Body": types.SimpleNamespace(read=lambda: body_bytes)}
        )

    boto3_mod.client = client_spl  # type: ignore[assignment]
    spl = cs.get_spl_top50_checkout_books()
    assert spl == [{"id": "S1"}]

    boto3_mod.client = orig_client  # restore


def test_cloud_storage_get_user_books_normalizes_shelves_and_handles_missing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    "Test cloud storage get user books normalizes shelves and handles missing."
    storage = _import_storage()
    cs = storage.CloudStorage()
    from backend import config as cfg

    # CloudStorage._table reads USER_BOOKS_TABLE from config; default fallback is "user_books"
    monkeypatch.setattr(cfg, "USER_BOOKS_TABLE", "user_books", raising=False)
    monkeypatch.setattr(cfg, "USER_BOOKS_PK", "user_email", raising=False)

    boto3_mod = sys.modules["boto3"]
    dynamo = boto3_mod.resource("dynamodb")
    table = dynamo.Table("user_books")

    table._next_get_item = {
        "Item": {
            "user_email": "u@example.com",
            "library": {
                "in_progress": [" A ", None, " "],
                "saved": [1, 2],
                "finished": [],
            },
        }
    }
    table.raise_on_get = None

    out = cs.get_user_books("u@example.com")
    assert out is not None
    # Whitespace stripped, None filtered
    assert out["library"]["in_progress"] == ["A"]
    # Numerics cast to string
    assert out["library"]["saved"] == ["1", "2"]
    assert out["library"]["finished"] == []

    # Exception path returns None
    table.raise_on_get = RuntimeError("fail")
    assert cs.get_user_books("u@example.com") is None
    table.raise_on_get = None


def test_cloud_storage_get_and_save_user_recommendations(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    "Test cloud storage get and save user recommendations."
    storage = _import_storage()
    cs = storage.CloudStorage()
    from backend import config as cfg

    monkeypatch.setattr(cfg, "USER_RECOMMENDATIONS_TABLE", "user_recommendations", raising=False)

    boto3_mod = sys.modules["boto3"]
    dynamo = boto3_mod.resource("dynamodb")
    table = dynamo.Table("user_recommendations")

    # No item -> None (get_item returns {} with no "Item" key)
    table._next_get_item = {}
    assert cs.get_user_recommendations("u@example.com") is None

    # With item -> returns _from_dynamo converted dict
    table._next_get_item = {"Item": {"user_email": "u@example.com", "x": Decimal("1")}}
    rec = cs.get_user_recommendations("u@example.com")
    assert rec == {"user_email": "u@example.com", "x": 1}

    # save_user_recommendations calls put_item with Decimal-converted values
    captured: Dict[str, Any] = {}

    def put_item(**kwargs: Any) -> dict:
        "Helper for put item."
        captured.update(kwargs)
        return {}

    table.put_item = put_item
    cs.save_user_recommendations("u@example.com", {"score": 1.25})
    assert "Item" in captured
    assert captured["Item"]["user_email"] == "u@example.com"
    assert captured["Item"]["score"] == Decimal("1.25")


def test_cloud_storage_get_events_by_city_and_for_book(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    "Test cloud storage get events by city and for book."
    storage = _import_storage()
    cs = storage.CloudStorage()
    from backend import config as cfg

    # No GSI configured -> empty list
    monkeypatch.setattr(cfg, "EVENTS_CITY_STATE_GSI", "", raising=False)
    assert cs.get_events_by_city("Seattle, WA") == []

    monkeypatch.setattr(cfg, "EVENTS_CITY_STATE_GSI", "CITY_GSI", raising=False)
    monkeypatch.setattr(cfg, "EVENTS_PARENT_ASIN_GSI", "PA_GSI", raising=False)
    monkeypatch.setattr(cfg, "EVENTS_TABLE", "events", raising=False)

    boto3_mod = sys.modules["boto3"]
    dynamo = boto3_mod.resource("dynamodb")
    table = dynamo.Table("events")

    def query(**kwargs: Any) -> dict:
        "Helper for query."
        if kwargs.get("IndexName") == "CITY_GSI":
            return {"Items": [{"city_state": "Seattle, WA", "ttl": Decimal("10")}]}
        return {"Items": [{"parent_asin": "P1", "ttl": Decimal("20")}]}

    table.query = query

    by_city = cs.get_events_by_city("Seattle, WA")
    assert by_city[0]["city_state"] == "Seattle, WA"
    assert by_city[0]["ttl"] == 10  # Decimal converted by _from_dynamo

    by_book = cs.get_events_for_book("P1", limit=5)
    assert by_book[0]["parent_asin"] == "P1"


def test_cloud_storage_forum_thread_helpers(monkeypatch: pytest.MonkeyPatch) -> None:
    "Test cloud storage forum thread helpers."
    storage = _import_storage()
    cs = storage.CloudStorage()
    from backend import config as cfg

    # No GSI -> empty list / None
    monkeypatch.setattr(cfg, "FORUM_POSTS_GSI", "", raising=False)
    assert cs.get_forum_thread_for_book("P1") == []
    assert cs.get_forum_thread("P1") is None

    monkeypatch.setattr(cfg, "FORUM_POSTS_GSI", "FORUM_GSI", raising=False)
    monkeypatch.setattr(cfg, "FORUM_POSTS_TABLE", "forum_posts", raising=False)

    boto3_mod = sys.modules["boto3"]
    dynamo = boto3_mod.resource("dynamodb")
    table = dynamo.Table("forum_posts")

    def query(**kwargs: Any) -> dict:
        "Helper for query."
        return {"Items": [{"parent_asin": "P1", "id": Decimal("7")}]}

    table.query = query

    posts = cs.get_forum_thread_for_book("P1")
    assert posts[0]["parent_asin"] == "P1"
    assert posts[0]["id"] == 7  # Decimal converted

    thread = cs.get_forum_thread("P1")
    assert thread == {"posts": posts}