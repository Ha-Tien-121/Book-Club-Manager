from __future__ import annotations

import importlib
import types
from decimal import Decimal


def _import_storage():
    mod = importlib.import_module("backend.storage")
    return importlib.reload(mod)


def test_from_dynamo_and_to_dynamo_recursive_conversions() -> None:
    storage = _import_storage()

    obj = {
        "a": Decimal("2"),
        "b": Decimal("2.5"),
        "c": [Decimal("1"), {"x": Decimal("3.0")}],
        "d": "s",
    }
    out = storage._from_dynamo(obj)
    assert out == {"a": 2, "b": 2.5, "c": [1, {"x": 3}], "d": "s"}

    back = storage._to_dynamo({"a": 1.25, "b": [2.0]})
    assert isinstance(back["a"], Decimal)
    assert str(back["a"]) == "1.25"
    assert isinstance(back["b"][0], Decimal)


def test_forum_post_to_item_normalizes_keys_and_id() -> None:
    storage = _import_storage()
    item = storage._forum_post_to_item({"id": "7", "title": "T"}, pk="pk", sk="sk", pk_value="POST")
    assert item["id"] == 7
    assert item["post_id"] == 7
    assert item["pk"] == "POST"
    assert item["sk"] == "7"

    # bad id -> 0
    item2 = storage._forum_post_to_item({"id": "nope"}, pk="pk", sk="sk", pk_value="POST")
    assert item2["id"] == 0
    assert item2["sk"] == "0"


def test_cloud_storage_save_forum_db_writes_posts_and_logs_on_error(monkeypatch) -> None:  # type: ignore[no-untyped-def]
    storage = _import_storage()
    cs = storage.CloudStorage()

    import boto3  # type: ignore

    dyn = boto3.resource("dynamodb")
    table = dyn.Table("forum_posts")

    put_items: list[dict] = []

    def _put_item(*, Item: dict, **_kw):  # type: ignore[no-untyped-def]
        put_items.append(Item)
        return {}

    table.put_item = _put_item  # type: ignore[assignment]

    cs.save_forum_db({"posts": [{"id": 1, "title": "A"}], "next_post_id": 2})
    assert len(put_items) == 2
    assert any(it.get("pk") == "META" for it in put_items)
    assert any(it.get("pk") == "POST" for it in put_items)

    # Force exception path: put_item raises
    def _boom(*_a, **_kw):  # type: ignore[no-untyped-def]
        raise RuntimeError("boom")

    table.put_item = _boom  # type: ignore[assignment]

    warnings: list[str] = []

    def _warn(msg: str, *args: object, **_kw2: object) -> None:
        warnings.append(msg % args if args else msg)

    monkeypatch.setattr(storage.logging, "warning", _warn)
    cs.save_forum_db({"posts": [{"id": 1, "title": "A"}], "next_post_id": 2})
    assert any("save_forum_db failed" in w for w in warnings)


def test_cloud_storage_get_books_metadata_batch_handles_bad_response(monkeypatch) -> None:  # type: ignore[no-untyped-def]
    storage = _import_storage()
    cs = storage.CloudStorage()

    import boto3  # type: ignore

    # Make client.batch_get_item return weird shapes and ensure it doesn't crash.
    orig_client = boto3.client

    class _Client:
        def __init__(self) -> None:
            self.calls = 0

        def batch_get_item(self, **_kw):  # type: ignore[no-untyped-def]
            self.calls += 1
            if self.calls == 1:
                return {"Responses": {"books": [{"parent_asin": "P1"}]}}
            return {}  # missing keys

    client = _Client()
    boto3.client = lambda service_name, **_kw: client if service_name == "dynamodb" else orig_client(service_name, **_kw)  # type: ignore[assignment]

    try:
        out = cs.get_books_metadata_batch(["P1", "P2"])
        # best-effort: may include P1, but must be a dict and not raise
        assert isinstance(out, dict)
    finally:
        boto3.client = orig_client  # type: ignore[assignment]

