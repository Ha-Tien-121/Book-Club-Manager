from __future__ import annotations

import importlib
from decimal import Decimal


def _import_storage():
    mod = importlib.import_module("backend.storage")
    return importlib.reload(mod)


def test_get_shard_key_heavy_prefix_uses_5_chars() -> None:
    storage = _import_storage()
    assert storage._get_shard_key("b000xyz") == "b000x"
    assert storage._get_shard_key("B000XYZ") == "b000x"
    assert storage._get_shard_key("abcd123") == "abcd"


def test_get_book_metadata_success_none_and_exception() -> None:
    storage = _import_storage()

    import boto3  # type: ignore

    dyn = boto3.resource("dynamodb")
    table = dyn.Table(storage.BOOKS_TABLE)

    table._next_get_item = {"Item": {"parent_asin": "P1", "average_rating": Decimal("4.0")}}
    out = storage.get_book_metadata("P1")
    assert out is not None
    assert out["parent_asin"] == "P1"
    assert out["average_rating"] == 4.0

    table._next_get_item = {}
    assert storage.get_book_metadata("P1") is None

    table.raise_on_get = RuntimeError("boom")
    assert storage.get_book_metadata("P1") is None
    table.raise_on_get = None


def test_get_event_details_success_none_and_exception() -> None:
    storage = _import_storage()

    import boto3  # type: ignore

    dyn = boto3.resource("dynamodb")
    table = dyn.Table(storage.EVENTS_TABLE)

    table._next_get_item = {"Item": {"event_id": "e1", "name": "Event"}}
    out = storage.get_event_details("e1")
    assert out == {"event_id": "e1", "name": "Event"}

    table._next_get_item = {}
    assert storage.get_event_details("e1") is None

    table.raise_on_get = RuntimeError("boom")
    assert storage.get_event_details("e1") is None
    table.raise_on_get = None

