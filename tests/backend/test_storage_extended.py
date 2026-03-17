from __future__ import annotations

import importlib
from decimal import Decimal
import types


def _import_storage():
    "Helper for  import storage."
    mod = importlib.import_module("backend.storage")
    return importlib.reload(mod)


def test_cloud_storage_get_user_account_and_events_round_trip() -> None:
    "Test cloud storage get user account and events round trip."
    storage = _import_storage()
    cs = storage.CloudStorage()

    # Seed fake Dynamo tables via the boto3 stub installed for tests.
    import boto3  # type: ignore

    dyn = boto3.resource("dynamodb")
    accounts = dyn.Table("user_accounts")
    events = dyn.Table("user_events")

    # CloudStorage reads pk from config; defaults are used here.
    accounts._next_get_item = {"Item": {"user_id": "u@example.com", "name": "U"}}
    out = cs.get_user_account("U@Example.com")
    assert out == {"user_id": "u@example.com", "name": "U"}

    events._next_get_item = {"Item": {"user_id": "u@example.com", "events": ["E1", " E2 "]}}
    ev = cs.get_user_events("u@example.com")
    assert ev is not None
    assert ev["events"] == ["E1", " E2 "]


def test_cloud_storage_get_books_metadata_batch_empty_and_decode_fallback() -> None:
    "Test cloud storage get books metadata batch empty and decode fallback."
    storage = _import_storage()
    cs = storage.CloudStorage()

    assert cs.get_books_metadata_batch([]) == {}

    # Client stub returns wire-format items; ensure function doesn't crash and
    # returns mapping by parent_asin when possible.
    import boto3  # type: ignore

    # Patch boto3.client so CloudStorage uses our stubbed batch_get_item.
    orig_client = boto3.client

    def _client(service_name: str, **_kw: object):
        "Helper for  client."
        assert service_name == "dynamodb"
        return types.SimpleNamespace(
            batch_get_item=lambda **_kw2: {
                "Responses": {
                    "books": [
                        {"parent_asin": {"S": "P1"}, "rating_number": {"N": "1"}},
                    ]
                }
            }
        )

    boto3.client = _client  # type: ignore[assignment]

    # Ensure table.name resolves to "books"
    dyn = boto3.resource("dynamodb")
    dyn.Table("books")._name = "books"

    out = cs.get_books_metadata_batch(["P1"])
    boto3.client = orig_client  # type: ignore[assignment]

    assert "P1" in out


def test_cloud_storage_load_forum_db_meta_counter_and_posts() -> None:
    "Test cloud storage load forum db meta counter and posts."
    storage = _import_storage()
    cs = storage.CloudStorage()

    import boto3  # type: ignore

    dyn = boto3.resource("dynamodb")
    table = dyn.Table("forum_posts")

    # META row for next_post_id
    table._next_get_item = {"Item": {"pk": "META", "sk": "next_post_id", "next_post_id": Decimal("7")}}
    # scan returns one post item + meta item; CloudStorage filters pk=POST
    table.scan = lambda **_kw: {  # type: ignore[assignment]
        "Items": [
            {"pk": "POST", "sk": "1", "id": Decimal("1"), "parent_asin": "P1"},
            {"pk": "META", "sk": "next_post_id", "next_post_id": Decimal("7")},
        ]
    }

    db = cs.load_forum_db()
    assert db["next_post_id"] == 7
    assert len(db["posts"]) == 1
    assert db["posts"][0]["id"] == 1

