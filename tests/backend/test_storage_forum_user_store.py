from __future__ import annotations

import importlib
import os
from decimal import Decimal


def _import_storage():
    mod = importlib.import_module("backend.storage")
    return importlib.reload(mod)


def test_cloud_storage_load_user_store_assembles_accounts_books_clubs_forum() -> None:
    storage = _import_storage()
    cs = storage.CloudStorage()

    import boto3  # type: ignore

    dyn = boto3.resource("dynamodb")
    dyn.Table("user_accounts")._next_get_item = {
        "Item": {"user_id": "u@example.com", "name": "U"}
    }
    dyn.Table("user_books")._next_get_item = {
        "Item": {
            "user_email": "u@example.com",
            "library": {"saved": [" P1 ", None], "in_progress": [], "finished": [""]},
            "genre_preferences": [{"genre": "F", "rank": 1}],
        }
    }
    dyn.Table("user_events")._next_get_item = {
        "Item": {"user_id": "u@example.com", "events": [" e1 ", None, ""]}
    }
    dyn.Table("user_forums")._next_get_item = {
        "Item": {"user_email": "u@example.com", "saved_forum_post_ids": [1]}
    }

    out = cs.load_user_store("U@Example.com")
    assert out["accounts"]["users"]["u@example.com"]["name"] == "U"
    assert out["books"]["u@example.com"]["library"]["saved"] == ["P1"]
    assert out["clubs"]["u@example.com"]["club_ids"] == ["e1"]
    assert out["forum"]["u@example.com"]["saved_forum_post_ids"] == [1]


def test_cloud_storage_forum_post_get_update_and_thread_gsi() -> None:
    storage = _import_storage()
    cs = storage.CloudStorage()

    import boto3  # type: ignore

    dyn = boto3.resource("dynamodb")
    table = dyn.Table("forum_posts")

    # get_forum_post parses ids and sets defaults
    table._next_get_item = {"Item": {"pk": "POST", "sk": "2", "title": "T"}}
    post = cs.get_forum_post(2)
    assert post is not None
    assert int(post["id"]) == 2
    assert int(post["post_id"]) == 2

    # update_forum_post writes put_item with normalized id fields
    captured: list[dict] = []

    def _put_item(*, Item: dict, **_kw):  # type: ignore[no-untyped-def]
        captured.append(Item)
        return {}

    table.put_item = _put_item  # type: ignore[assignment]
    cs.update_forum_post(3, {"title": "X", "parent_asin": "P1"})
    assert captured
    assert captured[-1].get("sk") == "3"

    # get_forum_thread_for_book requires GSI name; provide via env
    os.environ["FORUM_POSTS_GSI"] = "parent_asin_index"

    def _query(**_kw):  # type: ignore[no-untyped-def]
        return {"Items": [{"pk": "POST", "sk": "1", "parent_asin": "P1"}]}

    table.query = _query  # type: ignore[assignment]
    posts = cs.get_forum_thread_for_book("P1")
    assert isinstance(posts, list) and posts
    thread = cs.get_forum_thread("P1")
    assert thread is not None and "posts" in thread


def test_cloud_storage_save_forum_db_writes_posts_and_counter_row() -> None:
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

    cs.save_forum_db(
        {
            "posts": [{"id": 1, "title": "A", "parent_asin": "P1"}],
            "next_post_id": Decimal("5"),
        }
    )

    # One post item + one META counter row
    assert len(put_items) == 2
    assert any(it.get("pk") == "META" for it in put_items)

