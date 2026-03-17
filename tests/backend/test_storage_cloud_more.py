from __future__ import annotations

import importlib
import json
import os
import types


def _import_storage():
    mod = importlib.import_module("backend.storage")
    return importlib.reload(mod)


class _Body:
    def __init__(self, payload: str) -> None:
        self._payload = payload

    def read(self) -> bytes:  # matches boto3 StreamingBody API used in code
        return self._payload.encode("utf-8")


def test_cloud_storage_top50_and_spl_s3_branches(monkeypatch) -> None:  # type: ignore[no-untyped-def]
    storage = _import_storage()
    cs = storage.CloudStorage()

    # bucket missing -> []
    monkeypatch.delenv("DATA_BUCKET", raising=False)
    out = cs.get_top50_review_books()
    assert out == []

    # Success path: list payload
    monkeypatch.setenv("DATA_BUCKET", "bucket")

    import boto3  # type: ignore

    orig_client = boto3.client

    class _S3:
        def __init__(self, payload: str) -> None:
            self._payload = payload

        def get_object(self, **_kw):  # type: ignore[no-untyped-def]
            return {"Body": _Body(self._payload)}

    boto3.client = lambda service_name, **_kw: _S3(json.dumps([{"parent_asin": "P1"}])) if service_name == "s3" else orig_client(service_name, **_kw)  # type: ignore[assignment]
    assert cs.get_top50_review_books() == [{"parent_asin": "P1"}]

    # Success path: dict payload with "books"
    boto3.client = lambda service_name, **_kw: _S3(json.dumps({"books": [{"parent_asin": "P2"}]})) if service_name == "s3" else orig_client(service_name, **_kw)  # type: ignore[assignment]
    assert cs.get_top50_review_books() == [{"parent_asin": "P2"}]

    # Exception path in SPL -> []
    boto3.client = lambda service_name, **_kw: types.SimpleNamespace(get_object=lambda **_kw2: (_ for _ in ()).throw(RuntimeError("boom"))) if service_name == "s3" else orig_client(service_name, **_kw)  # type: ignore[assignment]
    assert cs.get_spl_top50_checkout_books() == []

    # SPL success: list payload
    boto3.client = lambda service_name, **_kw: _S3(json.dumps([{"parent_asin": "P3"}])) if service_name == "s3" else orig_client(service_name, **_kw)  # type: ignore[assignment]
    assert cs.get_spl_top50_checkout_books() == [{"parent_asin": "P3"}]

    # SPL success: dict payload with "items"
    boto3.client = lambda service_name, **_kw: _S3(json.dumps({"items": [{"parent_asin": "P4"}]})) if service_name == "s3" else orig_client(service_name, **_kw)  # type: ignore[assignment]
    assert cs.get_spl_top50_checkout_books() == [{"parent_asin": "P4"}]

    boto3.client = orig_client  # type: ignore[assignment]


def test_cloud_storage_events_gsi_queries_and_early_returns(monkeypatch) -> None:  # type: ignore[no-untyped-def]
    storage = _import_storage()
    cs = storage.CloudStorage()

    # No GSI -> []
    monkeypatch.setattr(storage._config, "EVENTS_CITY_STATE_GSI", None, raising=False)
    monkeypatch.setattr(storage._config, "EVENTS_PARENT_ASIN_GSI", None, raising=False)
    monkeypatch.delenv("EVENTS_CITY_STATE_GSI", raising=False)
    monkeypatch.delenv("EVENTS_PARENT_ASIN_GSI", raising=False)
    assert cs.get_events_by_city("Seattle, WA") == []
    assert cs.get_events_for_book("P1") == []

    import boto3  # type: ignore

    dyn = boto3.resource("dynamodb")
    events_table = dyn.Table("events")

    # Provide GSIs and stub query results
    os.environ["EVENTS_CITY_STATE_GSI"] = "city_state_index"
    os.environ["EVENTS_PARENT_ASIN_GSI"] = "parent_asin_index"

    def _query(**kwargs):  # type: ignore[no-untyped-def]
        # Ensure IndexName is passed through
        assert "IndexName" in kwargs
        return {"Items": [{"event_id": "e1"}, {"event_id": "e2"}]}

    events_table.query = _query  # type: ignore[assignment]

    assert cs.get_events_by_city("Seattle, WA") == [{"event_id": "e1"}, {"event_id": "e2"}]
    assert cs.get_events_for_book("P1", limit=2) == [{"event_id": "e1"}, {"event_id": "e2"}]


def test_cloud_storage_save_user_books_store_mode_and_save_user_events_cleaning() -> None:
    storage = _import_storage()
    cs = storage.CloudStorage()

    import boto3  # type: ignore

    dyn = boto3.resource("dynamodb")
    user_books = dyn.Table("user_books")
    user_events = dyn.Table("user_events")

    put_books: list[dict] = []
    put_events: list[dict] = []

    def _put_books(*, Item: dict, **_kw):  # type: ignore[no-untyped-def]
        put_books.append(Item)
        return {}

    def _put_events(*, Item: dict, **_kw):  # type: ignore[no-untyped-def]
        put_events.append(Item)
        return {}

    user_books.put_item = _put_books  # type: ignore[assignment]
    user_events.put_item = _put_events  # type: ignore[assignment]

    cs.save_user_books(
        {
            "books": {
                "U@Example.com": {"library": {"saved": [" P1 ", None], "in_progress": None, "finished": [""]}},
                "": {"library": {}},
            }
        }
    )
    assert len(put_books) == 1
    assert put_books[0]["user_email"] == "u@example.com"
    assert put_books[0]["library"]["saved"] == ["P1"]
    assert put_books[0]["library"]["in_progress"] == []

    cs.save_user_events("U@Example.com", {"events": [None, " e1 ", "", "e2"]})
    assert put_events
    assert put_events[-1]["user_email"] == "u@example.com"
    assert put_events[-1]["events"] == ["e1", "e2"]

