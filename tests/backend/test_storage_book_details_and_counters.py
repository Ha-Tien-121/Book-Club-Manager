from __future__ import annotations

import importlib
import types

import pandas as pd


def _import_storage():
    mod = importlib.import_module("backend.storage")
    return importlib.reload(mod)


def test_get_book_details_branches_via_mocked_parquet(monkeypatch, tmp_path) -> None:  # type: ignore[no-untyped-def]
    storage = _import_storage()

    # Empty parent_asin -> None
    assert storage.get_book_details("") is None

    # local_dir path is used; we don't need the file to exist since we mock read_parquet
    local_dir = tmp_path

    # Missing parent_asin column -> None
    monkeypatch.setattr(pd, "read_parquet", lambda *_a, **_kw: pd.DataFrame([{"x": 1}]))
    assert storage.get_book_details("P1", local_dir=str(local_dir)) is None

    # parent_asin present but no match -> None
    monkeypatch.setattr(
        pd,
        "read_parquet",
        lambda *_a, **_kw: pd.DataFrame([{"parent_asin": "OTHER"}]),
    )
    assert storage.get_book_details("P1", local_dir=str(local_dir)) is None

    # Match with average_rating cast success
    monkeypatch.setattr(
        pd,
        "read_parquet",
        lambda *_a, **_kw: pd.DataFrame([{"parent_asin": "P1", "average_rating": "4.0"}]),
    )
    out = storage.get_book_details("P1", local_dir=str(local_dir))
    assert out is not None
    assert out["parent_asin"] == "P1"
    assert out["average_rating"] == 4.0

    # Match with average_rating cast failure (keeps original)
    monkeypatch.setattr(
        pd,
        "read_parquet",
        lambda *_a, **_kw: pd.DataFrame([{"parent_asin": "P1", "average_rating": object()}]),
    )
    out2 = storage.get_book_details("P1", local_dir=str(local_dir))
    assert out2 is not None
    assert out2["parent_asin"] == "P1"


def test_increment_and_reset_library_actions_since_recs_success_and_exceptions() -> None:
    storage = _import_storage()

    import boto3  # type: ignore

    dyn = boto3.resource("dynamodb")
    table = dyn.Table(storage.USER_LIBRARY_TABLE)

    # increment: parses raw_count -> int and should_run_recommender threshold
    table._next_update_item = {"Attributes": {"actions_since_recs": "3"}}
    out = storage.increment_library_actions_since_recs("u@example.com", threshold=3)
    assert out == {
        "user_id": "u@example.com",
        "actions_since_recs": 3,
        "should_run_recommender": True,
    }

    # increment: bad raw_count -> 0
    table._next_update_item = {"Attributes": {"actions_since_recs": object()}}
    out2 = storage.increment_library_actions_since_recs("u@example.com", threshold=3)
    assert out2 is not None and out2["actions_since_recs"] == 0

    # increment exception -> None
    table.raise_on_update = RuntimeError("boom")
    assert storage.increment_library_actions_since_recs("u@example.com") is None
    table.raise_on_update = None

    # reset success -> True
    assert storage.reset_library_actions_since_recs("u@example.com") is True

    # reset exception -> False
    def _bad_update_item(**_kw):  # type: ignore[no-untyped-def]
        raise RuntimeError("boom")

    table.update_item = _bad_update_item  # type: ignore[assignment]
    assert storage.reset_library_actions_since_recs("u@example.com") is False

