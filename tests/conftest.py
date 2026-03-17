from __future__ import annotations

import sys
from pathlib import Path
import types


def _ensure_inner_project_on_path() -> None:
    """Make inner `Book-Club-Manager/` importable as top-level packages.

    The repo is nested:
      <repo_root>/Book-Club-Manager/backend/...
      <repo_root>/Book-Club-Manager/data/...

    Running pytest from <repo_root> needs that inner folder on sys.path.
    """
    repo_root = Path(__file__).resolve().parents[1]
    inner_root = repo_root / "Book-Club-Manager"
    if inner_root.is_dir() and str(inner_root) not in sys.path:
        sys.path.insert(0, str(inner_root))


_ensure_inner_project_on_path()


def _ensure_boto3_importable() -> None:
    """Provide a deterministic boto3 stub for tests.

    Even when real boto3 imports successfully (common on Python 3.11),
    unit tests should not depend on AWS credentials/region/network.
    """
    # Always install the stub for test runs (overrides real boto3 if present).
    boto3_mod = types.ModuleType("boto3")

    class _Key:
        def __init__(self, name: str):
            "Support __init__ for test doubles."
            self.name = name

        def eq(self, value: object) -> tuple[str, str, object]:
            "Helper for eq."
            return ("eq", self.name, value)

    class _FakeTable:
        def __init__(self) -> None:
            "Support __init__ for test doubles."
            self.get_item_calls: list[dict[str, object]] = []
            self.update_item_calls: list[dict[str, object]] = []
            self._next_get_item: dict[str, object] = {}
            self._next_update_item: dict[str, object] = {}
            self.raise_on_get = None
            self.raise_on_update = None
            self._name: str = "fake_table"

        def get_item(self, **kwargs: object) -> dict[str, object]:
            "Helper for get item."
            self.get_item_calls.append(kwargs)
            if self.raise_on_get:
                raise self.raise_on_get
            return dict(self._next_get_item)

        def update_item(self, **kwargs: object) -> dict[str, object]:
            "Helper for update item."
            self.update_item_calls.append(kwargs)
            if self.raise_on_update:
                raise self.raise_on_update
            return dict(self._next_update_item)

        def put_item(self, **kwargs: object) -> dict[str, object]:
            "Helper for put item."
            return {}

        def scan(self, **kwargs: object) -> dict[str, object]:
            "Helper for scan."
            return {"Items": []}

        def query(self, **kwargs: object) -> dict[str, object]:
            "Helper for query."
            return {"Items": []}

        @property
        def name(self) -> str:
            "Helper for name."
            return self._name

    class _FakeDynamo:
        def __init__(self) -> None:
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

    def resource(service_name: str, **_kw: object) -> object:
        "Helper for resource."
        assert service_name == "dynamodb"
        return _dynamo_singleton

    def client(service_name: str, **_kw: object) -> object:
        "Helper for client."
        return types.SimpleNamespace(batch_get_item=lambda **_kw2: {})

    boto3_mod.resource = resource  # type: ignore[attr-defined]
    boto3_mod.client = client  # type: ignore[attr-defined]

    dyn_mod = types.ModuleType("boto3.dynamodb")
    cond_mod = types.ModuleType("boto3.dynamodb.conditions")
    setattr(cond_mod, "Key", _Key)
    setattr(cond_mod, "Attr", object())

    # Minimal TypeDeserializer to satisfy optional imports.
    types_mod = types.ModuleType("boto3.dynamodb.types")

    class TypeDeserializer:
        def deserialize(self, value):  # type: ignore[no-untyped-def]
            "Helper for deserialize."
            if not isinstance(value, dict) or len(value) != 1:
                return value
            (t, v), = value.items()
            if t in ("S", "N"):
                return v
            if t == "BOOL":
                return bool(v)
            if t == "NULL":
                return None
            if t == "M" and isinstance(v, dict):
                return {k: self.deserialize(v2) for k, v2 in v.items()}
            if t == "L" and isinstance(v, list):
                return [self.deserialize(x) for x in v]
            return v

    setattr(types_mod, "TypeDeserializer", TypeDeserializer)

    sys.modules["boto3"] = boto3_mod
    sys.modules["boto3.dynamodb"] = dyn_mod
    sys.modules["boto3.dynamodb.conditions"] = cond_mod
    sys.modules["boto3.dynamodb.types"] = types_mod


_ensure_boto3_importable()


def pytest_ignore_collect(collection_path: Path, config):  # type: ignore[no-untyped-def]
    """Ignore known integration test that user asked to skip.

    Note: `-k` filters after collection; it does not prevent imports.
    """
    # Allow running this file directly (e.g. `pytest tests/test_streamlit_app.py`)
    # while still skipping it for broader suite runs.
    if len(getattr(config, "args", []) or []) == 1:
        try:
            only = Path(str((config.args or [])[0])).name
            if only == "test_streamlit_app.py":
                return False
        except Exception:
            pass
    return Path(str(collection_path)).name == "test_streamlit_app.py"

