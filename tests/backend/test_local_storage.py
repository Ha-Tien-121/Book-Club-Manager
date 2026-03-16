"""
Tests for Book-Club-Manager.backend.local_storage.

This module is a thin facade that re-exports LocalStorage from backend.storage.
We verify that importing backend.local_storage succeeds and that the
LocalStorage name it exposes matches backend.storage.LocalStorage.
"""

import sys
from pathlib import Path


def _ensure_inner_project_on_path() -> None:
    """Ensure inner Book-Club-Manager root is on sys.path when running from outer root."""
    tests_dir = Path(__file__).resolve().parents[1]
    repo_root = tests_dir.parent
    inner_root = repo_root / "Book-Club-Manager"
    if inner_root.is_dir() and str(inner_root) not in sys.path:
        sys.path.insert(0, str(inner_root))


_ensure_inner_project_on_path()


def test_local_storage_reexports_local_storage_class() -> None:
    """backend.local_storage.LocalStorage should be the same object as backend.storage.LocalStorage."""
    import backend.local_storage as local_mod  # type: ignore  # noqa: E402
    import backend.storage as storage_mod  # type: ignore  # noqa: E402

    assert hasattr(local_mod, "LocalStorage")
    assert local_mod.LocalStorage is storage_mod.LocalStorage

