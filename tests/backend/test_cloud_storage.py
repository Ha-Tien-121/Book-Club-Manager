"""
Tests for Book-Club-Manager.backend.cloud_storage.

This module is a thin facade that re-exports CloudStorage from backend.storage.
We verify that importing backend.cloud_storage succeeds and that the
CloudStorage name it exposes matches backend.storage.CloudStorage.
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


def test_cloud_storage_reexports_cloud_storage_class() -> None:
    """backend.cloud_storage.CloudStorage should be the same object as backend.storage.CloudStorage."""
    import backend.cloud_storage as cloud_mod  # type: ignore  # noqa: E402
    import backend.storage as storage_mod  # type: ignore  # noqa: E402

    assert hasattr(cloud_mod, "CloudStorage")
    assert cloud_mod.CloudStorage is storage_mod.CloudStorage

