"""
Tests for Book-Club-Manager.backend.api.

The current api module is a placeholder (docstring + commented example),
but we still verify that it imports cleanly from the outer repo root.
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


def test_api_module_imports_and_has_docstring() -> None:
    """Import backend.api and assert it has a non-empty module-level docstring."""
    import backend.api as api  # type: ignore

    doc = getattr(api, "__doc__", "") or ""
    assert isinstance(doc, str)
    assert "API layer" in doc

