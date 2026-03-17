"""Compatibility entrypoint for Streamlit tests.

The real application entrypoint lives in the nested `Book-Club-Manager/` folder.
The UI tests in `tests/test_streamlit_app.py` expect `streamlit_app.py` at the repo root.
"""

from __future__ import annotations

import sys
from pathlib import Path
import importlib.util


def _ensure_inner_project_on_path() -> None:
    repo_root = Path(__file__).resolve().parent
    inner_root = repo_root / "Book-Club-Manager"
    if inner_root.is_dir() and str(inner_root) not in sys.path:
        sys.path.insert(0, str(inner_root))


_ensure_inner_project_on_path()

_INNER_APP_PATH = (Path(__file__).resolve().parent / "Book-Club-Manager" / "streamlit_app.py").resolve()

# Load the inner entrypoint under a different module name to avoid importing this file again.
_spec = importlib.util.spec_from_file_location("_bookish_inner_streamlit_app", _INNER_APP_PATH)
if _spec is None or _spec.loader is None:
    raise ImportError(f"Could not load inner streamlit app from {_INNER_APP_PATH}")
_mod = importlib.util.module_from_spec(_spec)
sys.modules[_spec.name] = _mod
_spec.loader.exec_module(_mod)

# Re-export the inner module's globals (so Streamlit finds the same symbols).
globals().update({k: v for k, v in vars(_mod).items() if not k.startswith("_")})

