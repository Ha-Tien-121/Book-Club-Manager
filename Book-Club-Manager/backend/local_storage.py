"""Local storage facade.

This module re-exports the LocalStorage implementation from backend.storage,
so callers can depend on backend.local_storage.LocalStorage instead of the
monolithic storage module.
"""

from __future__ import annotations

from backend.storage import LocalStorage  # noqa: F401

