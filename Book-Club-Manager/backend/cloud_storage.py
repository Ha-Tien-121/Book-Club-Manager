"""Cloud storage facade.

This module re-exports the CloudStorage implementation from backend.storage,
so callers can depend on backend.cloud_storage.CloudStorage instead of the
monolithic storage module.
"""

from __future__ import annotations

from backend.storage import CloudStorage  # noqa: F401

