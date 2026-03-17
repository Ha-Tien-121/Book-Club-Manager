"""
Tests for Book-Club-Manager.backend.config.

Focus:
- Environment flags (APP_ENV, IS_LOCAL, IS_AWS).
- Base directory and path constants.
- DynamoDB table and key configuration from environment variables.
- S3 / CDN-related configuration.
- Numeric and boolean tuning parameters from env vars.
"""

import os
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


def test_default_environment_and_paths(monkeypatch: "pytest.MonkeyPatch") -> None:  # type: ignore[name-defined]
    """With no env overrides, config should default to local mode and expected paths."""
    # Clear relevant env vars
    for key in ["APP_ENV", "AWS_REGION", "DATA_BUCKET"]:
        monkeypatch.delenv(key, raising=False)

    import importlib

    config = importlib.reload(importlib.import_module("backend.config"))

    assert config.APP_ENV == "local"
    assert config.IS_LOCAL is True
    assert config.IS_AWS is False

    base_dir = config.BASE_DIR
    assert isinstance(base_dir, Path)
    assert (base_dir / "data" / "processed") == config.PROCESSED_DIR
    assert (base_dir / "data" / "users") == config.USERS_DIR
    # A couple of path constants should live under PROCESSED_DIR / USERS_DIR.
    assert str(config.USER_ACCOUNTS_PATH).startswith(str(config.PROCESSED_DIR))
    assert str(config.USER_EVENTS_PATH).startswith(str(config.USERS_DIR))


def test_environment_and_table_names_from_env(monkeypatch: "pytest.MonkeyPatch") -> None:  # type: ignore[name-defined]
    """APP_ENV and table-related vars should be read and normalized from env."""
    monkeypatch.setenv("APP_ENV", "aws")
    monkeypatch.setenv("AWS_REGION", "eu-central-1")
    monkeypatch.setenv("USER_ACCOUNTS_TABLE", "ua_table")
    monkeypatch.setenv("USER_BOOKS_PK", " user_id ")
    monkeypatch.setenv("FORUM_POSTS_PK", " pk_custom ")
    monkeypatch.setenv("FORUM_POSTS_GSI", "  ")

    import importlib

    config = importlib.reload(importlib.import_module("backend.config"))

    assert config.APP_ENV == "aws"
    assert config.IS_LOCAL is False
    assert config.IS_AWS is True
    assert config.AWS_REGION == "eu-central-1"
    assert config.USER_ACCOUNTS_TABLE == "ua_table"
    # Whitespace-stripped PK names.
    assert config.USER_BOOKS_PK == "user_id"
    assert config.FORUM_POSTS_PK == "pk_custom"
    # Blank GSI env -> None due to strip() or fallback.
    assert config.FORUM_POSTS_GSI is None


def test_s3_and_cdn_configuration(monkeypatch: "pytest.MonkeyPatch") -> None:  # type: ignore[name-defined]
    """DATA_BUCKET and CDN_BASE_URL should be derived correctly from env."""
    monkeypatch.setenv("DATA_BUCKET", "my-bucket")
    monkeypatch.setenv("AWS_REGION", "ap-southeast-1")
    monkeypatch.delenv("CDN_BASE_URL", raising=False)

    import importlib

    config = importlib.reload(importlib.import_module("backend.config"))

    assert config.DATA_BUCKET == "my-bucket"
    # Default CDN_BASE_URL derives from bucket and region when not set explicitly.
    assert (
        config.CDN_BASE_URL
        == "https://my-bucket.s3.ap-southeast-1.amazonaws.com"
    )


def test_numeric_and_boolean_tuning_from_env(monkeypatch: "pytest.MonkeyPatch") -> None:  # type: ignore[name-defined]
    """Numeric and boolean tuning config should parse from env with sensible defaults."""
    monkeypatch.setenv("BCRYPT_ROUNDS", "15")
    monkeypatch.setenv("USE_BOOK_ML_RECOMMENDER", "TrUe")
    monkeypatch.setenv("FORUM_PREVIEW_MAX_CHARS", " 300 ")
    monkeypatch.setenv("BOOK_DESCRIPTION_PREVIEW_CHARS", "")

    import importlib

    config = importlib.reload(importlib.import_module("backend.config"))

    assert config.BCRYPT_ROUNDS == 15
    assert config.USE_BOOK_ML_RECOMMENDER is True
    assert config.FORUM_PREVIEW_MAX_CHARS == 300
    # Empty env should fall back to default (600)
    assert config.BOOK_DESCRIPTION_PREVIEW_CHARS == 600

