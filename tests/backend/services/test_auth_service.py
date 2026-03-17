"""
Tests for backend.services.auth_service.

- create_user: success, validation, email taken, normalization, user_books bootstrap
- login_user: success, invalid password, missing user, normalization
- get_user: found, not found, normalization
- _check_password behavior via login (None hash, malformed hash)
"""

import sys
import types
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

# Ensure inner Book-Club-Manager backend package is importable when running from outer repo root.
_TESTS_DIR = Path(__file__).resolve().parents[2]
_REPO_ROOT = _TESTS_DIR.parent
_INNER_ROOT = _REPO_ROOT / "Book-Club-Manager"
if _INNER_ROOT.is_dir() and str(_INNER_ROOT) not in sys.path:
    sys.path.insert(0, str(_INNER_ROOT))

# Avoid loading real boto3 when backend.storage is imported (env/version issues).
if "boto3" not in sys.modules:
    _boto3 = MagicMock()
    _conditions = types.ModuleType("boto3.dynamodb.conditions")
    _conditions.Attr = MagicMock()
    _conditions.Key = MagicMock()
    _boto3.dynamodb.conditions = _conditions
    sys.modules["boto3"] = _boto3
    sys.modules["boto3.dynamodb"] = MagicMock()
    sys.modules["boto3.dynamodb.conditions"] = _conditions

# Import after boto3 stub; we patch `ensure_default_recommendations` where it is used
# (auth_service imports it directly).
from backend.services import auth_service  # noqa: E402


@patch("backend.services.auth_service.ensure_default_recommendations")
@patch("backend.services.auth_service.get_storage")
class TestCreateUser(unittest.TestCase):
    """Tests for create_user."""

    def test_create_user_success_returns_sanitized_record(
        self, mock_get_storage: MagicMock, mock_ensure_defaults: MagicMock
    ) -> None:
        store = MagicMock()
        store.get_user_account.return_value = {}
        store.get_user_books.return_value = {}
        mock_get_storage.return_value = store

        result = auth_service.create_user("alice@example.com", "secret123")

        self.assertIn("user_id", result)
        self.assertEqual(result["user_id"], "alice@example.com")
        self.assertEqual(result["email"], "alice@example.com")
        self.assertEqual(result["name"], "alice")
        self.assertNotIn("password_hash", result)
        store.save_user_account.assert_called_once()
        store.save_user_books.assert_called_once()
        mock_ensure_defaults.assert_called_with("alice@example.com")

    def test_create_user_normalizes_email_lowercase(
        self, mock_get_storage: MagicMock, mock_ensure_defaults: MagicMock  # noqa: ARG002
    ) -> None:
        store = MagicMock()
        store.get_user_account.return_value = {}
        store.get_user_books.return_value = {}
        mock_get_storage.return_value = store

        result = auth_service.create_user("  Alice@Example.COM  ", "pass")
        self.assertEqual(result["email"], "alice@example.com")
        self.assertEqual(result["name"], "alice")

    def test_create_user_empty_email_raises(
        self, mock_get_storage: MagicMock, mock_ensure_defaults: MagicMock  # noqa: ARG002
    ) -> None:
        mock_get_storage.return_value = MagicMock()

        with self.assertRaises(ValueError) as ctx:
            auth_service.create_user("", "password")
        self.assertIn("Email and Password required", str(ctx.exception))

    def test_create_user_empty_password_raises(
        self, mock_get_storage: MagicMock, mock_ensure_defaults: MagicMock  # noqa: ARG002
    ) -> None:
        store = MagicMock()
        store.get_user_account.return_value = {}
        mock_get_storage.return_value = store

        with self.assertRaises(ValueError) as ctx:
            auth_service.create_user("a@b.com", "   ")
        self.assertIn("Email and Password required", str(ctx.exception))

    def test_create_user_email_taken_raises(
        self, mock_get_storage: MagicMock, mock_ensure_defaults: MagicMock  # noqa: ARG002
    ) -> None:
        store = MagicMock()
        store.get_user_account.return_value = {"user_id": "existing@example.com"}
        mock_get_storage.return_value = store

        with self.assertRaises(ValueError) as ctx:
            auth_service.create_user("existing@example.com", "password")
        self.assertIn("Email has been taken", str(ctx.exception))
        store.save_user_account.assert_not_called()

    def test_create_user_skips_save_user_books_when_library_exists(
        self, mock_get_storage: MagicMock, mock_ensure_defaults: MagicMock
    ) -> None:
        store = MagicMock()
        store.get_user_account.return_value = {}
        store.get_user_books.return_value = {
            "library": {"in_progress": [], "saved": [], "finished": []},
            "genre_preferences": ["Fantasy"],
        }
        mock_get_storage.return_value = store

        auth_service.create_user("bob@example.com", "pass")
        store.save_user_account.assert_called_once()
        store.save_user_books.assert_not_called()
        mock_ensure_defaults.assert_called_with("bob@example.com")

    def test_create_user_handles_get_user_books_non_dict(
        self, mock_get_storage: MagicMock, mock_ensure_defaults: MagicMock  # noqa: ARG002
    ) -> None:
        store = MagicMock()
        store.get_user_account.return_value = {}
        store.get_user_books.return_value = None
        mock_get_storage.return_value = store

        result = auth_service.create_user("c@d.com", "p")
        self.assertEqual(result["email"], "c@d.com")
        store.save_user_books.assert_called_once()


@patch("backend.services.auth_service.get_storage")
class TestLoginUser(unittest.TestCase):
    """Tests for login_user."""

    def test_login_user_success_returns_sanitized_record(
        self, mock_get_storage: MagicMock
    ) -> None:
        store = MagicMock()
        store.get_user_account.return_value = {
            "user_id": "alice@example.com",
            "email": "alice@example.com",
            "name": "alice",
            "password_hash": "$2b$12$dummy.hash.here",  # will be checked by bcrypt
        }
        mock_get_storage.return_value = store

        with patch("backend.services.auth_service._check_password", return_value=True):
            result = auth_service.login_user("alice@example.com", "secret")
        self.assertEqual(result["email"], "alice@example.com")
        self.assertNotIn("password_hash", result)

    def test_login_user_invalid_password_raises(
        self, mock_get_storage: MagicMock
    ) -> None:
        store = MagicMock()
        store.get_user_account.return_value = {
            "user_id": "a@b.com",
            "password_hash": "$2b$12$realhash",
        }
        mock_get_storage.return_value = store

        with patch("backend.services.auth_service._check_password", return_value=False):
            with self.assertRaises(ValueError) as ctx:
                auth_service.login_user("a@b.com", "wrong")
        self.assertIn("Invalid email or password", str(ctx.exception))

    def test_login_user_missing_user_raises(
        self, mock_get_storage: MagicMock
    ) -> None:
        store = MagicMock()
        store.get_user_account.return_value = {}
        mock_get_storage.return_value = store

        with self.assertRaises(ValueError) as ctx:
            auth_service.login_user("nobody@example.com", "any")
        self.assertIn("Invalid email or password", str(ctx.exception))

    def test_login_user_normalizes_email(
        self, mock_get_storage: MagicMock
    ) -> None:
        store = MagicMock()
        store.get_user_account.return_value = {
            "user_id": "alice@example.com",
            "password_hash": "x",
        }
        mock_get_storage.return_value = store

        with patch("backend.services.auth_service._check_password", return_value=True):
            auth_service.login_user("  Alice@Example.COM  ", "p")
        store.get_user_account.assert_called_once_with("alice@example.com")


@patch("backend.services.auth_service.get_storage")
class TestGetUser(unittest.TestCase):
    """Tests for get_user."""

    def test_get_user_found_returns_account(
        self, mock_get_storage: MagicMock
    ) -> None:
        store = MagicMock()
        store.get_user_account.return_value = {
            "user_id": "alice@example.com",
            "email": "alice@example.com",
        }
        mock_get_storage.return_value = store

        result = auth_service.get_user("alice@example.com")
        self.assertEqual(result["user_id"], "alice@example.com")
        store.get_user_account.assert_called_once_with("alice@example.com")

    def test_get_user_not_found_returns_empty_dict(
        self, mock_get_storage: MagicMock
    ) -> None:
        store = MagicMock()
        store.get_user_account.return_value = {}
        mock_get_storage.return_value = store

        result = auth_service.get_user("missing@example.com")
        self.assertEqual(result, {})

    def test_get_user_normalizes_user_id(
        self, mock_get_storage: MagicMock
    ) -> None:
        store = MagicMock()
        store.get_user_account.return_value = {}
        mock_get_storage.return_value = store

        auth_service.get_user("  Alice@Example.COM  ")
        store.get_user_account.assert_called_once_with("alice@example.com")


class TestCheckPasswordViaBcrypt(unittest.TestCase):
    """Test _check_password behavior (and thus bcrypt integration) via login."""

    @patch("backend.services.auth_service.get_storage")
    def test_login_with_correct_password_succeeds(
        self, mock_get_storage: MagicMock
    ) -> None:
        store = MagicMock()
        password = "correct_password"
        store.get_user_account.return_value = {
            "user_id": "u@x.com",
            "email": "u@x.com",
            "password_hash": auth_service._hash_password(password),
        }
        mock_get_storage.return_value = store

        result = auth_service.login_user("u@x.com", password)
        self.assertEqual(result["email"], "u@x.com")
        self.assertNotIn("password_hash", result)

    @patch("backend.services.auth_service.get_storage")
    def test_login_with_wrong_password_raises(
        self, mock_get_storage: MagicMock
    ) -> None:
        store = MagicMock()
        store.get_user_account.return_value = {
            "user_id": "u@x.com",
            "password_hash": auth_service._hash_password("right"),
        }
        mock_get_storage.return_value = store

        with self.assertRaises(ValueError):
            auth_service.login_user("u@x.com", "wrong")

    @patch("backend.services.auth_service.get_storage")
    def test_check_password_none_hash_returns_false(
        self, mock_get_storage: MagicMock
    ) -> None:
        self.assertFalse(auth_service._check_password("anything", None))

    @patch("backend.services.auth_service.get_storage")
    def test_check_password_malformed_hash_returns_false(
        self, mock_get_storage: MagicMock
    ) -> None:
        with patch("backend.services.auth_service.bcrypt") as m_bcrypt:
            m_bcrypt.checkpw.side_effect = ValueError("bad hash")
            self.assertFalse(auth_service._check_password("pass", "not-a-valid-hash"))


class TestHashPassword(unittest.TestCase):
    """Test _hash_password"""

    def test_hash_password_returns_non_empty_string(self) -> None:
        from backend.services.auth_service import _hash_password

        out = _hash_password("secret")
        self.assertIsInstance(out, str)
        self.assertTrue(len(out) > 0)
        self.assertTrue(out.startswith("$2b$") or out.startswith("$2a$"))

    def test_hash_password_different_inputs_differ(self) -> None:
        a = auth_service._hash_password("a")
        b = auth_service._hash_password("b")
        self.assertNotEqual(a, b)



if __name__ == "__main__":
    unittest.main()
