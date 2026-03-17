"""
Tests for backend.services.forum_service.

Covers create_post, add_comment, like_post, like_comment, save_post,
get_posts, get_post, get_thread_for_book, filter_posts_by_tag, get_posts_sorted,
is_post_saved, is_post_liked, get_saved_posts_with_details.
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

# Avoid loading real boto3 when backend.storage is imported.
if "boto3" not in sys.modules:
    _boto3 = MagicMock()
    _conditions = types.ModuleType("boto3.dynamodb.conditions")
    _conditions.Attr = MagicMock()
    _conditions.Key = MagicMock()
    _boto3.dynamodb.conditions = _conditions
    sys.modules["boto3"] = _boto3
    sys.modules["boto3.dynamodb"] = MagicMock()
    sys.modules["boto3.dynamodb.conditions"] = _conditions

from backend.services import forum_service  # noqa: E402


@patch("backend.services.forum_service.get_storage")
@patch("backend.services.forum_service.time.time", return_value=1000)
class TestCreatePost(unittest.TestCase):
    """Tests for create_post."""

    def test_creates_post_and_persists(
        self, mock_time: MagicMock, mock_get_storage: MagicMock
    ) -> None:
        "Test creates post and persists."
        store = MagicMock()
        store.get_book_metadata.return_value = None
        store.load_forum_db.return_value = {"next_post_id": 1, "posts": []}
        mock_get_storage.return_value = store

        result = forum_service.create_post("alice@example.com", "My Title", "Body text")

        self.assertEqual(result["id"], 1)
        self.assertEqual(result["title"], "My Title")
        self.assertEqual(result["author"], "alice@example.com")
        self.assertEqual(result["text"], "Body text")
        self.assertEqual(result["replies"], 0)
        self.assertEqual(result["likes"], 0)
        self.assertEqual(result["created_at"], 1000)
        store.save_forum_db.assert_called_once()
        db = store.save_forum_db.call_args[0][0]
        self.assertEqual(db["next_post_id"], 2)
        self.assertEqual(len(db["posts"]), 1)

    def test_empty_title_raises(
        self, mock_time: MagicMock, mock_get_storage: MagicMock
    ) -> None:
        "Test empty title raises."
        mock_get_storage.return_value = MagicMock()
        with self.assertRaises(ValueError) as ctx:
            forum_service.create_post("a@b.com", "", "body")
        self.assertIn("title and text required", str(ctx.exception))

    def test_empty_text_raises(
        self, mock_time: MagicMock, mock_get_storage: MagicMock
    ) -> None:
        "Test empty text raises."
        mock_get_storage.return_value = MagicMock()
        with self.assertRaises(ValueError) as ctx:
            forum_service.create_post("a@b.com", "Title", "   ")
        self.assertIn("title and text required", str(ctx.exception))

    def test_normalizes_user_id_and_tags(
        self, mock_time: MagicMock, mock_get_storage: MagicMock
    ) -> None:
        "Test normalizes user id and tags."
        store = MagicMock()
        store.get_book_metadata.return_value = None
        store.load_forum_db.return_value = {"next_post_id": 5, "posts": []}
        mock_get_storage.return_value = store

        result = forum_service.create_post(
            "  Alice@Example.COM  ", "T", "B", tags=["Fantasy", "  fantasy  ", ""]
        )
        self.assertEqual(result["author"], "alice@example.com")
        self.assertEqual(result["tags"], ["Fantasy"])

    def test_looks_up_book_title_when_parent_asin_set(
        self, mock_time: MagicMock, mock_get_storage: MagicMock
    ) -> None:
        "Test looks up book title when parent asin set."
        store = MagicMock()
        store.get_book_metadata.return_value = {"title": "The Book"}
        store.load_forum_db.return_value = {"next_post_id": 1, "posts": []}
        mock_get_storage.return_value = store

        result = forum_service.create_post(
            "u@x.com", "T", "B", parent_asin="B123", book_title=None
        )
        self.assertEqual(result["parent_asin"], "B123")
        self.assertEqual(result["book_title"], "The Book")
        self.assertIn("The Book", result["tags"])

    def test_continues_when_book_metadata_raises(
        self, mock_time: MagicMock, mock_get_storage: MagicMock
    ) -> None:
        "Test continues when book metadata raises."
        store = MagicMock()
        store.get_book_metadata.side_effect = OSError("fail")
        store.load_forum_db.return_value = {"next_post_id": 1, "posts": []}
        mock_get_storage.return_value = store

        result = forum_service.create_post(
            "u@x.com", "T", "B", parent_asin="B123", book_title=None
        )
        self.assertEqual(result["parent_asin"], "B123")
        self.assertIsNone(result["book_title"])


@patch("backend.services.forum_service.get_storage")
class TestAddComment(unittest.TestCase):
    """Tests for add_comment."""

    @patch("backend.services.forum_service.time.time", return_value=2000)
    def test_appends_comment_and_updates_post(
        self, mock_time: MagicMock, mock_get_storage: MagicMock
    ) -> None:
        "Test appends comment and updates post."
        store = MagicMock()
        store.get_forum_post.return_value = {"id": 1, "comments": [], "replies": 0}
        mock_get_storage.return_value = store

        result = forum_service.add_comment(1, "bob@example.com", "Nice post!")

        self.assertEqual(len(result["comments"]), 1)
        self.assertEqual(result["comments"][0]["author"], "bob@example.com")
        self.assertEqual(result["comments"][0]["text"], "Nice post!")
        self.assertEqual(result["replies"], 1)
        store.update_forum_post.assert_called_once_with(1, result)

    def test_empty_text_raises(self, mock_get_storage: MagicMock) -> None:
        "Test empty text raises."
        mock_get_storage.return_value = MagicMock()
        with self.assertRaises(ValueError) as ctx:
            forum_service.add_comment(1, "u@x.com", "   ")
        self.assertIn("text required", str(ctx.exception))

    def test_post_not_found_raises(self, mock_get_storage: MagicMock) -> None:
        "Test post not found raises."
        store = MagicMock()
        store.get_forum_post.return_value = None
        mock_get_storage.return_value = store
        with self.assertRaises(ValueError) as ctx:
            forum_service.add_comment(99, "u@x.com", "hi")
        self.assertIn("post not found", str(ctx.exception))


@patch("backend.services.forum_service.get_storage")
class TestLikePost(unittest.TestCase):
    """Tests for like_post."""

    def test_adds_like_when_not_liked(self, mock_get_storage: MagicMock) -> None:
        "Test adds like when not liked."
        store = MagicMock()
        store.get_user_forums.return_value = {"liked_post_ids": []}
        store.get_forum_post.return_value = {"id": 1, "likes": 0}
        mock_get_storage.return_value = store

        result = forum_service.like_post(1, "u@x.com")

        self.assertEqual(result["likes"], 1)
        store.save_user_forums.assert_called_once()
        uf = store.save_user_forums.call_args[0][1]
        self.assertEqual(uf["liked_post_ids"], [1])

    def test_removes_like_when_already_liked(self, mock_get_storage: MagicMock) -> None:
        "Test removes like when already liked."
        store = MagicMock()
        store.get_user_forums.return_value = {"liked_post_ids": [1]}
        store.get_forum_post.return_value = {"id": 1, "likes": 1}
        mock_get_storage.return_value = store

        result = forum_service.like_post(1, "u@x.com")

        self.assertEqual(result["likes"], 0)
        uf = store.save_user_forums.call_args[0][1]
        self.assertEqual(uf["liked_post_ids"], [])

    def test_post_not_found_raises(self, mock_get_storage: MagicMock) -> None:
        "Test post not found raises."
        store = MagicMock()
        store.get_user_forums.return_value = {}
        store.get_forum_post.return_value = None
        mock_get_storage.return_value = store
        with self.assertRaises(ValueError) as ctx:
            forum_service.like_post(99, "u@x.com")
        self.assertIn("post not found", str(ctx.exception))


@patch("backend.services.forum_service.get_storage")
class TestLikeComment(unittest.TestCase):
    """Tests for like_comment."""

    def test_adds_like_to_comment(self, mock_get_storage: MagicMock) -> None:
        "Test adds like to comment."
        store = MagicMock()
        store.get_user_forums.return_value = {"liked_comment_ids": []}
        store.get_forum_post.return_value = {
            "id": 1,
            "comments": [{"likes": 0}, {"likes": 0}],
        }
        mock_get_storage.return_value = store

        result = forum_service.like_comment(1, 0, "u@x.com")

        self.assertEqual(result["comments"][0]["likes"], 1)
        uf = store.save_user_forums.call_args[0][1]
        self.assertIn("1:0", uf["liked_comment_ids"])

    def test_removes_like_when_comment_already_liked(self, mock_get_storage: MagicMock) -> None:
        "Test removes like when comment already liked."
        store = MagicMock()
        store.get_user_forums.return_value = {"liked_comment_ids": ["1:0"]}
        store.get_forum_post.return_value = {"id": 1, "comments": [{"likes": 1}]}
        mock_get_storage.return_value = store

        result = forum_service.like_comment(1, 0, "u@x.com")
        self.assertEqual(result["comments"][0]["likes"], 0)
        uf = store.save_user_forums.call_args[0][1]
        self.assertEqual(uf["liked_comment_ids"], [])

    def test_post_not_found_raises(self, mock_get_storage: MagicMock) -> None:
        "Test post not found raises."
        store = MagicMock()
        store.get_user_forums.return_value = {}
        store.get_forum_post.return_value = None
        mock_get_storage.return_value = store
        with self.assertRaises(ValueError) as ctx:
            forum_service.like_comment(99, 0, "u@x.com")
        self.assertIn("post not found", str(ctx.exception))

    def test_comment_not_found_raises(self, mock_get_storage: MagicMock) -> None:
        "Test comment not found raises."
        store = MagicMock()
        store.get_user_forums.return_value = {}
        store.get_forum_post.return_value = {"id": 1, "comments": [{"likes": 0}]}
        mock_get_storage.return_value = store
        with self.assertRaises(ValueError) as ctx:
            forum_service.like_comment(1, 5, "u@x.com")
        self.assertIn("comment not found", str(ctx.exception))


@patch("backend.services.forum_service.get_storage")
class TestSavePost(unittest.TestCase):
    """Tests for save_post."""

    def test_adds_to_saved_when_not_saved(self, mock_get_storage: MagicMock) -> None:
        "Test adds to saved when not saved."
        store = MagicMock()
        store.get_user_forums.return_value = {"saved_forum_post_ids": [], "liked_post_ids": [], "liked_comment_ids": []}
        mock_get_storage.return_value = store

        result = forum_service.save_post(1, "u@x.com")

        self.assertEqual(result["saved_forum_post_ids"], [1])

    def test_removes_from_saved_when_already_saved(self, mock_get_storage: MagicMock) -> None:
        "Test removes from saved when already saved."
        store = MagicMock()
        store.get_user_forums.return_value = {"saved_forum_post_ids": [1], "liked_post_ids": [], "liked_comment_ids": []}
        mock_get_storage.return_value = store

        result = forum_service.save_post(1, "u@x.com")

        self.assertEqual(result["saved_forum_post_ids"], [])


@patch("backend.services.forum_service.get_storage")
class TestGetPosts(unittest.TestCase):
    """Tests for get_posts."""

    def test_returns_posts_list(self, mock_get_storage: MagicMock) -> None:
        "Test returns posts list."
        store = MagicMock()
        store.load_forum_db.return_value = {"posts": [{"id": 1}, {"id": 2}]}
        mock_get_storage.return_value = store

        result = forum_service.get_posts()

        self.assertEqual(len(result), 2)
        self.assertEqual(result[0]["id"], 1)


@patch("backend.services.forum_service.get_storage")
class TestGetPost(unittest.TestCase):
    """Tests for get_post."""

    def test_returns_post_when_found(self, mock_get_storage: MagicMock) -> None:
        "Test returns post when found."
        store = MagicMock()
        store.get_forum_post.return_value = {"id": 1, "title": "A"}
        mock_get_storage.return_value = store

        result = forum_service.get_post(1)
        self.assertEqual(result["id"], 1)
        self.assertEqual(result["title"], "A")

    def test_returns_empty_dict_when_not_found(self, mock_get_storage: MagicMock) -> None:
        "Test returns empty dict when not found."
        store = MagicMock()
        store.get_forum_post.return_value = None
        mock_get_storage.return_value = store

        result = forum_service.get_post(99)
        self.assertEqual(result, {})


@patch("backend.services.forum_service.get_storage")
class TestGetThreadForBook(unittest.TestCase):
    """Tests for get_thread_for_book."""

    def test_returns_posts_for_book(self, mock_get_storage: MagicMock) -> None:
        "Test returns posts for book."
        store = MagicMock()
        store.get_forum_thread_for_book.return_value = [{"id": 1, "parent_asin": "B1"}]
        mock_get_storage.return_value = store

        result = forum_service.get_thread_for_book("B1")
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]["parent_asin"], "B1")

    def test_empty_parent_asin_returns_empty_list(self, mock_get_storage: MagicMock) -> None:
        "Test empty parent asin returns empty list."
        mock_get_storage.return_value = MagicMock()
        result = forum_service.get_thread_for_book("")
        self.assertEqual(result, [])
        result2 = forum_service.get_thread_for_book("   ")
        self.assertEqual(result2, [])


@patch("backend.services.forum_service.get_storage")
class TestFilterPostsByTag(unittest.TestCase):
    """Tests for filter_posts_by_tag."""

    def test_empty_query_returns_all_posts(self, mock_get_storage: MagicMock) -> None:
        "Test empty query returns all posts."
        store = MagicMock()
        store.load_forum_db.return_value = {"posts": [{"id": 1, "tags": ["A"]}]}
        mock_get_storage.return_value = store

        result = forum_service.filter_posts_by_tag("")
        self.assertEqual(len(result), 1)

    def test_filters_by_substring_match(self, mock_get_storage: MagicMock) -> None:
        "Test filters by substring match."
        store = MagicMock()
        store.load_forum_db.return_value = {
            "posts": [
                {"id": 1, "tags": ["Fantasy"]},
                {"id": 2, "tags": ["Sci-Fi"]},
            ]
        }
        mock_get_storage.return_value = store

        result = forum_service.filter_posts_by_tag("fantasy")
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]["id"], 1)


@patch("backend.services.forum_service.get_storage")
class TestGetPostsSorted(unittest.TestCase):
    """Tests for get_posts_sorted."""

    def test_top_likes_sort(self, mock_get_storage: MagicMock) -> None:
        "Test top likes sort."
        store = MagicMock()
        store.load_forum_db.return_value = {
            "posts": [
                {"id": 1, "likes": 1, "created_at": 100},
                {"id": 2, "likes": 10, "created_at": 50},
            ]
        }
        mock_get_storage.return_value = store

        result = forum_service.get_posts_sorted(sort="top_likes")
        self.assertEqual(result[0]["id"], 2)
        self.assertEqual(result[1]["id"], 1)

    def test_tag_filter_includes_matching_posts(self, mock_get_storage: MagicMock) -> None:
        "Test tag filter includes matching posts."
        store = MagicMock()
        store.load_forum_db.return_value = {
            "posts": [
                {"id": 1, "tags": ["Fantasy", "Adventure"], "likes": 0, "created_at": 100},
                {"id": 2, "tags": ["Sci-Fi"], "likes": 0, "created_at": 90},
            ]
        }
        mock_get_storage.return_value = store

        result = forum_service.get_posts_sorted(sort="newest", tag="fantasy")
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]["id"], 1)


@patch("backend.services.forum_service.get_storage")
class TestIsPostSaved(unittest.TestCase):
    """Tests for is_post_saved."""

    def test_returns_true_when_saved(self, mock_get_storage: MagicMock) -> None:
        "Test returns true when saved."
        store = MagicMock()
        store.get_user_forums.return_value = {"saved_forum_post_ids": [1, 2]}
        mock_get_storage.return_value = store

        self.assertTrue(forum_service.is_post_saved("u@x.com", 1))

    def test_returns_false_when_not_saved(self, mock_get_storage: MagicMock) -> None:
        "Test returns false when not saved."
        store = MagicMock()
        store.get_user_forums.return_value = {"saved_forum_post_ids": [2]}
        mock_get_storage.return_value = store

        self.assertFalse(forum_service.is_post_saved("u@x.com", 1))

    def test_empty_user_id_returns_false(self, mock_get_storage: MagicMock) -> None:
        "Test empty user id returns false."
        mock_get_storage.return_value = MagicMock()
        self.assertFalse(forum_service.is_post_saved("", 1))


@patch("backend.services.forum_service.get_storage")
class TestIsPostLiked(unittest.TestCase):
    """Tests for is_post_liked."""

    def test_returns_true_when_liked(self, mock_get_storage: MagicMock) -> None:
        "Test returns true when liked."
        store = MagicMock()
        store.get_user_forums.return_value = {"liked_post_ids": [1]}
        mock_get_storage.return_value = store

        self.assertTrue(forum_service.is_post_liked("u@x.com", 1))

    def test_returns_false_when_not_liked(self, mock_get_storage: MagicMock) -> None:
        "Test returns false when not liked."
        store = MagicMock()
        store.get_user_forums.return_value = {"liked_post_ids": [2]}
        mock_get_storage.return_value = store

        self.assertFalse(forum_service.is_post_liked("u@x.com", 1))

    def test_empty_user_id_returns_false(self, mock_get_storage: MagicMock) -> None:
        "Test empty user id returns false."
        mock_get_storage.return_value = MagicMock()
        self.assertFalse(forum_service.is_post_liked("", 1))


@patch("backend.services.forum_service.get_storage")
class TestGetSavedPostsWithDetails(unittest.TestCase):
    """Tests for get_saved_posts_with_details."""

    def test_returns_saved_posts_in_order(self, mock_get_storage: MagicMock) -> None:
        "Test returns saved posts in order."
        store = MagicMock()
        store.get_user_forums.return_value = {"saved_forum_post_ids": [2, 1]}
        store.get_forum_post.side_effect = lambda pid: {"id": pid, "title": f"Post {pid}"}
        mock_get_storage.return_value = store

        result = forum_service.get_saved_posts_with_details("u@x.com")
        self.assertEqual(len(result), 2)
        self.assertEqual(result[0]["id"], 2)
        self.assertEqual(result[1]["id"], 1)

    def test_omits_missing_posts(self, mock_get_storage: MagicMock) -> None:
        "Test omits missing posts."
        store = MagicMock()
        store.get_user_forums.return_value = {"saved_forum_post_ids": [1, 99, 2]}
        store.get_forum_post.side_effect = lambda pid: {"id": pid} if pid != 99 else None
        mock_get_storage.return_value = store

        result = forum_service.get_saved_posts_with_details("u@x.com")
        self.assertEqual(len(result), 2)
        self.assertEqual([p["id"] for p in result], [1, 2])

    def test_empty_user_id_returns_empty_list(self, mock_get_storage: MagicMock) -> None:
        "Test empty user id returns empty list."
        mock_get_storage.return_value = MagicMock()
        result = forum_service.get_saved_posts_with_details("")
        self.assertEqual(result, [])
if __name__ == "__main__":
    unittest.main()
