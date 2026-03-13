"""Forum service layer."""

from __future__ import annotations

from backend.config import USER_FORUM_PATH
from backend import storage


def create_post(user_id: str, title: str, text: str) -> dict:
    """Create a new public forum post."""
    user_id = str(user_id).strip().lower()
    title = str(title).strip()
    text = str(text).strip()
    if not title or not text:
        raise ValueError("title and text required")
    db = storage._load_forum_db()  # pylint: disable=protected-access
    post_id = int(db.get("next_post_id") or 1)
    post = {
        "id": post_id,
        "title": title,
        "author": user_id,
        "genre": None,
        "club": None,
        "club_id": None,
        "book_id": None,
        "book_title": None,
        "tags": [],
        "visibility": "public",
        "replies": 0,
        "likes": 0,
        "liked_by": [],
        "time_ago": "just now",
        "preview": text,
        "comments": [],
    }
    db.setdefault("posts", []).insert(0, post)
    db["next_post_id"] = post_id + 1
    storage._save_forum_db(db)  # pylint: disable=protected-access
    return dict(post)


def add_comment(post_id: int, user_id: str, text: str) -> dict:
    """Add a comment to a post."""
    user_id = str(user_id).strip().lower()
    text = str(text).strip()
    if not text:
        raise ValueError("text required")
    db = storage._load_forum_db()  # pylint: disable=protected-access
    for post in db.get("posts", []):
        if int(post.get("id", -1)) == int(post_id):
            post.setdefault("comments", []).append(
                {"author": user_id, "text": text, "likes": 0, "liked_by": []}
            )
            post["replies"] = len(post.get("comments", []))
            storage._save_forum_db(db)  # pylint: disable=protected-access
            return dict(post)
    raise ValueError("post not found")


def like_post(post_id: int, user_id: str) -> dict:
    """Toggle like on a post."""
    user_id = str(user_id).strip().lower()
    db = storage._load_forum_db()  # pylint: disable=protected-access
    for post in db.get("posts", []):
        if int(post.get("id", -1)) == int(post_id):
            liked_by = post.setdefault("liked_by", [])
            if user_id in liked_by:
                post["liked_by"] = [u for u in liked_by if u != user_id]
                post["likes"] = max(0, int(post.get("likes", 0)) - 1)
            else:
                liked_by.append(user_id)
                post["likes"] = int(post.get("likes", 0)) + 1
            storage._save_forum_db(db)  # pylint: disable=protected-access
            return dict(post)
    raise ValueError("post not found")


def save_post(post_id: int, user_id: str) -> dict:
    """Toggle saved post id for user."""
    user_id = str(user_id).strip().lower()
    forums = storage._read_json(USER_FORUM_PATH, {})  # pylint: disable=protected-access
    rec = forums.setdefault(user_id, {"forum_posts": [], "saved_forum_post_ids": []})
    saved = rec.setdefault("saved_forum_post_ids", [])
    if int(post_id) in [int(x) for x in saved]:
        rec["saved_forum_post_ids"] = [x for x in saved if int(x) != int(post_id)]
    else:
        saved.append(int(post_id))
    storage._save_user_forums_all(forums)  # pylint: disable=protected-access
    return dict(rec)


def get_posts() -> list[dict]:
    """Return all forum posts."""
    db = storage._load_forum_db()  # pylint: disable=protected-access
    return list(db.get("posts") or [])


def get_post(post_id: int) -> dict:
    """Return a single forum post."""
    for post in get_posts():
        if int(post.get("id", -1)) == int(post_id):
            return dict(post)
    return {}


def filter_posts_by_tag(query: str) -> list[dict]:
    """Filter posts by tag query."""
    query = str(query or "").strip().lower()
    if not query:
        return get_posts()
    out = []
    for post in get_posts():
        tags = post.get("tags") or []
        blob = " ".join(
            [str(t) for t in tags]
            + [str(post.get("genre") or ""), str(post.get("club") or "")]
        ).lower()
        if query in blob:
            out.append(post)
    return out

