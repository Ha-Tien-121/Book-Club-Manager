"""Forum service layer."""

from __future__ import annotations

import time

from backend.storage import get_storage


def create_post(user_id: str, title: str, text: str) -> dict:
    """Create a new public forum post. Uses storage backend (load_forum_db + save_forum_db)."""
    user_id = str(user_id).strip().lower()
    title = str(title).strip()
    text = str(text).strip()
    if not title or not text:
        raise ValueError("title and text required")
    store = get_storage()
    db = store.load_forum_db()
    post_id = int(db.get("next_post_id") or 1)
    post = {
        "id": post_id,
        "title": title,
        "author": user_id,
        "parent_asin": None,
        "book_title": None,
        "tags": [],
        "replies": 0,
        "likes": 0,
        "liked_by": [],
        "created_at": int(time.time()),
        "preview": text,
        "comments": [],
    }
    db.setdefault("posts", []).insert(0, post)
    db["next_post_id"] = post_id + 1
    store.save_forum_db(db)
    return dict(post)


def add_comment(post_id: int, user_id: str, text: str) -> dict:
    """Add a comment to a post. Uses storage backend (get_forum_post + update_forum_post)."""
    user_id = str(user_id).strip().lower()
    text = str(text).strip()
    if not text:
        raise ValueError("text required")
    store = get_storage()
    post = store.get_forum_post(post_id)
    if not post:
        raise ValueError("post not found")
    comments = list(post.get("comments") or [])
    comments.append({"author": user_id, "text": text, "likes": 0, "liked_by": []})
    post["comments"] = comments
    post["replies"] = len(comments)
    store.update_forum_post(post_id, post)
    return post


def like_post(post_id: int, user_id: str) -> dict:
    """Toggle like on a post. Who liked is stored in user_forums (liked_post_ids); post keeps likes count."""
    post_id = int(post_id)
    user_id = str(user_id).strip().lower()
    store = get_storage()
    uf = store.get_user_forums(user_id)
    liked = list(uf.get("liked_post_ids") or [])
    if post_id in liked:
        liked = [x for x in liked if x != post_id]
        delta = -1
    else:
        liked = list(liked) + [post_id]
        delta = 1
    uf["liked_post_ids"] = liked
    store.save_user_forums(user_id, uf)
    post = store.get_forum_post(post_id)
    if not post:
        raise ValueError("post not found")
    post["likes"] = max(0, int(post.get("likes") or 0) + delta)
    store.update_forum_post(post_id, post)
    return post


def like_comment(post_id: int, comment_idx: int, user_id: str) -> dict:
    """Toggle like on a comment. Who liked is stored in user_forums (liked_comment_ids)."""
    post_id = int(post_id)
    comment_idx = int(comment_idx)
    user_id = str(user_id).strip().lower()
    key = f"{post_id}:{comment_idx}"
    store = get_storage()
    uf = store.get_user_forums(user_id)
    liked = list(uf.get("liked_comment_ids") or [])
    if key in liked:
        liked = [x for x in liked if x != key]
        delta = -1
    else:
        liked = list(liked) + [key]
        delta = 1
    uf["liked_comment_ids"] = liked
    store.save_user_forums(user_id, uf)
    post = store.get_forum_post(post_id)
    if not post:
        raise ValueError("post not found")
    comments = list(post.get("comments") or [])
    if comment_idx < 0 or comment_idx >= len(comments):
        raise ValueError("comment not found")
    comments[comment_idx]["likes"] = max(0, int(comments[comment_idx].get("likes") or 0) + delta)
    post["comments"] = comments
    store.update_forum_post(post_id, post)
    return post


def save_post(post_id: int, user_id: str) -> dict:
    """Toggle saved post id for user. Uses storage backend (get_user_forums + save_user_forums)."""
    user_id = str(user_id).strip().lower()
    store = get_storage()
    rec = store.get_user_forums(user_id)
    saved = list(rec.get("saved_forum_post_ids") or [])
    post_id_int = int(post_id)
    if post_id_int in [int(x) for x in saved]:
        saved = [x for x in saved if int(x) != post_id_int]
    else:
        saved = list(saved) + [post_id_int]
    rec["saved_forum_post_ids"] = saved
    store.save_user_forums(user_id, rec)
    return dict(rec)


def get_posts() -> list[dict]:
    """Return all forum posts. Uses storage backend."""
    store = get_storage()
    db = store.load_forum_db()
    return list(db.get("posts") or [])


def get_post(post_id: int) -> dict:
    """Return a single forum post. Uses storage backend."""
    store = get_storage()
    post = store.get_forum_post(post_id)
    return dict(post) if post else {}


def filter_posts_by_tag(query: str) -> list[dict]:
    """Filter posts by tag query."""
    query = str(query or "").strip().lower()
    if not query:
        return get_posts()
    out = []
    for post in get_posts():
        tags = post.get("tags") or []
        blob = " ".join(str(t) for t in tags).lower()
        if query in blob:
            out.append(post)
    return out

